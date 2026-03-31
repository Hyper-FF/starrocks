// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// eBPF program for tracing synchronous (pread64) IO in StarRocks BE.
//
// Attachment strategy:
//   1. uprobe on IOProfiler::set_context(uint32_t tag, uint64_t tablet_id)
//      Captures the IO context (tag + tablet_id) for each thread before the
//      function body runs, even when IOProfiler profiling mode is disabled.
//      Mangled symbol: _ZN10starrocks10IOProfiler11set_contextEjm
//
//   2. tracepoint/syscalls/sys_enter_pread64
//      Records per-thread pread start timestamp and arguments.
//
//   3. tracepoint/syscalls/sys_exit_pread64
//      Computes latency, looks up thread context, updates per-CPU stats and
//      emits slow-IO events.
//
// Build requirements:
//   - Linux kernel >= 5.8 (BPF ring buffer, BPF_MAP_TYPE_PERCPU_HASH)
//   - BTF-enabled kernel (CONFIG_DEBUG_INFO_BTF=y) for CO-RE
//   - libbpf >= 0.6
//   - vmlinux.h generated via: bpftool btf dump file /sys/kernel/btf/vmlinux format c

#include "vmlinux.h"
#include <bpf/bpf_core_read.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

#include "sync_io_trace.h"

char LICENSE[] SEC("license") = "GPL";

// ---- Configurable parameters (set from user-space via skeleton globals) ----

// PID of the starrocks_be process to trace (0 = trace all PIDs, for testing only)
const volatile __u32 target_pid = 0;

// Emit a slow-IO ring buffer event when pread latency >= this value (nanoseconds)
const volatile __u64 slow_threshold_ns = SR_IO_SLOW_THRESHOLD_NS;

// ---- BPF Maps ----

// Thread context map: TID -> {tag, tablet_id}
// Updated by the IOProfiler::set_context uprobe.
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, SR_IO_MAX_THREADS);
    __type(key, __u32);
    __type(value, struct sr_tid_ctx);
} tid_context SEC(".maps");

// Pread entry map: TID -> {start_ns, offset, fd}
// Written at sys_enter_pread64, read and deleted at sys_exit_pread64.
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, SR_IO_MAX_THREADS);
    __type(key, __u32);
    __type(value, struct sr_pread_entry);
} tid_pread_entry SEC(".maps");

// Per-CPU IO statistics: {tag, tablet_id} -> sr_io_stat
// Using PERCPU_HASH avoids kernel-side atomic operations on the histogram
// array. User-space aggregates across CPUs when reading.
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_HASH);
    __uint(max_entries, SR_IO_MAX_CONTEXTS);
    __type(key, struct sr_io_key);
    __type(value, struct sr_io_stat);
} io_stat_map SEC(".maps");

// Ring buffer for slow IO events (capacity: 1 MiB)
struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 1 << 20);
} slow_io_events SEC(".maps");

// ---- Helpers ----

static __always_inline bool pid_matches(void) {
    if (target_pid == 0) return true;
    __u64 pid_tgid = bpf_get_current_pid_tgid();
    return ((__u32)(pid_tgid >> 32)) == target_pid;
}

// Returns the log2 floor of ns, clamped to [0, SR_IO_HIST_SLOTS-1].
// Used to compute the histogram bucket index.
static __always_inline __u32 latency_to_hist_slot(__u64 ns) {
    if (ns == 0) return 0;
    __u32 slot = 63 - __builtin_clzll(ns);
    return slot < SR_IO_HIST_SLOTS ? slot : SR_IO_HIST_SLOTS - 1;
}

// Look up or lazily create the per-CPU stat entry for the given key.
// Returns NULL only if the map is full (SR_IO_MAX_CONTEXTS exceeded).
static __always_inline struct sr_io_stat* get_or_create_stat(struct sr_io_key* key) {
    struct sr_io_stat* stat = bpf_map_lookup_elem(&io_stat_map, key);
    if (stat) return stat;

    // First observation for this {tag, tablet_id}: initialise a zero entry.
    struct sr_io_stat zero = {};
    long err = bpf_map_update_elem(&io_stat_map, key, &zero, BPF_NOEXIST);
    if (err && err != -17 /* EEXIST, lost race on another CPU */) return NULL;
    return bpf_map_lookup_elem(&io_stat_map, key);
}

// ---- Program 1: Capture IO context from IOProfiler::set_context ----
//
// Mangled symbol for starrocks::IOProfiler::set_context(uint32_t, uint64_t):
//   _ZN10starrocks10IOProfiler11set_contextEjm
//
// Calling convention (System V AMD64 ABI, static C++ method):
//   rdi = tag        (uint32_t / 'j')
//   rsi = tablet_id  (uint64_t / 'm', unsigned long on LP64)
//
// We probe BEFORE the function body runs so we always capture the arguments,
// even if the function returns early (e.g. when profiling mode is IOMODE_NONE).
SEC("uprobe")
int BPF_KPROBE(uprobe_set_context, __u32 tag, __u64 tablet_id) {
    if (!pid_matches()) return 0;

    __u64 pid_tgid = bpf_get_current_pid_tgid();
    __u32 tid = (__u32)pid_tgid;

    if (tablet_id != 0) {
        struct sr_tid_ctx ctx = {.tag = tag, ._pad = 0, .tablet_id = tablet_id};
        bpf_map_update_elem(&tid_context, &tid, &ctx, BPF_ANY);
    } else {
        // tablet_id == 0 is used by the IOProfiler::Scope destructor to restore
        // the outer context; treat it as "context cleared" for this thread.
        bpf_map_delete_elem(&tid_context, &tid);
    }
    return 0;
}

// ---- Program 2: Record pread start time and arguments ----
//
// tracepoint/syscalls/sys_enter_pread64 fields:
//   int fd, char* buf, size_t count, loff_t pos
SEC("tracepoint/syscalls/sys_enter_pread64")
int tp_enter_pread64(struct trace_event_raw_sys_enter* ctx) {
    if (!pid_matches()) return 0;

    __u64 pid_tgid = bpf_get_current_pid_tgid();
    __u32 tid = (__u32)pid_tgid;

    struct sr_pread_entry entry = {
            .start_ns = bpf_ktime_get_ns(),
            .offset = (__u64)ctx->args[3], // loff_t pos
            .fd = (long)ctx->args[0],      // int fd
    };
    bpf_map_update_elem(&tid_pread_entry, &tid, &entry, BPF_ANY);
    return 0;
}

// ---- Program 3: Compute latency and update stats ----
//
// tracepoint/syscalls/sys_exit_pread64 fields:
//   long ret  (bytes read, or negative errno)
SEC("tracepoint/syscalls/sys_exit_pread64")
int tp_exit_pread64(struct trace_event_raw_sys_exit* ctx) {
    if (!pid_matches()) return 0;

    __u64 pid_tgid = bpf_get_current_pid_tgid();
    __u32 tid = (__u32)pid_tgid;

    // Retrieve and remove the entry written at sys_enter.
    struct sr_pread_entry* entry = bpf_map_lookup_elem(&tid_pread_entry, &tid);
    if (!entry) return 0; // No matching entry (e.g. started tracing mid-call)

    __u64 latency_ns = bpf_ktime_get_ns() - entry->start_ns;
    __u64 file_offset = entry->offset;
    __s64 fd = entry->fd;
    bpf_map_delete_elem(&tid_pread_entry, &tid);

    long ret = ctx->ret;
    if (ret <= 0) return 0; // Failed read or EOF — not interesting for latency

    // Retrieve the IO context for this thread (if any).
    struct sr_tid_ctx* tctx = bpf_map_lookup_elem(&tid_context, &tid);
    __u32 tag = tctx ? tctx->tag : SR_IO_TAG_NONE;
    __u64 tablet_id = tctx ? tctx->tablet_id : 0;

    // Update per-CPU statistics for this {tag, tablet_id}.
    struct sr_io_key key = {.tag = tag, ._pad = 0, .tablet_id = tablet_id};
    struct sr_io_stat* stat = get_or_create_stat(&key);
    if (stat) {
        stat->read_ops++;
        stat->read_bytes += (__u64)ret;
        stat->read_time_ns += latency_ns;
        if (latency_ns > stat->read_max_ns) stat->read_max_ns = latency_ns;
        __u32 slot = latency_to_hist_slot(latency_ns);
        stat->hist[slot]++;
    }

    // Emit a slow-IO event to user-space when latency exceeds the threshold.
    if (latency_ns >= slow_threshold_ns) {
        struct sr_io_event* ev = bpf_ringbuf_reserve(&slow_io_events, sizeof(*ev), 0);
        if (ev) {
            ev->timestamp_ns = bpf_ktime_get_ns();
            ev->latency_ns = latency_ns;
            ev->tablet_id = tablet_id;
            ev->file_offset = file_offset;
            ev->bytes = (long)ret;
            ev->pid = (__u32)(pid_tgid >> 32);
            ev->tid = tid;
            ev->tag = tag;
            ev->fd = (__s32)fd;
            bpf_ringbuf_submit(ev, 0);
        }
    }

    return 0;
}
