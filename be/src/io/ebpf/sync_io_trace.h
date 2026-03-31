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

// Shared definitions between eBPF kernel programs and user-space C++ code.
// This header must be valid both as a C header (for BPF programs) and as a
// C++ header (included by sync_io_tracer.h).

#pragma once

#ifdef __cplusplus
#include <cstdint>
using __u8 = uint8_t;
using __u32 = uint32_t;
using __u64 = uint64_t;
using __s64 = int64_t;
#else
#include <stdint.h>
#endif

// ---- IO Tag constants (mirrors IOProfiler::TAG) ----
#define SR_IO_TAG_NONE       0
#define SR_IO_TAG_QUERY      1
#define SR_IO_TAG_LOAD       2
#define SR_IO_TAG_PKINDEX    3
#define SR_IO_TAG_COMPACTION 4
#define SR_IO_TAG_CLONE      5
#define SR_IO_TAG_ALTER      6
#define SR_IO_TAG_MIGRATE    7
#define SR_IO_TAG_SIZE       8
#define SR_IO_TAG_SPILL      9

// ---- Histogram ----
// Latency histogram buckets: log2(nanoseconds), slots 0..25
// Slot k covers pread latency in [2^k, 2^(k+1)) ns
// Slot 0:  0–1 ns   Slot 10:  1–2 µs   Slot 20:  1–2 ms
// Slot 30: 1–2 s    (capped at SR_IO_HIST_SLOTS-1)
#define SR_IO_HIST_SLOTS 26

// Default slow-IO threshold: emit an event when pread latency exceeds 10ms
#define SR_IO_SLOW_THRESHOLD_NS (10ULL * 1000 * 1000)

// Maximum number of concurrent threads tracked in BPF maps
#define SR_IO_MAX_THREADS 65536

// Maximum number of distinct {tag, tablet_id} pairs tracked
#define SR_IO_MAX_CONTEXTS 4096

// ---- BPF map value types ----

// Key for the per-context IO statistics map.
// Mirrors the IOStatEntry id encoding: upper 16 bits = tag, lower 48 bits = tablet_id.
struct sr_io_key {
    __u32 tag;
    __u32 _pad; // alignment padding
    __u64 tablet_id;
};

// Per-CPU IO statistics accumulated in the kernel.
// User-space sums across CPUs when reading.
struct sr_io_stat {
    __u64 read_ops;
    __u64 read_bytes;
    __u64 read_time_ns;
    __u64 read_max_ns;
    __u64 hist[SR_IO_HIST_SLOTS]; // log2 latency histogram (counts per bucket)
};

// Event emitted to user-space ring buffer when a slow pread is detected.
struct sr_io_event {
    __u64 timestamp_ns;  // bpf_ktime_get_ns() at pread return
    __u64 latency_ns;    // pread duration
    __u64 tablet_id;     // 0 if context unknown
    __u64 file_offset;   // pread offset argument
    __s64 bytes;         // pread return value (bytes read)
    __u32 pid;           // process id
    __u32 tid;           // thread id
    __u32 tag;           // SR_IO_TAG_*
    __s32 fd;            // file descriptor
};

// Internal BPF map value: per-thread IO context set by IOProfiler::set_context()
struct sr_tid_ctx {
    __u32 tag;
    __u32 _pad;
    __u64 tablet_id;
};

// Internal BPF map value: pread entry state captured at sys_enter_pread64
struct sr_pread_entry {
    __u64 start_ns;  // bpf_ktime_get_ns() at syscall entry
    __u64 offset;    // pread offset argument
    __s64 fd;        // file descriptor argument
};
