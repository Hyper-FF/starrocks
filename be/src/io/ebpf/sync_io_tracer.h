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

// SyncIOTracer: user-space driver for the sync_io_trace eBPF program.
//
// Responsibilities:
//   - Load and verify the eBPF object file (sync_io_trace.bpf.o).
//   - Attach a uprobe to IOProfiler::set_context() in the running BE binary
//     to capture per-thread {tag, tablet_id} context.
//   - Attach tracepoints for sys_enter/exit_pread64 to measure syscall latency.
//   - Periodically aggregate per-CPU statistics from the io_stat_map.
//   - Poll the ring buffer and dispatch slow-IO events to a caller-supplied
//     callback.
//   - Expose the aggregated stats via get_stats() for HTTP/metrics export.
//
// Thread safety:
//   start() and stop() must not be called concurrently.
//   get_stats() and poll_slow_events() may be called from any thread while
//   the tracer is running; they each acquire an internal mutex.
//
// Kernel requirements:
//   Linux >= 5.8, CONFIG_DEBUG_INFO_BTF=y, CAP_BPF + CAP_PERFMON (uprobe),
//   CAP_SYS_ADMIN (syscall tracepoints).

#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "io/ebpf/sync_io_trace.h"

// Forward declaration: opaque libbpf handles managed in the .cpp.
struct bpf_object;
struct bpf_link;
struct ring_buffer;

namespace starrocks::io {

// Aggregated IO statistics for a single {tag, tablet_id} pair.
// This is the user-space view after summing across all CPUs.
struct SyncIOStats {
    uint32_t tag{0};
    uint64_t tablet_id{0};

    uint64_t read_ops{0};
    uint64_t read_bytes{0};
    uint64_t read_time_ns{0};
    uint64_t read_max_ns{0};

    // Log2 latency histogram matching SR_IO_HIST_SLOTS buckets.
    // hist[k] = number of preads with latency in [2^k, 2^(k+1)) ns.
    uint64_t hist[SR_IO_HIST_SLOTS]{};

    // Convenience: P50 / P99 latency estimated from histogram (ns).
    uint64_t p50_ns() const;
    uint64_t p99_ns() const;

    // Human-readable one-line summary.
    std::string to_string() const;

    // Header line matching to_string() column widths.
    static std::string header_string();
};

// A single slow-IO event as delivered to the user callback.
struct SlowIOEvent {
    uint64_t timestamp_ns;
    uint64_t latency_ns;
    uint64_t tablet_id;
    uint64_t file_offset;
    int64_t bytes;
    uint32_t pid;
    uint32_t tid;
    uint32_t tag;
    int32_t fd;

    std::string to_string() const;
};

// Callback type for slow IO events.
using SlowIOCallback = std::function<void(const SlowIOEvent&)>;

class SyncIOTracer {
public:
    SyncIOTracer() = default;
    ~SyncIOTracer();

    // Non-copyable, non-movable.
    SyncIOTracer(const SyncIOTracer&) = delete;
    SyncIOTracer& operator=(const SyncIOTracer&) = delete;

    // Start tracing the given process.
    //
    //   pid            - PID of the starrocks_be process.  Pass 0 to trace
    //                    all processes (useful for testing; not recommended in
    //                    production).
    //   binary_path    - Absolute path to the starrocks_be ELF binary.  Used
    //                    to resolve the IOProfiler::set_context uprobe symbol.
    //   bpf_obj_path   - Path to the compiled sync_io_trace.bpf.o object file.
    //   slow_threshold - Emit slow-IO events for preads taking longer than this
    //                    many nanoseconds (default: SR_IO_SLOW_THRESHOLD_NS).
    Status start(uint32_t pid, const std::string& binary_path, const std::string& bpf_obj_path,
                 uint64_t slow_threshold_ns = SR_IO_SLOW_THRESHOLD_NS);

    // Stop tracing and release all eBPF resources.
    void stop();

    bool is_running() const { return _running.load(std::memory_order_relaxed); }

    // Read all current per-{tag,tablet_id} statistics from the BPF map.
    // Aggregates per-CPU entries and returns a snapshot.
    // Returns an empty vector if the tracer is not running.
    std::vector<SyncIOStats> get_stats() const;

    // Drain the slow-IO ring buffer, invoking `cb` for each event.
    // Safe to call from any thread; returns the number of events processed.
    int poll_slow_events(const SlowIOCallback& cb, int timeout_ms = 0);

    // Convenience: return top-N entries sorted by total read bytes.
    std::vector<SyncIOStats> get_topn_by_bytes(size_t n) const;

    // Convenience: return top-N entries sorted by total read time.
    std::vector<SyncIOStats> get_topn_by_latency(size_t n) const;

    // Reset (zero) all BPF statistics maps without stopping the tracer.
    Status reset_stats();

private:
    Status _load_bpf_object(const std::string& obj_path);
    Status _attach_uprobe(const std::string& binary_path);
    Status _attach_tracepoints();

    // Ring buffer event handler (static trampoline for libbpf callback ABI).
    static int _rb_event_handler(void* ctx, void* data, size_t data_sz);

    std::atomic<bool> _running{false};
    mutable std::mutex _mutex;

    // libbpf handles
    struct bpf_object* _bpf_obj{nullptr};
    struct bpf_link* _uprobe_link{nullptr};
    struct bpf_link* _tp_enter_link{nullptr};
    struct bpf_link* _tp_exit_link{nullptr};
    struct ring_buffer* _rb{nullptr};

    // BPF map file descriptors (cached after load)
    int _tid_context_fd{-1};
    int _io_stat_map_fd{-1};
    int _slow_io_events_fd{-1};

    // Number of CPUs — needed to iterate per-CPU map values.
    int _num_cpus{0};

    // Pending slow-IO callback set during poll_slow_events().
    // Only valid for the duration of the poll call; access protected by libbpf
    // (single-threaded ring_buffer__poll invocation per call).
    const SlowIOCallback* _current_cb{nullptr};
};

} // namespace starrocks::io
