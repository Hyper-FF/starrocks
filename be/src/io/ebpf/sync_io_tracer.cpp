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

#include "io/ebpf/sync_io_tracer.h"

#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <unistd.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <sstream>

#include "common/logging.h"
#include "io/io_profiler.h" // for IOProfiler::tag_to_string()

namespace starrocks::io {

// ---- SyncIOStats helpers ----

static uint64_t percentile_from_hist(const uint64_t hist[SR_IO_HIST_SLOTS], double pct) {
    uint64_t total = 0;
    for (int i = 0; i < SR_IO_HIST_SLOTS; i++) total += hist[i];
    if (total == 0) return 0;

    uint64_t target = static_cast<uint64_t>(total * pct / 100.0);
    uint64_t cumulative = 0;
    for (int i = 0; i < SR_IO_HIST_SLOTS; i++) {
        cumulative += hist[i];
        if (cumulative >= target) {
            // Bucket i covers [2^i, 2^(i+1)) ns; return midpoint.
            return (1ULL << i) + (1ULL << i) / 2;
        }
    }
    return 1ULL << (SR_IO_HIST_SLOTS - 1);
}

uint64_t SyncIOStats::p50_ns() const {
    return percentile_from_hist(hist, 50.0);
}

uint64_t SyncIOStats::p99_ns() const {
    return percentile_from_hist(hist, 99.0);
}

static std::string ns_to_human(uint64_t ns) {
    if (ns < 1000) return std::to_string(ns) + "ns";
    if (ns < 1000000) return std::to_string(ns / 1000) + "µs";
    if (ns < 1000000000ULL) return std::to_string(ns / 1000000) + "ms";
    return std::to_string(ns / 1000000000ULL) + "s";
}

std::string SyncIOStats::header_string() {
    return fmt::format("{:>12} {:>12} {:>10} {:>12} {:>10} {:>10} {:>10}", "tablet_id", "tag", "read_ops",
                       "read_bytes", "avg_lat", "p50_lat", "p99_lat");
}

std::string SyncIOStats::to_string() const {
    uint64_t avg_ns = read_ops > 0 ? read_time_ns / read_ops : 0;
    return fmt::format("{:>12} {:>12} {:>10} {:>12} {:>10} {:>10} {:>10}", tablet_id,
                       IOProfiler::tag_to_string(tag), read_ops, read_bytes, ns_to_human(avg_ns),
                       ns_to_human(p50_ns()), ns_to_human(p99_ns()));
}

std::string SlowIOEvent::to_string() const {
    return fmt::format("[slow_io] tag={} tablet={} fd={} offset={} bytes={} latency={} tid={}", tag, tablet_id, fd,
                       file_offset, bytes, ns_to_human(latency_ns), tid);
}

// ---- SyncIOTracer ----

SyncIOTracer::~SyncIOTracer() {
    stop();
}

// Silence libbpf log output unless VLOG is enabled.
static int libbpf_log_cb(enum libbpf_print_level level, const char* format, va_list args) {
    if (level == LIBBPF_DEBUG && !VLOG_IS_ON(3)) return 0;
    char buf[512];
    vsnprintf(buf, sizeof(buf), format, args);
    // Strip trailing newline for glog compatibility.
    size_t len = strlen(buf);
    if (len > 0 && buf[len - 1] == '\n') buf[len - 1] = '\0';
    if (level == LIBBPF_WARN) {
        LOG(WARNING) << "[libbpf] " << buf;
    } else if (level == LIBBPF_INFO) {
        VLOG(2) << "[libbpf] " << buf;
    } else {
        VLOG(3) << "[libbpf] " << buf;
    }
    return 0;
}

Status SyncIOTracer::start(uint32_t pid, const std::string& binary_path, const std::string& bpf_obj_path,
                           uint64_t slow_threshold) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_running.load(std::memory_order_relaxed)) {
        return Status::InternalError("SyncIOTracer is already running");
    }

    libbpf_set_print(libbpf_log_cb);

    _num_cpus = libbpf_num_possible_cpus();
    if (_num_cpus <= 0) {
        return Status::InternalError(fmt::format("libbpf_num_possible_cpus() failed: {}", _num_cpus));
    }

    RETURN_IF_ERROR(_load_bpf_object(bpf_obj_path));

    // Set the target PID and slow-IO threshold via global variables in the BPF
    // object (declared as `const volatile` in the BPF program).
    struct bpf_map* rodata_map = bpf_object__find_map_by_name(_bpf_obj, "sync_io_.rodata");
    if (rodata_map) {
        // rodata is a struct; patch individual fields via bpf_map__set_initial_value
        // if the skeleton API is not available.  For simplicity we rely on the
        // libbpf global-variable API introduced in libbpf 0.6.
        struct {
            uint32_t target_pid;
            uint64_t slow_threshold_ns;
        } rodata_vals = {pid, slow_threshold};
        if (bpf_map__set_initial_value(rodata_map, &rodata_vals, sizeof(rodata_vals)) != 0) {
            LOG(WARNING) << "SyncIOTracer: could not set rodata (pid/threshold will use defaults)";
        }
    }

    if (bpf_object__load(_bpf_obj) != 0) {
        int err = errno;
        bpf_object__close(_bpf_obj);
        _bpf_obj = nullptr;
        return Status::InternalError(fmt::format("bpf_object__load failed: {}", strerror(err)));
    }

    // Cache BPF map file descriptors.
    auto map_fd = [&](const char* name) -> int {
        struct bpf_map* m = bpf_object__find_map_by_name(_bpf_obj, name);
        return m ? bpf_map__fd(m) : -1;
    };
    _tid_context_fd = map_fd("tid_context");
    _io_stat_map_fd = map_fd("io_stat_map");
    _slow_io_events_fd = map_fd("slow_io_events");

    if (_io_stat_map_fd < 0 || _slow_io_events_fd < 0) {
        bpf_object__close(_bpf_obj);
        _bpf_obj = nullptr;
        return Status::InternalError("Required BPF maps not found in object file");
    }

    RETURN_IF_ERROR(_attach_uprobe(binary_path));
    RETURN_IF_ERROR(_attach_tracepoints());

    // Set up ring buffer consumer.
    _rb = ring_buffer__new(_slow_io_events_fd, _rb_event_handler, this, nullptr);
    if (!_rb) {
        stop();
        return Status::InternalError(fmt::format("ring_buffer__new failed: {}", strerror(errno)));
    }

    _running.store(true, std::memory_order_release);
    LOG(INFO) << "SyncIOTracer started: pid=" << pid << " binary=" << binary_path
              << " slow_threshold=" << slow_threshold << "ns";
    return Status::OK();
}

Status SyncIOTracer::_load_bpf_object(const std::string& obj_path) {
    struct bpf_object_open_opts opts = {};
    opts.sz = sizeof(opts);
    // BTF custom path could be set here if /sys/kernel/btf/vmlinux is absent.

    _bpf_obj = bpf_object__open_file(obj_path.c_str(), &opts);
    if (!_bpf_obj || libbpf_get_error(_bpf_obj)) {
        int err = errno;
        _bpf_obj = nullptr;
        return Status::InternalError(
                fmt::format("bpf_object__open_file({}) failed: {}", obj_path, strerror(err)));
    }
    return Status::OK();
}

// Mangled symbol for starrocks::IOProfiler::set_context(uint32_t, uint64_t).
// Verify with: nm -D starrocks_be | c++filt | grep "IOProfiler::set_context"
// or:          nm -D starrocks_be | grep "_ZN10starrocks10IOProfiler11set_contextEjm"
static constexpr const char* k_set_context_symbol = "_ZN10starrocks10IOProfiler11set_contextEjm";

Status SyncIOTracer::_attach_uprobe(const std::string& binary_path) {
    struct bpf_program* prog = bpf_object__find_program_by_name(_bpf_obj, "uprobe_set_context");
    if (!prog) {
        return Status::InternalError("BPF program 'uprobe_set_context' not found in object file");
    }

    // Resolve symbol offset within the ELF binary.
    long sym_offset = -1;
    {
        // Use libbpf's uprobe_resolve_perf_path / bpf_program__attach_uprobe_opts
        // (available from libbpf 1.0) for automatic symbol resolution.
        // Fall back to a manual nm-based lookup for older libbpf versions.
        struct bpf_uprobe_opts uopts = {};
        uopts.sz = sizeof(uopts);
        uopts.func_name = k_set_context_symbol;
        uopts.retprobe = false;

        _uprobe_link = bpf_program__attach_uprobe_opts(prog,
                                                       -1, // all processes; filter by PID in BPF
                                                       binary_path.c_str(), 0 /* offset */,
                                                       &uopts);
        if (!_uprobe_link || libbpf_get_error(_uprobe_link)) {
            int err = errno;
            _uprobe_link = nullptr;
            return Status::InternalError(fmt::format("Failed to attach uprobe to {}:{}: {}", binary_path,
                                                     k_set_context_symbol, strerror(err)));
        }
    }

    LOG(INFO) << "SyncIOTracer: attached uprobe to " << binary_path << " symbol=" << k_set_context_symbol;
    return Status::OK();
}

Status SyncIOTracer::_attach_tracepoints() {
    auto attach_tp = [&](const char* prog_name, const char* tp_category,
                         const char* tp_name) -> std::pair<Status, struct bpf_link*> {
        struct bpf_program* prog = bpf_object__find_program_by_name(_bpf_obj, prog_name);
        if (!prog) {
            return {Status::InternalError(fmt::format("BPF program '{}' not found", prog_name)), nullptr};
        }
        struct bpf_link* link = bpf_program__attach_tracepoint(prog, tp_category, tp_name);
        if (!link || libbpf_get_error(link)) {
            int err = errno;
            return {Status::InternalError(fmt::format("Failed to attach tracepoint {}/{}: {}", tp_category,
                                                      tp_name, strerror(err))),
                    nullptr};
        }
        return {Status::OK(), link};
    };

    auto [st1, enter_link] = attach_tp("tp_enter_pread64", "syscalls", "sys_enter_pread64");
    RETURN_IF_ERROR(st1);
    _tp_enter_link = enter_link;

    auto [st2, exit_link] = attach_tp("tp_exit_pread64", "syscalls", "sys_exit_pread64");
    RETURN_IF_ERROR(st2);
    _tp_exit_link = exit_link;

    LOG(INFO) << "SyncIOTracer: attached tracepoints syscalls/sys_{enter,exit}_pread64";
    return Status::OK();
}

void SyncIOTracer::stop() {
    std::lock_guard<std::mutex> lk(_mutex);
    if (!_running.exchange(false, std::memory_order_acq_rel)) return;

    if (_rb) {
        ring_buffer__free(_rb);
        _rb = nullptr;
    }
    if (_uprobe_link) {
        bpf_link__destroy(_uprobe_link);
        _uprobe_link = nullptr;
    }
    if (_tp_enter_link) {
        bpf_link__destroy(_tp_enter_link);
        _tp_enter_link = nullptr;
    }
    if (_tp_exit_link) {
        bpf_link__destroy(_tp_exit_link);
        _tp_exit_link = nullptr;
    }
    if (_bpf_obj) {
        bpf_object__close(_bpf_obj);
        _bpf_obj = nullptr;
    }
    _tid_context_fd = _io_stat_map_fd = _slow_io_events_fd = -1;

    LOG(INFO) << "SyncIOTracer stopped";
}

// ---- Stats aggregation ----

std::vector<SyncIOStats> SyncIOTracer::get_stats() const {
    std::lock_guard<std::mutex> lk(_mutex);
    if (!_running.load(std::memory_order_relaxed) || _io_stat_map_fd < 0) return {};

    // Allocate a buffer large enough to hold one sr_io_stat per CPU.
    size_t value_size = sizeof(struct sr_io_stat) * _num_cpus;
    std::vector<uint8_t> value_buf(value_size);

    std::vector<SyncIOStats> results;

    struct sr_io_key key = {}, next_key = {};
    bool first = true;
    while (true) {
        int err;
        if (first) {
            err = bpf_map_get_next_key(_io_stat_map_fd, nullptr, &next_key);
            first = false;
        } else {
            err = bpf_map_get_next_key(_io_stat_map_fd, &key, &next_key);
        }
        if (err) break; // ENOENT means we've iterated all keys
        key = next_key;

        if (bpf_map_lookup_elem(_io_stat_map_fd, &key, value_buf.data()) != 0) continue;

        // Sum across all CPUs.
        SyncIOStats s;
        s.tag = key.tag;
        s.tablet_id = key.tablet_id;
        const auto* per_cpu = reinterpret_cast<const struct sr_io_stat*>(value_buf.data());
        for (int cpu = 0; cpu < _num_cpus; cpu++) {
            s.read_ops += per_cpu[cpu].read_ops;
            s.read_bytes += per_cpu[cpu].read_bytes;
            s.read_time_ns += per_cpu[cpu].read_time_ns;
            s.read_max_ns = std::max(s.read_max_ns, per_cpu[cpu].read_max_ns);
            for (int slot = 0; slot < SR_IO_HIST_SLOTS; slot++) {
                s.hist[slot] += per_cpu[cpu].hist[slot];
            }
        }
        results.push_back(s);
    }
    return results;
}

std::vector<SyncIOStats> SyncIOTracer::get_topn_by_bytes(size_t n) const {
    auto stats = get_stats();
    std::sort(stats.begin(), stats.end(),
              [](const SyncIOStats& a, const SyncIOStats& b) { return a.read_bytes > b.read_bytes; });
    if (stats.size() > n) stats.resize(n);
    return stats;
}

std::vector<SyncIOStats> SyncIOTracer::get_topn_by_latency(size_t n) const {
    auto stats = get_stats();
    std::sort(stats.begin(), stats.end(),
              [](const SyncIOStats& a, const SyncIOStats& b) { return a.read_time_ns > b.read_time_ns; });
    if (stats.size() > n) stats.resize(n);
    return stats;
}

Status SyncIOTracer::reset_stats() {
    std::lock_guard<std::mutex> lk(_mutex);
    if (!_running.load(std::memory_order_relaxed) || _io_stat_map_fd < 0) {
        return Status::InternalError("tracer is not running");
    }

    std::vector<struct sr_io_key> keys;
    struct sr_io_key key = {}, next_key = {};
    bool first = true;
    while (true) {
        int err = first ? bpf_map_get_next_key(_io_stat_map_fd, nullptr, &next_key)
                        : bpf_map_get_next_key(_io_stat_map_fd, &key, &next_key);
        first = false;
        if (err) break;
        key = next_key;
        keys.push_back(key);
    }
    for (const auto& k : keys) {
        bpf_map_delete_elem(_io_stat_map_fd, &k);
    }
    return Status::OK();
}

// ---- Ring buffer slow-IO event handling ----

int SyncIOTracer::_rb_event_handler(void* ctx, void* data, size_t data_sz) {
    if (data_sz < sizeof(struct sr_io_event)) return 0;
    auto* tracer = static_cast<SyncIOTracer*>(ctx);
    if (!tracer->_current_cb) return 0;

    const auto* raw = static_cast<const struct sr_io_event*>(data);
    SlowIOEvent ev;
    ev.timestamp_ns = raw->timestamp_ns;
    ev.latency_ns = raw->latency_ns;
    ev.tablet_id = raw->tablet_id;
    ev.file_offset = raw->file_offset;
    ev.bytes = raw->bytes;
    ev.pid = raw->pid;
    ev.tid = raw->tid;
    ev.tag = raw->tag;
    ev.fd = raw->fd;
    (*tracer->_current_cb)(ev);
    return 0;
}

int SyncIOTracer::poll_slow_events(const SlowIOCallback& cb, int timeout_ms) {
    // ring_buffer__poll is not thread-safe; serialise callers.
    std::lock_guard<std::mutex> lk(_mutex);
    if (!_running.load(std::memory_order_relaxed) || !_rb) return 0;
    _current_cb = &cb;
    int n = ring_buffer__poll(_rb, timeout_ms);
    _current_cb = nullptr;
    return n < 0 ? 0 : n;
}

} // namespace starrocks::io
