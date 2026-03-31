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

// HTTP action for eBPF-based synchronous IO tracing.
//
// Endpoints:
//
//   POST /api/ebpf_io/start
//     Query params:
//       pid          - PID to trace (default: current process)
//       binary       - absolute path to starrocks_be binary
//                      (default: /proc/self/exe resolved)
//       bpf_obj      - path to sync_io_trace.bpf.o
//       slow_ms      - slow IO threshold in milliseconds (default: 10)
//     Response: 200 OK / 4xx on error
//
//   POST /api/ebpf_io/stop
//     Stops the running tracer and releases all eBPF resources.
//
//   GET  /api/ebpf_io/stats
//     Query params:
//       topn         - return at most N entries (default: 20)
//       sort         - "bytes" (default) or "latency"
//     Response: plain-text table of per-{tag,tablet_id} statistics
//
//   POST /api/ebpf_io/reset
//     Clears all accumulated BPF statistics without stopping the tracer.
//
//   GET  /api/ebpf_io/slow_events
//     Drains the ring buffer and returns pending slow-IO events as JSON.

#pragma once

#include <memory>

#include "http/http_handler.h"
#include "io/ebpf/sync_io_tracer.h"

namespace starrocks {

class ExecEnv;

class EbpfIOAction : public HttpHandler {
public:
    explicit EbpfIOAction(ExecEnv* exec_env);
    ~EbpfIOAction() override = default;

    void handle(HttpRequest* req) override;

private:
    void _handle_start(HttpRequest* req);
    void _handle_stop(HttpRequest* req);
    void _handle_stats(HttpRequest* req);
    void _handle_reset(HttpRequest* req);
    void _handle_slow_events(HttpRequest* req);

    ExecEnv* _exec_env;
    std::unique_ptr<io::SyncIOTracer> _tracer;
};

} // namespace starrocks
