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

#include "http/action/ebpf_io_action.h"

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <unistd.h>

#include <filesystem>
#include <sstream>

#include "common/logging.h"
#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/http_status.h"

namespace starrocks {

static std::string resolve_self_exe() {
    char buf[PATH_MAX] = {};
    ssize_t len = readlink("/proc/self/exe", buf, sizeof(buf) - 1);
    return len > 0 ? std::string(buf, len) : "";
}

static uint32_t param_u32(HttpRequest* req, const std::string& key, uint32_t default_val) {
    const std::string& val = req->get_query_string_value(key);
    if (val.empty()) return default_val;
    try {
        return static_cast<uint32_t>(std::stoul(val));
    } catch (...) {
        return default_val;
    }
}

static size_t param_size(HttpRequest* req, const std::string& key, size_t default_val) {
    const std::string& val = req->get_query_string_value(key);
    if (val.empty()) return default_val;
    try {
        return static_cast<size_t>(std::stoull(val));
    } catch (...) {
        return default_val;
    }
}

EbpfIOAction::EbpfIOAction(ExecEnv* exec_env) : _exec_env(exec_env) {
    _tracer = std::make_unique<io::SyncIOTracer>();
}

void EbpfIOAction::handle(HttpRequest* req) {
    const std::string& op = req->param("op");
    if (op == "start") {
        _handle_start(req);
    } else if (op == "stop") {
        _handle_stop(req);
    } else if (op == "stats") {
        _handle_stats(req);
    } else if (op == "reset") {
        _handle_reset(req);
    } else if (op == "slow_events") {
        _handle_slow_events(req);
    } else {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST,
                                "Unknown op. Valid ops: start, stop, stats, reset, slow_events");
    }
}

void EbpfIOAction::_handle_start(HttpRequest* req) {
    if (_tracer->is_running()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "Tracer is already running. Call /stop first.");
        return;
    }

    uint32_t pid = param_u32(req, "pid", static_cast<uint32_t>(getpid()));

    std::string binary_path = req->get_query_string_value("binary");
    if (binary_path.empty()) {
        binary_path = resolve_self_exe();
    }
    if (binary_path.empty() || !std::filesystem::exists(binary_path)) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST,
                                fmt::format("binary path not found: '{}'", binary_path));
        return;
    }

    std::string bpf_obj = req->get_query_string_value("bpf_obj");
    if (bpf_obj.empty() || !std::filesystem::exists(bpf_obj)) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST,
                                fmt::format("bpf_obj path not found: '{}'. "
                                            "Build sync_io_trace.bpf.o and pass its path.",
                                            bpf_obj));
        return;
    }

    uint32_t slow_ms = param_u32(req, "slow_ms", 10);
    uint64_t slow_ns = static_cast<uint64_t>(slow_ms) * 1000 * 1000;

    auto st = _tracer->start(pid, binary_path, bpf_obj, slow_ns);
    if (!st.ok()) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                fmt::format("Failed to start tracer: {}", st.message()));
        return;
    }

    HttpChannel::send_reply(req, HttpStatus::OK,
                            fmt::format("eBPF IO tracer started: pid={} slow_threshold={}ms\n", pid, slow_ms));
}

void EbpfIOAction::_handle_stop(HttpRequest* req) {
    if (!_tracer->is_running()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "Tracer is not running.");
        return;
    }
    _tracer->stop();
    HttpChannel::send_reply(req, HttpStatus::OK, "eBPF IO tracer stopped.\n");
}

void EbpfIOAction::_handle_stats(HttpRequest* req) {
    if (!_tracer->is_running()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "Tracer is not running. Call /start first.");
        return;
    }

    size_t topn = param_size(req, "topn", 20);
    const std::string& sort_by = req->get_query_string_value("sort");

    std::vector<io::SyncIOStats> stats;
    if (sort_by == "latency") {
        stats = _tracer->get_topn_by_latency(topn);
    } else {
        stats = _tracer->get_topn_by_bytes(topn);
    }

    std::ostringstream oss;
    oss << io::SyncIOStats::header_string() << "\n";
    for (const auto& s : stats) {
        oss << s.to_string() << "\n";
    }
    HttpChannel::send_reply(req, HttpStatus::OK, oss.str());
}

void EbpfIOAction::_handle_reset(HttpRequest* req) {
    auto st = _tracer->reset_stats();
    if (!st.ok()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, st.message().to_string());
        return;
    }
    HttpChannel::send_reply(req, HttpStatus::OK, "Stats reset.\n");
}

void EbpfIOAction::_handle_slow_events(HttpRequest* req) {
    if (!_tracer->is_running()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "Tracer is not running.");
        return;
    }

    rapidjson::StringBuffer buf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buf);
    writer.StartArray();

    _tracer->poll_slow_events([&](const io::SlowIOEvent& ev) {
        writer.StartObject();
        writer.Key("timestamp_ns");
        writer.Uint64(ev.timestamp_ns);
        writer.Key("latency_ns");
        writer.Uint64(ev.latency_ns);
        writer.Key("tablet_id");
        writer.Uint64(ev.tablet_id);
        writer.Key("file_offset");
        writer.Uint64(ev.file_offset);
        writer.Key("bytes");
        writer.Int64(ev.bytes);
        writer.Key("pid");
        writer.Uint(ev.pid);
        writer.Key("tid");
        writer.Uint(ev.tid);
        writer.Key("tag");
        writer.Uint(ev.tag);
        writer.Key("fd");
        writer.Int(ev.fd);
        writer.EndObject();
    });

    writer.EndArray();
    HttpChannel::send_reply(req, HttpStatus::OK, buf.GetString());
}

} // namespace starrocks
