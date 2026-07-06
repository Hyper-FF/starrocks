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

#include "exec/data_sinks/multi_olap_table_sink.h"

namespace starrocks {

MultiOlapTableSink::MultiOlapTableSink(ObjectPool* pool, const std::vector<TExpr>& texprs)
        : _pool(pool), _texprs(texprs) {}

Status MultiOlapTableSink::init(const TDataSink& sink, RuntimeState* state) {
    Status status;
    for (const auto& olap_table_sink : sink.multi_olap_table_sinks) {
        auto olap_sink = std::make_unique<OlapTableSink>(_pool, _texprs, &status, state);
        RETURN_IF_ERROR(status);
        RETURN_IF_ERROR(olap_sink->init(olap_table_sink, state));
        _sinks.emplace_back(std::move(olap_sink));
    }

    return status;
}

Status MultiOlapTableSink::prepare(RuntimeState* state) {
    for (auto& sink : _sinks) {
        RETURN_IF_ERROR(sink->prepare(state));
    }
    return Status::OK();
}

Status MultiOlapTableSink::send_chunk(RuntimeState* state, Chunk* chunk) {
    for (auto& sink : _sinks) {
        RETURN_IF_ERROR(sink->send_chunk(state, chunk));
    }
    return Status::OK();
}

Status MultiOlapTableSink::open(RuntimeState* state) {
    for (auto& sink : _sinks) {
        RETURN_IF_ERROR(sink->open(state));
    }
    return Status::OK();
}

Status MultiOlapTableSink::advance(RuntimeState* state) {
    for (auto& sink : _sinks) {
        RETURN_IF_ERROR(sink->advance(state));
    }
    return Status::OK();
}

AsyncSinkState MultiOlapTableSink::state() const {
    // Aggregate = the least-advanced child (kOpening < kOpen < kClosing < kClosed): we are only
    // fully kOpen when every child is open, and only kClosed when every child has committed.
    auto least = AsyncSinkState::kClosed;
    for (const auto& sink : _sinks) {
        if (static_cast<int>(sink->state()) < static_cast<int>(least)) {
            least = sink->state();
        }
    }
    return least;
}

void MultiOlapTableSink::mark_finishing() {
    for (auto& sink : _sinks) {
        sink->mark_finishing();
    }
}

Status MultiOlapTableSink::send_chunk_nonblocking(RuntimeState* state, const ChunkPtr& chunk) {
    for (auto& sink : _sinks) {
        RETURN_IF_ERROR(sink->send_chunk_nonblocking(state, chunk));
    }
    return Status::OK();
}

bool MultiOlapTableSink::is_full() {
    for (auto& sink : _sinks) {
        if (sink->is_full()) {
            return true;
        }
    }
    return false;
}

Status MultiOlapTableSink::close(RuntimeState* state, const Status& close_status) {
    for (auto& sink : _sinks) {
        RETURN_IF_ERROR(sink->close(state, close_status));
    }
    return Status::OK();
}

RuntimeProfile* MultiOlapTableSink::profile() {
    RuntimeProfile* profile = nullptr;
    for (auto& sink : _sinks) {
        if (profile == nullptr) {
            profile = sink->profile();
        } else {
            profile->merge(sink->profile());
        }
    }
    return profile;
}

void MultiOlapTableSink::add_olap_table_sink(std::unique_ptr<OlapTableSink> sink) {
    _sinks.emplace_back(std::move(sink));
}

std::unique_ptr<OlapTableSink>& MultiOlapTableSink::get_olap_table_sink(int index) {
    return _sinks[index];
}

void MultiOlapTableSink::set_profile(RuntimeProfile* profile) {
    for (auto& sink : _sinks) {
        sink->set_profile(profile);
    }
}

} // namespace starrocks
