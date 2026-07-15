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

#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <shared_mutex>

#include "exec_primitive/pipeline/operator_factory.h"

namespace starrocks {
class ExprContext;
namespace pipeline {
class QueryContext;

// Async variant of ProjectOperator for projections that contain a (blocking) UDF.
//
// The projection evaluation -- including the synchronous Arrow Flight RPC to the UDF worker --
// is submitted to the connector-scan executor's background thread pool instead of running inline in
// push_chunk. While the RPC is in flight the operator reports "not ready", so the pipeline driver
// yields its execution thread; when the RPC completes the background task wakes the driver through
// its observer. This keeps a slow UDF from holding a pipeline thread and starving other
// drivers scheduled on it.
//
// Up to `max_inflight` chunks per driver may be in flight (config
// udf_async_max_inflight_chunks): with 1 the worker idles between chunks; with >1 the worker
// stays busy (pipelined). Each in-flight chunk uses its own UDF worker/stub/stream -- selected by
// a per-slot logical driver id (real_driver_id * max_inflight + slot) that ArrowFunctionCallExpr
// keys the stub by -- so concurrent chunks never share one DoExchange stream. Chunks are assigned a
// monotonic sequence number and emitted in input order via a reorder buffer.
class AsyncUdfProjectOperator final : public Operator {
public:
    AsyncUdfProjectOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                             std::vector<int32_t>& column_ids, const std::vector<ExprContext*>& expr_ctxs,
                             const std::vector<bool>& type_is_nullable,
                             const std::vector<int32_t>& common_sub_column_ids,
                             const std::vector<ExprContext*>& common_sub_expr_ctxs, int32_t max_inflight)
            : Operator(factory, id, "async_udf_project", plan_node_id, false, driver_sequence),
              _column_ids(column_ids),
              _expr_ctxs(expr_ctxs),
              _type_is_nullable(type_is_nullable),
              _common_sub_column_ids(common_sub_column_ids),
              _common_sub_expr_ctxs(common_sub_expr_ctxs),
              _max_inflight(max_inflight < 1 ? 1 : max_inflight) {}

    ~AsyncUdfProjectOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override;

    bool need_input() const override;

    bool is_finished() const override;

    bool pending_finish() const override;

    bool ignore_empty_eos() const override { return false; }

    Status set_finishing(RuntimeState* state) override {
        _input_finished = true;
        return Status::OK();
    }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;

private:
    // The actual projection computation (identical semantics to ProjectOperator); runs on the
    // background thread and performs the blocking Flight RPC.
    StatusOr<ChunkPtr> _evaluate(const ChunkPtr& chunk);

    void _set_io_status(const Status& status) {
        std::unique_lock l(_lock);
        if (_io_status.ok()) {
            _io_status = status;
        }
    }
    Status _io_status_locked() const {
        std::shared_lock l(_lock);
        return _io_status;
    }
    // Number of chunks accepted but not yet emitted (in flight + buffered).
    int64_t _outstanding() const { return _next_input_seq - _next_output_seq; }

    const std::vector<int32_t>& _column_ids;
    const std::vector<ExprContext*>& _expr_ctxs;
    const std::vector<bool>& _type_is_nullable;
    const std::vector<int32_t>& _common_sub_column_ids;
    const std::vector<ExprContext*>& _common_sub_expr_ctxs;
    const int32_t _max_inflight;

    // Touched only on the pipeline (driver) thread.
    bool _input_finished = false;
    int64_t _next_input_seq = 0;  // seq to assign to the next accepted chunk
    int64_t _next_output_seq = 0; // seq of the next chunk to emit

    // Shared with the background tasks.
    std::atomic_int32_t _num_running = 0;
    mutable std::shared_mutex _lock;
    Status _io_status;
    std::map<int64_t, ChunkPtr> _results; // completed results by seq, guarded by _lock
    std::weak_ptr<QueryContext> _query_ctx;

    RuntimeProfile::Counter* _expr_compute_timer = nullptr;
    RuntimeProfile::Counter* _common_sub_expr_compute_timer = nullptr;
    RuntimeProfile::Counter* _submit_task_counter = nullptr;
    RuntimeProfile::Counter* _rpc_wait_timer = nullptr;
};

class AsyncUdfProjectOperatorFactory final : public OperatorFactory {
public:
    AsyncUdfProjectOperatorFactory(int32_t id, int32_t plan_node_id, std::vector<int32_t>&& column_ids,
                                    std::vector<ExprContext*>&& expr_ctxs, std::vector<bool>&& type_is_nullable,
                                    std::vector<int32_t>&& common_sub_column_ids,
                                    std::vector<ExprContext*>&& common_sub_expr_ctxs, int32_t max_inflight)
            : OperatorFactory(id, "async_udf_project", plan_node_id),
              _column_ids(std::move(column_ids)),
              _expr_ctxs(std::move(expr_ctxs)),
              _type_is_nullable(std::move(type_is_nullable)),
              _common_sub_column_ids(std::move(common_sub_column_ids)),
              _common_sub_expr_ctxs(std::move(common_sub_expr_ctxs)),
              _max_inflight(max_inflight) {}

    ~AsyncUdfProjectOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<AsyncUdfProjectOperator>(this, _id, _plan_node_id, driver_sequence, _column_ids,
                                                          _expr_ctxs, _type_is_nullable, _common_sub_column_ids,
                                                          _common_sub_expr_ctxs, _max_inflight);
    }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    // The async result is delivered by waking the driver's observer, so this operator plays nicely
    // with the event scheduler.
    bool support_event_scheduler() const override { return true; }

private:
    std::vector<int32_t> _column_ids;
    std::vector<ExprContext*> _expr_ctxs;
    std::vector<bool> _type_is_nullable;
    std::vector<int32_t> _common_sub_column_ids;
    std::vector<ExprContext*> _common_sub_expr_ctxs;
    const int32_t _max_inflight;
};

} // namespace pipeline
} // namespace starrocks
