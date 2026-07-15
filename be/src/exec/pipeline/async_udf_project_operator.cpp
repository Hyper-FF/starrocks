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

#include "exec/pipeline/async_udf_project_operator.h"

#include "base/time/time.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "compute_env/global_dict/parser.h"
#include "compute_env/query/fragment_runtime_state.h"
#include "compute_env/workgroup/pipeline_executor_set.h"
#include "compute_env/workgroup/scan_executor.h"
#include "compute_env/workgroup/scan_task.h"
#include "compute_env/workgroup/work_group.h"
#include "exec/runtime/query_context.h"
#include "exec_primitive/pipeline/primitives/pipeline_observer.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "gutil/casts.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

Status AsyncUdfProjectOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _expr_compute_timer = ADD_TIMER(_unique_metrics, "ExprComputeTime");
    _common_sub_expr_compute_timer = ADD_TIMER(_unique_metrics, "CommonSubExprComputeTime");
    _submit_task_counter = ADD_COUNTER(_unique_metrics, "SubmitTaskCount", TUnit::UNIT);
    _rpc_wait_timer = ADD_TIMER(_unique_metrics, "AsyncTaskQueueTime");
    _query_ctx = state->query_ctx()->get_shared_ptr();
    return Status::OK();
}

void AsyncUdfProjectOperator::close(RuntimeState* state) {
    {
        std::unique_lock l(_lock);
        _results.clear();
    }
    Operator::close(state);
}

bool AsyncUdfProjectOperator::has_output() const {
    std::shared_lock l(_lock);
    return !_io_status.ok() || _results.find(_next_output_seq) != _results.end();
}

bool AsyncUdfProjectOperator::need_input() const {
    if (_input_finished) {
        return false;
    }
    // Accept while there is a free in-flight slot, and no error has occurred.
    if (_outstanding() >= _max_inflight) {
        return false;
    }
    std::shared_lock l(_lock);
    return _io_status.ok();
}

bool AsyncUdfProjectOperator::is_finished() const {
    if (!_input_finished || _outstanding() > 0) {
        return false;
    }
    std::shared_lock l(_lock);
    return _io_status.ok();
}

bool AsyncUdfProjectOperator::pending_finish() const {
    // Keep the driver (and this operator) alive until all in-flight RPCs finish.
    return _num_running.load() > 0;
}

StatusOr<ChunkPtr> AsyncUdfProjectOperator::pull_chunk(RuntimeState* state) {
    RETURN_IF_ERROR(_io_status_locked());
    std::unique_lock l(_lock);
    auto it = _results.find(_next_output_seq);
    if (it == _results.end()) {
        return nullptr;
    }
    ChunkPtr chunk = std::move(it->second);
    _results.erase(it);
    ++_next_output_seq;
    return chunk;
}

Status AsyncUdfProjectOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    const int64_t seq = _next_input_seq++;

    // The empty last-chunk marker carries no rows; pass it through in order (no RPC needed).
    if (chunk->is_empty()) {
        DCHECK(chunk->owner_info().is_last_chunk());
        std::unique_lock l(_lock);
        _results[seq] = chunk;
        return Status::OK();
    }

    // Each in-flight slot uses its own UDF worker/stub/stream. ArrowFunctionCallExpr keys the
    // stub by the thread-local driver id, so give this slot a distinct logical driver id and re-apply
    // it on the background thread. Because at most _max_inflight chunks are outstanding, their
    // (seq % _max_inflight) slots are distinct, so concurrent chunks never collide on one stream.
    const int32_t real_driver_id = CurrentThread::current().get_driver_id();
    const int32_t slot = static_cast<int32_t>(seq % _max_inflight);
    const int32_t logical_driver_id = real_driver_id * _max_inflight + slot;

    workgroup::ScanTask task;
    task.workgroup = state->fragment_runtime_state()->workgroup();
    task.work_function = [wp = _query_ctx, this, state, chunk, seq, logical_driver_id,
                          create_ts = MonotonicNanos()](workgroup::YieldContext& ctx) {
        auto query_ctx = wp.lock();
        if (query_ctx == nullptr) {
            _num_running.fetch_sub(1);
            return;
        }
        SCOPED_SET_TRACE_INFO(logical_driver_id, state->query_id(), state->fragment_instance_id());
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(state->instance_mem_tracker());
        DeferOp defer([this]() {
            _num_running.fetch_sub(1);
            if (auto* ob = observer(); ob != nullptr) {
                ob->source_trigger();
            }
        });
        COUNTER_UPDATE(_rpc_wait_timer, MonotonicNanos() - create_ts);
        auto result = _evaluate(chunk);
        if (!result.ok()) {
            _set_io_status(result.status());
            return;
        }
        std::unique_lock l(_lock);
        _results[seq] = std::move(result.value());
    };

    _num_running.fetch_add(1);
    COUNTER_UPDATE(_submit_task_counter, 1);
    // Submit to the connector-scan executor (the pool for external/network-bound I/O), matching the
    // UDF's network Flight RPC and keeping it off the local scan executor. force_submit so the
    // task is never dropped; per-driver in-flight count is already bounded by _max_inflight.
    task.workgroup->executors()->connector_scan_executor()->force_submit(std::move(task));
    return Status::OK();
}

StatusOr<ChunkPtr> AsyncUdfProjectOperator::_evaluate(const ChunkPtr& chunk) {
    ChunkPtr result;
    TRY_CATCH_ALLOC_SCOPE_START()
    {
        SCOPED_TIMER(_common_sub_expr_compute_timer);
        for (size_t i = 0; i < _common_sub_column_ids.size(); ++i) {
            ASSIGN_OR_RETURN(auto col, _common_sub_expr_ctxs[i]->evaluate(chunk.get()));
            chunk->append_column(std::move(col), _common_sub_column_ids[i]);
            RETURN_IF_HAS_ERROR(_common_sub_expr_ctxs);
        }
    }

    Columns result_columns(_column_ids.size());
    {
        SCOPED_TIMER(_expr_compute_timer);
        size_t num_rows = chunk->num_rows();
        for (size_t i = 0; i < _column_ids.size(); ++i) {
            ASSIGN_OR_RETURN(result_columns[i], _expr_ctxs[i]->evaluate(chunk.get()));

            if (result_columns[i]->only_null()) {
                auto mutable_col = ColumnHelper::create_column(_expr_ctxs[i]->root()->type(), true);
                mutable_col->append_nulls(num_rows);
                result_columns[i] = std::move(mutable_col);
            } else if (result_columns[i]->is_constant()) {
                MutableColumnPtr new_column = ColumnHelper::create_column(_expr_ctxs[i]->root()->type(), false);
                auto* const_column = down_cast<const ConstColumn*>(result_columns[i].get());
                new_column->append(*const_column->data_column(), 0, 1);
                new_column->assign(num_rows, 0);
                result_columns[i] = std::move(new_column);
            }

            if (_type_is_nullable[i] && !result_columns[i]->is_nullable()) {
                result_columns[i] = NullableColumn::create(result_columns[i], NullColumn::create(num_rows, 0));
            }
        }
        RETURN_IF_HAS_ERROR(_expr_ctxs);
    }

    result = std::make_shared<Chunk>();
    for (size_t i = 0; i < result_columns.size(); ++i) {
        result->append_column(std::move(result_columns[i]), _column_ids[i]);
    }
    result->owner_info() = chunk->owner_info();
    TRY_CATCH_ALLOC_SCOPE_END()
    return result;
}

Status AsyncUdfProjectOperator::reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) {
    _input_finished = false;
    _next_input_seq = 0;
    _next_output_seq = 0;
    std::unique_lock l(_lock);
    _results.clear();
    _io_status = Status::OK();
    return Status::OK();
}

Status AsyncUdfProjectOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(ExprExecutor::prepare(_expr_ctxs, state));
    RETURN_IF_ERROR(ExprExecutor::prepare(_common_sub_expr_ctxs, state));

    DictOptimizeParser::set_output_slot_id(&_common_sub_expr_ctxs, _common_sub_column_ids);
    DictOptimizeParser::set_output_slot_id(&_expr_ctxs, _column_ids);

    RETURN_IF_ERROR(ExprExecutor::open(_common_sub_expr_ctxs, state));
    RETURN_IF_ERROR(ExprExecutor::open(_expr_ctxs, state));

    return Status::OK();
}

void AsyncUdfProjectOperatorFactory::close(RuntimeState* state) {
    ExprExecutor::close(_expr_ctxs, state);
    ExprExecutor::close(_common_sub_expr_ctxs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
