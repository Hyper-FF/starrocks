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

#include "exec/pipeline/sink/olap_table_sink_operator.h"

#include "compute_env/result/buffer_control_block.h"
#include "compute_env/result/result_buffer_mgr.h"
#include "compute_env/workgroup/pipeline_executor_set.h"
#include "compute_env/workgroup/work_group.h"
#include "exec/data_sinks/tablet_sink.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/fragment_context_cancel.h"
#include "exec_primitive/pipeline/primitives/driver_executor.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
Status OlapTableSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));

    state->set_per_fragment_instance_idx(_sender_id);

    _sink->set_profile(_unique_metrics.get());
    RETURN_IF_ERROR(_sink->prepare(state));

    // Kick off the async open (the first advance() issues the open RPCs; state() becomes kOpen once
    // they return and need_input()/pending_finish() pump advance() again).
    RETURN_IF_ERROR(_sink->advance(state));

    return Status::OK();
}

void OlapTableSinkOperator::close(RuntimeState* state) {
    Operator::close(state);
}

StatusOr<ChunkPtr> OlapTableSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::NotSupported("Shouldn't pull chunk from olap table sink operator");
}

bool OlapTableSinkOperator::is_finished() const {
    return _is_finished;
}

bool OlapTableSinkOperator::pending_finish() const {
    // Pump the sink FSM one non-blocking step (finalize open, flush any auto-partition chunk, send
    // EOS, drain, commit); pending until it reaches the terminal kClosed.
    if (auto st = _sink->advance(_fragment_ctx->runtime_state()); !st.ok()) {
        cancel_fragment_context(_fragment_ctx, st);
        return false;
    }
    return _sink->state() != AsyncSinkState::kClosed;
}

Status OlapTableSinkOperator::set_cancelled(RuntimeState* state) {
    auto final_status = state->fragment_ctx()->final_status();
    std::string reason = "Cancelled by pipeline engine, reason: " + final_status.to_string();
    return _sink->close(state, Status::Cancelled(reason));
}

Status OlapTableSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;

    if (_num_sinkers.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        _fragment_ctx->workgroup()->executors()->driver_executor()->report_audit_statistics(state->query_ctx(),
                                                                                            state->fragment_ctx());
    }
    // Signal end-of-input. The sink withholds EOS internally until any stashed auto-partition chunk
    // is flushed (EOS after data), so this is unconditional.
    _sink->mark_finishing();
    return Status::OK();
}

bool OlapTableSinkOperator::need_input() const {
    if (_is_finished) {
        return false;
    }
    // Pump the FSM forward; this finalizes the async open once the open RPCs return (kOpening ->
    // kOpen) and flushes an auto-partition chunk when its partition is ready. It is a no-op once
    // kOpen with nothing pending, so it is cheap on the hot per-chunk path.
    if (auto st = _sink->advance(_fragment_ctx->runtime_state()); !st.ok()) {
        cancel_fragment_context(_fragment_ctx, st);
        return false;
    }
    return _sink->state() == AsyncSinkState::kOpen && !_sink->is_full();
}

Status OlapTableSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    if (chunk->num_rows() == 0) {
        return Status::OK();
    }
    // The sink accepts the chunk without blocking; if it kicks off automatic partition creation it
    // retains the chunk and resends it itself from a later advance() (is_full() gates us meanwhile).
    return _sink->send_chunk_nonblocking(state, chunk);
}

OperatorPtr OlapTableSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    _increment_num_sinkers_no_barrier();
    if (driver_sequence == 0) {
        return std::make_shared<OlapTableSinkOperator>(this, _id, _plan_node_id, driver_sequence, _cur_sender_id++,
                                                       _sink0, _fragment_ctx, _num_sinkers);
    } else {
        return std::make_shared<OlapTableSinkOperator>(this, _id, _plan_node_id, driver_sequence, _cur_sender_id++,
                                                       _sinks[driver_sequence - 1].get(), _fragment_ctx, _num_sinkers);
    }
}

Status OlapTableSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    return Status::OK();
}

void OlapTableSinkOperatorFactory::close(RuntimeState* state) {
    OperatorFactory::close(state);
}
} // namespace starrocks::pipeline
