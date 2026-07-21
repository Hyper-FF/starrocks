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

#include "exec/pipeline/hashjoin/hash_join_probe_operator.h"

#include "exec/hash_joiner.h"
#include "exec/pipeline/hashjoin/hash_joiner_factory.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

HashJoinProbeOperator::HashJoinProbeOperator(OperatorFactory* factory, int32_t id, const string& name,
                                             int32_t plan_node_id, int32_t driver_sequence, HashJoinerPtr join_prober,
                                             HashJoinerPtr join_builder, HashJoinerFactoryPtr hash_joiner_factory)
        : OperatorWithDependency(factory, id, name, plan_node_id, false, driver_sequence),
          _join_prober(std::move(join_prober)),
          _join_builder(std::move(join_builder)),
          _hash_joiner_factory(std::move(hash_joiner_factory)) {}

void HashJoinProbeOperator::close(RuntimeState* state) {
    // Work-stealing: close the read-only peer probers created for stolen partitions. Each holds
    // a clone_readable table (shared items via shared_ptr), so closing just drops this driver's
    // clone; the owning partition's build table is unaffected.
    for (auto& [partition_id, peer_prober] : _peer_probers) {
        peer_prober->close(state);
    }
    _peer_probers.clear();
    _stolen_output.clear();

    if (_join_prober != _join_builder) {
        _join_prober->unref(state);
    }

    _join_builder->decr_prober(state);

    OperatorWithDependency::close(state);
}

Status HashJoinProbeOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorWithDependency::prepare(state));

    _join_builder->incr_prober();

    if (_join_builder != _join_prober) {
        _join_prober->ref();
    }

    RETURN_IF_ERROR(_join_prober->prepare_prober(state, _unique_metrics.get()));
    _join_builder->attach_probe_observer(state, observer());

    return Status::OK();
}

bool HashJoinProbeOperator::has_output() const {
    // Work-stealing: join output produced from stolen chunks is emitted first.
    return !_stolen_output.empty() || _join_prober->has_output();
}

bool HashJoinProbeOperator::need_input() const {
    if (_join_prober->need_input()) {
        return true;
    }

    if (is_ready()) {
        // If hasn't referenced hash table, return true to reference hash table in push_chunk.
        return !_join_prober->has_referenced_hash_table();
    }
    return false;
}

bool HashJoinProbeOperator::is_finished() const {
    // Not finished while stolen join output is still pending emit.
    if (!_stolen_output.empty()) {
        return false;
    }
    return _join_prober->is_done() || _join_builder->is_done();
}

bool HashJoinProbeOperator::is_ready() const {
    return _join_builder->is_build_done();
}

Status HashJoinProbeOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    RETURN_IF_ERROR(_reference_builder_hash_table_once());
    RETURN_IF_ERROR(_join_prober->push_chunk(state, std::move(const_cast<ChunkPtr&>(chunk))));
    return Status::OK();
}

StatusOr<ChunkPtr> HashJoinProbeOperator::pull_chunk(RuntimeState* state) {
    // Work-stealing: drain join output produced from stolen chunks first.
    if (!_stolen_output.empty()) {
        ChunkPtr chunk = std::move(_stolen_output.front());
        _stolen_output.pop_front();
        return chunk;
    }
    RETURN_IF_ERROR(_reference_builder_hash_table_once());
    return _join_prober->pull_chunk(state);
}

Status HashJoinProbeOperator::set_finishing(RuntimeState* state) {
    // TODO: notify one will be ok
    auto notify = _join_builder->defer_notify_build();
    RETURN_IF_ERROR(_join_prober->probe_input_finished(state));
    _join_prober->enter_post_probe_phase();
    return Status::OK();
}

Status HashJoinProbeOperator::set_finished(RuntimeState* state) {
    _join_prober->enter_eos_phase();
    _join_builder->set_prober_finished();
    return Status::OK();
}

Status HashJoinProbeOperator::_reference_builder_hash_table_once() {
    if (!is_ready()) {
        return Status::OK();
    }

    if (_join_prober->has_referenced_hash_table()) {
        return Status::OK();
    }

    TRY_CATCH_ALLOC_SCOPE_START()
    _join_prober->reference_hash_table(_join_builder.get());
    TRY_CATCH_ALLOC_SCOPE_END()
    return Status::OK();
}

// ===== work-stealing: partition-aware probe (see PIPELINE_WORK_STEALING_PLAN.md) =====

bool HashJoinProbeOperator::accepts_stolen_input() const {
    // Only a hash-PARTITIONED join whose type never mutates the build side during probe
    // (INNER / LEFT_*) can look up a foreign-partition chunk against a peer read-only table.
    return _join_prober->distribution_mode() == TJoinDistributionMode::PARTITIONED &&
           !has_post_probe(_join_prober->join_type());
}

StatusOr<HashJoiner*> HashJoinProbeOperator::_peer_prober(RuntimeState* state, int32_t partition_id) {
    if (auto it = _peer_probers.find(partition_id); it != _peer_probers.end()) {
        return it->second.get();
    }
    if (_hash_joiner_factory == nullptr) {
        return nullptr;
    }
    HashJoinerPtr peer_builder = _hash_joiner_factory->builder_for_partition(partition_id);
    if (peer_builder == nullptr || !peer_builder->is_build_done()) {
        return nullptr;
    }
    // A fresh joiner whose prober references (clone_readable) the peer partition's build table.
    // The clone shares the peer's hash-table items via shared_ptr, so it stays alive
    // independently of the peer driver's own prober; it owns a private probe state, so
    // concurrent probing of the same read-only table is safe.
    auto peer_prober = std::make_shared<HashJoiner>(_hash_joiner_factory->hash_join_param());
    RETURN_IF_ERROR(peer_prober->prepare_prober(state, _unique_metrics.get()));
    TRY_CATCH_ALLOC_SCOPE_START()
    // fresh_probe_state=true: the peer partition is being probed concurrently by its own driver,
    // so snapshot only its immutable built table and give this thief a fresh probe state -- copying
    // the peer's live probe scratch would be a data race and silently drop rows.
    peer_prober->reference_hash_table(peer_builder.get(), /*fresh_probe_state=*/true);
    // The fresh probe state above is unprepared (its scratch buffers are default-constructed, not
    // copied from the peer's prepared state); prepare it now against the referenced table so the
    // first probe does not dereference uninitialized probe scratch. Operates only on this thief's
    // own clone, never the peer.
    RETURN_IF_ERROR(peer_prober->reset_probe(state));
    TRY_CATCH_ALLOC_SCOPE_END()
    auto* raw = peer_prober.get();
    _peer_probers.emplace(partition_id, std::move(peer_prober));
    return raw;
}

Status HashJoinProbeOperator::push_stolen_chunk(RuntimeState* state, const ChunkPtr& chunk, int32_t partition_id) {
    ASSIGN_OR_RETURN(HashJoiner* peer_prober, _peer_prober(state, partition_id));
    if (peer_prober == nullptr) {
        return Status::InternalError("partition-aware steal: peer build table unavailable");
    }
    // If the peer partition's build short-circuited to EOS (e.g. an INNER/SEMI join whose build
    // side for this partition is empty), reset_probe() left the prober in EOS WITHOUT preparing a
    // probe state -- probing it would dereference uninitialized scratch. Such a partition yields no
    // join output, exactly as the owner would produce, so drop the stolen chunk's output safely.
    if (peer_prober->is_done()) {
        return Status::OK();
    }
    // One-shot probe of the stolen chunk against the peer partition's read-only table; drain all
    // its join output into the stolen-output buffer. INNER / LEFT_* need no post-probe pass.
    RETURN_IF_ERROR(peer_prober->push_chunk(state, ChunkPtr(chunk)));
    while (peer_prober->has_output()) {
        ASSIGN_OR_RETURN(ChunkPtr out, peer_prober->pull_chunk(state));
        if (out != nullptr && !out->is_empty()) {
            _stolen_output.emplace_back(std::move(out));
        }
    }
    return Status::OK();
}

Status HashJoinProbeOperator::reset_state(RuntimeState* state, const vector<ChunkPtr>& refill_chunks) {
    RETURN_IF_ERROR(_reference_builder_hash_table_once());
    // Reset probe state only when it has valid state after referencing the build hash table.
    if (_join_prober->has_referenced_hash_table()) {
        RETURN_IF_ERROR(_join_prober->reset_probe(state));
    }
    return Status::OK();
}

OperatorExecStatsSnapshot HashJoinProbeOperator::exec_stats_snapshot() const {
    OperatorExecStatsSnapshot snapshot;
    snapshot.plan_node_id = _plan_node_id;
    snapshot.update_pull_rows = true;
    snapshot.pull_rows = COUNTER_VALUE(_pull_row_num_counter);
    if (_conjuncts_input_counter != nullptr && _conjuncts_output_counter != nullptr) {
        snapshot.update_pred_filter_rows = true;
        snapshot.pred_filter_rows = COUNTER_VALUE(_conjuncts_input_counter) - COUNTER_VALUE(_conjuncts_output_counter);
    }
    if (_bloom_filter_eval_context.join_runtime_filter_input_counter != nullptr) {
        snapshot.update_rf_filter_rows = true;
        int64_t input_rows = COUNTER_VALUE(_bloom_filter_eval_context.join_runtime_filter_input_counter);
        int64_t output_rows = COUNTER_VALUE(_bloom_filter_eval_context.join_runtime_filter_output_counter);
        snapshot.rf_filter_rows = input_rows - output_rows;
    }
    return snapshot;
}

HashJoinProbeOperatorFactory::HashJoinProbeOperatorFactory(int32_t id, int32_t plan_node_id,
                                                           HashJoinerFactoryPtr hash_joiner_factory)
        : OperatorFactory(id, "hash_join_probe", plan_node_id), _hash_joiner_factory(std::move(hash_joiner_factory)) {}

Status HashJoinProbeOperatorFactory::prepare(RuntimeState* state) {
    return OperatorFactory::prepare(state);
}
void HashJoinProbeOperatorFactory::close(RuntimeState* state) {
    OperatorFactory::close(state);
}

OperatorPtr HashJoinProbeOperatorFactory::create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<HashJoinProbeOperator>(this, _id, _name, _plan_node_id, driver_sequence,
                                                   _hash_joiner_factory->create_prober(dop, driver_sequence),
                                                   _hash_joiner_factory->get_builder(dop, driver_sequence),
                                                   _hash_joiner_factory);
}

} // namespace starrocks::pipeline
