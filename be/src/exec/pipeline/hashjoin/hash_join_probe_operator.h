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

#include <deque>
#include <unordered_map>

#include "exec/pipeline/hashjoin/hash_joiner_factory.h"
#include "exec/pipeline/hashjoin/hash_joiner_fwd.h"
#include "exec/pipeline/operator_with_dependency.h"
#include "exec_primitive/pipeline/operator_factory.h"
#include "exec_primitive/pipeline/pipeline_fwd.h"

namespace starrocks::pipeline {

using HashJoiner = starrocks::HashJoiner;

class HashJoinProbeOperator : public OperatorWithDependency {
public:
    HashJoinProbeOperator(OperatorFactory* factory, int32_t id, const string& name, int32_t plan_node_id,
                          int32_t driver_sequence, HashJoinerPtr join_prober, HashJoinerPtr join_builder,
                          HashJoinerFactoryPtr hash_joiner_factory);
    ~HashJoinProbeOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override;
    bool need_input() const override;

    bool is_finished() const override;
    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;

    bool is_ready() const override;
    std::string get_name() const override {
        return strings::Substitute("$0(HashJoiner=$1)", Operator::get_name(), _join_prober.get());
    }

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    // ===== work-stealing: partition-aware probe (see PIPELINE_WORK_STEALING_PLAN.md) =====
    // This probe can process a stolen chunk belonging to a foreign partition by looking it up
    // against that partition's peer build table (read-only after build). Restricted to
    // hash-PARTITIONED distribution and non-post-probe join types (INNER / LEFT_*), which never
    // mutate the build side during probe.
    bool accepts_stolen_input() const override;
    bool steal_partition_safe(int32_t partition_id) const override;
    Status push_stolen_chunk(RuntimeState* state, const ChunkPtr& chunk, int32_t partition_id) override;
    // Ready to accept a stolen foreign-partition chunk once every partition's build is complete,
    // so any peer partition's build table is present and read-only.
    bool steal_input_ready() const override {
        return _hash_joiner_factory != nullptr && _hash_joiner_factory->all_builds_ready();
    }

    Status reset_state(starrocks::RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;
    OperatorExecStatsSnapshot exec_stats_snapshot() const override;

protected:
    /// Reference the read-only hash table from builder in the first pull_chunk.
    Status _reference_builder_hash_table_once();

    // Lazily create (and cache) a prober bound read-only to partition `partition_id`'s peer
    // build table. The ok-value is nullptr if that partition's build is unavailable/incomplete;
    // an error status is returned only on allocation failure while referencing the table.
    StatusOr<HashJoiner*> _peer_prober(RuntimeState* state, int32_t partition_id);

protected:
    const HashJoinerPtr _join_prober;
    // For non-broadcast join, _join_builder is identical to _join_prober.
    // For broadcast join, _join_prober references the hash table owned by _join_builder,
    // so increase the reference number of _join_builder to prevent it closing early.
    const HashJoinerPtr _join_builder;

    // work-stealing: reach sibling builders by partition id (all_builds_ready/builder_for_partition).
    const HashJoinerFactoryPtr _hash_joiner_factory;
    // one read-only peer prober per foreign partition this driver has stolen from.
    std::unordered_map<int32_t, HashJoinerPtr> _peer_probers;
    // join output produced from stolen chunks, drained by has_output()/pull_chunk(). Only ever
    // touched by this operator's own driver thread (steal routing happens inside process()).
    std::deque<ChunkPtr> _stolen_output;
};

class HashJoinProbeOperatorFactory : public OperatorFactory {
public:
    HashJoinProbeOperatorFactory(int32_t id, int32_t plan_node_id, HashJoinerFactoryPtr hash_joiner);

    ~HashJoinProbeOperatorFactory() override = default;

    bool support_event_scheduler() const override { return true; }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

protected:
    HashJoinerFactoryPtr _hash_joiner_factory;
};

} // namespace starrocks::pipeline
