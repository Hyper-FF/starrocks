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

#include "exec_primitive/pipeline/source_operator.h"

namespace starrocks {
class DataStreamRecvr;
class RowDescriptor;
namespace pipeline {
class ExchangeSourceOperator : public SourceOperator {
public:
    ExchangeSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : SourceOperator(factory, id, "exchange_source", plan_node_id, false, driver_sequence) {}

    ~ExchangeSourceOperator() override = default;

    Status prepare(RuntimeState* state) override;

    Status prepare_local_state(RuntimeState* state) override;

    bool has_output() const override;

    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    // ===== work-stealing: partition-aware (see PIPELINE_WORK_STEALING_PLAN.md) =====
    // For a pipeline-level hash shuffle, receiver queue `driver_sequence` holds exactly
    // partition driver_sequence's rows, so a whole received chunk can be handed to an idle
    // sibling and probed against that partition's peer build table. The receiver queue is a
    // lock-free MPMC queue, so a concurrent steal pop is safe.
    bool support_steal() const override;
    size_t stealable_backlog() const override;
    StatusOr<StealUnit> try_steal_unit() override;

    std::string get_name() const override;

private:
    std::shared_ptr<DataStreamRecvr> _stream_recvr = nullptr;
    std::atomic<bool> _is_finishing = false;
};

class ExchangeSourceOperatorFactory final : public SourceOperatorFactory {
public:
    ExchangeSourceOperatorFactory(int32_t id, int32_t plan_node_id, const TExchangeNode& texchange_node,
                                  int32_t num_sender, const RowDescriptor& row_desc, bool enable_pipeline_level_shuffle)
            : SourceOperatorFactory(id, "exchange_source", plan_node_id),
              _texchange_node(texchange_node),
              _num_sender(num_sender),
              _row_desc(row_desc),
              _enable_pipeline_level_shuffle(enable_pipeline_level_shuffle) {}

    ~ExchangeSourceOperatorFactory() override;

    bool support_event_scheduler() const override { return true; }

    const TExchangeNode& texchange_node() { return _texchange_node; }

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        ++_stream_recvr_cnt;
        // FIXME: it is unsafe to pass the raw `this` pointer to construct a shared_ptr object.
        // The shared_ptr object may live longer than the object `this` pointed to.
        return std::make_shared<ExchangeSourceOperator>(this, _id, _plan_node_id, driver_sequence);
    }

    bool could_local_shuffle() const override;
    TPartitionType::type partition_type() const override;

    // Work-stealing: true iff the sender did a pipeline-level HASH_PARTITIONED shuffle, so each
    // receiver queue is partition-pure (queue i == partition i == driver_sequence i) and a
    // stolen chunk can be tagged with its partition for a partition-aware probe.
    bool is_pipeline_level_hash_shuffle() const;

    std::shared_ptr<DataStreamRecvr> create_stream_recvr(RuntimeState* state);
    void close_stream_recvr();

    SourceOperatorFactory::AdaptiveState adaptive_initial_state() const override { return AdaptiveState::ACTIVE; }

private:
    const TExchangeNode& _texchange_node;
    const int32_t _num_sender;
    const RowDescriptor& _row_desc;
    const bool _enable_pipeline_level_shuffle;
    std::shared_ptr<DataStreamRecvr> _stream_recvr = nullptr;
    std::atomic<int64_t> _stream_recvr_cnt = 0;
};

} // namespace pipeline
} // namespace starrocks
