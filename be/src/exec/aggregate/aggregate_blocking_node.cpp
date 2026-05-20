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

#include "exec/aggregate/aggregate_blocking_node.h"

#include <memory>
#include <type_traits>

#include "base/simd/simd.h"
#include "exec/aggregator.h"
#include "exec/pipeline/aggregate/aggregate_blocking_sink_operator.h"
#include "exec/pipeline/aggregate/aggregate_blocking_source_operator.h"
#include "exec/pipeline/aggregate/sorted_aggregate_streaming_sink_operator.h"
#include "exec/pipeline/aggregate/sorted_aggregate_streaming_source_operator.h"
#include "exec/pipeline/aggregate/spillable_aggregate_blocking_sink_operator.h"
#include "exec/pipeline/aggregate/spillable_aggregate_blocking_source_operator.h"
#include "exec/pipeline/aggregate/spillable_partitionwise_aggregate_operator.h"
#include "exec/pipeline/chunk_accumulate_operator.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/exec_node_pipeline_adapter.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/query_cache/conjugate_operator.h"
#include "exprs/chunk_predicate_evaluator.h"
#include "exprs/expr_factory.h"
#include "runtime/current_thread.h"

namespace starrocks {

template <class AggFactory, class SourceFactory, class SinkFactory>
StatusOr<pipeline::OpFactories> AggregateBlockingNode::_decompose_to_pipeline(pipeline::OpFactories& ops_with_sink,
                                                                              pipeline::PipelineBuilderContext* context,
                                                                              bool per_bucket_optimize) {
    using namespace pipeline;

    AggPipelineHooks hooks;
    hooks.needs_spill_interpolation = std::is_same_v<SinkFactory, SpillableAggregateBlockingSinkOperatorFactory> ||
                                      std::is_same_v<SinkFactory, SpillablePartitionWiseAggregateSinkOperatorFactory>;
    hooks.is_partitionwise = std::is_same_v<SourceFactory, SpillablePartitionWiseAggregateSourceOperatorFactory>;
    hooks.init_rf_before_bucket_wrap = true;

    hooks.primary = [this](AggrMode aggr_mode, const SpillProcessChannelFactoryPtr& spill_channel_factory,
                           SourceOperatorFactory* upstream_source_op,
                           PipelineBuilderContext* ctx) -> std::tuple<OpFactoryPtr, SourceOperatorFactoryPtr> {
        auto aggregator_factory = std::make_shared<AggFactory>(_tnode);
        aggregator_factory->set_aggr_mode(aggr_mode);
        auto sink_op = std::make_shared<SinkFactory>(ctx->next_operator_id(), id(), aggregator_factory,
                                                     _build_runtime_filters, spill_channel_factory);
        auto source_op = std::make_shared<SourceFactory>(ctx->next_operator_id(), id(), aggregator_factory);
        ctx->inherit_upstream_source_properties(source_op.get(), upstream_source_op);
        return {std::move(sink_op), std::move(source_op)};
    };

    if constexpr (std::is_same_v<SourceFactory, SpillablePartitionWiseAggregateSourceOperatorFactory>) {
        hooks.blocking = [this](AggrMode aggr_mode, const SpillProcessChannelFactoryPtr& spill_channel_factory,
                                SourceOperatorFactory* upstream_source_op,
                                PipelineBuilderContext* ctx) -> std::tuple<OpFactoryPtr, SourceOperatorFactoryPtr> {
            auto aggregator_factory = std::make_shared<AggFactory>(_tnode);
            aggregator_factory->set_aggr_mode(aggr_mode);
            auto sink_op = std::make_shared<AggregateBlockingSinkOperatorFactory>(
                    ctx->next_operator_id(), id(), aggregator_factory, _build_runtime_filters, spill_channel_factory);
            auto source_op = std::make_shared<AggregateBlockingSourceOperatorFactory>(ctx->next_operator_id(), id(),
                                                                                      aggregator_factory);
            ctx->inherit_upstream_source_properties(source_op.get(), upstream_source_op);
            return {std::move(sink_op), std::move(source_op)};
        };
        hooks.set_partitionwise_conjugate = [](SourceOperatorFactoryPtr& source,
                                               query_cache::ConjugateOperatorFactoryPtr conjugate) {
            std::dynamic_pointer_cast<SpillablePartitionWiseAggregateSourceOperatorFactory>(source)->set_pw_agg_factory(
                    std::move(conjugate));
        };
    }

    return _decompose_with_hooks(ops_with_sink, context, per_bucket_optimize, hooks);
}

StatusOr<pipeline::OpFactories> AggregateBlockingNode::decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    context->has_aggregation = true;
    ASSIGN_OR_RETURN(auto ops_with_sink, _children[0]->decompose_to_pipeline(context));
    auto& agg_node = _tnode.agg_node;

    bool sorted_streaming_aggregate = _tnode.agg_node.__isset.use_sort_agg && _tnode.agg_node.use_sort_agg;
    bool use_per_bucket_optimize =
            _tnode.agg_node.__isset.use_per_bucket_optimize && _tnode.agg_node.use_per_bucket_optimize;
    bool has_group_by_keys = agg_node.__isset.grouping_exprs && !_tnode.agg_node.grouping_exprs.empty();
    bool could_local_shuffle = context->could_local_shuffle(ops_with_sink);

    auto try_interpolate_local_shuffle = [this, context](auto& ops) {
        return context->maybe_interpolate_local_shuffle_exchange(runtime_state(), id(), ops, [this]() {
            std::vector<ExprContext*> group_by_expr_ctxs;
            WARN_IF_ERROR(ExprFactory::create_expr_trees(_pool, _tnode.agg_node.grouping_exprs, &group_by_expr_ctxs,
                                                         runtime_state(), true),
                          "create grouping expr failed");
            return group_by_expr_ctxs;
        });
    };

    if (!sorted_streaming_aggregate) {
        // 1. Finalize aggregation:
        //   - Without group by clause, it cannot be parallelized and need local passthough.
        //   - With group by clause, it can be parallelized and need local shuffle when could_local_shuffle is true.
        // 2. Non-finalize aggregation:
        //   - Without group by clause, it can be parallelized and needn't local shuffle.
        //   - With group by clause, it can be parallelized and need local shuffle when could_local_shuffle is true.
        if (agg_node.need_finalize) {
            if (!has_group_by_keys) {
                ops_with_sink =
                        context->maybe_interpolate_local_passthrough_exchange(runtime_state(), id(), ops_with_sink);
            } else if (could_local_shuffle) {
                ops_with_sink = try_interpolate_local_shuffle(ops_with_sink);
            }
        } else {
            if (!has_group_by_keys) {
                // Do nothing.
            } else if (could_local_shuffle) {
                ops_with_sink = try_interpolate_local_shuffle(ops_with_sink);
            }
        }
    }

    use_per_bucket_optimize &= dynamic_cast<LocalExchangeSourceOperatorFactory*>(ops_with_sink.back().get()) == nullptr;

    OpFactories ops_with_source;
    if (sorted_streaming_aggregate) {
        ASSIGN_OR_RETURN(
                ops_with_source,
                (_decompose_to_pipeline<StreamingAggregatorFactory, SortedAggregateStreamingSourceOperatorFactory,
                                        SortedAggregateStreamingSinkOperatorFactory>(ops_with_sink, context, false)));
    } else {
        // disable spill when group by with a small limit
        // having clause filters rows after aggregation, so a small limit does not bound the
        // aggregation input size; only disable spill when there is no having clause.
        bool enable_agg_spill = runtime_state()->enable_spill() && runtime_state()->enable_agg_spill();
        if (limit() != -1 && limit() < runtime_state()->chunk_size() && _tnode.conjuncts.empty()) {
            enable_agg_spill = false;
        }
        if (enable_agg_spill && has_group_by_keys) {
            if (runtime_state()->enable_spill_partitionwise_agg()) {
                ASSIGN_OR_RETURN(
                        ops_with_source,
                        (_decompose_to_pipeline<AggregatorFactory, SpillablePartitionWiseAggregateSourceOperatorFactory,
                                                SpillablePartitionWiseAggregateSinkOperatorFactory>(ops_with_sink,
                                                                                                    context, false)));
            } else {
                ASSIGN_OR_RETURN(
                        ops_with_source,
                        (_decompose_to_pipeline<AggregatorFactory, SpillableAggregateBlockingSourceOperatorFactory,
                                                SpillableAggregateBlockingSinkOperatorFactory>(ops_with_sink, context,
                                                                                               false)));
            }
        } else {
            ASSIGN_OR_RETURN(ops_with_source,
                             (_decompose_to_pipeline<AggregatorFactory, AggregateBlockingSourceOperatorFactory,
                                                     AggregateBlockingSinkOperatorFactory>(
                                     ops_with_sink, context, use_per_bucket_optimize && has_group_by_keys)));
        }
    }

    // insert local shuffle after sorted streaming aggregate
    if (_tnode.agg_node.need_finalize && sorted_streaming_aggregate && could_local_shuffle && has_group_by_keys) {
        ops_with_source = try_interpolate_local_shuffle(ops_with_source);
    }

    if (limit() != -1) {
        ops_with_source.emplace_back(
                std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }

    if (!_tnode.conjuncts.empty() || ops_with_source.back()->has_runtime_filters()) {
        pipeline::may_add_chunk_accumulate_operator(ops_with_source, context, id());
    }

    ops_with_source = context->maybe_interpolate_debug_ops(runtime_state(), _id, ops_with_source);

    return ops_with_source;
}

} // namespace starrocks
