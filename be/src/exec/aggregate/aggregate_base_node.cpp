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

#include "exec/aggregate/aggregate_base_node.h"

#include "exec/aggregator.h"
#include "exec/pipeline/bucket_process_operator.h"
#include "exec/pipeline/exec_node_pipeline_adapter.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/source_operator.h"
#include "exec/pipeline/spill_process_channel.h"
#include "exec/query_cache/conjugate_operator.h"
#include "exprs/expr_factory.h"

namespace starrocks {

AggregateBaseNode::AggregateBaseNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : PipelineNode(pool, tnode, descs), _tnode(tnode) {}

AggregateBaseNode::~AggregateBaseNode() {
    if (runtime_state() != nullptr) {
        close(runtime_state());
    }
}

Status AggregateBaseNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    RETURN_IF_ERROR(
            ExprFactory::create_expr_trees(_pool, tnode.agg_node.grouping_exprs, &_group_by_expr_ctxs, state, true));
    for (auto& expr : _group_by_expr_ctxs) {
        auto& type_desc = expr->root()->type();
        if (!type_desc.support_groupby()) {
            return Status::NotSupported(fmt::format("group by type {} is not supported", type_desc.debug_string()));
        }
    }
    if (tnode.agg_node.__isset.build_runtime_filters) {
        for (const auto& desc : tnode.agg_node.build_runtime_filters) {
            auto* rf_desc = _pool->add(new RuntimeFilterBuildDescriptor());
            RETURN_IF_ERROR(rf_desc->init(_pool, desc, state));
            _build_runtime_filters.emplace_back(rf_desc);
        }
    }
    return Status::OK();
}

void AggregateBaseNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }
    if (_aggregator != nullptr) {
        if (_aggregator->is_hash_set()) {
            _mem_tracker->set(_aggregator->hash_set_memory_usage());
        } else {
            _mem_tracker->set(_aggregator->hash_map_memory_usage());
        }
        _num_rows_returned = _aggregator->num_rows_returned();
        _aggregator->close(state);
        _aggregator.reset();
    }
    ExecNode::close(state);
}

void AggregateBaseNode::push_down_tuple_slot_mappings(RuntimeState* state,
                                                      const std::vector<TupleSlotMapping>& parent_mappings) {
    _tuple_slot_mappings = parent_mappings;

    DCHECK(_tuple_ids.size() == 1);
    for (auto& expr_ctx : _group_by_expr_ctxs) {
        if (expr_ctx->root()->is_slotref()) {
            auto ref = dynamic_cast<ColumnRef*>(expr_ctx->root());
            DCHECK(ref != nullptr);
            _tuple_slot_mappings.emplace_back(ref->tuple_id(), ref->slot_id(), _tuple_ids[0], ref->slot_id());
        }
    }

    for (auto& child : _children) {
        child->push_down_tuple_slot_mappings(state, _tuple_slot_mappings);
    }
}

StatusOr<pipeline::OpFactories> AggregateBaseNode::_decompose_with_hooks(pipeline::OpFactories& ops_with_sink,
                                                                         pipeline::PipelineBuilderContext* context,
                                                                         bool per_bucket_optimize,
                                                                         const AggPipelineHooks& hooks) {
    using namespace pipeline;

    auto degree_of_parallelism = context->source_operator(ops_with_sink)->degree_of_parallelism();
    auto spill_channel_factory = std::make_shared<SpillProcessChannelFactory>(degree_of_parallelism);

    if (hooks.needs_spill_interpolation) {
        context->interpolate_spill_process(id(), spill_channel_factory, degree_of_parallelism);
    }

    const bool should_cache = context->should_interpolate_cache_operator(id(), ops_with_sink[0]);
    auto* upstream_source_op = context->source_operator(ops_with_sink);

    // Same shape `interpolate_cache_operator` expects, so we can hand it directly below.
    auto operators_generator = [&hooks, &spill_channel_factory, upstream_source_op, context,
                                should_cache](bool post_cache) -> std::tuple<OpFactoryPtr, SourceOperatorFactoryPtr> {
        AggrMode aggr_mode = should_cache ? (post_cache ? AM_BLOCKING_POST_CACHE : AM_BLOCKING_PRE_CACHE) : AM_DEFAULT;
        return hooks.primary(aggr_mode, spill_channel_factory, upstream_source_op, context);
    };

    auto [agg_sink_op, agg_source_op] = operators_generator(false);

    if (hooks.is_partitionwise) {
        DCHECK(hooks.blocking) << "partitionwise hook must provide a blocking operators generator";
        DCHECK(hooks.set_partitionwise_conjugate) << "partitionwise hook must provide a conjugate setter";
        auto [blocking_sink_op, blocking_source_op] =
                hooks.blocking(AM_BLOCKING_POST_CACHE, spill_channel_factory, upstream_source_op, context);
        query_cache::ConjugateOperatorFactoryPtr conjugate_op =
                std::make_shared<query_cache::ConjugateOperatorFactory>(blocking_sink_op, blocking_source_op);
        hooks.set_partitionwise_conjugate(agg_source_op, std::move(conjugate_op));
    }

    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(2, std::move(this->runtime_filter_collector()));
    auto bucket_process_context_factory = std::make_shared<BucketProcessContextFactory>();

    if (hooks.init_rf_before_bucket_wrap) {
        // Aggregate-style: init RF on the inner sink, then wrap.
        pipeline::init_runtime_filter_for_operator(*this, agg_sink_op.get(), context, rc_rf_probe_collector);
        if (per_bucket_optimize) {
            agg_sink_op = std::make_shared<BucketProcessSinkOperatorFactory>(
                    context->next_operator_id(), id(), bucket_process_context_factory, std::move(agg_sink_op));
        }
    } else {
        // Distinct-style: wrap with bucket process first, then init RF on the wrapper.
        if (per_bucket_optimize) {
            agg_sink_op = std::make_shared<BucketProcessSinkOperatorFactory>(
                    context->next_operator_id(), id(), bucket_process_context_factory, std::move(agg_sink_op));
        }
        pipeline::init_runtime_filter_for_operator(*this, agg_sink_op.get(), context, rc_rf_probe_collector);
    }

    // Source-side RF init follows the same order convention as the sink: init on the inner source
    // before bucket-wrap (Aggregate) or on the outer wrapper after bucket-wrap (Distinct).
    auto wrap_source_if_needed = [&] {
        if (per_bucket_optimize) {
            auto bucket_source_operator = std::make_shared<BucketProcessSourceOperatorFactory>(
                    context->next_operator_id(), id(), bucket_process_context_factory, std::move(agg_source_op));
            context->inherit_upstream_source_properties(bucket_source_operator.get(), upstream_source_op);
            agg_source_op = std::move(bucket_source_operator);
        }
    };

    if (hooks.init_rf_before_bucket_wrap) {
        pipeline::init_runtime_filter_for_operator(*this, agg_source_op.get(), context, rc_rf_probe_collector);
        wrap_source_if_needed();
    } else {
        wrap_source_if_needed();
        pipeline::init_runtime_filter_for_operator(*this, agg_source_op.get(), context, rc_rf_probe_collector);
    }

    ops_with_sink.push_back(std::move(agg_sink_op));

    OpFactories ops_with_source;
    ops_with_source.push_back(std::move(agg_source_op));

    if (should_cache) {
        ops_with_source =
                context->interpolate_cache_operator(id(), ops_with_sink, ops_with_source, operators_generator);
    }
    context->add_pipeline(ops_with_sink);

    return ops_with_source;
}

void AggregateBaseNode::push_down_join_runtime_filter(RuntimeState* state, RuntimeFilterProbeCollector* collector) {
    // accept runtime filters from parent if possible.
    _runtime_filter_collector.push_down(state, id(), collector, _tuple_ids, _local_rf_waiting_set);

    // check to see if runtime filters can be rewritten
    auto& descriptors = _runtime_filter_collector.descriptors();
    RuntimeFilterProbeCollector pushdown_collector;

    auto iter = descriptors.begin();
    while (iter != descriptors.end()) {
        RuntimeFilterProbeDescriptor* rf_desc = iter->second;
        if (!rf_desc->can_push_down_runtime_filter()) {
            ++iter;
            continue;
        }
        SlotId slot_id;
        // bound to this tuple and probe expr is slot ref.
        if (!rf_desc->is_bound(_tuple_ids) || !rf_desc->is_probe_slot_ref(&slot_id)) {
            ++iter;
            continue;
        }

        bool match = false;
        for (ExprContext* group_expr_ctx : _group_by_expr_ctxs) {
            if (group_expr_ctx->root()->is_slotref()) {
                auto* slot = down_cast<ColumnRef*>(group_expr_ctx->root());
                if (slot->slot_id() == slot_id) {
                    match = true;
                    break;
                }
            }
        }

        if (match) {
            pushdown_collector.add_descriptor(rf_desc);
            iter = descriptors.erase(iter);
        } else {
            ++iter;
        }
    }

    // push down rewritten runtime filters to children
    if (!pushdown_collector.empty()) {
        push_down_join_runtime_filter_to_children(state, &pushdown_collector);
        pushdown_collector.close(state);
    }
}

} // namespace starrocks
