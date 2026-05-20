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

#include <functional>
#include <tuple>

#include "common/statusor.h"
#include "exec/aggregator_fwd.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline_node.h"

namespace starrocks {

class RuntimeFilterBuildDescriptor;
class RuntimeFilterProbeCollector;

namespace pipeline {
class SpillProcessChannelFactory;
using SpillProcessChannelFactoryPtr = std::shared_ptr<SpillProcessChannelFactory>;
} // namespace pipeline

namespace query_cache {
class ConjugateOperatorFactory;
using ConjugateOperatorFactoryPtr = std::shared_ptr<ConjugateOperatorFactory>;
} // namespace query_cache

enum AggrMode : int;

// Shared assembly skeleton used by AggregateBlockingNode and DistinctBlockingNode to translate a
// blocking aggregate/distinct ExecNode into its sink/source operator pair, applying the standard
// boilerplate around spill interpolation, runtime-filter wiring, optional per-bucket processing
// and query-cache interpolation. Callers provide call-site-specific construction via hooks so the
// helper itself stays free of the unique sink/source factory types.
struct AggPipelineHooks {
    // (aggr_mode, spill_channel_factory, upstream_source_op, context) -> (sink, source)
    // Returns a freshly constructed aggregator factory + sink + source bound to the *primary* sink/source
    // factory types selected by the caller. Both `interpolate_cache_operator` and the partitionwise fallback
    // re-invoke this with a different aggr_mode.
    using OperatorsGenerator = std::function<std::tuple<pipeline::OpFactoryPtr, pipeline::SourceOperatorFactoryPtr>(
            AggrMode, const pipeline::SpillProcessChannelFactoryPtr&, pipeline::SourceOperatorFactory*,
            pipeline::PipelineBuilderContext*)>;

    OperatorsGenerator primary;
    // Same shape as `primary` but constructs the *blocking* sink/source pair used as the conjugate
    // fallback when the primary is a partitionwise-spill source. Left empty when !is_partitionwise.
    OperatorsGenerator blocking;
    // Installs the conjugate factory on the partitionwise source (e.g. set_pw_agg_factory /
    // set_pw_distinct_factory). Left empty when !is_partitionwise.
    std::function<void(pipeline::SourceOperatorFactoryPtr&, query_cache::ConjugateOperatorFactoryPtr)>
            set_partitionwise_conjugate;

    // True when the primary sink type triggers `context->interpolate_spill_process`.
    bool needs_spill_interpolation = false;
    // True when the primary source type is a partitionwise-spill source.
    bool is_partitionwise = false;
    // Aggregate inits the runtime filter on the inner sink before bucket-wrapping; Distinct inits it
    // on the outer bucket-wrapped sink. The two were copy-paste siblings that diverged here, so we
    // preserve the existing per-call-site behavior rather than silently picking one.
    bool init_rf_before_bucket_wrap = false;
};

class AggregateBaseNode : public PipelineNode {
public:
    AggregateBaseNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~AggregateBaseNode() override;
    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    void close(RuntimeState* state) override;
    void push_down_join_runtime_filter(RuntimeState* state, RuntimeFilterProbeCollector* collector) override;
    void push_down_tuple_slot_mappings(RuntimeState* state,
                                       const std::vector<TupleSlotMapping>& parent_mappings) override;

protected:
    StatusOr<pipeline::OpFactories> _decompose_with_hooks(pipeline::OpFactories& ops_with_sink,
                                                          pipeline::PipelineBuilderContext* context,
                                                          bool per_bucket_optimize, const AggPipelineHooks& hooks);

    const TPlanNode& _tnode;
    // _group_by_expr_ctxs used by the pipeline execution engine to push down rf to children nodes before
    // pipeline decomposition.
    std::vector<ExprContext*> _group_by_expr_ctxs;
    AggregatorPtr _aggregator = nullptr;
    bool _child_eos = false;
    std::vector<RuntimeFilterBuildDescriptor*> _build_runtime_filters;
};

} // namespace starrocks
