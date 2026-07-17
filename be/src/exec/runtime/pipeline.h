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

#include <ctime>
#include <utility>

#include "exec_primitive/pipeline/operator_factory.h"
#include "exec_primitive/pipeline/pipeline_fwd.h"
#include "exec_primitive/pipeline/primitives/driver_observer.h"
#include "exec_primitive/pipeline/primitives/pipeline_group.h"
#include "exec_primitive/pipeline/source_operator.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

class RuntimeState;

namespace pipeline {

class Pipeline : public DriverObserver {
public:
    Pipeline() = delete;
    Pipeline(uint32_t id, OpFactories op_factories, PipelineGroupRawPtr group);

    uint32_t get_id() const { return _id; }

    Operators create_operators(int32_t degree_of_parallelism, int32_t i) {
        Operators operators;
        operators.reserve(_op_factories.size());
        for (const auto& factory : _op_factories) {
            operators.emplace_back(factory->create(degree_of_parallelism, i));
        }
        return operators;
    }
    const Drivers& drivers() const;
    Drivers& mutable_drivers();
    void on_driver_finished(RuntimeState* state) override;
    void clear_drivers();

    SourceOperatorFactory* source_operator_factory() {
        DCHECK(!_op_factories.empty());
        return down_cast<SourceOperatorFactory*>(_op_factories[0].get());
    }
    const SourceOperatorFactory* source_operator_factory() const {
        DCHECK(!_op_factories.empty());
        return down_cast<SourceOperatorFactory*>(_op_factories[0].get());
    }
    OperatorFactory* sink_operator_factory() {
        DCHECK(!_op_factories.empty());
        return _op_factories[_op_factories.size() - 1].get();
    }
    size_t degree_of_parallelism() const;

    RuntimeProfile* runtime_profile() { return _runtime_profile.get(); }
    void setup_pipeline_profile(RuntimeState* runtime_state);
    void setup_drivers_profile(const DriverPtr& driver);

    Status prepare(RuntimeState* state) {
        for (auto& op : _op_factories) {
            RETURN_IF_ERROR(op->prepare(state));
        }
        return Status::OK();
    }
    void close(RuntimeState* state) {
        for (auto& op : _op_factories) {
            op->close(state);
        }
    }

    void acquire_runtime_filter(RuntimeState* state) {
        for (auto& op : _op_factories) {
            op->acquire_runtime_filter(state);
        }
    }

    std::string to_readable_string() const {
        std::stringstream ss;
        ss << "operator-chain: [";
        for (size_t i = 0; i < _op_factories.size(); ++i) {
            if (i == 0) {
                ss << _op_factories[i]->get_name();
            } else {
                ss << " -> " << _op_factories[i]->get_name();
            }
        }
        ss << "]";
        return ss.str();
    }

    size_t output_amplification_factor() const;
    Event* pipeline_event() const { return _pipeline_event.get(); }

    // ===== work-stealing (see PIPELINE_WORK_STEALING_PLAN.md) =====
    // Compute the length of the pipeline's "stealable prefix": operator indexes
    // [0, steal_barrier_idx) may process work units stolen from a sibling driver.
    // The barrier is the index of the first non-stealable operator; 0 means the
    // whole pipeline is unstealable. Depends only on the op-factory chain, so it
    // is computed once at construction.
    void compute_steal_barrier();
    size_t steal_barrier_idx() const { return _steal_barrier_idx; }
    // True when every operator in the pipeline is stealable, i.e. a foreign (or here
    // partition-free) work unit can flow all the way to the sink correctly. This is the
    // gate for the partition-free (partition_id == -1) steal path; partition-aware
    // consumers (a later phase) will relax it to a partial barrier.
    bool fully_stealable() const { return _steal_barrier_idx > 0 && _steal_barrier_idx == _op_factories.size(); }

private:
    uint32_t _id = 0;
    std::shared_ptr<RuntimeProfile> _runtime_profile = nullptr;
    OpFactories _op_factories;
    size_t _steal_barrier_idx = 0;
    Drivers _drivers;
    std::atomic<size_t> _num_finished_drivers = 0;

    EventPtr _pipeline_event;
    PipelineGroupRawPtr _group = nullptr;
};

} // namespace pipeline
} // namespace starrocks
