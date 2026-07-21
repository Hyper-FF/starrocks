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

#include <memory>
#include <vector>

#include "exec/hash_joiner.h"

namespace starrocks::pipeline {

using HashJoiner = starrocks::HashJoiner;
using HashJoinerPtr = std::shared_ptr<HashJoiner>;
using HashJoinerMap = std::unordered_map<int32_t, HashJoinerPtr>;
class HashJoinerFactory;
using HashJoinerFactoryPtr = std::shared_ptr<HashJoinerFactory>;

class HashJoinerFactory {
public:
    HashJoinerFactory(starrocks::HashJoinerParam& param) : _param(param) {}

    Status prepare(RuntimeState* state);
    void close(RuntimeState* state);

    /// We must guarantee that:
    /// 1. All the builders must be created earlier than prober.
    /// 2. prober_dop is a multiple of builder_dop.
    HashJoinerPtr create_builder(int32_t builder_dop, int32_t builder_driver_seq);
    HashJoinerPtr create_prober(int32_t prober_dop, int32_t prober_driver_seq);
    HashJoinerPtr get_builder(int32_t prober_dop, int32_t prober_driver_seq);

    // ===== work-stealing: partition-aware probe (see PIPELINE_WORK_STEALING_PLAN.md) =====
    // A stolen probe chunk carries the victim's partition id; to look it up correctly a thief
    // must probe against that partition's build hash table, which is only safe once EVERY
    // partition's build has completed (post-build the table is read-only). This is the
    // all-builds-ready barrier a steal-enabled probe pipeline waits on.
    bool all_builds_ready() const {
        // Every partition's build must have both REGISTERED and completed. Iterating only the
        // already-registered entries is not sufficient: builders register incrementally as their
        // drivers prepare (create_builder), so a partially-populated map can spuriously report
        // "all ready" while some partition's builder is still missing. A thief that then steals a
        // chunk for a not-yet-registered partition would hit builder_for_partition()==nullptr and
        // its chunk would be dropped -> silently lost rows. Require the full set first.
        if (_builder_dop <= 0 || _builder_map.size() != static_cast<size_t>(_builder_dop)) {
            return false;
        }
        for (const auto& kv : _builder_map) {
            const HashJoinerPtr& builder = kv.second;
            if (builder == nullptr || !builder->is_build_done()) {
                return false;
            }
        }
        return true;
    }

    // The builder joiner that owns partition `partition_id`'s (read-only) hash table.
    HashJoinerPtr builder_for_partition(int32_t partition_id) const {
        if (_builder_dop <= 0) {
            return nullptr;
        }
        auto it = _builder_map.find(partition_id % _builder_dop);
        return it != _builder_map.end() ? it->second : nullptr;
    }

    const starrocks::HashJoinerParam& hash_join_param() { return _param; }

private:
    HashJoinerPtr _create_joiner(HashJoinerMap& joiner_map, int32_t driver_sequence);

    HashJoinerParam _param;
    HashJoinerMap _builder_map;
    HashJoinerMap _prober_map;

    int32_t _builder_dop = 0;
    int32_t _prober_dop = 0;
};

} // namespace starrocks::pipeline
