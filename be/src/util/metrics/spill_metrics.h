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
#include <string>
#include <string_view>

#include "base/metrics.h"

namespace starrocks {

// Closed set of spillable operator types. The enum exists so hot paths
// can index into the metric bucket array in O(1), avoiding any string
// comparison or label map lookup.
enum class SpillOperatorType : uint8_t {
    kUnknown = 0,
    kHashJoinBuild,
    kHashJoinProbe,
    kNestloopJoinBuild,
    kAggBlocking,
    kAggDistinctBlocking,
    kDistinctBlocking,
    kLocalSort,
    kMcastLocalExchange,
    // keep last
    kCount,
};

class SpillMetrics {
public:
    static constexpr size_t kOperatorCount = static_cast<size_t>(SpillOperatorType::kCount);
    static constexpr size_t kStorageCount = 2; // local, remote

    struct LabeledCounters {
        std::unique_ptr<IntCounter> trigger_total;
        std::unique_ptr<IntCounter> bytes_write_total;
        std::unique_ptr<IntCounter> bytes_read_total;
        std::unique_ptr<IntCounter> blocks_write_total;
        std::unique_ptr<IntCounter> blocks_read_total;
        std::unique_ptr<IntCounter> write_io_duration_ns_total;
        std::unique_ptr<IntCounter> read_io_duration_ns_total;
    };

    SpillMetrics(MetricRegistry* registry);
    ~SpillMetrics() = default;

    // O(1) array indexing, no allocation, no locking. Safe on hot paths.
    LabeledCounters* get(SpillOperatorType op, bool is_remote) {
        return &_buckets[static_cast<size_t>(op)][is_remote ? 1 : 0];
    }

    IntGauge* local_disk_bytes_used() { return _local_disk_bytes_used.get(); }
    IntGauge* remote_disk_bytes_used() { return _remote_disk_bytes_used.get(); }

    // Map SpilledOptions::name to a SpillOperatorType. Returns kUnknown
    // for unrecognized names. Callers should invoke this once at Spiller
    // setup time and cache the result.
    static SpillOperatorType parse_operator_type(std::string_view name);

private:
    LabeledCounters _buckets[kOperatorCount][kStorageCount];
    std::unique_ptr<IntGauge> _local_disk_bytes_used;
    std::unique_ptr<IntGauge> _remote_disk_bytes_used;
};

} // namespace starrocks
