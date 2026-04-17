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

#include "util/metrics/spill_metrics.h"

#include <array>
#include <unordered_map>

namespace starrocks {

namespace {

// Keep aligned with enum SpillOperatorType; index = enum value.
constexpr std::array<const char*, SpillMetrics::kOperatorCount> kOperatorTypeLabels = {
        "unknown",                // kUnknown
        "hash-join-build",        // kHashJoinBuild
        "hash-join-probe",        // kHashJoinProbe
        "nestloop-join-build",    // kNestloopJoinBuild
        "agg-blocking",           // kAggBlocking
        "agg-distinct-blocking",  // kAggDistinctBlocking
        "distinct-blocking",      // kDistinctBlocking
        "local-sort",             // kLocalSort
        "mcast-local-exchange",   // kMcastLocalExchange
};

constexpr const char* kStorageTypeLabels[SpillMetrics::kStorageCount] = {
        "local",  // is_remote == false
        "remote", // is_remote == true
};

} // namespace

SpillOperatorType SpillMetrics::parse_operator_type(std::string_view name) {
    // Keyed by the string literals assigned to SpilledOptions::name in each
    // spillable operator. Update this table (and kOperatorTypeLabels above)
    // when a new spillable operator is introduced.
    static const std::unordered_map<std::string_view, SpillOperatorType> kMap = {
            {"hash-join-build", SpillOperatorType::kHashJoinBuild},
            {"hash-join-probe", SpillOperatorType::kHashJoinProbe},
            {"spillable-nestloop-join-build", SpillOperatorType::kNestloopJoinBuild},
            {"agg-blocking-spill", SpillOperatorType::kAggBlocking},
            {"agg-distinct-blocking-spill", SpillOperatorType::kAggDistinctBlocking},
            {"distinct-blocking-spill", SpillOperatorType::kDistinctBlocking},
            {"local-sort-spill", SpillOperatorType::kLocalSort},
            {"mcast_local_exchange", SpillOperatorType::kMcastLocalExchange},
    };
    auto it = kMap.find(name);
    return it != kMap.end() ? it->second : SpillOperatorType::kUnknown;
}

SpillMetrics::SpillMetrics(MetricRegistry* registry) {
    _local_disk_bytes_used = std::make_unique<IntGauge>(MetricUnit::BYTES);
    registry->register_metric("spill_disk_bytes_used", MetricLabels().add("storage_type", kStorageTypeLabels[0]),
                              _local_disk_bytes_used.get());

    _remote_disk_bytes_used = std::make_unique<IntGauge>(MetricUnit::BYTES);
    registry->register_metric("spill_disk_bytes_used", MetricLabels().add("storage_type", kStorageTypeLabels[1]),
                              _remote_disk_bytes_used.get());

    for (size_t op_idx = 0; op_idx < kOperatorCount; ++op_idx) {
        for (size_t storage_idx = 0; storage_idx < kStorageCount; ++storage_idx) {
            auto& bucket = _buckets[op_idx][storage_idx];
            MetricLabels labels;
            labels.add("operator_type", kOperatorTypeLabels[op_idx]);
            labels.add("storage_type", kStorageTypeLabels[storage_idx]);

            bucket.trigger_total = std::make_unique<IntCounter>(MetricUnit::OPERATIONS);
            registry->register_metric("query_spill_trigger_total", labels, bucket.trigger_total.get());

            bucket.bytes_write_total = std::make_unique<IntCounter>(MetricUnit::BYTES);
            registry->register_metric("query_spill_bytes_write_total", labels, bucket.bytes_write_total.get());

            bucket.bytes_read_total = std::make_unique<IntCounter>(MetricUnit::BYTES);
            registry->register_metric("query_spill_bytes_read_total", labels, bucket.bytes_read_total.get());

            bucket.blocks_write_total = std::make_unique<IntCounter>(MetricUnit::OPERATIONS);
            registry->register_metric("query_spill_blocks_write_total", labels, bucket.blocks_write_total.get());

            bucket.blocks_read_total = std::make_unique<IntCounter>(MetricUnit::OPERATIONS);
            registry->register_metric("query_spill_blocks_read_total", labels, bucket.blocks_read_total.get());

            bucket.write_io_duration_ns_total = std::make_unique<IntCounter>(MetricUnit::NANOSECONDS);
            registry->register_metric("query_spill_write_io_duration_ns_total", labels,
                                      bucket.write_io_duration_ns_total.get());

            bucket.read_io_duration_ns_total = std::make_unique<IntCounter>(MetricUnit::NANOSECONDS);
            registry->register_metric("query_spill_read_io_duration_ns_total", labels,
                                      bucket.read_io_duration_ns_total.get());
        }
    }
}

} // namespace starrocks
