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

#include <unordered_map>

namespace starrocks {

const char* SpillMetrics::operator_type_label(SpillOperatorType op) {
    switch (op) {
    case SpillOperatorType::kHashJoinBuild:
        return "hash-join-build";
    case SpillOperatorType::kHashJoinProbe:
        return "hash-join-probe";
    case SpillOperatorType::kNestloopJoinBuild:
        return "nestloop-join-build";
    case SpillOperatorType::kAggBlocking:
        return "agg-blocking";
    case SpillOperatorType::kAggDistinctBlocking:
        return "agg-distinct-blocking";
    case SpillOperatorType::kDistinctBlocking:
        return "distinct-blocking";
    case SpillOperatorType::kLocalSort:
        return "local-sort";
    case SpillOperatorType::kMcastLocalExchange:
        return "mcast-local-exchange";
    case SpillOperatorType::kUnknown:
    case SpillOperatorType::kCount:
        break;
    }
    return "unknown";
}

SpillOperatorType SpillMetrics::parse_operator_type(std::string_view name) {
    // Keyed by the string literals assigned to SpilledOptions::name in each
    // spillable operator. Kept alongside operator_type_label so it is easy
    // to spot when a new operator is added.
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
    registry->register_metric("spill_disk_bytes_used", MetricLabels().add("storage_type", storage_type_label(false)),
                              _local_disk_bytes_used.get());

    _remote_disk_bytes_used = std::make_unique<IntGauge>(MetricUnit::BYTES);
    registry->register_metric("spill_disk_bytes_used", MetricLabels().add("storage_type", storage_type_label(true)),
                              _remote_disk_bytes_used.get());

    for (size_t op_idx = 0; op_idx < kOperatorCount; ++op_idx) {
        auto op = static_cast<SpillOperatorType>(op_idx);
        for (size_t storage_idx = 0; storage_idx < kStorageCount; ++storage_idx) {
            bool is_remote = storage_idx == 1;
            auto& bucket = _buckets[op_idx][storage_idx];
            MetricLabels labels;
            labels.add("operator_type", operator_type_label(op));
            labels.add("storage_type", storage_type_label(is_remote));

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
