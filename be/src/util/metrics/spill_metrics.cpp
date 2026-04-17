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

namespace starrocks {

SpillMetrics::SpillMetrics(MetricRegistry* registry) : _registry(registry) {
    _local_disk_bytes_used = std::make_unique<IntGauge>(MetricUnit::BYTES);
    _registry->register_metric("spill_disk_bytes_used", MetricLabels().add("storage_type", kStorageTypeLocal),
                               _local_disk_bytes_used.get());

    _remote_disk_bytes_used = std::make_unique<IntGauge>(MetricUnit::BYTES);
    _registry->register_metric("spill_disk_bytes_used", MetricLabels().add("storage_type", kStorageTypeRemote),
                               _remote_disk_bytes_used.get());
}

SpillMetrics::LabeledCounters* SpillMetrics::_get_or_register(const std::string& operator_type,
                                                              const std::string& storage_type) {
    std::string key;
    key.reserve(operator_type.size() + 1 + storage_type.size());
    key.append(operator_type).push_back('\0');
    key.append(storage_type);

    std::lock_guard<std::mutex> l(_mutex);
    auto it = _counters.find(key);
    if (it != _counters.end()) {
        return it->second.get();
    }

    auto bucket = std::make_unique<LabeledCounters>();
    MetricLabels labels;
    labels.add("operator_type", operator_type);
    labels.add("storage_type", storage_type);

    bucket->trigger_total = std::make_unique<IntCounter>(MetricUnit::OPERATIONS);
    _registry->register_metric("query_spill_trigger_total", labels, bucket->trigger_total.get());

    bucket->bytes_write_total = std::make_unique<IntCounter>(MetricUnit::BYTES);
    _registry->register_metric("query_spill_bytes_write_total", labels, bucket->bytes_write_total.get());

    bucket->bytes_read_total = std::make_unique<IntCounter>(MetricUnit::BYTES);
    _registry->register_metric("query_spill_bytes_read_total", labels, bucket->bytes_read_total.get());

    bucket->blocks_write_total = std::make_unique<IntCounter>(MetricUnit::OPERATIONS);
    _registry->register_metric("query_spill_blocks_write_total", labels, bucket->blocks_write_total.get());

    bucket->blocks_read_total = std::make_unique<IntCounter>(MetricUnit::OPERATIONS);
    _registry->register_metric("query_spill_blocks_read_total", labels, bucket->blocks_read_total.get());

    bucket->write_io_duration_ns_total = std::make_unique<IntCounter>(MetricUnit::NANOSECONDS);
    _registry->register_metric("query_spill_write_io_duration_ns_total", labels,
                               bucket->write_io_duration_ns_total.get());

    bucket->read_io_duration_ns_total = std::make_unique<IntCounter>(MetricUnit::NANOSECONDS);
    _registry->register_metric("query_spill_read_io_duration_ns_total", labels,
                               bucket->read_io_duration_ns_total.get());

    auto* raw = bucket.get();
    _counters.emplace(std::move(key), std::move(bucket));
    return raw;
}

SpillMetrics::LabeledCounters* SpillMetrics::get(const std::string& operator_type,
                                                 const std::string& storage_type) {
    return _get_or_register(operator_type, storage_type);
}

IntCounter* SpillMetrics::trigger_total(const std::string& operator_type, const std::string& storage_type) {
    return _get_or_register(operator_type, storage_type)->trigger_total.get();
}

IntCounter* SpillMetrics::bytes_write_total(const std::string& operator_type, const std::string& storage_type) {
    return _get_or_register(operator_type, storage_type)->bytes_write_total.get();
}

IntCounter* SpillMetrics::bytes_read_total(const std::string& operator_type, const std::string& storage_type) {
    return _get_or_register(operator_type, storage_type)->bytes_read_total.get();
}

IntCounter* SpillMetrics::blocks_write_total(const std::string& operator_type, const std::string& storage_type) {
    return _get_or_register(operator_type, storage_type)->blocks_write_total.get();
}

IntCounter* SpillMetrics::blocks_read_total(const std::string& operator_type, const std::string& storage_type) {
    return _get_or_register(operator_type, storage_type)->blocks_read_total.get();
}

IntCounter* SpillMetrics::write_io_duration_ns_total(const std::string& operator_type,
                                                     const std::string& storage_type) {
    return _get_or_register(operator_type, storage_type)->write_io_duration_ns_total.get();
}

IntCounter* SpillMetrics::read_io_duration_ns_total(const std::string& operator_type,
                                                    const std::string& storage_type) {
    return _get_or_register(operator_type, storage_type)->read_io_duration_ns_total.get();
}

IntGauge* SpillMetrics::disk_bytes_used(const std::string& storage_type) {
    if (storage_type == kStorageTypeRemote) {
        return _remote_disk_bytes_used.get();
    }
    return _local_disk_bytes_used.get();
}

} // namespace starrocks
