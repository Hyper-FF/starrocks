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

#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "base/metrics.h"

namespace starrocks {

// Per-query spill metrics labeled by operator_type and storage_type.
// Labels are registered lazily on first access to keep the label set
// restricted to operator/storage pairs that actually spill.
class SpillMetrics {
public:
    static constexpr const char* kStorageTypeLocal = "local";
    static constexpr const char* kStorageTypeRemote = "remote";

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

    // Fetch (and lazily register) the counter bucket for a given
    // (operator_type, storage_type) pair. Prefer this when multiple
    // counters from the same bucket are updated together - it avoids
    // re-taking the registration mutex for each accessor.
    LabeledCounters* get(const std::string& operator_type, const std::string& storage_type);

    // Convenience single-counter accessors.
    IntCounter* trigger_total(const std::string& operator_type, const std::string& storage_type);
    IntCounter* bytes_write_total(const std::string& operator_type, const std::string& storage_type);
    IntCounter* bytes_read_total(const std::string& operator_type, const std::string& storage_type);
    IntCounter* blocks_write_total(const std::string& operator_type, const std::string& storage_type);
    IntCounter* blocks_read_total(const std::string& operator_type, const std::string& storage_type);
    IntCounter* write_io_duration_ns_total(const std::string& operator_type, const std::string& storage_type);
    IntCounter* read_io_duration_ns_total(const std::string& operator_type, const std::string& storage_type);

    // spill_disk_bytes_used gauge, labeled by storage_type only. The
    // GlobalMetricsRegistry update hook refreshes these from the spill
    // DirManagers.
    IntGauge* disk_bytes_used(const std::string& storage_type);

private:
    LabeledCounters* _get_or_register(const std::string& operator_type, const std::string& storage_type);

    MetricRegistry* _registry;

    std::mutex _mutex;
    // key = operator_type + "\0" + storage_type
    std::map<std::string, std::unique_ptr<LabeledCounters>> _counters;

    std::unique_ptr<IntGauge> _local_disk_bytes_used;
    std::unique_ptr<IntGauge> _remote_disk_bytes_used;
};

} // namespace starrocks
