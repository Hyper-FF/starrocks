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

// Private header for AgentServer::Impl – not part of the public API.
// Split out so that agent_server.cpp can be compiled as multiple
// translation units to reduce peak compilation memory.

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "agent/agent_server.h"
#include "common/system/cpu_info.h"

namespace starrocks {

class ThreadPool;
class PushTaskWorkerPool;
class PublishVersionTaskWorkerPool;
class DeleteTaskWorkerPool;
class ReportTaskWorkerPool;
class ReportDiskStateTaskWorkerPool;
class ReportOlapTableTaskWorkerPool;
class ReportWorkgroupTaskWorkerPool;
class ReportResourceUsageTaskWorkerPool;
class ReportDataCacheMetricsTaskWorkerPool;

// ---- constants shared across agent_server*.cpp ----

constexpr size_t DEFAULT_DYNAMIC_THREAD_POOL_QUEUE_SIZE = 2048;
constexpr size_t MIN_CLONE_TASK_THREADS_IN_POOL = 2;
constexpr int32_t REPLICATION_CPU_CORES_MULTIPLIER = 4;

#ifndef BE_TEST
inline constexpr uint32_t REPORT_TASK_WORKER_COUNT = 1;
inline constexpr uint32_t REPORT_DISK_STATE_WORKER_COUNT = 1;
inline constexpr uint32_t REPORT_OLAP_TABLE_WORKER_COUNT = 1;
inline constexpr uint32_t REPORT_WORKGROUP_WORKER_COUNT = 1;
inline constexpr uint32_t REPORT_RESOURCE_USAGE_WORKER_COUNT = 1;
inline constexpr uint32_t REPORT_DATACACHE_METRICS_WORKER_COUNT = 1;
#endif

// ---- helper functions shared across agent_server*.cpp ----

/* Calculate real number of threads.
 * if num_threads > 0, return num_threads
 * if num_threads < 0, return -num_threads * cpu_cores
 * if num_threads == 0, return cpu_cores_multiplier * cpu_cores
 */
inline int32_t calc_real_num_threads(int32_t num_threads, int32_t cpu_cores_multiplier = 1) {
    if (num_threads == 0) {
        num_threads = -cpu_cores_multiplier;
    }
    if (num_threads < 0) {
        num_threads = -num_threads;
        num_threads *= CpuInfo::num_cores();
    }
    if (num_threads < 1) {
        num_threads = 1;
    }
    return num_threads;
}

inline int32_t calc_clone_thread_pool_size(size_t num_store_paths, int32_t parallel_clone_task_per_path) {
    return std::max(static_cast<int32_t>(num_store_paths) * parallel_clone_task_per_path,
                    static_cast<int32_t>(MIN_CLONE_TASK_THREADS_IN_POOL));
}

// ---- Impl class definition ----

class AgentServer::Impl {
public:
    explicit Impl(ExecEnv* exec_env, bool is_compute_node);

    ~Impl();

    Status init();

    void stop();

    void submit_tasks(TAgentResult& agent_result, const std::vector<TAgentTaskRequest>& tasks);

    void make_snapshot(TAgentResult& agent_result, const TSnapshotRequest& snapshot_request);

    void release_snapshot(TAgentResult& agent_result, const std::string& snapshot_path);

    void publish_cluster_state(TAgentResult& agent_result, const TAgentPublishRequest& request);

    void update_max_thread_by_type(int type, int new_val);

    ThreadPool* get_thread_pool(int type) const;

    void stop_task_worker_pool(TaskWorkerType type) const;

    DISALLOW_COPY_AND_MOVE(Impl);

private:
    ExecEnv* _exec_env;

    std::unique_ptr<ThreadPool> _thread_pool_publish_version;
    std::unique_ptr<ThreadPool> _thread_pool_clone;
    std::unique_ptr<ThreadPool> _thread_pool_drop;
    std::unique_ptr<ThreadPool> _thread_pool_create_tablet;
    std::unique_ptr<ThreadPool> _thread_pool_alter_tablet;
    std::unique_ptr<ThreadPool> _thread_pool_clear_transaction;
    std::unique_ptr<ThreadPool> _thread_pool_storage_medium_migrate;
    std::unique_ptr<ThreadPool> _thread_pool_check_consistency;
    std::unique_ptr<ThreadPool> _thread_pool_compaction;
    std::unique_ptr<ThreadPool> _thread_pool_compaction_control;
    std::unique_ptr<ThreadPool> _thread_pool_update_schema;

    std::unique_ptr<ThreadPool> _thread_pool_upload;
    std::unique_ptr<ThreadPool> _thread_pool_download;
    std::unique_ptr<ThreadPool> _thread_pool_make_snapshot;
    std::unique_ptr<ThreadPool> _thread_pool_release_snapshot;
    std::unique_ptr<ThreadPool> _thread_pool_move_dir;
    std::unique_ptr<ThreadPool> _thread_pool_update_tablet_meta_info;
    std::unique_ptr<ThreadPool> _thread_pool_drop_auto_increment_map;
    std::unique_ptr<ThreadPool> _thread_pool_remote_snapshot;
    std::unique_ptr<ThreadPool> _thread_pool_replicate_snapshot;

    std::unique_ptr<PushTaskWorkerPool> _push_workers;
    std::unique_ptr<PublishVersionTaskWorkerPool> _publish_version_workers;
    std::unique_ptr<DeleteTaskWorkerPool> _delete_workers;

    // These 3 worker-pool do not accept tasks from FE.
    // It is self triggered periodically and reports to Fe master
    std::unique_ptr<ReportTaskWorkerPool> _report_task_workers;
    std::unique_ptr<ReportDiskStateTaskWorkerPool> _report_disk_state_workers;
    std::unique_ptr<ReportOlapTableTaskWorkerPool> _report_tablet_workers;
    std::unique_ptr<ReportWorkgroupTaskWorkerPool> _report_workgroup_workers;
    std::unique_ptr<ReportResourceUsageTaskWorkerPool> _report_resource_usage_workers;
    std::unique_ptr<ReportDataCacheMetricsTaskWorkerPool> _report_datacache_metrics_workers;

    // Compute node only need _report_resource_usage_workers and _report_task_workers
    const bool _is_compute_node;
};

} // namespace starrocks
