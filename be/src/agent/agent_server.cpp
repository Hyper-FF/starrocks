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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/agent/agent_server.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "agent/agent_server.h"

#include <filesystem>
#include <string>
#include <vector>

#include "agent/agent_server_impl.h"
#include "agent/task_worker_pool.h"
#include "common/config_agent_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_storage_fwd.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/thread/threadpool.h"
#include "runtime/exec_env.h"
#include "runtime/starrocks_metrics.h"
#include "storage/olap_define.h"
#include "util/global_metrics_registry.h"

namespace starrocks {

AgentServer::Impl::Impl(ExecEnv* exec_env, bool is_compute_node)
        : _exec_env(exec_env), _is_compute_node(is_compute_node) {}

AgentServer::Impl::~Impl() = default;

Status AgentServer::Impl::init() {
    if (!_is_compute_node) {
        for (auto& path : _exec_env->store_paths()) {
            try {
                std::string dpp_download_path_str = path.path + DPP_PREFIX;
                std::filesystem::path dpp_download_path(dpp_download_path_str);
                if (std::filesystem::exists(dpp_download_path)) {
                    std::filesystem::remove_all(dpp_download_path);
                }
            } catch (...) {
                LOG(WARNING) << "std exception when remove dpp download path. path=" << path.path;
            }
        }

#define BUILD_DYNAMIC_TASK_THREAD_POOL(name, min_threads, max_threads, queue_size, pool) \
    BUILD_DYNAMIC_TASK_THREAD_POOL_WITH_IDLE(name, min_threads, max_threads, queue_size, \
                                             ThreadPoolDefaultIdleTimeoutMS, pool)

#define BUILD_DYNAMIC_TASK_THREAD_POOL_WITH_IDLE(name, min_threads, max_threads, queue_size, idle_timeout, pool) \
    do {                                                                                                         \
        RETURN_IF_ERROR(ThreadPoolBuilder(#name)                                                                 \
                                .set_min_threads(min_threads)                                                    \
                                .set_max_threads(max_threads)                                                    \
                                .set_max_queue_size(queue_size)                                                  \
                                .set_idle_timeout(MonoDelta::FromMilliseconds(idle_timeout))                     \
                                .build(&pool));                                                                  \
        REGISTER_THREAD_POOL_METRICS(name, pool);                                                                \
    } while (false)

// The ideal queue size of threadpool should be larger than the maximum number of tablet of a partition.
// But it seems that there's no limit for the number of tablets of a partition.
// Since a large queue size brings a little overhead, a big one is chosen here.
#ifdef BE_TEST
        BUILD_DYNAMIC_TASK_THREAD_POOL(publish_version, 1, 3, DEFAULT_DYNAMIC_THREAD_POOL_QUEUE_SIZE,
                                       _thread_pool_publish_version);
#else
        int max_publish_version_worker_count = calc_real_num_threads(config::transaction_publish_version_worker_count);
        max_publish_version_worker_count =
                std::max(max_publish_version_worker_count, MIN_TRANSACTION_PUBLISH_WORKER_COUNT);
        int min_publish_version_worker_count =
                std::max(config::transaction_publish_version_thread_pool_num_min, MIN_TRANSACTION_PUBLISH_WORKER_COUNT);
        BUILD_DYNAMIC_TASK_THREAD_POOL_WITH_IDLE(publish_version, min_publish_version_worker_count,
                                                 max_publish_version_worker_count, std::numeric_limits<int>::max(),
                                                 config::transaction_publish_version_thread_pool_idle_time_ms,
                                                 _thread_pool_publish_version);
#endif
        int real_drop_tablet_worker_count = (config::drop_tablet_worker_count > 0)
                                                    ? config::drop_tablet_worker_count
                                                    : std::max((int)(CpuInfo::num_cores() / 2), (int)1);
        BUILD_DYNAMIC_TASK_THREAD_POOL(drop, 1, real_drop_tablet_worker_count, std::numeric_limits<int>::max(),
                                       _thread_pool_drop);

        BUILD_DYNAMIC_TASK_THREAD_POOL(create_tablet, 1, config::create_tablet_worker_count,
                                       std::numeric_limits<int>::max(), _thread_pool_create_tablet);

        BUILD_DYNAMIC_TASK_THREAD_POOL(alter_tablet, 0, config::alter_tablet_worker_count,
                                       std::numeric_limits<int>::max(), _thread_pool_alter_tablet);

        BUILD_DYNAMIC_TASK_THREAD_POOL(clear_transaction, 0, config::clear_transaction_task_worker_count,
                                       std::numeric_limits<int>::max(), _thread_pool_clear_transaction);

        BUILD_DYNAMIC_TASK_THREAD_POOL(storage_medium_migrate, 0, config::storage_medium_migrate_count,
                                       std::numeric_limits<int>::max(), _thread_pool_storage_medium_migrate);

        BUILD_DYNAMIC_TASK_THREAD_POOL(check_consistency, 0, config::check_consistency_worker_count,
                                       std::numeric_limits<int>::max(), _thread_pool_check_consistency);

        BUILD_DYNAMIC_TASK_THREAD_POOL(manual_compaction, 0, 1, std::numeric_limits<int>::max(),
                                       _thread_pool_compaction);

        BUILD_DYNAMIC_TASK_THREAD_POOL(compaction_control, 0, 1, std::numeric_limits<int>::max(),
                                       _thread_pool_compaction_control);

        BUILD_DYNAMIC_TASK_THREAD_POOL(update_schema, 0, config::update_schema_worker_count,
                                       std::numeric_limits<int>::max(), _thread_pool_update_schema);

        BUILD_DYNAMIC_TASK_THREAD_POOL(upload, 0, calc_real_num_threads(config::upload_worker_count),
                                       std::numeric_limits<int>::max(), _thread_pool_upload);

        BUILD_DYNAMIC_TASK_THREAD_POOL(download, 0, calc_real_num_threads(config::download_worker_count),
                                       std::numeric_limits<int>::max(), _thread_pool_download);

        BUILD_DYNAMIC_TASK_THREAD_POOL(make_snapshot, 0, config::make_snapshot_worker_count,
                                       std::numeric_limits<int>::max(), _thread_pool_make_snapshot);

        BUILD_DYNAMIC_TASK_THREAD_POOL(release_snapshot, 0, config::release_snapshot_worker_count,
                                       std::numeric_limits<int>::max(), _thread_pool_release_snapshot);

        BUILD_DYNAMIC_TASK_THREAD_POOL(move_dir, 0, calc_real_num_threads(config::download_worker_count),
                                       std::numeric_limits<int>::max(), _thread_pool_move_dir);

        BUILD_DYNAMIC_TASK_THREAD_POOL(update_tablet_meta_info, 0,
                                       std::max(1, config::update_tablet_meta_info_worker_count),
                                       std::numeric_limits<int>::max(), _thread_pool_update_tablet_meta_info);

        BUILD_DYNAMIC_TASK_THREAD_POOL(drop_auto_increment_map_dir, 0, 1, std::numeric_limits<int>::max(),
                                       _thread_pool_drop_auto_increment_map);

        // Currently FE can have at most num_of_storage_path * schedule_slot_num_per_path(default 2) clone tasks
        // scheduled simultaneously, but previously we have only 3 clone worker threads by default,
        // so this is to keep the dop of clone task handling in sync with FE.
        //
        // TODO(shangyiming): using dynamic thread pool to handle task directly instead of using TaskThreadPool
        // Currently, the task submission and processing logic is deeply coupled with TaskThreadPool, change that will
        // need to modify many interfaces. So for now we still use TaskThreadPool to submit clone tasks, but with
        // only a single worker thread, then we use dynamic thread pool to handle the task concurrently in clone task
        // callback, so that we can match the dop of FE clone task scheduling.
        BUILD_DYNAMIC_TASK_THREAD_POOL(
                clone, 0,
                calc_clone_thread_pool_size(_exec_env->store_paths().size(), config::parallel_clone_task_per_path),
                DEFAULT_DYNAMIC_THREAD_POOL_QUEUE_SIZE, _thread_pool_clone);

        BUILD_DYNAMIC_TASK_THREAD_POOL(
                remote_snapshot, 0,
                calc_real_num_threads(config::replication_threads, REPLICATION_CPU_CORES_MULTIPLIER),
                std::numeric_limits<int>::max(), _thread_pool_remote_snapshot);

        BUILD_DYNAMIC_TASK_THREAD_POOL(
                replicate_snapshot, 0,
                calc_real_num_threads(config::replication_threads, REPLICATION_CPU_CORES_MULTIPLIER),
                std::numeric_limits<int>::max(), _thread_pool_replicate_snapshot);

        // It is the same code to create workers of each type, so we use a macro
        // to make code to be more readable.
#ifndef BE_TEST
#define CREATE_AND_START_POOL(pool_name, CLASS_NAME, worker_num) \
    pool_name.reset(new CLASS_NAME(_exec_env, worker_num));      \
    pool_name->start();
#else
#define CREATE_AND_START_POOL(pool_name, CLASS_NAME, worker_num)
#endif // BE_TEST

        CREATE_AND_START_POOL(_publish_version_workers, PublishVersionTaskWorkerPool, CpuInfo::num_cores())
        // Both PUSH and REALTIME_PUSH type use _push_workers
        CREATE_AND_START_POOL(_push_workers, PushTaskWorkerPool,
                              config::push_worker_count_high_priority + config::push_worker_count_normal_priority)
        CREATE_AND_START_POOL(_delete_workers, DeleteTaskWorkerPool,
                              config::delete_worker_count_normal_priority + config::delete_worker_count_high_priority)
        CREATE_AND_START_POOL(_report_disk_state_workers, ReportDiskStateTaskWorkerPool, REPORT_DISK_STATE_WORKER_COUNT)
        CREATE_AND_START_POOL(_report_tablet_workers, ReportOlapTableTaskWorkerPool, REPORT_OLAP_TABLE_WORKER_COUNT)
        CREATE_AND_START_POOL(_report_workgroup_workers, ReportWorkgroupTaskWorkerPool, REPORT_WORKGROUP_WORKER_COUNT)
    }
    CREATE_AND_START_POOL(_report_resource_usage_workers, ReportResourceUsageTaskWorkerPool,
                          REPORT_RESOURCE_USAGE_WORKER_COUNT)
    CREATE_AND_START_POOL(_report_datacache_metrics_workers, ReportDataCacheMetricsTaskWorkerPool,
                          REPORT_DATACACHE_METRICS_WORKER_COUNT)
    CREATE_AND_START_POOL(_report_task_workers, ReportTaskWorkerPool, REPORT_TASK_WORKER_COUNT)
#undef CREATE_AND_START_POOL

    return Status::OK();
}

void AgentServer::Impl::stop() {
    if (!_is_compute_node) {
        _thread_pool_publish_version->shutdown();
        _thread_pool_drop->shutdown();
        _thread_pool_create_tablet->shutdown();
        _thread_pool_alter_tablet->shutdown();
        _thread_pool_clear_transaction->shutdown();
        _thread_pool_storage_medium_migrate->shutdown();
        _thread_pool_check_consistency->shutdown();
        _thread_pool_compaction->shutdown();
        _thread_pool_compaction_control->shutdown();
        _thread_pool_update_schema->shutdown();
        _thread_pool_upload->shutdown();
        _thread_pool_download->shutdown();
        _thread_pool_make_snapshot->shutdown();
        _thread_pool_release_snapshot->shutdown();
        _thread_pool_move_dir->shutdown();
        _thread_pool_update_tablet_meta_info->shutdown();
        _thread_pool_drop_auto_increment_map->shutdown();

#ifndef BE_TEST
        _thread_pool_clone->shutdown();
        _thread_pool_remote_snapshot->shutdown();
        _thread_pool_replicate_snapshot->shutdown();
#define STOP_POOL(type, pool_name) pool_name->stop();
#else
#define STOP_POOL(type, pool_name)
#endif // BE_TEST
        STOP_POOL(PUBLISH_VERSION, _publish_version_workers);
        // Both PUSH and REALTIME_PUSH type use _push_workers
        STOP_POOL(PUSH, _push_workers);
        STOP_POOL(DELETE, _delete_workers);
        STOP_POOL(REPORT_DISK_STATE, _report_disk_state_workers);
        STOP_POOL(REPORT_OLAP_TABLE, _report_tablet_workers);
        STOP_POOL(REPORT_WORKGROUP, _report_workgroup_workers);
    }
    STOP_POOL(REPORT_WORKGROUP, _report_resource_usage_workers);
    STOP_POOL(REPORT_DATACACHE_METRICS, _report_datacache_metrics_workers);
    STOP_POOL(REPORT_TASK, _report_task_workers);
#undef STOP_POOL
}

// ---- AgentServer thin delegating methods ----

AgentServer::AgentServer(ExecEnv* exec_env, bool is_compute_node)
        : _impl(std::make_unique<AgentServer::Impl>(exec_env, is_compute_node)) {}

AgentServer::~AgentServer() = default;

void AgentServer::submit_tasks(TAgentResult& agent_result, const std::vector<TAgentTaskRequest>& tasks) {
    _impl->submit_tasks(agent_result, tasks);
}

void AgentServer::make_snapshot(TAgentResult& agent_result, const TSnapshotRequest& snapshot_request) {
    _impl->make_snapshot(agent_result, snapshot_request);
}

void AgentServer::release_snapshot(TAgentResult& agent_result, const std::string& snapshot_path) {
    _impl->release_snapshot(agent_result, snapshot_path);
}

void AgentServer::publish_cluster_state(TAgentResult& agent_result, const TAgentPublishRequest& request) {
    _impl->publish_cluster_state(agent_result, request);
}

void AgentServer::update_max_thread_by_type(int type, int new_val) {
    _impl->update_max_thread_by_type(type, new_val);
}

ThreadPool* AgentServer::get_thread_pool(int type) const {
    return _impl->get_thread_pool(type);
}

void AgentServer::stop_task_worker_pool(TaskWorkerType type) const {
    return _impl->stop_task_worker_pool(type);
}

Status AgentServer::init() {
    return _impl->init();
}

void AgentServer::stop() {
    return _impl->stop();
}

} // namespace starrocks
