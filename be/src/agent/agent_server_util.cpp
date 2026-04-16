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

// get_thread_pool, update_max_thread_by_type, stop_task_worker_pool,
// make_snapshot, release_snapshot, and publish_cluster_state split out
// of agent_server.cpp to reduce peak compilation memory.

#include "agent/agent_server_impl.h"

#include <string>

#include "agent/task_worker_pool.h"
#include "base/testutil/sync_point.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/thread/threadpool.h"
#include "runtime/exec_env.h"
#include "storage/snapshot_manager.h"

namespace starrocks {

void AgentServer::Impl::make_snapshot(TAgentResult& t_agent_result, const TSnapshotRequest& snapshot_request) {
    std::string snapshot_path;
    auto st = SnapshotManager::instance()->make_snapshot(snapshot_request, &snapshot_path);
    if (!st.ok()) {
        LOG(WARNING) << "fail to make_snapshot. tablet_id:" << snapshot_request.tablet_id << " msg:" << st.to_string();
    } else {
        LOG(INFO) << "success to make_snapshot. tablet_id:" << snapshot_request.tablet_id << " path:" << snapshot_path;
        t_agent_result.__set_snapshot_path(snapshot_path);
    }

    st.to_thrift(&t_agent_result.status);
    t_agent_result.__set_snapshot_format(snapshot_request.preferred_snapshot_format);
    t_agent_result.__set_allow_incremental_clone(true);
}

void AgentServer::Impl::release_snapshot(TAgentResult& t_agent_result, const std::string& snapshot_path) {
    Status ret_st = SnapshotManager::instance()->release_snapshot(snapshot_path);
    if (!ret_st.ok()) {
        LOG(WARNING) << "Fail to release_snapshot. snapshot_path:" << snapshot_path;
    } else {
        LOG(INFO) << "success to release_snapshot. snapshot_path:" << snapshot_path;
    }
    ret_st.to_thrift(&t_agent_result.status);
}

void AgentServer::Impl::publish_cluster_state(TAgentResult& t_agent_result, const TAgentPublishRequest& request) {
    Status status = Status::NotSupported("deprecated method(publish_cluster_state) was invoked");
    status.to_thrift(&t_agent_result.status);
}

void AgentServer::Impl::update_max_thread_by_type(int type, int new_val) {
    Status st;
    switch (type) {
    case TTaskType::UPLOAD:
        st = _thread_pool_upload->update_max_threads(calc_real_num_threads(new_val));
        break;
    case TTaskType::DOWNLOAD:
        st = _thread_pool_download->update_max_threads(calc_real_num_threads(new_val));
        break;
    case TTaskType::MOVE:
        st = _thread_pool_move_dir->update_max_threads(calc_real_num_threads(new_val));
        break;
    case TTaskType::REMOTE_SNAPSHOT:
        st = _thread_pool_remote_snapshot->update_max_threads(
                calc_real_num_threads(new_val, REPLICATION_CPU_CORES_MULTIPLIER));
        break;
    case TTaskType::REPLICATE_SNAPSHOT:
        st = _thread_pool_replicate_snapshot->update_max_threads(
                calc_real_num_threads(new_val, REPLICATION_CPU_CORES_MULTIPLIER));
        break;
    case TTaskType::CLONE: {
        ThreadPool* thread_pool = get_thread_pool(type);
        if (thread_pool) {
            st = thread_pool->update_max_threads(calc_clone_thread_pool_size(_exec_env->store_paths().size(), new_val));
        } else {
            LOG(WARNING) << "Failed to update max thread, cannot get thread pool by task type: "
                         << to_string((TTaskType::type)type);
        }
        break;
    }
    default: {
        ThreadPool* thread_pool = get_thread_pool(type);
        if (thread_pool) {
            st = thread_pool->update_max_threads(new_val);
        } else {
            LOG(WARNING) << "Failed to update max thread, cannot get thread pool by task type: "
                         << to_string((TTaskType::type)type);
        }
        break;
    }
    }
    LOG_IF(ERROR, !st.ok()) << st;
}

#define STOP_IF_NOT_NULL(worker_pool) \
    if (worker_pool != nullptr) {     \
        worker_pool->stop();          \
    }

void AgentServer::Impl::stop_task_worker_pool(TaskWorkerType type) const {
    switch (type) {
    case TaskWorkerType::PUSH:
        STOP_IF_NOT_NULL(_push_workers);
        break;
    case TaskWorkerType::PUBLISH_VERSION:
        STOP_IF_NOT_NULL(_publish_version_workers);
        break;
    case TaskWorkerType::DELETE:
        STOP_IF_NOT_NULL(_delete_workers);
        break;
    case TaskWorkerType::REPORT_TASK:
        STOP_IF_NOT_NULL(_report_task_workers);
        break;
    case TaskWorkerType::REPORT_DISK_STATE:
        STOP_IF_NOT_NULL(_report_disk_state_workers);
        break;
    case TaskWorkerType::REPORT_OLAP_TABLE:
        STOP_IF_NOT_NULL(_report_tablet_workers);
        break;
    case TaskWorkerType::REPORT_WORKGROUP:
        STOP_IF_NOT_NULL(_report_workgroup_workers);
        STOP_IF_NOT_NULL(_report_resource_usage_workers);
        break;
    case TaskWorkerType::REPORT_DATACACHE_METRICS:
        STOP_IF_NOT_NULL(_report_datacache_metrics_workers);
        break;
    default:
        break;
    }
}

#undef STOP_IF_NOT_NULL

ThreadPool* AgentServer::Impl::get_thread_pool(int type) const {
    // TODO: more thread pools.
    ThreadPool* ret = nullptr;
    switch (type) {
    case TTaskType::PUBLISH_VERSION:
        ret = _thread_pool_publish_version.get();
        break;
    case TTaskType::CLONE:
        ret = _thread_pool_clone.get();
        break;
    case TTaskType::DROP:
        ret = _thread_pool_drop.get();
        break;
    case TTaskType::CREATE:
        ret = _thread_pool_create_tablet.get();
        break;
    case TTaskType::STORAGE_MEDIUM_MIGRATE:
        ret = _thread_pool_storage_medium_migrate.get();
        break;
    case TTaskType::MAKE_SNAPSHOT:
        ret = _thread_pool_make_snapshot.get();
        break;
    case TTaskType::RELEASE_SNAPSHOT:
        ret = _thread_pool_release_snapshot.get();
        break;
    case TTaskType::CHECK_CONSISTENCY:
        ret = _thread_pool_check_consistency.get();
        break;
    case TTaskType::COMPACTION:
        ret = _thread_pool_compaction.get();
        break;
    case TTaskType::COMPACTION_CONTROL:
        ret = _thread_pool_compaction_control.get();
        break;
    case TTaskType::UPDATE_SCHEMA:
        ret = _thread_pool_update_schema.get();
        break;
    case TTaskType::UPLOAD:
        ret = _thread_pool_upload.get();
        break;
    case TTaskType::DOWNLOAD:
        ret = _thread_pool_download.get();
        break;
    case TTaskType::MOVE:
        ret = _thread_pool_move_dir.get();
        break;
    case TTaskType::UPDATE_TABLET_META_INFO:
        ret = _thread_pool_update_tablet_meta_info.get();
        break;
    case TTaskType::ALTER:
        ret = _thread_pool_alter_tablet.get();
        break;
    case TTaskType::CLEAR_TRANSACTION_TASK:
        ret = _thread_pool_clear_transaction.get();
        break;
    case TTaskType::DROP_AUTO_INCREMENT_MAP:
        ret = _thread_pool_drop_auto_increment_map.get();
        break;
    case TTaskType::REMOTE_SNAPSHOT:
        ret = _thread_pool_remote_snapshot.get();
        break;
    case TTaskType::REPLICATE_SNAPSHOT:
        ret = _thread_pool_replicate_snapshot.get();
        break;
    case TTaskType::PUSH:
    case TTaskType::REALTIME_PUSH:
    case TTaskType::ROLLUP:
    case TTaskType::SCHEMA_CHANGE:
    case TTaskType::CANCEL_DELETE:
    case TTaskType::CLEAR_REMOTE_FILE:
    case TTaskType::CLEAR_ALTER_TASK:
    case TTaskType::RECOVER_TABLET:
    case TTaskType::STREAM_LOAD:
    case TTaskType::INSTALL_PLUGIN:
    case TTaskType::UNINSTALL_PLUGIN:
    case TTaskType::NUM_TASK_TYPE:
        break;
    }
    TEST_SYNC_POINT_CALLBACK("AgentServer::Impl::get_thread_pool:1", &ret);
    return ret;
}

} // namespace starrocks
