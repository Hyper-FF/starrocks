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

// submit_tasks() split out of agent_server.cpp to reduce peak
// compilation memory – the HANDLE_TASK macro instantiates many
// std::bind / std::shared_ptr templates.

#include "agent/agent_server_impl.h"

#include <thrift/protocol/TDebugProtocol.h>

#include <string>
#include <vector>

#include "agent/agent_task.h"
#include "agent/task_signatures_manager.h"
#include "agent/task_worker_pool.h"
#include "base/phmap/phmap.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/system/master_info.h"
#include "common/thread/threadpool.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

using TTaskTypeHash = std::hash<std::underlying_type<TTaskType::type>::type>;

// TODO(lingbin): each task in the batch may have it own status or FE must check and
// resend request when something is wrong(BE may need some logic to guarantee idempotence.
void AgentServer::Impl::submit_tasks(TAgentResult& agent_result, const std::vector<TAgentTaskRequest>& tasks) {
    Status ret_st;
    auto master_address = get_master_address();
    if (master_address.hostname.empty() || master_address.port == 0) {
        ret_st = Status::Cancelled("Have not get FE Master heartbeat yet");
        ret_st.to_thrift(&agent_result.status);
        return;
    }

    phmap::flat_hash_map<TTaskType::type, std::vector<const TAgentTaskRequest*>, TTaskTypeHash> task_divider;
    phmap::flat_hash_map<TPushType::type, std::vector<const TAgentTaskRequest*>, TTaskTypeHash> push_divider;

    for (const auto& task : tasks) {
        VLOG_RPC << "submit one task: " << apache::thrift::ThriftDebugString(task).c_str();
        TTaskType::type task_type = task.task_type;
        int64_t signature = task.signature;

#define HANDLE_TYPE(t_task_type, req_member)                                                        \
    case t_task_type:                                                                               \
        if (task.__isset.req_member) {                                                              \
            task_divider[t_task_type].push_back(&task);                                             \
        } else {                                                                                    \
            ret_st = Status::InvalidArgument(                                                       \
                    strings::Substitute("task(signature=$0) has wrong request member", signature)); \
        }                                                                                           \
        break;

        // TODO(lingbin): It still too long, divided these task types into several categories
        switch (task_type) {
            HANDLE_TYPE(TTaskType::CREATE, create_tablet_req);
            HANDLE_TYPE(TTaskType::DROP, drop_tablet_req);
            HANDLE_TYPE(TTaskType::PUBLISH_VERSION, publish_version_req);
            HANDLE_TYPE(TTaskType::CLEAR_TRANSACTION_TASK, clear_transaction_task_req);
            HANDLE_TYPE(TTaskType::CLONE, clone_req);
            HANDLE_TYPE(TTaskType::STORAGE_MEDIUM_MIGRATE, storage_medium_migrate_req);
            HANDLE_TYPE(TTaskType::CHECK_CONSISTENCY, check_consistency_req);
            HANDLE_TYPE(TTaskType::COMPACTION, compaction_req);
            HANDLE_TYPE(TTaskType::COMPACTION_CONTROL, compaction_control_req);
            HANDLE_TYPE(TTaskType::UPLOAD, upload_req);
            HANDLE_TYPE(TTaskType::UPDATE_SCHEMA, update_schema_req);
            HANDLE_TYPE(TTaskType::DOWNLOAD, download_req);
            HANDLE_TYPE(TTaskType::MAKE_SNAPSHOT, snapshot_req);
            HANDLE_TYPE(TTaskType::RELEASE_SNAPSHOT, release_snapshot_req);
            HANDLE_TYPE(TTaskType::MOVE, move_dir_req);
            HANDLE_TYPE(TTaskType::UPDATE_TABLET_META_INFO, update_tablet_meta_info_req);
            HANDLE_TYPE(TTaskType::DROP_AUTO_INCREMENT_MAP, drop_auto_increment_map_req);
            HANDLE_TYPE(TTaskType::REMOTE_SNAPSHOT, remote_snapshot_req);
            HANDLE_TYPE(TTaskType::REPLICATE_SNAPSHOT, replicate_snapshot_req);

        case TTaskType::REALTIME_PUSH:
            if (!task.__isset.push_req) {
                ret_st = Status::InvalidArgument(
                        strings::Substitute("task(signature=$0) has wrong request member", signature));
                break;
            }
            if (task.push_req.push_type == TPushType::LOAD_V2 || task.push_req.push_type == TPushType::DELETE ||
                task.push_req.push_type == TPushType::CANCEL_DELETE) {
                push_divider[task.push_req.push_type].push_back(&task);
            } else {
                ret_st = Status::InvalidArgument(
                        strings::Substitute("task(signature=$0, type=$1, push_type=$2) has wrong push_type", signature,
                                            task_type, task.push_req.push_type));
            }
            break;
        case TTaskType::ALTER:
            if (task.__isset.alter_tablet_req || task.__isset.alter_tablet_req_v2) {
                task_divider[TTaskType::ALTER].push_back(&task);
            } else {
                ret_st = Status::InvalidArgument(
                        strings::Substitute("task(signature=$0) has wrong request member", signature));
            }
            break;
        default:
            ret_st = Status::InvalidArgument(
                    strings::Substitute("task(signature=$0, type=$1) has wrong task type", signature, task_type));
            break;
        }
#undef HANDLE_TYPE

        if (!ret_st.ok()) {
            LOG(WARNING) << "fail to submit task. reason: " << ret_st.message() << ", task: " << task;
            // For now, all tasks in the batch share one status, so if any task
            // was failed to submit, we can only return error to FE(even when some
            // tasks have already been successfully submitted).
            // However, Fe does not check the return status of submit_tasks() currently,
            // and it is not sure that FE will retry when something is wrong, so here we
            // only print an warning log and go on(i.e. do not break current loop),
            // to ensure every task can be submitted once. It is OK for now, because the
            // ret_st can be error only when it encounters an wrong task_type and
            // req-member in TAgentTaskRequest, which is basically impossible.
            // TODO(lingbin): check the logic in FE again later.
        }
    }

#define HANDLE_TASK(t_task_type, all_tasks, do_func, AGENT_REQ, request, env)                                          \
    {                                                                                                                  \
        std::string submit_log = "Submit task success. type=" + to_string(t_task_type) + ", signatures=";              \
        size_t log_count = 0;                                                                                          \
        size_t queue_len = 0;                                                                                          \
        for (auto* task : all_tasks) {                                                                                 \
            auto pool = get_thread_pool(t_task_type);                                                                  \
            auto signature = task->signature;                                                                          \
            std::pair<bool, size_t> register_pair = register_task_info(task_type, signature);                          \
            if (register_pair.first) {                                                                                 \
                if (log_count++ < 100) {                                                                               \
                    submit_log += std::to_string(signature) + ",";                                                     \
                }                                                                                                      \
                queue_len = register_pair.second;                                                                      \
                ret_st = pool->submit_func(                                                                            \
                        std::bind(do_func, std::make_shared<AGENT_REQ>(*task, task->request, time(nullptr)), env));    \
                if (!ret_st.ok()) {                                                                                    \
                    LOG(WARNING) << "fail to submit task. reason: " << ret_st.message() << ", task: " << task;         \
                }                                                                                                      \
            } else {                                                                                                   \
                LOG(INFO) << "Submit task failed, already exists type=" << t_task_type << ", signature=" << signature; \
            }                                                                                                          \
        }                                                                                                              \
        if (queue_len > 0) {                                                                                           \
            if (log_count >= 100) {                                                                                    \
                submit_log += "...,";                                                                                  \
            }                                                                                                          \
            LOG(INFO) << submit_log << " task_count_in_queue=" << queue_len;                                           \
        }                                                                                                              \
    }

    // batch submit tasks
    for (const auto& task_item : task_divider) {
        const auto& task_type = task_item.first;
        auto all_tasks = task_item.second;
        switch (task_type) {
        case TTaskType::CREATE:
            HANDLE_TASK(TTaskType::CREATE, all_tasks, run_create_tablet_task, CreateTabletAgentTaskRequest,
                        create_tablet_req, _exec_env);
            break;
        case TTaskType::DROP:
            HANDLE_TASK(TTaskType::DROP, all_tasks, run_drop_tablet_task, DropTabletAgentTaskRequest, drop_tablet_req,
                        _exec_env);
            break;
        case TTaskType::PUBLISH_VERSION: {
            for (const auto& task : all_tasks) {
                _publish_version_workers->submit_task(*task);
            }
            break;
        }
        case TTaskType::CLEAR_TRANSACTION_TASK:
            HANDLE_TASK(TTaskType::CLEAR_TRANSACTION_TASK, all_tasks, run_clear_transaction_task,
                        ClearTransactionAgentTaskRequest, clear_transaction_task_req, _exec_env);
            break;
        case TTaskType::CLONE:
            HANDLE_TASK(TTaskType::CLONE, all_tasks, run_clone_task, CloneAgentTaskRequest, clone_req, _exec_env);
            break;
        case TTaskType::STORAGE_MEDIUM_MIGRATE:
            HANDLE_TASK(TTaskType::STORAGE_MEDIUM_MIGRATE, all_tasks, run_storage_medium_migrate_task,
                        StorageMediumMigrateTaskRequest, storage_medium_migrate_req, _exec_env);
            break;
        case TTaskType::CHECK_CONSISTENCY:
            HANDLE_TASK(TTaskType::CHECK_CONSISTENCY, all_tasks, run_check_consistency_task,
                        CheckConsistencyTaskRequest, check_consistency_req, _exec_env);
            break;
        case TTaskType::COMPACTION:
            HANDLE_TASK(TTaskType::COMPACTION, all_tasks, run_compaction_task, CompactionTaskRequest, compaction_req,
                        _exec_env);
            break;
        case TTaskType::COMPACTION_CONTROL:
            HANDLE_TASK(TTaskType::COMPACTION_CONTROL, all_tasks, run_compaction_control_task,
                        CompactionControlTaskRequest, compaction_control_req, _exec_env);
            break;
        case TTaskType::UPDATE_SCHEMA:
            HANDLE_TASK(TTaskType::UPDATE_SCHEMA, all_tasks, run_update_schema_task, UpdateSchemaTaskRequest,
                        update_schema_req, _exec_env);
            break;
        case TTaskType::UPLOAD:
            HANDLE_TASK(TTaskType::UPLOAD, all_tasks, run_upload_task, UploadAgentTaskRequest, upload_req, _exec_env);
            break;
        case TTaskType::DOWNLOAD:
            HANDLE_TASK(TTaskType::DOWNLOAD, all_tasks, run_download_task, DownloadAgentTaskRequest, download_req,
                        _exec_env);
            break;
        case TTaskType::MAKE_SNAPSHOT:
            HANDLE_TASK(TTaskType::MAKE_SNAPSHOT, all_tasks, run_make_snapshot_task, SnapshotAgentTaskRequest,
                        snapshot_req, _exec_env);
            break;
        case TTaskType::RELEASE_SNAPSHOT:
            HANDLE_TASK(TTaskType::RELEASE_SNAPSHOT, all_tasks, run_release_snapshot_task,
                        ReleaseSnapshotAgentTaskRequest, release_snapshot_req, _exec_env);
            break;
        case TTaskType::MOVE:
            HANDLE_TASK(TTaskType::MOVE, all_tasks, run_move_dir_task, MoveDirAgentTaskRequest, move_dir_req,
                        _exec_env);
            break;
        case TTaskType::UPDATE_TABLET_META_INFO:
            HANDLE_TASK(TTaskType::UPDATE_TABLET_META_INFO, all_tasks, run_update_meta_info_task,
                        UpdateTabletMetaInfoAgentTaskRequest, update_tablet_meta_info_req, _exec_env);
            break;
        case TTaskType::DROP_AUTO_INCREMENT_MAP:
            HANDLE_TASK(TTaskType::DROP_AUTO_INCREMENT_MAP, all_tasks, run_drop_auto_increment_map_task,
                        DropAutoIncrementMapAgentTaskRequest, drop_auto_increment_map_req, _exec_env);
            break;
        case TTaskType::REMOTE_SNAPSHOT:
            HANDLE_TASK(TTaskType::REMOTE_SNAPSHOT, all_tasks, run_remote_snapshot_task, RemoteSnapshotAgentTaskRequest,
                        remote_snapshot_req, _exec_env);
            break;
        case TTaskType::REPLICATE_SNAPSHOT:
            HANDLE_TASK(TTaskType::REPLICATE_SNAPSHOT, all_tasks, run_replicate_snapshot_task,
                        ReplicateSnapshotAgentTaskRequest, replicate_snapshot_req, _exec_env);
            break;
        case TTaskType::REALTIME_PUSH:
        case TTaskType::PUSH: {
            // should not run here
            break;
        }
        case TTaskType::ALTER:
            HANDLE_TASK(TTaskType::ALTER, all_tasks, run_alter_tablet_task, AlterTabletAgentTaskRequest,
                        alter_tablet_req_v2, _exec_env);
            break;
        default:
            ret_st = Status::InvalidArgument(strings::Substitute("tasks(type=$0) has wrong task type", task_type));
            LOG(WARNING) << "fail to batch submit task. reason: " << ret_st.message();
        }
    }

    // batch submit push tasks
    if (!push_divider.empty()) {
        LOG(INFO) << "begin batch submit task: " << tasks[0].task_type;
        for (const auto& push_item : push_divider) {
            const auto& push_type = push_item.first;
            auto all_push_tasks = push_item.second;
            switch (push_type) {
            case TPushType::LOAD_V2:
                _push_workers->submit_tasks(all_push_tasks);
                break;
            case TPushType::DELETE:
            case TPushType::CANCEL_DELETE:
                _delete_workers->submit_tasks(all_push_tasks);
                break;
            default:
                ret_st = Status::InvalidArgument(strings::Substitute("tasks(type=$0, push_type=$1) has wrong task type",
                                                                     TTaskType::PUSH, push_type));
                LOG(WARNING) << "fail to batch submit push task. reason: " << ret_st.message();
            }
        }
    }

    ret_st.to_thrift(&agent_result.status);
}

} // namespace starrocks
