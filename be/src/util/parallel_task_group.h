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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks {

// ParallelTaskGroup runs a batch of Status-returning tasks on a PriorityThreadPool
// and waits for all of them to complete. It aggregates the first error (subsequent
// errors are logged), and falls back to inline execution on the calling thread when
// the pool's queue refuses to accept a task (try_offer returns false).
//
// Usage:
//   ParallelTaskGroup group(pool);
//   group.submit([]{ return do_work_1(); });
//   group.submit([]{ return do_work_2(); });
//   RETURN_IF_ERROR(group.run_and_wait());
//
// The object is single-use. After run_and_wait() returns, do not call submit() or
// run_and_wait() again. Construct a new instance for each batch.
//
// Lifecycle: the synchronization state lives behind a shared_ptr held by both the
// group and every dispatched task. Once run_and_wait() returns, the group may be
// destroyed even if cancellation left tasks still in flight; tasks will safely
// drop the last reference when they finish.
//
// Cancellation: pass a cancel_check callback to run_and_wait() to allow early
// return when an external cancellation signal fires. Cancellation does NOT abort
// already-running tasks; it only stops the wait loop and lets the caller proceed.
class ParallelTaskGroup {
public:
    using Task = std::function<Status()>;

    explicit ParallelTaskGroup(PriorityThreadPool* pool) : _pool(pool), _state(std::make_shared<SharedState>()) {}

    ParallelTaskGroup(const ParallelTaskGroup&) = delete;
    ParallelTaskGroup& operator=(const ParallelTaskGroup&) = delete;

    void submit(Task task) { _state->tasks.emplace_back(std::move(task)); }

    size_t size() const { return _state->tasks.size(); }

    // Dispatches all submitted tasks to the pool, runs any tasks the pool refuses
    // to accept on the current thread, then blocks until every task has finished.
    // Returns the first error observed, or OK if all tasks succeeded.
    //
    // If cancel_check is provided, it is polled while waiting; when it returns
    // true, run_and_wait() returns Status::Cancelled even if tasks are still in
    // flight. Tasks already dispatched continue to run but can no longer corrupt
    // the group: every piece of state they touch (the tasks themselves, the
    // counter, the mutex) lives in SharedState behind a shared_ptr, so the
    // caller may safely destroy the group object as soon as run_and_wait()
    // returns.
    Status run_and_wait(const std::function<bool()>& cancel_check = {}) {
        if (_state->tasks.empty()) {
            return Status::OK();
        }
        auto state = _state;
        state->pending.store(static_cast<int>(state->tasks.size()), std::memory_order_relaxed);

        std::vector<size_t> inline_indices;
        inline_indices.reserve(state->tasks.size());
        for (size_t i = 0; i < state->tasks.size(); ++i) {
            // Capture `state` (shared_ptr by value) so the lambda owns its data
            // even if the group is destroyed after a cancellation.
            bool submitted = _pool->try_offer([state, i]() { _run_one(state, i); });
            if (!submitted) {
                inline_indices.push_back(i);
            }
        }
        for (size_t i : inline_indices) {
            _run_one(state, i);
        }

        std::unique_lock<std::mutex> lock(state->mu);
        if (cancel_check) {
            while (state->pending.load(std::memory_order_acquire) != 0) {
                if (cancel_check()) {
                    return Status::Cancelled("parallel task group cancelled");
                }
                state->cv.wait_for(lock, std::chrono::milliseconds(_kCancelPollMs));
            }
        } else {
            state->cv.wait(lock, [&] { return state->pending.load(std::memory_order_acquire) == 0; });
        }
        return state->first_error;
    }

private:
    static constexpr int _kCancelPollMs = 50;

    struct SharedState {
        std::vector<Task> tasks;     // immutable after run_and_wait() begins
        std::mutex mu;
        std::condition_variable cv;
        std::atomic<int> pending{0};
        Status first_error; // guarded by mu for writes; read after pending hits 0
    };

    static void _run_one(const std::shared_ptr<SharedState>& state, size_t index) {
        Status status;
        try {
            status = state->tasks[index]();
        } catch (const std::exception& e) {
            status = Status::InternalError(std::string("parallel task threw: ") + e.what());
        }
        if (!status.ok()) {
            std::lock_guard<std::mutex> lock(state->mu);
            if (state->first_error.ok()) {
                state->first_error = status;
            } else {
                LOG(WARNING) << "parallel task group: dropping secondary error: " << status;
            }
        }
        if (state->pending.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            std::lock_guard<std::mutex> lock(state->mu);
            state->cv.notify_all();
        }
    }

    PriorityThreadPool* const _pool;
    std::shared_ptr<SharedState> _state;
};

} // namespace starrocks
