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

#include "util/parallel_task_group.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "util/priority_thread_pool.hpp"

namespace starrocks {

namespace {

// A pool small enough to easily exercise the try_offer fallback path.
constexpr uint32_t kPoolThreads = 4;
constexpr uint32_t kPoolQueueSize = 16;

class ParallelTaskGroupTest : public ::testing::Test {
protected:
    void SetUp() override { _pool = std::make_unique<PriorityThreadPool>("ptg-ut", kPoolThreads, kPoolQueueSize); }
    void TearDown() override {
        _pool->shutdown();
        _pool->join();
        _pool.reset();
    }

    std::unique_ptr<PriorityThreadPool> _pool;
};

} // namespace

TEST_F(ParallelTaskGroupTest, EmptyGroupReturnsOk) {
    ParallelTaskGroup group(_pool.get());
    ASSERT_TRUE(group.run_and_wait().ok());
}

TEST_F(ParallelTaskGroupTest, AllTasksSucceed) {
    ParallelTaskGroup group(_pool.get());
    std::atomic<int> counter{0};
    constexpr int kTasks = 64;
    for (int i = 0; i < kTasks; ++i) {
        group.submit([&counter]() {
            counter.fetch_add(1);
            return Status::OK();
        });
    }
    ASSERT_TRUE(group.run_and_wait().ok());
    ASSERT_EQ(kTasks, counter.load());
}

TEST_F(ParallelTaskGroupTest, FirstErrorIsReturned) {
    ParallelTaskGroup group(_pool.get());
    std::atomic<int> counter{0};
    constexpr int kTasks = 32;
    for (int i = 0; i < kTasks; ++i) {
        group.submit([&counter, i]() {
            counter.fetch_add(1);
            if (i == 7) {
                return Status::InternalError("boom-7");
            }
            return Status::OK();
        });
    }
    Status s = group.run_and_wait();
    ASSERT_FALSE(s.ok());
    ASSERT_NE(std::string_view::npos, s.message().find("boom-"));
    // Every task must finish even when one errors out.
    ASSERT_EQ(kTasks, counter.load());
}

TEST_F(ParallelTaskGroupTest, MultipleErrorsKeepOnlyFirst) {
    ParallelTaskGroup group(_pool.get());
    std::atomic<int> finished{0};
    constexpr int kTasks = 16;
    for (int i = 0; i < kTasks; ++i) {
        group.submit([&finished, i]() {
            // Stagger so that order is roughly deterministic without being strict.
            std::this_thread::sleep_for(std::chrono::milliseconds(i));
            finished.fetch_add(1);
            return Status::InternalError("err-" + std::to_string(i));
        });
    }
    Status s = group.run_and_wait();
    ASSERT_FALSE(s.ok());
    ASSERT_EQ(kTasks, finished.load());
    // We don't strictly require err-0 (scheduling jitter), but it must be SOME err-N.
    ASSERT_NE(std::string_view::npos, s.message().find("err-"));
}

TEST_F(ParallelTaskGroupTest, FallbackInlineWhenPoolSaturated) {
    // Hold every worker thread to force try_offer to spill into the inline path.
    std::mutex gate_mu;
    std::condition_variable gate_cv;
    bool open = false;
    std::atomic<int> blocked_workers{0};
    for (uint32_t i = 0; i < kPoolThreads; ++i) {
        _pool->offer([&]() {
            blocked_workers.fetch_add(1);
            std::unique_lock<std::mutex> lock(gate_mu);
            gate_cv.wait(lock, [&] { return open; });
        });
    }
    while (blocked_workers.load() < static_cast<int>(kPoolThreads)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    // Fill the queue so try_offer must reject some submissions.
    std::atomic<int> queued_executed{0};
    for (uint32_t i = 0; i < kPoolQueueSize; ++i) {
        ASSERT_TRUE(_pool->try_offer([&queued_executed]() { queued_executed.fetch_add(1); }));
    }

    ParallelTaskGroup group(_pool.get());
    std::atomic<int> ran{0};
    constexpr int kTasks = 8;
    auto caller_tid = std::this_thread::get_id();
    std::atomic<int> ran_on_caller{0};
    for (int i = 0; i < kTasks; ++i) {
        group.submit([&]() {
            ran.fetch_add(1);
            if (std::this_thread::get_id() == caller_tid) {
                ran_on_caller.fetch_add(1);
            }
            return Status::OK();
        });
    }
    // Unblock the pool a bit later so the inline fallback path is the one that
    // actually drains the work. With pool fully saturated, every task should run
    // inline.
    Status s = group.run_and_wait();
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(kTasks, ran.load());
    ASSERT_EQ(kTasks, ran_on_caller.load());

    // Release the pool workers and let queued no-ops drain before teardown.
    {
        std::lock_guard<std::mutex> lock(gate_mu);
        open = true;
    }
    gate_cv.notify_all();
    while (queued_executed.load() < static_cast<int>(kPoolQueueSize)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

TEST_F(ParallelTaskGroupTest, CancelReturnsEarly) {
    ParallelTaskGroup group(_pool.get());
    std::atomic<bool> release{false};
    std::atomic<int> running{0};
    std::atomic<int> finished{0};
    constexpr int kTasks = 4;
    for (int i = 0; i < kTasks; ++i) {
        group.submit([&]() {
            running.fetch_add(1);
            while (!release.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            finished.fetch_add(1);
            return Status::OK();
        });
    }
    std::atomic<bool> cancelled{false};
    std::thread canceller([&]() {
        while (running.load() == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        cancelled.store(true);
    });

    Status s = group.run_and_wait([&]() { return cancelled.load(); });
    ASSERT_TRUE(s.is_cancelled()) << s;

    // Release in-flight tasks and wait for them to actually exit before the
    // test-local atomics they reference go out of scope.
    release.store(true);
    canceller.join();
    while (finished.load() < kTasks) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

TEST_F(ParallelTaskGroupTest, ExceptionInTaskBecomesError) {
    ParallelTaskGroup group(_pool.get());
    group.submit([]() -> Status {
        throw std::runtime_error("oops");
    });
    group.submit([]() { return Status::OK(); });
    Status s = group.run_and_wait();
    ASSERT_FALSE(s.ok());
    ASSERT_NE(std::string_view::npos, s.message().find("oops"));
}

} // namespace starrocks
