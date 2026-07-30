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

#include "util/brpc_stub_cache.h"

#include <gtest/gtest.h>
#include <testutil/assert.h>

#include "runtime/exec_env.h"
#include "util/failpoint/fail_point.h"

namespace starrocks {

class BrpcStubCacheTest : public testing::Test {
public:
    BrpcStubCacheTest() = default;
    ~BrpcStubCacheTest() override = default;
    void SetUp() override {
        _saved_brpc_max_connections_per_server = config::brpc_max_connections_per_server;
        _saved_brpc_stub_expire_s = config::brpc_stub_expire_s;
        config::brpc_max_connections_per_server = 1;
        _env._pipeline_timer = new pipeline::PipelineTimer();
        ASSERT_OK(_env._pipeline_timer->start());
    }
    void TearDown() override {
        delete _env._pipeline_timer;
        _env._pipeline_timer = nullptr;
        config::brpc_max_connections_per_server = _saved_brpc_max_connections_per_server;
        config::brpc_stub_expire_s = _saved_brpc_stub_expire_s;
    }

private:
    ExecEnv _env;
    int32_t _saved_brpc_max_connections_per_server = 0;
    int32_t _saved_brpc_stub_expire_s = 0;
};

TEST_F(BrpcStubCacheTest, normal) {
    BrpcStubCache cache(&_env);
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub1 = cache.get_stub(address);
    ASSERT_NE(nullptr, stub1);
    address.port = 124;
    auto stub2 = cache.get_stub(address);
    ASSERT_NE(nullptr, stub2);
    ASSERT_NE(stub1, stub2);
    address.port = 123;
    auto stub3 = cache.get_stub(address);
    ASSERT_EQ(stub1, stub3);
}

TEST_F(BrpcStubCacheTest, invalid) {
    BrpcStubCache cache(&_env);
    TNetworkAddress address;
    address.hostname = "invalid.cm.invalid";
    address.port = 123;
    auto stub1 = cache.get_stub(address);
    ASSERT_EQ(nullptr, stub1);
}

TEST_F(BrpcStubCacheTest, reset) {
    BrpcStubCache cache(&_env);
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub1 = cache.get_stub(address);
    ASSERT_NE(nullptr, stub1);
    auto istub1 = stub1->stub();

    stub1->reset_channel();
    auto istub2 = stub1->stub();

    ASSERT_NE(istub1, istub2);
}

TEST_F(BrpcStubCacheTest, test_http_stub) {
    HttpBrpcStubCache cache;
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub1 = cache.get_http_stub(address);
    ASSERT_NE(nullptr, *stub1);
    address.port = 124;
    auto stub2 = cache.get_http_stub(address);
    ASSERT_NE(nullptr, *stub2);
    ASSERT_NE(*stub1, *stub2);
    address.port = 123;
    auto stub3 = cache.get_http_stub(address);
    ASSERT_NE(nullptr, *stub3);
    ASSERT_EQ(*stub1, *stub3);

    address.hostname = "invalid.cm.invalid";
    auto stub4 = cache.get_http_stub(address);
    ASSERT_EQ(nullptr, *stub4);
}

TEST_F(BrpcStubCacheTest, test_cleanup) {
    config::brpc_stub_expire_s = 1;
    BrpcStubCache cache(&_env);
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub1 = cache.get_stub(address);
    ASSERT_NE(nullptr, stub1);
    auto stub2 = cache.get_stub(address);
    ASSERT_EQ(stub2, stub1);

    sleep(2);
    auto stub3 = cache.get_stub(address);
    ASSERT_NE(stub3, stub1);
}

TEST_F(BrpcStubCacheTest, test_http_cleanup) {
    config::brpc_stub_expire_s = 1;
    HttpBrpcStubCache cache;
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub1 = cache.get_http_stub(address);
    ASSERT_NE(nullptr, *stub1);
    auto stub2 = cache.get_http_stub(address);
    ASSERT_EQ(*stub2, *stub1);

    sleep(2);
    auto stub3 = cache.get_http_stub(address);
    ASSERT_NE(*stub3, *stub1);
}

TEST_F(BrpcStubCacheTest, test_destructor_joins_inflight_cleanup_tasks) {
    config::brpc_stub_expire_s = 1;
    auto cache = std::make_unique<BrpcStubCache>(&_env);
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub = cache->get_stub(address);
    ASSERT_NE(nullptr, stub);

    cache.reset();

    auto cache2 = std::make_unique<BrpcStubCache>(&_env);
    auto fresh_stub = cache2->get_stub(address);
    ASSERT_NE(nullptr, fresh_stub);
}

TEST_F(BrpcStubCacheTest, test_active_access_keeps_stub_alive_across_expire_window) {
    config::brpc_stub_expire_s = 2;
    BrpcStubCache cache(&_env);
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub1 = cache.get_stub(address);
    ASSERT_NE(nullptr, stub1);

    for (int i = 0; i < 3; ++i) {
        sleep(1);
        auto stub = cache.get_stub(address);
        ASSERT_EQ(stub1, stub) << "stub must not be evicted while being accessed";
    }
}

TEST_F(BrpcStubCacheTest, test_http_active_access_keeps_stub_alive_across_expire_window) {
    config::brpc_stub_expire_s = 2;
    HttpBrpcStubCache cache;
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub1 = cache.get_http_stub(address);
    ASSERT_NE(nullptr, *stub1);

    for (int i = 0; i < 3; ++i) {
        sleep(1);
        auto stub = cache.get_http_stub(address);
        ASSERT_NE(nullptr, *stub);
        ASSERT_EQ(*stub1, *stub) << "http stub must not be evicted while being accessed";
    }
}

// Deterministic regression test for the cleanup-task use-after-free.
//
// EndpointCleanupTask::Run() reschedules by handing the pool's sole owning reference to a freshly
// created task (replace_cleanup_task_locked), which drops the currently running task's last
// reference, and then keeps reading its own members (_deadline/_cache/_endpoint). This is only safe
// because the task runs on the guarded PipelineTimerTask path (RunTimerTask -> doRun() holds
// `auto self = shared_from_this()`); on the unguarded LightTimerTask path the reschedule would free
// `this` mid-Run() -> heap-use-after-free (ASan aborts on the timer thread).
//
// The reschedule (self-replace) branch is driven deterministically, without relying on wall-clock
// timing: the cleanup task is re-armed to fire immediately while its deadline is still far in the
// future, so the fire takes the reschedule branch rather than the evict branch.
TEST_F(BrpcStubCacheTest, cleanup_task_reschedule_no_use_after_free) {
    config::brpc_stub_expire_s = 3600; // deadline stays in the future -> Run() takes the reschedule branch
    BrpcStubCache cache(&_env);
    butil::EndPoint ep;
    ASSERT_EQ(0, butil::str2endpoint("127.0.0.1:123", &ep));

    auto stub = cache.get_stub(ep);
    ASSERT_NE(nullptr, stub);

    // The pool holds the only owning reference to the scheduled cleanup task; take a raw pointer so
    // that the reschedule below drops the last reference (as it does when the bthread timer fires it).
    EndpointCleanupTask<BrpcStubCache>* task = nullptr;
    {
        std::lock_guard<SpinLock> l(cache._lock);
        auto* pool = cache._stub_map.seek(ep);
        ASSERT_NE(nullptr, pool);
        task = (*pool)->_cleanup_task.get();
    }
    ASSERT_NE(nullptr, task);

    // Cancel the far-future firing and re-arm the task to fire now. Its deadline is still ~now+3600s,
    // so when it fires it hits the reschedule (self-replace) branch.
    cache._pipeline_timer->unschedule(task);
    timespec now_ts = butil::microseconds_to_timespec(butil::gettimeofday_us());
    ASSERT_OK(cache._pipeline_timer->schedule(task, now_ts));

    // Wait for the fire to swap in the rescheduled task. On the unguarded path the fire self-frees
    // `task` and ASan aborts before we observe the swap; on the guarded path it completes cleanly.
    bool rescheduled = false;
    for (int i = 0; i < 500 && !rescheduled; ++i) { // up to ~5s
        {
            std::lock_guard<SpinLock> l(cache._lock);
            auto* pool = cache._stub_map.seek(ep);
            rescheduled = (pool != nullptr && (*pool)->_cleanup_task.get() != task);
        }
        usleep(10 * 1000); // 10ms
    }
    ASSERT_TRUE(rescheduled) << "cleanup task did not reschedule";

    // Reaching here without an ASan abort means no use-after-free; the stub is still cached.
    ASSERT_NE(nullptr, cache.get_stub(ep));
}

} // namespace starrocks
