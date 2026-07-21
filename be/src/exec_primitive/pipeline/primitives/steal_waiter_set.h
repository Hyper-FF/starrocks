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
#include <cstdint>
#include <vector>

#include "exec_primitive/pipeline/primitives/pipeline_observer.h"

namespace starrocks::pipeline {

// Work-stealing keep-alive: a per-lane registry of parked "thieves" waiting to be woken when a
// lane they can steal from accumulates backlog. Shared by every steal source (network receiver,
// local exchange). Producers (a receiver's brpc thread, a local-exchange sink) call notify_all()
// once a lane crosses the stealable-backlog threshold; drained steal-eligible drivers register on
// park and deregister on wake / finish.
//
// Lock-free: register/deregister is a single atomic exchange; notify_all scans a small (== dop)
// array and is skipped entirely when there are no waiters. Safe for concurrent register/deregister
// (executor threads) vs notify_all (producer threads).
class StealWaiterSet {
public:
    // Sized once, before execution (single-threaded setup). One slot per lane (== driver sequence).
    void init(size_t lanes) { _waiters = std::vector<std::atomic<PipelineObserver*>>(lanes); }

    size_t lanes() const { return _waiters.size(); }

    void register_waiter(int32_t lane, PipelineObserver* observer) {
        if (observer == nullptr || lane < 0 || lane >= static_cast<int32_t>(_waiters.size())) {
            return;
        }
        auto* prev = _waiters[lane].exchange(observer, std::memory_order_acq_rel);
        if (prev == nullptr) {
            _num_waiters.fetch_add(1, std::memory_order_relaxed);
        }
    }

    void deregister_waiter(int32_t lane) {
        if (lane < 0 || lane >= static_cast<int32_t>(_waiters.size())) {
            return;
        }
        auto* prev = _waiters[lane].exchange(nullptr, std::memory_order_acq_rel);
        if (prev != nullptr) {
            _num_waiters.fetch_sub(1, std::memory_order_relaxed);
        }
    }

    bool has_waiters() const { return _num_waiters.load(std::memory_order_acquire) > 0; }

    // Wake every registered waiter. Called by a producer when a lane crosses the backlog threshold,
    // and on finish/cancel so parked thieves can observe termination. steal_trigger() reschedules a
    // parked thief without the own-lane predicate that gates the normal source wake.
    void notify_all() {
        if (!has_waiters()) {
            return;
        }
        for (auto& w : _waiters) {
            if (auto* obs = w.load(std::memory_order_acquire)) {
                obs->steal_trigger();
            }
        }
    }

private:
    std::vector<std::atomic<PipelineObserver*>> _waiters;
    std::atomic<int32_t> _num_waiters{0};
};

} // namespace starrocks::pipeline
