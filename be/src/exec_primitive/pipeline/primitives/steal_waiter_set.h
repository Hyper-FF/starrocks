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
    void init(size_t lanes) {
        _waiters = std::vector<std::atomic<PipelineObserver*>>(lanes);
        _lane_armed = std::vector<std::atomic<bool>>(lanes);
    }

    size_t lanes() const { return _waiters.size(); }

    void register_waiter(int32_t lane, PipelineObserver* observer) {
        if (observer == nullptr || lane < 0 || lane >= static_cast<int32_t>(_waiters.size())) {
            return;
        }
        auto* prev = _waiters[lane].exchange(observer, std::memory_order_acq_rel);
        if (prev == nullptr) {
            _num_waiters.fetch_add(1, std::memory_order_relaxed);
        }
        // Re-arm every producer lane's edge-trigger: this newly-parked thief must be woken on the
        // next backlog crossing on ANY lane it could steal from. The producer's enqueue only raises
        // backlog (it never disarms), so registration is where the latch is reset; without this a
        // lane that already fired once would never wake a later thief. Cheap (dop bools) and
        // infrequent (only when a thief parks). The driver re-checks backlog right after registering
        // (register-then-recheck), so a crossing racing this reset is not missed.
        for (auto& armed : _lane_armed) {
            armed.store(false, std::memory_order_release);
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

    // Edge-triggered, per producer-lane wake. Fire the registered waiters only when producer `lane`'s
    // backlog FIRST crosses `threshold`; re-arm when it drains back below. This is the storm guard:
    // without it a wake would fire on EVERY enqueued chunk (a hot lane pushing thousands/sec) times
    // every parked thief -- an N*N notify storm. With it, at most one wake per crossing, and none at
    // all while no thief is parked (has_waiters() short-circuits). Called by the producer after
    // enqueue, on the lock-free path. `threshold` is the same stealable-backlog threshold the thief
    // applies, so a wake never fires for a backlog the thief would decline.
    void notify_lane_backlog(int32_t lane, size_t backlog, size_t threshold) {
        if (lane < 0 || lane >= static_cast<int32_t>(_lane_armed.size())) {
            return;
        }
        const bool crossed = backlog >= threshold;
        // exchange returns the previous armed state; only fire on the rising edge (false -> true).
        const bool was_armed = _lane_armed[lane].exchange(crossed, std::memory_order_acq_rel);
        if (crossed && !was_armed) {
            notify_all();
        }
    }

    // Wake every registered waiter unconditionally. Used on finish/cancel so parked thieves observe
    // termination (low-frequency, not a storm source). The per-chunk producer path uses the
    // edge-triggered notify_lane_backlog() above instead.
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
    // Per producer-lane edge-trigger latch: true once that lane's backlog crossed the threshold,
    // reset when it drains below. Prevents re-firing on every chunk while backlog stays high.
    std::vector<std::atomic<bool>> _lane_armed;
    std::atomic<int32_t> _num_waiters{0};
};

} // namespace starrocks::pipeline
