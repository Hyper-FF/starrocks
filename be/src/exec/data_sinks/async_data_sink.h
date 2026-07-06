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

#include "column/vectorized_fwd.h"
#include "exec_primitive/data_sink.h"

namespace starrocks {

// Lifecycle of an AsyncDataSink, advanced non-blockingly by the caller via advance().
//   kOpening : open RPCs issued, not yet ready to accept chunks
//   kOpen    : open finished, accepting chunks (subject to is_full() back-pressure)
//   kClosing : end-of-input signalled (mark_finishing), EOS sent, draining in-flight requests
//   kClosed  : fully closed and committed (terminal)
enum class AsyncSinkState { kOpening, kOpen, kClosing, kClosed };

/**
 * @brief A DataSink whose open/send/close work is asynchronous and driven as a state machine.
 *
 * The pipeline drives the sink by repeatedly calling advance() (which performs one non-blocking
 * step of open finalize / EOS / drain / commit) and reading state(). The three
 * try_open/is_open_done/open_wait and try_close/is_close_done/close_wait triplets that used to be
 * orchestrated by the caller are subsumed by advance()/state(); implementations keep whatever
 * internal machinery they need private. The synchronous DataSink open()/send_chunk()/close()
 * interface (used by the non-pipeline executor and unit tests) is inherited unchanged.
 */
class AsyncDataSink : public DataSink {
public:
    /**
     * @brief Advance the async lifecycle by at most one non-blocking step.
     *
     * Finalizes open once the open RPCs return; after mark_finishing(), sends EOS and drains the
     * in-flight requests, then commits. Idempotent and cheap once state()==kClosed, so it is safe
     * to call on every driver tick. Returns an error status if the underlying async work failed.
     */
    virtual Status advance(RuntimeState* state) = 0;

    /**
     * @brief The current lifecycle state. Pure query (no side effects); reflects the progress made
     * by the most recent advance() call.
     */
    virtual AsyncSinkState state() const = 0;

    /**
     * @brief Signal that no more chunks will be appended. The next advance() transitions the sink
     * from kOpen to kClosing (sending EOS).
     */
    virtual void mark_finishing() = 0;

    /**
     * @brief Whether the sink is full and cannot accept more data right now (back-pressure within
     * kOpen). Orthogonal to state().
     */
    virtual bool is_full() = 0;

    // Append a chunk without blocking. Precondition: state()==kOpen and !is_full(). If the chunk
    // triggers automatic partition creation, the sink stashes it internally and resends it from a
    // later advance() once the partition exists (is_full() stays true until then) — the caller does
    // not manage any retry. Takes a ChunkPtr so the sink can retain the chunk across advance() ticks.
    virtual Status send_chunk_nonblocking(RuntimeState* state, const ChunkPtr& chunk) = 0;

    virtual void set_profile(RuntimeProfile* profile) = 0;
};

} // namespace starrocks