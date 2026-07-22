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

#include "exec/pipeline/exchange/local_exchange_source_operator.h"

#include "column/chunk.h"
#include "column/chunk_extra_data.h"
#include "exec/pipeline/exchange/local_exchange.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

// Used for PassthroughExchanger.
// The input chunk is most likely full, so we don't merge it to avoid copying chunk data.
void LocalExchangeSourceOperator::add_chunk(ChunkPtr chunk) {
    auto notify = defer_notify();
    bool wake_siblings = false;
    {
        std::lock_guard<std::mutex> l(_chunk_lock);
        if (_is_finished) {
            return;
        }
        auto memory_entry = std::make_shared<ChunkBufferMemoryEntry>(_memory_manager.get(), chunk->memory_usage(),
                                                                     chunk->num_rows());
        _full_chunk_queue.push(PassthroughChunk{std::move(chunk), std::move(memory_entry)});
        // Work-stealing: when this backlog first crosses the threshold, wake idle siblings so
        // they can steal a chunk. Edge-triggered (see _steal_notified) to avoid a notify storm,
        // and gated on the session switch so there is zero behavior change when disabled.
        if (!_steal_notified && support_steal() &&
            _factory->runtime_state()->query_options().enable_pipeline_work_stealing &&
            _full_chunk_queue.size() >= _steal_backlog_threshold()) {
            _steal_notified = true;
            wake_siblings = true;
        }
    }
    if (wake_siblings) {
        // Wake parked steal waiters (must run outside _chunk_lock). Targeted steal_trigger via the
        // factory's registry, not the broadcast notify_source_observers -- the broadcast is gated
        // by on_source_update's own-lane predicate, which never reschedules a drained thief.
        down_cast<LocalExchangeSourceOperatorFactory*>(_factory)->steal_waiters().notify_all();
    }
}

// Used for PartitionExchanger.
// Only enqueue the partition chunk information here, and merge chunk in pull_chunk().
// The shared `memory_entry` accounts for the source chunk's memory once across all
// partition shards; it is only released back to the manager when the last shard is
// pulled (or cleared on set_finished). The caller (Partitioner::send_chunk) is
// responsible for unpacking const columns BEFORE constructing the entry, so the
// recorded memory matches the buffered post-unpack footprint.
Status LocalExchangeSourceOperator::add_chunk(ChunkPtr chunk, const std::shared_ptr<std::vector<uint32_t>>& indexes,
                                              uint32_t from, uint32_t size,
                                              std::shared_ptr<ChunkBufferMemoryEntry> memory_entry) {
    auto notify = defer_notify();
    // Work-stealing keep-alive (partition mode): edge-triggered per-lane wake of parked thieves,
    // fired AFTER the lock is released (never notify while locked) and only when stealing is on.
    // notify_lane_backlog fires at most once per rising backlog crossing (not per chunk), so a hot
    // producer does not cause an N*N notify storm. Backlog is captured under the lock; the buffered
    // slice count is a cheap monotone proxy for the thief's precise stealable_backlog() (a spurious
    // wake at most costs one re-park -- the thief re-checks stealable_backlog() and partition safety).
    bool steal_on = false;
    size_t backlog = 0;
    DeferOp steal_notify([&]() {
        if (steal_on) {
            down_cast<LocalExchangeSourceOperatorFactory*>(_factory)->steal_waiters().notify_lane_backlog(
                    _driver_sequence, backlog, _steal_backlog_threshold());
        }
    });
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (_is_finished) {
        return Status::OK();
    }

    _partition_chunk_queue.emplace(std::move(chunk), indexes, from, size, std::move(memory_entry));
    _partition_rows_num += size;
    steal_on = support_steal() && _factory->runtime_state()->query_options().enable_pipeline_work_stealing;
    backlog = _partition_chunk_queue.size();

    return Status::OK();
}

Status LocalExchangeSourceOperator::add_chunk(const std::vector<std::optional<std::string>>& partition_key,
                                              const std::vector<std::pair<TypeDescriptor, ColumnPtr>>& partition_datum,
                                              ChunkUniquePtr chunk) {
    auto notify = defer_notify();
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (_is_finished) {
        return Status::OK();
    }

    // unpack chunk's const column, since Chunk#append_selective cannot be const column
    chunk->unpack_and_duplicate_const_columns();
    auto num_rows = chunk->num_rows();
    auto memory_entry =
            std::make_shared<ChunkBufferMemoryEntry>(_memory_manager.get(), chunk->memory_usage(), num_rows);

    auto& partial = _partition_key2partial_chunks[partition_key];
    partial.queue.push(KeyPartitionChunk{std::move(chunk), std::move(memory_entry)});
    partial.num_rows += num_rows;
    partial.partition_key_datum = partition_datum;
    return Status::OK();
}

bool LocalExchangeSourceOperator::is_finished() const {
    std::lock_guard<std::mutex> l(_chunk_lock);
    return _is_finished && _stolen_chunk == nullptr && _full_chunk_queue.empty() && !_partition_rows_num &&
           _key_partition_pending_chunk_empty();
}

bool LocalExchangeSourceOperator::has_output() const {
    std::lock_guard<std::mutex> l(_chunk_lock);

    return _stolen_chunk != nullptr || !_full_chunk_queue.empty() ||
           _partition_rows_num >= _factory->runtime_state()->chunk_size() || _key_partition_max_rows() > 0 ||
           ((_is_finished || _memory_manager->is_full()) && _partition_rows_num > 0);
}

Status LocalExchangeSourceOperator::set_finished(RuntimeState* state) {
    auto* exchanger = down_cast<LocalExchangeSourceOperatorFactory*>(_factory)->exchanger();
    exchanger->finish_source();
    // notify local-exchange sink
    // notify-condition 1. mem-buffer full 2. all finished
    auto notify = exchanger->defer_notify_sink();
    std::lock_guard<std::mutex> l(_chunk_lock);
    _is_finished = true;

    // Drop all buffered chunks. Every queue entry holds a ChunkBufferMemoryEntry whose
    // destructor refunds memory/rows back to the shared manager, so no manual accounting
    // is needed here.
    _full_chunk_queue = {};
    _partition_chunk_queue = {};
    _partition_key2partial_chunks.clear();
    _partition_rows_num = 0;
    _stolen_chunk = nullptr;
    return Status::OK();
}

StatusOr<ChunkPtr> LocalExchangeSourceOperator::pull_chunk(RuntimeState* state) {
    // notify sink
    {
        // Work-stealing: a chunk stolen from a sibling is emitted first and only once.
        // Emit it before arming the sink notify: a stolen chunk did not free space in this
        // source's own buffer, so it must not wake the sink.
        std::lock_guard<std::mutex> l(_chunk_lock);
        if (_stolen_chunk != nullptr) {
            return std::move(_stolen_chunk);
        }
    }
    auto* exchanger = down_cast<LocalExchangeSourceOperatorFactory*>(_factory)->exchanger();
    auto notify = exchanger->defer_notify_sink();
    ChunkPtr chunk = _pull_passthrough_chunk(state);
    if (chunk == nullptr && _key_partition_pending_chunk_empty()) {
        chunk = _pull_shuffle_chunk(state);
    } else if (chunk == nullptr && !_key_partition_pending_chunk_empty()) {
        chunk = _pull_key_partition_chunk(state);
    }
    return std::move(chunk);
}

std::string LocalExchangeSourceOperator::get_name() const {
    std::string finished = is_finished() ? "X" : "O";
    return fmt::format("{}_{}_{}({}) {{ has_output:{}}}", _name, _plan_node_id, (void*)this, finished, has_output());
}

const size_t min_local_memory_limit = 1LL * 1024 * 1024;

void LocalExchangeSourceOperator::enter_release_memory_mode() {
    // limit the memory limit to a very small value so that the upstream will not write new data until all the data has been pushed to the downstream operators
    size_t max_memory_usage = min_local_memory_limit * _memory_manager->get_max_input_dop();
    _memory_manager->update_max_memory_usage(max_memory_usage);
}

void LocalExchangeSourceOperator::set_execute_mode(int performance_level) {
    enter_release_memory_mode();
}

ChunkPtr LocalExchangeSourceOperator::_pull_passthrough_chunk(RuntimeState* state) {
    PassthroughChunk popped;
    {
        std::lock_guard<std::mutex> l(_chunk_lock);
        if (_full_chunk_queue.empty()) {
            return nullptr;
        }
        popped = std::move(_full_chunk_queue.front());
        _full_chunk_queue.pop();
        // work-stealing: re-arm the sibling-wakeup edge once the backlog drains below threshold.
        if (_steal_notified && _full_chunk_queue.size() < _steal_backlog_threshold()) {
            _steal_notified = false;
        }
    }
    // popped.memory_entry destructs at function exit and refunds memory/rows to the
    // manager outside the lock.
    return std::move(popped.chunk);
}

ChunkPtr LocalExchangeSourceOperator::_pull_shuffle_chunk(RuntimeState* state) {
    std::vector<PartitionChunk> selected_partition_chunks;
    size_t num_rows = 0;
    // Lock during pop partition chunks from queue.
    {
        std::lock_guard<std::mutex> l(_chunk_lock);

        DCHECK(!_partition_chunk_queue.empty());

        while (!_partition_chunk_queue.empty() &&
               num_rows + _partition_chunk_queue.front().size <= state->chunk_size()) {
            num_rows += _partition_chunk_queue.front().size;
            selected_partition_chunks.emplace_back(std::move(_partition_chunk_queue.front()));
            _partition_chunk_queue.pop();
        }
        _partition_rows_num -= num_rows;
    }
    if (selected_partition_chunks.empty()) {
        throw std::runtime_error("local exchange gets empty shuffled chunk.");
    }
    // Unlock during merging partition chunks into a full chunk.
    ChunkPtr chunk = selected_partition_chunks[0].chunk->clone_empty_with_slot();
    chunk->reserve(num_rows);
    for (const auto& partition_chunk : selected_partition_chunks) {
        // NOTE: unpack column if `partition_chunk.chunk` constains const column
        chunk->append_selective(*partition_chunk.chunk, partition_chunk.indexes->data(), partition_chunk.from,
                                partition_chunk.size);
    }
    // selected_partition_chunks goes out of scope here. Each PartitionChunk drops its
    // shared_ptr<ChunkBufferMemoryEntry>; once the last shard for a given source chunk is
    // released, the entry destructor refunds its memory/rows to the manager.
    return chunk;
}

ChunkPtr LocalExchangeSourceOperator::_pull_key_partition_chunk(RuntimeState* state) {
    std::vector<KeyPartitionChunk> selected_partition_chunks;
    size_t num_rows = 0;
    ChunkExtraColumnsDataPtr chunk_extra_data;
    std::vector<std::pair<TypeDescriptor, ColumnPtr>> partition_key_datum;
    {
        std::lock_guard<std::mutex> l(_chunk_lock);
        auto it = _max_row_partition_chunks();
        PartialChunks& partial_chunks = it->second;
        partition_key_datum = partial_chunks.partition_key_datum;
        DCHECK(!partial_chunks.queue.empty());

        while (!partial_chunks.queue.empty() &&
               num_rows + partial_chunks.queue.front().chunk->num_rows() <= state->chunk_size()) {
            num_rows += partial_chunks.queue.front().chunk->num_rows();
            selected_partition_chunks.push_back(std::move(partial_chunks.queue.front()));
            partial_chunks.queue.pop();
        }

        partial_chunks.num_rows -= num_rows;
    }
    Columns partition_key_columns;
    std::vector<ChunkExtraColumnsMeta> extra_metas;
    for (auto& datum : partition_key_datum) {
        auto res = ColumnHelper::create_column(datum.first, true);
        res->append_datum(datum.second->get(0));
        auto ptr = ConstColumn::create(std::move(res), 1);
        partition_key_columns.emplace_back(ptr);
        extra_metas.push_back(ChunkExtraColumnsMeta{datum.first, true /*useless*/, true /*useless*/});
    }
    chunk_extra_data = std::make_shared<ChunkExtraColumnsData>(extra_metas, std::move(partition_key_columns));
    // Unlock during merging partition chunks into a full chunk.
    ChunkPtr chunk = selected_partition_chunks[0].chunk->clone_empty_with_slot();
    chunk->reserve(num_rows);
    chunk->set_extra_data(chunk_extra_data);
    for (const auto& partition_chunk : selected_partition_chunks) {
        // NOTE: unpack column if `partition_chunk.chunk` constains const column
        chunk->append(*partition_chunk.chunk);
    }
    // selected_partition_chunks goes out of scope here; each entry destructor refunds
    // its chunk's memory/rows to the manager outside the lock.
    return chunk;
}

int64_t LocalExchangeSourceOperator::_key_partition_max_rows() const {
    int64_t max_rows = 0;
    for (const auto& partition : _partition_key2partial_chunks) {
        max_rows = std::max(partition.second.num_rows, max_rows);
    }

    return max_rows;
}

std::map<std::vector<std::optional<std::string>>, LocalExchangeSourceOperator::PartialChunks>::iterator
LocalExchangeSourceOperator::_max_row_partition_chunks() {
    auto max_it = std::max_element(_partition_key2partial_chunks.begin(), _partition_key2partial_chunks.end(),
                                   [](auto& lhs, auto& rhs) { return lhs.second.num_rows < rhs.second.num_rows; });

    return max_it;
}

// ===== work-stealing (see PIPELINE_WORK_STEALING_PLAN.md) =====

size_t LocalExchangeSourceOperator::_steal_backlog_threshold() const {
    const int t = _factory->runtime_state()->query_options().pipeline_steal_backlog_threshold;
    return t > 0 ? static_cast<size_t>(t) : 1;
}

bool LocalExchangeSourceOperator::support_steal() const {
    // A source can hand out a stealable unit when it is either partition-free (passthrough:
    // try_steal_unit yields a whole chunk tagged partition_id == -1) or hash-partitioned
    // (shuffle: yields a materialized chunk tagged with this source's partition == driver_seq,
    // consumed by a partition-aware probe). Note this is decoupled from the factory's
    // is_stealable() (which drives only the partition-free barrier).
    auto* exchanger = down_cast<LocalExchangeSourceOperatorFactory*>(_factory)->exchanger();
    return exchanger != nullptr && (exchanger->source_partition_free() || exchanger->is_hash_partitioned());
}

void LocalExchangeSourceOperator::register_steal_waiter() {
    down_cast<LocalExchangeSourceOperatorFactory*>(_factory)->steal_waiters().register_waiter(_driver_sequence,
                                                                                             observer());
}

void LocalExchangeSourceOperator::deregister_steal_waiter() {
    down_cast<LocalExchangeSourceOperatorFactory*>(_factory)->steal_waiters().deregister_waiter(_driver_sequence);
}

size_t LocalExchangeSourceOperator::stealable_backlog() const {
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (!_full_chunk_queue.empty()) {
        return _full_chunk_queue.size(); // passthrough: whole chunks
    }
    // partition (hash shuffle): number of full chunks that can be materialized out of the
    // buffered per-partition rows.
    const size_t chunk_size = _factory->runtime_state()->chunk_size();
    return chunk_size > 0 ? _partition_rows_num / chunk_size : 0;
}

StatusOr<StealUnit> LocalExchangeSourceOperator::try_steal_unit() {
    // Passthrough family: hand out one whole, partition-free chunk (partition_id == -1).
    PassthroughChunk popped;
    {
        std::lock_guard<std::mutex> l(_chunk_lock);
        // Re-check under the lock: the owning driver may have drained the queue since the
        // thief observed stealable_backlog(), and _pull_shuffle_chunk-style paths DCHECK
        // non-empty, so a stealer must never trust a stale has_output()/backlog reading.
        if (_is_finished) {
            return StealUnit{};
        }
        if (!_full_chunk_queue.empty()) {
            popped = std::move(_full_chunk_queue.front());
            _full_chunk_queue.pop();
            if (_steal_notified && _full_chunk_queue.size() < _steal_backlog_threshold()) {
                _steal_notified = false;
            }
        }
    }
    if (popped.chunk != nullptr) {
        // popped.memory_entry destructs here, refunding the shared manager (the chunk left this
        // buffer); the thief carries only the raw chunk, consumed immediately on its own driver.
        StealUnit unit;
        unit.chunk = std::move(popped.chunk);
        unit.partition_id = -1;
        return unit;
    }
    // Partition (hash shuffle) family: materialize one chunk of this partition's rows.
    return _try_steal_shuffle_unit();
}

StatusOr<StealUnit> LocalExchangeSourceOperator::_try_steal_shuffle_unit() {
    std::vector<PartitionChunk> selected;
    size_t num_rows = 0;
    const size_t chunk_size = _factory->runtime_state()->chunk_size();
    {
        std::lock_guard<std::mutex> l(_chunk_lock);
        // Only steal a full chunk; leave the tail for the owner to drain.
        if (_is_finished || _partition_rows_num < chunk_size) {
            return StealUnit{};
        }
        while (!_partition_chunk_queue.empty() &&
               num_rows + _partition_chunk_queue.front().size <= chunk_size) {
            num_rows += _partition_chunk_queue.front().size;
            selected.emplace_back(std::move(_partition_chunk_queue.front()));
            _partition_chunk_queue.pop();
        }
        _partition_rows_num -= num_rows;
    }
    if (selected.empty()) {
        return StealUnit{};
    }
    // Merge the sliced shards into a fresh chunk outside the lock (mirrors _pull_shuffle_chunk);
    // each shard's shared memory entry refunds the manager as it drops at scope end. The thief
    // carries a brand-new materialized chunk, so there is no shared-buffer entanglement.
    ChunkPtr chunk = selected[0].chunk->clone_empty_with_slot();
    chunk->reserve(num_rows);
    for (const auto& partition_chunk : selected) {
        chunk->append_selective(*partition_chunk.chunk, partition_chunk.indexes->data(), partition_chunk.from,
                                partition_chunk.size);
    }
    StealUnit unit;
    unit.chunk = std::move(chunk);
    // A partition source's rows all belong to its own partition == driver_sequence; tag the unit
    // so a partition-aware probe looks it up against the matching peer build table.
    unit.partition_id = get_driver_sequence();
    return unit;
}

Status LocalExchangeSourceOperator::accept_stolen_unit(StealUnit unit) {
    if (unit.chunk == nullptr) {
        return Status::InternalError("local exchange steal expects a chunk work unit");
    }
    std::lock_guard<std::mutex> l(_chunk_lock);
    // A thief holds at most one stolen chunk at a time (the driver steals once per round and
    // only when its own source is empty). Refuse rather than drop if one is still pending.
    if (_stolen_chunk != nullptr) {
        return Status::InternalError("local exchange source already holds a stolen chunk");
    }
    if (_is_finished) {
        return Status::InternalError("local exchange source is finished");
    }
    _stolen_chunk = std::move(unit.chunk);
    return Status::OK();
}

bool LocalExchangeSourceOperatorFactory::is_stealable() const {
    return _exchanger != nullptr && _exchanger->source_partition_free();
}

} // namespace starrocks::pipeline
