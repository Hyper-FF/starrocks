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

#include <gtest/gtest.h>

#include <memory>
#include <numeric>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "exec/chunk_buffer_memory_manager.h"
#include "exec/exec_env.h"
#include "exec/pipeline/exchange/local_exchange.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/query_context.h"
#include "gutil/casts.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

namespace starrocks::pipeline {

class LocalExchangeMemoryTest : public ::testing::Test {
public:
    void SetUp() override {
        _exec_env = ExecEnv::GetInstance();

        _query_context = std::make_shared<QueryContext>();
        _query_context->set_query_execution_services(&_exec_env->query_execution_services());
        _query_context->init_mem_tracker(-1, RuntimeEnv::GetInstance()->process_mem_tracker());

        TQueryOptions query_options;
        query_options.batch_size = 4096;
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(_fragment_id, query_options, query_globals,
                                                        &_exec_env->query_execution_services(), _exec_env);
        _query_context->attach_to_runtime_state(_runtime_state.get());
        _runtime_state->init_instance_mem_tracker();

        _memory_manager = std::make_shared<ChunkBufferMemoryManager>(_dop, 1024 * 1024);
        _source_op_factory = std::make_unique<LocalExchangeSourceOperatorFactory>(0, 1, _memory_manager);
        _source_op_factory->set_runtime_state(_runtime_state.get());
        for (size_t i = 0; i < _dop; i++) {
            _sources.emplace_back(_source_op_factory->create(_dop, i));
        }
        // Wire up an exchanger so pull_chunk / set_finished have a valid factory exchanger.
        _exchanger = std::make_unique<PassthroughExchanger>(_memory_manager, _source_op_factory.get());
    }

protected:
    LocalExchangeSourceOperator* _source(size_t i) {
        return down_cast<LocalExchangeSourceOperator*>(_sources[i].get());
    }

    static ChunkPtr _make_int_chunk(size_t num_rows) {
        auto chunk = std::make_shared<Chunk>();
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false);
        for (size_t i = 0; i < num_rows; i++) {
            col->append_datum(Datum(static_cast<int32_t>(i)));
        }
        chunk->append_column(std::move(col), 1);
        return chunk;
    }

    std::shared_ptr<ChunkBufferMemoryManager> _memory_manager;
    std::unique_ptr<LocalExchangeSourceOperatorFactory> _source_op_factory;
    std::unique_ptr<PassthroughExchanger> _exchanger;
    TUniqueId _fragment_id;
    ExecEnv* _exec_env;
    std::shared_ptr<QueryContext> _query_context;
    std::shared_ptr<RuntimeState> _runtime_state;
    std::vector<OperatorPtr> _sources;
    size_t _dop = 3;
};

// ChunkBufferMemoryEntry should add memory/rows to the manager on construction and
// refund them exactly once on destruction.
TEST_F(LocalExchangeMemoryTest, chunk_buffer_memory_entry_raii) {
    constexpr size_t kMemory = 1024;
    constexpr size_t kRows = 32;
    EXPECT_EQ(0, _memory_manager->get_memory_usage());
    {
        auto entry = std::make_shared<ChunkBufferMemoryEntry>(_memory_manager.get(), kMemory, kRows);
        EXPECT_EQ(kMemory, _memory_manager->get_memory_usage());
        EXPECT_EQ(kMemory, entry->memory_usage());
        EXPECT_EQ(kRows, entry->num_rows());

        // A second shared_ptr to the same entry must NOT double-account.
        auto alias = entry;
        EXPECT_EQ(kMemory, _memory_manager->get_memory_usage());
    }
    EXPECT_EQ(0, _memory_manager->get_memory_usage());
}

// Passthrough: a chunk added to the queue accounts its memory once, and pulling it
// refunds the manager.
TEST_F(LocalExchangeMemoryTest, passthrough_add_pull_accounting) {
    auto chunk = _make_int_chunk(100);
    const size_t mem = chunk->memory_usage();
    const size_t rows = chunk->num_rows();
    ASSERT_GT(mem, 0);

    EXPECT_EQ(0, _memory_manager->get_memory_usage());
    _source(0)->add_chunk(chunk);
    EXPECT_EQ(mem, _memory_manager->get_memory_usage());

    auto pulled = _source(0)->pull_chunk(_runtime_state.get());
    ASSERT_OK(pulled);
    ASSERT_TRUE(pulled.value() != nullptr);
    EXPECT_EQ(rows, pulled.value()->num_rows());
    EXPECT_EQ(0, _memory_manager->get_memory_usage());
}

// Partition shuffle fan-out: a chunk shared across N source shards must add its memory
// to the manager exactly once, not N times.
TEST_F(LocalExchangeMemoryTest, partition_fanout_accounts_chunk_once) {
    auto chunk = _make_int_chunk(_dop * 4);
    const size_t mem = chunk->memory_usage();
    const size_t rows = chunk->num_rows();
    ASSERT_GT(mem, 0);

    EXPECT_EQ(0, _memory_manager->get_memory_usage());
    auto entry = std::make_shared<ChunkBufferMemoryEntry>(_memory_manager.get(), mem, rows);
    EXPECT_EQ(mem, _memory_manager->get_memory_usage());

    auto indexes = std::make_shared<std::vector<uint32_t>>(rows);
    std::iota(indexes->begin(), indexes->end(), 0);

    const size_t per_shard = rows / _dop;
    for (size_t i = 0; i < _dop; ++i) {
        ASSERT_OK(_source(i)->add_chunk(chunk, indexes, i * per_shard, per_shard, entry));
        // Memory must remain at exactly the chunk's memory regardless of how many
        // shards have been enqueued.
        EXPECT_EQ(mem, _memory_manager->get_memory_usage());
    }

    // Drop the local reference; sources still hold the entry, so the record stays.
    entry.reset();
    EXPECT_EQ(mem, _memory_manager->get_memory_usage());
}

// The manager record for a fanned-out chunk must persist until the LAST source shard
// is consumed; pulling from earlier sources should not refund anything.
TEST_F(LocalExchangeMemoryTest, partition_memory_released_on_last_pull) {
    auto chunk = _make_int_chunk(_dop * 4);
    const size_t mem = chunk->memory_usage();
    const size_t rows = chunk->num_rows();
    ASSERT_GT(mem, 0);

    auto entry = std::make_shared<ChunkBufferMemoryEntry>(_memory_manager.get(), mem, rows);
    auto indexes = std::make_shared<std::vector<uint32_t>>(rows);
    std::iota(indexes->begin(), indexes->end(), 0);

    const size_t per_shard = rows / _dop;
    for (size_t i = 0; i < _dop; ++i) {
        ASSERT_OK(_source(i)->add_chunk(chunk, indexes, i * per_shard, per_shard, entry));
    }
    entry.reset();
    EXPECT_EQ(mem, _memory_manager->get_memory_usage());

    // Pull from every source except the last; memory must still be charged because
    // the final source still holds a reference to the shared entry.
    for (size_t i = 0; i + 1 < _dop; ++i) {
        auto pulled = _source(i)->pull_chunk(_runtime_state.get());
        ASSERT_OK(pulled);
        ASSERT_TRUE(pulled.value() != nullptr);
        EXPECT_EQ(per_shard, pulled.value()->num_rows());
        EXPECT_EQ(mem, _memory_manager->get_memory_usage());
    }

    // Pulling from the last source releases the entry and refunds memory.
    auto pulled = _source(_dop - 1)->pull_chunk(_runtime_state.get());
    ASSERT_OK(pulled);
    ASSERT_TRUE(pulled.value() != nullptr);
    EXPECT_EQ(0, _memory_manager->get_memory_usage());
}

// Two distinct fanned-out chunks should be accounted independently — each should add
// its own memory exactly once and be refunded only when fully consumed.
TEST_F(LocalExchangeMemoryTest, multiple_partition_chunks_independent) {
    auto chunk_a = _make_int_chunk(_dop * 4);
    auto chunk_b = _make_int_chunk(_dop * 4);
    const size_t mem_a = chunk_a->memory_usage();
    const size_t mem_b = chunk_b->memory_usage();
    ASSERT_GT(mem_a, 0);
    ASSERT_GT(mem_b, 0);

    auto entry_a = std::make_shared<ChunkBufferMemoryEntry>(_memory_manager.get(), mem_a, chunk_a->num_rows());
    auto entry_b = std::make_shared<ChunkBufferMemoryEntry>(_memory_manager.get(), mem_b, chunk_b->num_rows());

    auto indexes_a = std::make_shared<std::vector<uint32_t>>(chunk_a->num_rows());
    auto indexes_b = std::make_shared<std::vector<uint32_t>>(chunk_b->num_rows());
    std::iota(indexes_a->begin(), indexes_a->end(), 0);
    std::iota(indexes_b->begin(), indexes_b->end(), 0);

    const size_t per_shard = chunk_a->num_rows() / _dop;
    for (size_t i = 0; i < _dop; ++i) {
        ASSERT_OK(_source(i)->add_chunk(chunk_a, indexes_a, i * per_shard, per_shard, entry_a));
        ASSERT_OK(_source(i)->add_chunk(chunk_b, indexes_b, i * per_shard, per_shard, entry_b));
    }
    entry_a.reset();
    entry_b.reset();
    EXPECT_EQ(mem_a + mem_b, _memory_manager->get_memory_usage());

    // Each pull merges all enqueued shards for that source up to chunk_size, so a
    // single pull releases this source's contribution to BOTH entries.
    for (size_t i = 0; i + 1 < _dop; ++i) {
        auto pulled = _source(i)->pull_chunk(_runtime_state.get());
        ASSERT_OK(pulled);
        EXPECT_EQ(mem_a + mem_b, _memory_manager->get_memory_usage());
    }
    auto pulled = _source(_dop - 1)->pull_chunk(_runtime_state.get());
    ASSERT_OK(pulled);
    EXPECT_EQ(0, _memory_manager->get_memory_usage());
}

// set_finished must refund every byte/row buffered across passthrough, partition, and
// key-partition queues.
TEST_F(LocalExchangeMemoryTest, set_finished_refunds_all_queues) {
    auto chunk_pass = _make_int_chunk(50);
    auto chunk_part = _make_int_chunk(_dop * 4);

    _source(0)->add_chunk(chunk_pass);

    auto entry = std::make_shared<ChunkBufferMemoryEntry>(_memory_manager.get(), chunk_part->memory_usage(),
                                                          chunk_part->num_rows());
    auto indexes = std::make_shared<std::vector<uint32_t>>(chunk_part->num_rows());
    std::iota(indexes->begin(), indexes->end(), 0);
    const size_t per_shard = chunk_part->num_rows() / _dop;
    for (size_t i = 0; i < _dop; ++i) {
        ASSERT_OK(_source(i)->add_chunk(chunk_part, indexes, i * per_shard, per_shard, entry));
    }
    entry.reset();

    EXPECT_GT(_memory_manager->get_memory_usage(), 0);

    for (auto& source : _sources) {
        ASSERT_OK(source->set_finished(_runtime_state.get()));
    }
    EXPECT_EQ(0, _memory_manager->get_memory_usage());
}

// Regression: a chunk built from const columns has tiny pre-unpack memory_usage
// (O(1) per column) but is materialized to O(num_rows) by Partitioner::send_chunk
// before being buffered. The memory manager must record the post-unpack footprint,
// otherwise back-pressure under-counts buffered memory by orders of magnitude.
TEST_F(LocalExchangeMemoryTest, const_column_accounted_after_unpack) {
    constexpr size_t kNumRows = 4096;

    // Chunk with a single const-INT column over kNumRows virtual rows.
    auto inner = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false);
    inner->append_datum(Datum(static_cast<int32_t>(42)));
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ConstColumn::create(std::move(inner), kNumRows), 1);
    ASSERT_EQ(kNumRows, chunk->num_rows());

    const size_t pre_unpack_mem = chunk->memory_usage();

    // Partition + send a copy of the chunk. RandomPartitioner needs no expr context.
    auto partitioner = std::make_unique<RandomPartitioner>(_source_op_factory.get());
    auto partition_row_indexes = std::make_shared<std::vector<uint32_t>>(kNumRows);
    ASSERT_OK(partitioner->partition_chunk(chunk, _dop, *partition_row_indexes));
    ASSERT_OK(partitioner->send_chunk(chunk, partition_row_indexes));

    // After send_chunk, the const column has been materialized into a regular column
    // of kNumRows datums; the per-shard buffered memory is therefore much larger than
    // what chunk->memory_usage() would have reported pre-unpack.
    const size_t post_unpack_mem = chunk->memory_usage();
    EXPECT_GT(post_unpack_mem, pre_unpack_mem) << "const-column unpack should grow chunk memory_usage";

    // The manager must record the post-unpack footprint, not the tiny pre-unpack
    // value. Equality (rather than >=) ensures we accounted exactly once.
    EXPECT_EQ(post_unpack_mem, _memory_manager->get_memory_usage());
}

// is_full() must reflect actual chunk memory, not an N-times-inflated value. With a
// chunk whose true memory fits inside the manager limit but whose Nx-inflated value
// would exceed it, single-count accounting must keep the manager not-full.
TEST_F(LocalExchangeMemoryTest, fanout_does_not_falsely_trip_is_full) {
    // per-driver limit = 1MB, dop = 3 → manager max = 3MB. Use a "claimed" chunk
    // memory of 1.5MB: real single-count = 1.5MB < 3MB (not full); a broken N=3
    // accounting would be 4.5MB ≥ 3MB (full).
    auto chunk = _make_int_chunk(_dop * 4);
    constexpr size_t kClaimedChunkMemory = 1500UL * 1024;

    auto entry =
            std::make_shared<ChunkBufferMemoryEntry>(_memory_manager.get(), kClaimedChunkMemory, chunk->num_rows());
    auto indexes = std::make_shared<std::vector<uint32_t>>(chunk->num_rows());
    std::iota(indexes->begin(), indexes->end(), 0);
    const size_t per_shard = chunk->num_rows() / _dop;
    for (size_t i = 0; i < _dop; ++i) {
        ASSERT_OK(_source(i)->add_chunk(chunk, indexes, i * per_shard, per_shard, entry));
    }
    entry.reset();

    EXPECT_EQ(kClaimedChunkMemory, _memory_manager->get_memory_usage());
    EXPECT_FALSE(_memory_manager->is_full());
}

// Early-finish on one source must release ONLY that source's contribution to a
// shared fan-out entry; the chunk's memory record must persist while at least one
// other source still holds a reference. After the remaining sources drain, memory
// drops to zero — i.e. early finish does not leak nor pile up memory.
TEST_F(LocalExchangeMemoryTest, early_finish_one_source_keeps_other_refs) {
    auto chunk = _make_int_chunk(_dop * 4);
    const size_t mem = chunk->memory_usage();
    ASSERT_GT(mem, 0);

    auto entry = std::make_shared<ChunkBufferMemoryEntry>(_memory_manager.get(), mem, chunk->num_rows());
    auto indexes = std::make_shared<std::vector<uint32_t>>(chunk->num_rows());
    std::iota(indexes->begin(), indexes->end(), 0);
    const size_t per_shard = chunk->num_rows() / _dop;
    for (size_t i = 0; i < _dop; ++i) {
        ASSERT_OK(_source(i)->add_chunk(chunk, indexes, i * per_shard, per_shard, entry));
    }
    entry.reset();
    EXPECT_EQ(mem, _memory_manager->get_memory_usage());

    // Source 0 finishes early. Its queue clears and its ref to the shared entry drops,
    // but sources 1..N-1 still hold the entry, so the chunk's memory stays accounted.
    ASSERT_OK(_source(0)->set_finished(_runtime_state.get()));
    EXPECT_EQ(mem, _memory_manager->get_memory_usage());

    // Drain the remaining sources; memory only returns to zero after the last ref drops.
    for (size_t i = 1; i + 1 < _dop; ++i) {
        auto pulled = _source(i)->pull_chunk(_runtime_state.get());
        ASSERT_OK(pulled);
        EXPECT_EQ(mem, _memory_manager->get_memory_usage());
    }
    auto pulled = _source(_dop - 1)->pull_chunk(_runtime_state.get());
    ASSERT_OK(pulled);
    EXPECT_EQ(0, _memory_manager->get_memory_usage());
}

// After one source has finished, subsequent send_chunk fan-outs must only accumulate
// references in the remaining active sources — memory still scales as 1x per chunk,
// not by the (already-dead) finished sources.
TEST_F(LocalExchangeMemoryTest, send_after_partial_finish_only_charges_active_sources) {
    // Source 0 finishes before any chunk arrives.
    ASSERT_OK(_source(0)->set_finished(_runtime_state.get()));
    EXPECT_EQ(0, _memory_manager->get_memory_usage());

    // Fan a chunk out via the partitioner. The finished source's add_chunk no-ops,
    // and add_chunk takes the entry by value, so its parameter copy is dropped on
    // return — the entry ends up held only by the active sources.
    auto chunk = _make_int_chunk(_dop * 4);
    auto partitioner = std::make_unique<RandomPartitioner>(_source_op_factory.get());
    auto partition_row_indexes = std::make_shared<std::vector<uint32_t>>(chunk->num_rows());
    ASSERT_OK(partitioner->partition_chunk(chunk, _dop, *partition_row_indexes));
    ASSERT_OK(partitioner->send_chunk(chunk, partition_row_indexes));

    const size_t mem = chunk->memory_usage();
    EXPECT_EQ(mem, _memory_manager->get_memory_usage());

    // Drain the active sources; memory drops to zero after the last one pulls.
    for (size_t i = 1; i + 1 < _dop; ++i) {
        auto pulled = _source(i)->pull_chunk(_runtime_state.get());
        ASSERT_OK(pulled);
        EXPECT_EQ(mem, _memory_manager->get_memory_usage());
    }
    auto pulled = _source(_dop - 1)->pull_chunk(_runtime_state.get());
    ASSERT_OK(pulled);
    EXPECT_EQ(0, _memory_manager->get_memory_usage());
}

// When ALL sources have already finished, send_chunk briefly registers the entry
// (constructor side-effect) but every per-source add_chunk no-ops, so the entry has
// no holders by the time send_chunk returns and the manager goes back to zero.
// Verifies that finishing all consumers does not strand chunks in the manager.
TEST_F(LocalExchangeMemoryTest, send_after_all_sources_finished_refunds_immediately) {
    for (auto& source : _sources) {
        ASSERT_OK(source->set_finished(_runtime_state.get()));
    }
    EXPECT_EQ(0, _memory_manager->get_memory_usage());

    auto chunk = _make_int_chunk(_dop * 4);
    auto partitioner = std::make_unique<RandomPartitioner>(_source_op_factory.get());
    auto partition_row_indexes = std::make_shared<std::vector<uint32_t>>(chunk->num_rows());
    ASSERT_OK(partitioner->partition_chunk(chunk, _dop, *partition_row_indexes));
    ASSERT_OK(partitioner->send_chunk(chunk, partition_row_indexes));

    EXPECT_EQ(0, _memory_manager->get_memory_usage());
}

// ===== work-stealing (see PIPELINE_WORK_STEALING_PLAN.md) =====

// A passthrough local exchange is stealable and its sources opt into stealing.
TEST_F(LocalExchangeMemoryTest, steal_support_predicate) {
    EXPECT_TRUE(_source_op_factory->is_stealable());
    EXPECT_TRUE(_source(0)->support_steal());
    EXPECT_EQ(0u, _source(0)->stealable_backlog()); // nothing buffered yet
}

// try_steal_unit detaches exactly one whole, partition-free chunk from the victim and
// refunds its memory on the victim side; accept_stolen_unit hands it to a sibling, which
// then emits it via has_output()/pull_chunk(). The stolen chunk bypasses the shared
// ChunkBufferMemoryManager (already refunded), so manager usage tracks only the victim's
// remaining buffer.
TEST_F(LocalExchangeMemoryTest, steal_transfers_one_chunk_between_siblings) {
    auto c0 = _make_int_chunk(100);
    auto c1 = _make_int_chunk(100);
    const size_t mem = c0->memory_usage();
    _source(0)->add_chunk(c0);
    _source(0)->add_chunk(c1);
    EXPECT_EQ(2u, _source(0)->stealable_backlog());
    EXPECT_EQ(2 * mem, _memory_manager->get_memory_usage());

    auto unit_or = _source(0)->try_steal_unit();
    ASSERT_OK(unit_or);
    StealUnit unit = std::move(unit_or.value());
    ASSERT_TRUE(unit.valid());
    EXPECT_TRUE(unit.chunk != nullptr);
    EXPECT_EQ(-1, unit.partition_id);
    EXPECT_EQ(1u, _source(0)->stealable_backlog());
    EXPECT_EQ(mem, _memory_manager->get_memory_usage()); // one chunk refunded on the victim side

    EXPECT_FALSE(_source(1)->has_output());
    ASSERT_OK(_source(1)->accept_stolen_unit(std::move(unit)));
    EXPECT_TRUE(_source(1)->has_output());

    auto pulled = _source(1)->pull_chunk(_runtime_state.get());
    ASSERT_OK(pulled);
    ASSERT_TRUE(pulled.value() != nullptr);
    EXPECT_EQ(100, pulled.value()->num_rows());
    EXPECT_EQ(mem, _memory_manager->get_memory_usage()); // stolen chunk never re-accounted
    EXPECT_FALSE(_source(1)->has_output());
}

// Stealing from an empty (or drained) source yields an invalid unit, never a crash.
TEST_F(LocalExchangeMemoryTest, steal_from_empty_is_invalid) {
    auto unit_or = _source(0)->try_steal_unit();
    ASSERT_OK(unit_or);
    EXPECT_FALSE(unit_or.value().valid());
}

// A thief holds at most one stolen chunk; a second accept before draining is refused
// rather than silently dropping the pending chunk.
TEST_F(LocalExchangeMemoryTest, steal_second_accept_refused_while_pending) {
    _source(0)->add_chunk(_make_int_chunk(10));
    _source(0)->add_chunk(_make_int_chunk(10));
    auto u1 = _source(0)->try_steal_unit();
    ASSERT_OK(u1);
    auto u2 = _source(0)->try_steal_unit();
    ASSERT_OK(u2);
    ASSERT_OK(_source(1)->accept_stolen_unit(std::move(u1.value())));
    EXPECT_FALSE(_source(1)->accept_stolen_unit(std::move(u2.value())).ok());
}

// End-to-end row conservation: chunks buffered on one source, one stolen by a sibling,
// draining both yields exactly the original row count -- no loss, no duplication.
TEST_F(LocalExchangeMemoryTest, steal_conserves_rows) {
    size_t total_added = 0;
    for (size_t r : {30u, 40u, 50u}) {
        _source(0)->add_chunk(_make_int_chunk(r));
        total_added += r;
    }

    auto unit_or = _source(0)->try_steal_unit();
    ASSERT_OK(unit_or);
    ASSERT_TRUE(unit_or.value().valid());
    ASSERT_OK(_source(1)->accept_stolen_unit(std::move(unit_or.value())));

    size_t total_pulled = 0;
    for (int s : {0, 1}) {
        while (_source(s)->has_output()) {
            auto pulled = _source(s)->pull_chunk(_runtime_state.get());
            ASSERT_OK(pulled);
            if (pulled.value() != nullptr) {
                total_pulled += pulled.value()->num_rows();
            }
        }
    }
    EXPECT_EQ(total_added, total_pulled);
    EXPECT_EQ(0, _memory_manager->get_memory_usage());
}

// Partition (hash shuffle) steal: try_steal_unit materializes one chunk's worth of the
// victim partition's buffered slices into a fresh chunk tagged with the victim's partition id
// (== driver_sequence), refunding the materialized slices' memory and leaving the tail behind.
TEST_F(LocalExchangeMemoryTest, steal_shuffle_unit_materializes_and_tags) {
    // Feed source(0) three 2000-row partition slices (6000 rows > chunk_size 4096).
    const size_t slice_rows = 2000;
    auto chunk = _make_int_chunk(slice_rows);
    const size_t mem = chunk->memory_usage();
    auto indexes = std::make_shared<std::vector<uint32_t>>(slice_rows);
    std::iota(indexes->begin(), indexes->end(), 0);
    for (int i = 0; i < 3; ++i) {
        auto entry = std::make_shared<ChunkBufferMemoryEntry>(_memory_manager.get(), mem, slice_rows);
        ASSERT_OK(_source(0)->add_chunk(chunk, indexes, 0, slice_rows, entry));
    }
    EXPECT_EQ(3 * mem, _memory_manager->get_memory_usage());
    EXPECT_EQ(1u, _source(0)->stealable_backlog()); // 6000 rows / 4096 = 1 full stealable chunk

    auto unit_or = _source(0)->try_steal_unit();
    ASSERT_OK(unit_or);
    StealUnit unit = std::move(unit_or.value());
    ASSERT_TRUE(unit.valid());
    ASSERT_TRUE(unit.chunk != nullptr);
    EXPECT_EQ(0, unit.partition_id); // source(0) owns partition 0
    // Two 2000-row slices fit under chunk_size (4096); the third would overflow, so it stays.
    EXPECT_EQ(2 * slice_rows, unit.chunk->num_rows());
    // The two materialized slices' memory entries were refunded; one slice remains buffered.
    EXPECT_EQ(mem, _memory_manager->get_memory_usage());
}

} // namespace starrocks::pipeline
