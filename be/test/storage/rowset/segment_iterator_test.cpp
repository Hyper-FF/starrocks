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

#include "storage/rowset/segment_iterator.h"

#include <fmt/core.h>

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/column_helper.h"
#include "common/config_exec_fwd.h"
#include "common/config_rowset_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "common/object_pool.h"
#include "exec/runtime_filter/runtime_filter_probe.h"
#include "fs/fs_memory.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "gtest/gtest.h"
#include "runtime/global_dict/types.h"
#include "runtime/global_dict/types_fwd_decl.h"
#include "storage/chunk_helper.h"
#include "storage/column_predicate_rewriter.h"
#include "storage/olap_common.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/dictcode_column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/runtime_filter_predicate.h"
#include "storage/tablet_schema_helper.h"
#include "types/logical_type.h"

namespace starrocks {

class SegmentIteratorTest : public ::testing::Test {
public:
    void SetUp() override {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(_fs->create_dir(kSegmentDir).ok());
    }

    void TearDown() override {}

    const std::string kSegmentDir = "/segment_test";
    std::shared_ptr<MemoryFileSystem> _fs = nullptr;
};

namespace test {
struct TabletSchemaBuilder {
private:
    std::vector<ColumnPB> _column_pbs;
    ColumnPB _create_pb(int32_t id, const std::string& name, bool nullable, LogicalType type, bool key) {
        ColumnPB col;

        col.set_unique_id(id);
        col.set_name(name);
        col.set_is_key(key);
        col.set_is_nullable(nullable);

        if (type == TYPE_INT) {
            col.set_type("INT");
            col.set_length(4);
            col.set_index_length(4);
        } else if (type == TYPE_VARCHAR) {
            col.set_type("VARCHAR");
            col.set_length(128);
            col.set_index_length(16);
        } else if (type == TYPE_CHAR) {
            col.set_type("CHAR");
            col.set_length(20);
            col.set_index_length(20);
        }

        col.set_default_value("0");
        col.set_aggregation("NONE");
        col.set_is_bf_column(false);
        col.set_has_bitmap_index(false);
        return col;
    }

public:
    TabletSchemaBuilder& create(int32_t id, bool nullable, LogicalType type, bool key = false) {
        if (type == TYPE_INT) {
            _column_pbs.emplace_back(_create_pb(id, std::to_string(id), nullable, type, key));
        } else if (type == TYPE_VARCHAR) {
            _column_pbs.emplace_back(_create_pb(id, std::to_string(id), nullable, type, key));
        } else if (type == TYPE_CHAR) {
            _column_pbs.emplace_back(_create_pb(id, std::to_string(id), nullable, type, key));
        } else {
            __builtin_unreachable();
        }
        return *this;
    }
    TabletSchemaBuilder& set_length(size_t length) {
        _column_pbs.back().set_length(length);
        return *this;
    }

    std::unique_ptr<TabletSchema> build() { return TabletSchemaHelper::create_tablet_schema(_column_pbs); }
};

struct TabletDataBuilder {
    TabletDataBuilder(SegmentWriter& writer_, std::shared_ptr<TabletSchema> schema, size_t chunk_size_,
                      size_t num_rows_)
            : writer(writer_), _schema(std::move(schema)), chunk_size(chunk_size_), num_rows(num_rows_) {}

    template <class Provider>
    Status append(int32_t idx, Provider&& provider) {
        std::vector<uint32_t> column_indexes = {static_cast<unsigned int>(idx)};

        RETURN_IF_ERROR(writer.init(column_indexes, true));

        auto schema = ChunkHelper::convert_schema(_schema, column_indexes);
        auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto cols = chunk->mutable_columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->append_datum(provider(static_cast<int32_t>(i * chunk_size + j)));
            }
            RETURN_IF_ERROR(writer.append_chunk(*chunk));
        }

        RETURN_IF_ERROR(writer.finalize_columns(&index_size));
        return Status::OK();
    }

    Status finalize_footer() { return writer.finalize_footer(&file_size); }

private:
    SegmentWriter& writer;
    std::shared_ptr<TabletSchema> _schema;
    const size_t chunk_size;
    const size_t num_rows;

    uint64_t file_size = 0;
    uint64_t index_size = 0;
};

struct VecSchemaBuilder {
    VecSchemaBuilder& add(int32_t id, const std::string& name, LogicalType type, bool nullable = false) {
        auto f = std::make_shared<Field>(id, name, type, -1, -1, nullable);
        f->set_uid(id);
        vec_schema.append(f);
        return *this;
    }
    Schema build() { return std::move(vec_schema); }

private:
    Schema vec_schema;
};
} // namespace test

// This case is only triggered by dictionary inconsistencies.
// NOLINTNEXTLINE
TEST_F(SegmentIteratorTest, TestGlobalDictNotSuperSetWithUnusedColumn) {
    using namespace starrocks::test;

    std::string file_name = kSegmentDir + "/low_card_cols_unused_column";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;
    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_INT, true)
                                                          .create(2, false, TYPE_VARCHAR)
                                                          .create(3, false, TYPE_INT)
                                                          .create(4, false, TYPE_INT)
                                                          .create(5, false, TYPE_VARCHAR)
                                                          .build();
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    const int32_t chunk_size = config::vector_chunk_size;
    const size_t num_rows = 10000;

    auto i32_provider = [](int32_t i) { return i; };
    std::vector<std::string> values(64);
    for (int i = 0; i < values.size(); ++i) {
        values[i] = fmt::format("prefix-{}", i);
    }
    auto slice_provider = [&values](int32_t i) { return Slice(values[i % values.size()]); };

    // tablet data builder
    TabletDataBuilder segment_data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(segment_data_builder.append(0, i32_provider));
    ASSERT_OK(segment_data_builder.append(1, slice_provider));
    ASSERT_OK(segment_data_builder.append(2, i32_provider));
    ASSERT_OK(segment_data_builder.append(3, i32_provider));
    ASSERT_OK(segment_data_builder.append(4, slice_provider));
    ASSERT_OK(segment_data_builder.finalize_footer());

    //
    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    SegmentReadOptions seg_options;
    OlapReaderStatistics stats;
    seg_options.fs = _fs;
    seg_options.stats = &stats;
    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_INT)
            .add(1, "c1", TYPE_VARCHAR)
            .add(2, "c2", TYPE_INT)
            .add(3, "c3", TYPE_INT)
            .add(4, "c4", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();
    ObjectPool pool;
    SegmentReadOptions seg_opts;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;

    //
    ColumnIdToGlobalDictMap dict_map;
    GlobalDictMap g_dict1;
    GlobalDictMap g_dict2;
    for (int i = 0; i < values.size() - 1; ++i) {
        g_dict1[Slice(values[i])] = i;
        g_dict2[Slice(values[i])] = i;
    }
    g_dict2[Slice(values[values.size() - 1])] = values.size() - 1;
    dict_map[1] = &g_dict1;
    dict_map[4] = &g_dict2;
    seg_opts.global_dictmaps = &dict_map;
    seg_opts.tablet_schema = tablet_schema;

    std::unique_ptr<ColumnPredicate> predicate;
    predicate.reset(new_column_ge_predicate(get_type_info(TYPE_VARCHAR), 1, "prefix"));
    PredicateAndNode pred_root;
    pred_root.add_child(PredicateColumnNode{predicate.get()});
    seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    ASSERT_OK(chunk_iter->init_encoded_schema(dict_map));
    std::unordered_set<uint32_t> set;
    set.insert(1);
    ASSERT_OK(chunk_iter->init_output_schema(set));

    auto res_chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), chunk_size);

    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    res_chunk->reset();
    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    res_chunk->reset();
    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    res_chunk->reset();
}

// NOLINTNEXTLINE
TEST_F(SegmentIteratorTest, TestGlobalDictNoLocalDictWithUnusedColumn) {
    // prepare dict data
    const int slice_num = 2;
    std::vector<std::string> values;
    const int overflow_sz = 1024 * 1024 + 10; // 1M
    for (int i = 0; i < slice_num; ++i) {
        std::string bigstr;
        bigstr.reserve(overflow_sz);
        for (int j = 0; j < overflow_sz; ++j) {
            bigstr.push_back(j);
        }
        bigstr.push_back(i);
        values.emplace_back(std::move(bigstr));
    }

    std::sort(values.begin(), values.end());

    std::vector<Slice> data_strs;
    for (const auto& data : values) {
        data_strs.emplace_back(data);
    }

    using namespace starrocks::test;

    std::string file_name = kSegmentDir + "/no_dict_unused_column";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_INT, true)
                                                          .create(2, false, TYPE_VARCHAR)
                                                          .set_length(overflow_sz + 10)
                                                          .build();

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 1024;
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    int32_t chunk_size = config::vector_chunk_size;
    size_t num_rows = slice_num;

    auto i32_provider = [](int32_t i) { return i; };
    auto slice_provider = [&data_strs](int32_t i) { return data_strs[i % data_strs.size()]; };

    // tablet data builder
    TabletDataBuilder segment_data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(segment_data_builder.append(0, i32_provider));
    ASSERT_OK(segment_data_builder.append(1, slice_provider));
    ASSERT_OK(segment_data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    SegmentReadOptions seg_options;
    OlapReaderStatistics stats;
    seg_options.fs = _fs;
    seg_options.stats = &stats;
    seg_options.tablet_schema = tablet_schema;

    ColumnIteratorOptions iter_opts;
    ASSIGN_OR_ABORT(auto read_file, _fs->new_random_access_file(segment->file_name()));
    iter_opts.stats = &stats;
    iter_opts.use_page_cache = false;
    iter_opts.read_file = read_file.get();
    iter_opts.check_dict_encoding = true;
    iter_opts.reader_type = READER_QUERY;

    ASSIGN_OR_ABORT(auto scalar_iter, segment->new_column_iterator(tablet_schema->column(1), nullptr));
    ASSERT_OK(scalar_iter->init(iter_opts));
    ASSERT_FALSE(scalar_iter->all_page_dict_encoded());

    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_INT).add(1, "c1", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();

    ObjectPool pool;
    SegmentReadOptions seg_opts;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = tablet_schema;

    ColumnIdToGlobalDictMap dict_map;
    GlobalDictMap g_dict;
    for (int i = 0; i < slice_num; ++i) {
        g_dict[Slice(values[i])] = i;
    }
    dict_map[1] = &g_dict;

    seg_opts.global_dictmaps = &dict_map;
    std::unique_ptr<ColumnPredicate> predicate;
    predicate.reset(new_column_ge_predicate(get_type_info(TYPE_VARCHAR), 1, values[0].c_str()));
    PredicateAndNode pred_root;
    pred_root.add_child(PredicateColumnNode{predicate.get()});
    seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    ASSERT_OK(chunk_iter->init_encoded_schema(dict_map));
    std::unordered_set<uint32_t> set;
    set.insert(1);
    ASSERT_OK(chunk_iter->init_output_schema(set));

    auto res_chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), chunk_size);

    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    res_chunk->reset();
}

// Verify predicate late materialization keeps non-predicate columns correct.
TEST_F(SegmentIteratorTest, TestPredicateLateMaterializationMaterializesRestColumns) {
    using namespace starrocks::test;

    // Force late materialization always on for determinism.
    auto prev_ratio = config::late_materialization_ratio;
    config::late_materialization_ratio = 1000;
    DeferOp reset_ratio([&]() { config::late_materialization_ratio = prev_ratio; });

    std::string file_name = kSegmentDir + "/predicate_late_materialize_all";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_INT, true)
                                                          .create(2, false, TYPE_INT, false)
                                                          .create(3, false, TYPE_INT, false)
                                                          .build();

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 32;
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    const int32_t chunk_size = 64;
    const size_t num_rows = 50;

    auto c0_provider = [](int32_t i) { return i; };
    auto c1_provider = [](int32_t i) { return i % 10; };   // predicate column
    auto c2_provider = [](int32_t i) { return 1000 + i; }; // late materialized column

    TabletDataBuilder data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(data_builder.append(0, c0_provider));
    ASSERT_OK(data_builder.append(1, c1_provider));
    ASSERT_OK(data_builder.append(2, c2_provider));
    ASSERT_OK(data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    VecSchemaBuilder schema_builder;
    // ids must be ordinal, keep contiguous from 0
    schema_builder.add(0, "c0", TYPE_INT).add(1, "c1", TYPE_INT).add(2, "c2", TYPE_INT);
    auto vec_schema = schema_builder.build();

    std::unique_ptr<ColumnPredicate> predicate(new_column_eq_predicate(get_type_info(TYPE_INT), 1, "5"));
    PredicateAndNode pred_root;
    pred_root.add_child(PredicateColumnNode{predicate.get()});

    SegmentReadOptions seg_opts;
    OlapReaderStatistics stats;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = tablet_schema;
    seg_opts.enable_predicate_col_late_materialize = true;
    seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    ASSERT_OK(chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
    ASSERT_OK(chunk_iter->init_output_schema(std::unordered_set<uint32_t>()));

    auto res_chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), config::vector_chunk_size);
    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    ASSERT_EQ(res_chunk->num_rows(), 5); // rows where c1 == 5

    auto c0_col = ColumnHelper::cast_to_raw<TYPE_INT>(res_chunk->get_column_by_index(0));
    auto c1_col = ColumnHelper::cast_to_raw<TYPE_INT>(res_chunk->get_column_by_index(1));
    auto c2_col = ColumnHelper::cast_to_raw<TYPE_INT>(res_chunk->get_column_by_index(2));
    for (size_t i = 0; i < res_chunk->num_rows(); ++i) {
        ASSERT_EQ(c1_col->get_data()[i], 5);
        ASSERT_EQ(c2_col->get_data()[i] - c0_col->get_data()[i], 1000);
    }

    res_chunk->reset();
    ASSERT_TRUE(chunk_iter->get_next(res_chunk.get()).is_end_of_file());
}

// Regression reproducer for StarRocks#72927 — SIGSEGV in
// SegmentIterator::_switch_context when a join runtime filter is pushed down
// onto the segment scan alongside late-materialized predicate columns.
//
// Pre-fix (#72953), repeated execution of a query that
//   (1) enabled join runtime-filter pushdown on a column scan, and
//   (2) was planned with predicate late materialization (two ScanContexts
//       linked cyclically through _context_list[0] <-> _context_list[1]),
// could corrupt iterator pointers in one ScanContext while the other was
// being seeked from _switch_context, surfacing as a vtable deref crash whose
// fault address decoded as ASCII garbage ("OeergeD", "corrats", ...).
//
// This test drives the same code path:
//   - forces late materialization via late_materialization_ratio=1000 so that
//     _init_context builds both _context_list[0] (late-mat) and [1] (full),
//   - enables enable_join_runtime_filter_pushdown,
//   - registers a RuntimeFilterPredicate on the predicate column with a stub
//     RuntimeFilterProbeDescriptor whose runtime_filter() returns nullptr (so
//     predicate evaluation no-ops, but _build_column_oriented_rf still runs
//     and ScanContext::runtime_filters_by_column is wired up),
//   - re-creates the SegmentIterator several times to make any latent
//     iterator-lifetime corruption observable to ASan / heap reuse.
//
// On a healthy build this test simply succeeds. Under ASan it provides a
// reproducible hook for the original crash class.
TEST_F(SegmentIteratorTest, regression_72927_runtime_filter_pushdown_with_late_materialization) {
    using namespace starrocks::test;

    auto prev_ratio = config::late_materialization_ratio;
    config::late_materialization_ratio = 1000;
    DeferOp reset_ratio([&]() { config::late_materialization_ratio = prev_ratio; });

    std::string file_name = kSegmentDir + "/rf_pushdown_late_materialize";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_INT, true)
                                                          .create(2, false, TYPE_INT, false)
                                                          .create(3, false, TYPE_INT, false)
                                                          .build();

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 32;
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    const int32_t chunk_size = 64;
    const size_t num_rows = 200;

    auto c0_provider = [](int32_t i) { return i; };
    auto c1_provider = [](int32_t i) { return i % 10; };
    auto c2_provider = [](int32_t i) { return 1000 + i; };

    TabletDataBuilder data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(data_builder.append(0, c0_provider));
    ASSERT_OK(data_builder.append(1, c1_provider));
    ASSERT_OK(data_builder.append(2, c2_provider));
    ASSERT_OK(data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_INT).add(1, "c1", TYPE_INT).add(2, "c2", TYPE_INT);
    auto vec_schema = schema_builder.build();

    // Stub RuntimeFilterProbeDescriptor: never has a runtime filter attached,
    // so RuntimeFilterPredicate::init() returns false and the predicate eval
    // becomes a no-op, while the segment-iterator side of the pushdown
    // (_build_column_oriented_rf, ScanContext::runtime_filters_by_column,
    // _switch_context) is still fully wired up.
    auto rf_desc = std::make_unique<RuntimeFilterProbeDescriptor>();
    // Keep ownership alive for the lifetime of the seg_opts copy.
    std::vector<std::unique_ptr<RuntimeFilterPredicate>> rf_pred_owner;
    rf_pred_owner.emplace_back(std::make_unique<RuntimeFilterPredicate>(rf_desc.get(), /*column_id=*/1));

    // Drive _init_context / _switch_context many times. Heap reuse between
    // iterations is what tipped over the original bug after 2-5 runs.
    constexpr int kIterations = 10;
    for (int i = 0; i < kIterations; ++i) {
        OlapReaderStatistics stats;

        // Predicate must outlive the segment iterator since PredicateTree
        // stores a raw ColumnPredicate*.
        std::unique_ptr<ColumnPredicate> predicate(new_column_eq_predicate(get_type_info(TYPE_INT), 1, "5"));
        PredicateAndNode pred_root;
        pred_root.add_child(PredicateColumnNode{predicate.get()});

        RuntimeFilterPredicates rf_preds(/*driver_sequence=*/0);
        rf_preds.add_predicate(rf_pred_owner.back().get());

        SegmentReadOptions seg_opts;
        seg_opts.fs = _fs;
        seg_opts.stats = &stats;
        seg_opts.tablet_schema = tablet_schema;
        seg_opts.enable_predicate_col_late_materialize = true;
        seg_opts.enable_join_runtime_filter_pushdown = true;
        seg_opts.runtime_filter_preds = std::move(rf_preds);
        seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

        auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
        ASSERT_OK(chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
        ASSERT_OK(chunk_iter->init_output_schema(std::unordered_set<uint32_t>()));

        auto res_chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), config::vector_chunk_size);
        size_t total_rows = 0;
        Status st;
        while ((st = chunk_iter->get_next(res_chunk.get())).ok()) {
            total_rows += res_chunk->num_rows();
            res_chunk->reset();
        }
        ASSERT_TRUE(st.is_end_of_file()) << "iteration " << i << ": " << st.to_string();
        // c1 = row_idx % 10 == 5 → 20 hits out of 200 rows.
        ASSERT_EQ(total_rows, 20u) << "iteration " << i;
    }
}

// Reproduce the crash class from issue #72927: a runtime filter registered on a
// non-predicate column triggers DCHECK(_column_ids_to_column_iterators.contains(cid))
// inside _build_column_oriented_rf when the late-materialization context is built.
// In the original bug the SIGSEGV came later (in _switch_context), but the DCHECK
// pinpoints the same root condition.  Run the debug binary to observe SIGABRT.
TEST_F(SegmentIteratorTest, regression_72927_rf_on_non_predicate_col_triggers_dcheck) {
    using namespace starrocks::test;

    auto prev_ratio = config::late_materialization_ratio;
    config::late_materialization_ratio = 1000;
    DeferOp reset_ratio([&]() { config::late_materialization_ratio = prev_ratio; });

    std::string file_name = kSegmentDir + "/rf_pushdown_dcheck";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_INT, true)
                                                          .create(2, false, TYPE_INT, false)
                                                          .create(3, false, TYPE_INT, false)
                                                          .build();

    SegmentWriterOptions wopts;
    wopts.num_rows_per_block = 32;
    SegmentWriter seg_writer(std::move(wfile), 0, tablet_schema, wopts);

    TabletDataBuilder data_builder(seg_writer, tablet_schema, 64, 200);
    ASSERT_OK(data_builder.append(0, [](int32_t i) { return i; }));
    ASSERT_OK(data_builder.append(1, [](int32_t i) { return i % 10; }));
    ASSERT_OK(data_builder.append(2, [](int32_t i) { return 1000 + i; }));
    ASSERT_OK(data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);

    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_INT).add(1, "c1", TYPE_INT).add(2, "c2", TYPE_INT);
    auto vec_schema = schema_builder.build();

    auto rf_desc = std::make_unique<RuntimeFilterProbeDescriptor>();

#if DCHECK_IS_ON()
    // RF is on cid=2 (c2).  The predicate is on cid=1, so in the
    // late-materialization context only cid=1 has a column iterator.
    // _build_column_oriented_rf then fires:
    //   DCHECK(_column_ids_to_column_iterators.contains(cid))  // cid=2, absent → abort
    ASSERT_DEATH(
            {
                OlapReaderStatistics stats;
                std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_INT), 1, "5"));
                PredicateAndNode pred_root;
                pred_root.add_child(PredicateColumnNode{pred.get()});

                RuntimeFilterPredicate rf_pred(rf_desc.get(), /*column_id=*/2);
                RuntimeFilterPredicates rf_preds(/*driver_sequence=*/0);
                rf_preds.add_predicate(&rf_pred);

                SegmentReadOptions seg_opts;
                seg_opts.fs = _fs;
                seg_opts.stats = &stats;
                seg_opts.tablet_schema = tablet_schema;
                seg_opts.enable_predicate_col_late_materialize = true;
                seg_opts.enable_join_runtime_filter_pushdown = true;
                seg_opts.runtime_filter_preds = std::move(rf_preds);
                seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

                auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
                (void)chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS);
                (void)chunk_iter->init_output_schema(std::unordered_set<uint32_t>());
                auto res_chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), 64);
                (void)chunk_iter->get_next(res_chunk.get());
            },
            "_column_ids_to_column_iterators");
#else
    GTEST_SKIP() << "DCHECK is disabled; run a debug build to observe SIGABRT from "
                    "segment_iterator.cpp _build_column_oriented_rf";
#endif
}

// Direct reproduction of the SIGSEGV mechanism behind #72927.  In the
// production trace, BE built a GlobalDictCodeColumnIterator on a flat-JSON
// sub-column whose underlying iterator did not actually hold the local dict
// the FE was promised (because SegmentMetaCollecter mis-reported it; see
// PR #72953).  Later, decode_dict_codes -> SIMDGather::gather dereferenced
// the iterator's _local_to_global pointer with codes that were not valid
// indices into it, producing the segfault that surfaced as a crash in
// _switch_context after a couple of iterations.
//
// This test rebuilds the exact failing state directly:
//   - GlobalDictCodeColumnIterator constructed with _local_to_global = nullptr
//   - non-zero local codes pushed into decode_dict_codes
//   - SIMDGather reads nullptr[code]  ->  SIGSEGV
//
// Wrapped in ASSERT_DEATH so the suite passes while documenting the crash.
TEST(GlobalDictCodeColumnIteratorRegressionTest, regression_72927_decode_with_null_local_to_global) {
    ASSERT_DEATH(
            {
                // parent=nullptr is safe here: decode_dict_codes never touches the
                // wrapped parent iterator (it only reads _local_to_global / _dict_size).
                GlobalDictCodeColumnIterator iter(/*cid=*/0, /*parent=*/nullptr,
                                                  /*code_convert_data=*/nullptr,
                                                  /*dict_size=*/8);

                auto codes = Int32Column::create();
                // Codes 1..4 are within the (fake) dict_size=8 window, so the
                // bounds DCHECK on line 65 of dictcode_column_iterator.cpp does
                // not fire and we reach the SIMDGather call that segfaults.
                codes->append(1);
                codes->append(2);
                codes->append(3);
                codes->append(4);

                auto words = Int32Column::create();
                (void)iter.decode_dict_codes(*codes, words.get());
            },
            "");
}

// Verify `_only_output_one_predicate_col_with_filter_push_down` fast path.
TEST_F(SegmentIteratorTest, TestPredicateLateMaterializationSingleColumnPushdown) {
    using namespace starrocks::test;

    std::string file_name = kSegmentDir + "/predicate_late_materialize_pushdown";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_VARCHAR, true).set_length(16).build();

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 32;
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    const int32_t chunk_size = 64;
    const size_t num_rows = 100;
    std::string keep = "keep";
    std::string drop = "drop";
    auto val_provider = [&](int32_t i) { return Slice(i < 50 ? keep : drop); };

    TabletDataBuilder data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(data_builder.append(0, val_provider));
    ASSERT_OK(data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();

    std::unique_ptr<ColumnPredicate> predicate(new_column_eq_predicate(get_type_info(TYPE_VARCHAR), 0, keep.c_str()));
    PredicateAndNode pred_root;
    pred_root.add_child(PredicateColumnNode{predicate.get()});

    SegmentReadOptions seg_opts;
    OlapReaderStatistics stats;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = tablet_schema;
    seg_opts.enable_predicate_col_late_materialize = true;
    seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    ASSERT_OK(chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
    ASSERT_OK(chunk_iter->init_output_schema(std::unordered_set<uint32_t>()));

    auto res_chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), config::vector_chunk_size);
    size_t total = 0;
    while (true) {
        res_chunk->reset();
        auto st = chunk_iter->get_next(res_chunk.get());
        if (st.is_end_of_file()) break;
        ASSERT_OK(st);
        total += res_chunk->num_rows();
        auto col = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(res_chunk->get_column_by_index(0));
        for (size_t i = 0; i < res_chunk->num_rows(); ++i) {
            ASSERT_EQ(col->get_slice(i), Slice(keep));
        }
    }
    ASSERT_EQ(total, 50);
    ASSERT_GE(stats.rows_vec_cond_filtered, 50);
}

// NOLINTNEXTLINE
TEST_F(SegmentIteratorTest, TestGlobalDictNotSuperSet) {
    using namespace starrocks::test;

    std::string file_name = kSegmentDir + "/low_card_cols";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;
    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema =
            builder.create(1, false, TYPE_INT, true).create(2, false, TYPE_VARCHAR).build();
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    const int32_t chunk_size = config::vector_chunk_size;
    const size_t num_rows = 10000;

    const int slice_num = 64;
    std::string prefix = "lowcard-";
    std::vector<std::string> values;
    for (int i = 0; i < slice_num; ++i) {
        values.push_back(prefix + std::to_string(i));
    }

    std::sort(values.begin(), values.end());

    std::vector<Slice> data_strs;
    for (const auto& data : values) {
        data_strs.emplace_back(data);
    }

    auto i32_provider = [](int32_t i) { return i; };
    auto slice_provider = [&data_strs](int32_t i) { return data_strs[i % data_strs.size()]; };

    // tablet data builder
    TabletDataBuilder segment_data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(segment_data_builder.append(0, i32_provider));
    ASSERT_OK(segment_data_builder.append(1, slice_provider));
    ASSERT_OK(segment_data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    SegmentReadOptions seg_options;
    OlapReaderStatistics stats;
    seg_options.fs = _fs;
    seg_options.stats = &stats;

    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_INT).add(1, "c1", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();

    ObjectPool pool;
    SegmentReadOptions seg_opts;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;

    auto* con = pool.add(new ConjunctivePredicates());
    auto type_varchar = get_type_info(TYPE_VARCHAR);
    con->add(pool.add(new_column_ge_predicate(type_varchar, 1, Slice(values[8]))));
    seg_opts.delete_predicates.add(*con);

    ColumnIdToGlobalDictMap dict_map;
    GlobalDictMap g_dict;
    for (int i = 0; i < 8; ++i) {
        g_dict[Slice(values[i])] = i;
    }
    dict_map[1] = &g_dict;

    seg_opts.global_dictmaps = &dict_map;

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    ASSERT_OK(chunk_iter->init_encoded_schema(dict_map));
    ASSERT_OK(chunk_iter->init_output_schema(std::unordered_set<uint32_t>()));

    auto res_chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), chunk_size);

    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    res_chunk->reset();
    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    res_chunk->reset();
    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    res_chunk->reset();
}

// NOLINTNEXTLINE
TEST_F(SegmentIteratorTest, TestGlobalDictNoLocalDict) {
    // prepare dict data
    const int slice_num = 2;
    std::vector<std::string> values;
    const int overflow_sz = 1024 * 1024 + 10; // 1M
    for (int i = 0; i < slice_num; ++i) {
        std::string bigstr;
        bigstr.reserve(overflow_sz);
        for (int j = 0; j < overflow_sz; ++j) {
            bigstr.push_back(j);
        }
        bigstr.push_back(i);
        values.emplace_back(bigstr);
    }

    std::sort(values.begin(), values.end());

    std::vector<Slice> data_strs;
    for (const auto& data : values) {
        data_strs.emplace_back(data);
    }

    using namespace starrocks::test;

    std::string file_name = kSegmentDir + "/no_dict";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_INT, true)
                                                          .create(2, false, TYPE_VARCHAR)
                                                          .set_length(overflow_sz + 10)
                                                          .build();

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 1024;
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    int32_t chunk_size = config::vector_chunk_size;
    size_t num_rows = slice_num;

    auto i32_provider = [](int32_t i) { return i; };
    auto slice_provider = [&data_strs](int32_t i) { return data_strs[i % data_strs.size()]; };

    // tablet data builder
    TabletDataBuilder segment_data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(segment_data_builder.append(0, i32_provider));
    ASSERT_OK(segment_data_builder.append(1, slice_provider));
    ASSERT_OK(segment_data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    SegmentReadOptions seg_options;
    OlapReaderStatistics stats;
    seg_options.fs = _fs;
    seg_options.stats = &stats;

    ColumnIteratorOptions iter_opts;
    ASSIGN_OR_ABORT(auto read_file, _fs->new_random_access_file(segment->file_name()));
    iter_opts.stats = &stats;
    iter_opts.use_page_cache = false;
    iter_opts.read_file = read_file.get();
    iter_opts.check_dict_encoding = true;
    iter_opts.reader_type = READER_QUERY;
    ASSIGN_OR_ABORT(auto scalar_iter, segment->new_column_iterator(tablet_schema->column(1), nullptr));
    ASSERT_OK(scalar_iter->init(iter_opts));
    ASSERT_FALSE(scalar_iter->all_page_dict_encoded());

    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_INT).add(1, "c1", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();

    ObjectPool pool;
    SegmentReadOptions seg_opts;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;

    ColumnIdToGlobalDictMap dict_map;
    GlobalDictMap g_dict;
    for (int i = 0; i < slice_num; ++i) {
        g_dict[Slice(values[i])] = i;
    }
    dict_map[1] = &g_dict;

    seg_opts.global_dictmaps = &dict_map;

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    ASSERT_TRUE(chunk_iter->init_encoded_schema(dict_map).ok());
    ASSERT_OK(chunk_iter->init_output_schema(std::unordered_set<uint32_t>()));

    auto res_chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), chunk_size);

    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    res_chunk->reset();
}

TEST_F(SegmentIteratorTest, testBasicColumnHashIsCongruentFilter) {
    const int slice_num = 6;
    std::vector<std::string> values;
    const int overflow_sz = 32;
    for (int i = 0; i < slice_num; ++i) {
        std::string bigstr;
        bigstr.reserve(overflow_sz);
        for (int j = 0; j < overflow_sz; ++j) {
            bigstr.push_back(j);
        }
        bigstr.push_back(i);
        values.emplace_back(std::move(bigstr));
    }

    std::sort(values.begin(), values.end());

    std::vector<Slice> data_strs;
    for (const auto& data : values) {
        data_strs.emplace_back(data);
    }

    using namespace starrocks::test;

    std::string file_name = kSegmentDir + "/basic_column_hash_is_congruent_filter";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_VARCHAR, true)
                                                          .set_length(2048)
                                                          .create(2, false, TYPE_VARCHAR, false)
                                                          .set_length(2048)
                                                          .build();
    SegmentWriterOptions opts;
    opts.num_rows_per_block = 1024;
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    int32_t chunk_size = config::vector_chunk_size;
    size_t num_rows = slice_num;

    auto slice_provider = [&data_strs](int32_t i) { return data_strs[i % data_strs.size()]; };

    // tablet data builder
    TabletDataBuilder segment_data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(segment_data_builder.append(0, slice_provider));
    ASSERT_OK(segment_data_builder.append(1, slice_provider));
    ASSERT_OK(segment_data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    SegmentReadOptions seg_options;
    OlapReaderStatistics stats;
    seg_options.fs = _fs;
    seg_options.stats = &stats;

    ColumnIdToGlobalDictMap dict_map;
    GlobalDictMap g_dict;
    for (int i = 0; i < slice_num; ++i) {
        g_dict[Slice(values[i])] = i;
    }
    dict_map[1] = &g_dict;
    seg_options.global_dictmaps = &dict_map;

    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_VARCHAR).add(1, "c1", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_options);
    int num_columns = 2;
    ASSERT_OK(chunk_iter->init_output_schema(std::unordered_set<uint32_t>()));

    auto res_chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), chunk_size);
    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    ASSERT_TRUE(res_chunk->num_rows() == 6);
    ASSERT_TRUE(res_chunk->num_columns() == num_columns);
}

// Test CHAR column storage with VARCHAR predicate zone map filtering after fast schema evolution
TEST_F(SegmentIteratorTest, testCharToVarcharZoneMapFilter) {
    // Create tablet schema with CHAR column
    std::shared_ptr<TabletSchema> tablet_schema = test::TabletSchemaBuilder()
                                                          .create(0, false, TYPE_INT, true)  // Primary key column
                                                          .create(1, false, TYPE_CHAR, true) // CHAR column
                                                          .build();

    // Create test data with CHAR values
    std::vector<std::string> char_values = {"abc", "def", "ghi", "jkl"};

    // Build segment using TabletDataBuilder pattern
    std::string file_name = kSegmentDir + "/char_to_varchar_zone_map_test";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
    SegmentWriterOptions opts;
    opts.num_rows_per_block = 2; // Expect two data blocks
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    // Write all key columns together first
    std::vector<uint32_t> key_column_indexes{0, 1};
    ASSERT_OK(writer.init(key_column_indexes, true));

    auto schema = ChunkHelper::convert_schema(tablet_schema, key_column_indexes);
    auto chunk = ChunkHelper::new_chunk(schema, 1024);

    // Add data rows - create providers for each column
    auto int_provider = [](int32_t i) { return Datum(i); };
    auto char_provider = [&char_values](int32_t i) { return Datum(Slice(char_values[i])); };

    // Fill the chunk with data
    chunk->reset();
    auto cols = chunk->mutable_columns();
    for (int i = 0; i < 4; ++i) {
        cols[0]->append_datum(int_provider(i));
        cols[1]->append_datum(char_provider(i));
    }
    ASSERT_OK(writer.append_chunk(*chunk));

    uint64_t index_size = 0;
    ASSERT_OK(writer.finalize_columns(&index_size));

    uint64_t file_size = 0;
    ASSERT_OK(writer.finalize_footer(&file_size));

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), 4);

    // Create VARCHAR query schema
    test::VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_INT).add(1, "c1", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();

    // Create TabletSchema for query with VARCHAR column (schema evolution from CHAR to VARCHAR)
    test::TabletSchemaBuilder query_schema_builder;
    std::shared_ptr<TabletSchema> query_tablet_schema =
            query_schema_builder.create(0, false, TYPE_INT, true).create(1, false, TYPE_VARCHAR, true).build();

    // Test 1: VARCHAR predicate that should match CHAR data
    {
        SegmentReadOptions seg_opts;
        seg_opts.fs = _fs;
        OlapReaderStatistics stats;
        seg_opts.stats = &stats;
        seg_opts.tablet_schema = query_tablet_schema;

        ObjectPool pool;
        auto type_varchar = get_type_info(TYPE_VARCHAR);
        auto predicate = pool.add(new_column_eq_predicate(type_varchar, 1, "abc"));
        PredicateAndNode pred_root;
        pred_root.add_child(PredicateColumnNode{predicate});
        seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

        // Set up zone map predicate tree for zone map filtering
        ASSERT_OK(ZonemapPredicatesRewriter::rewrite_predicate_tree(&pool, seg_opts.pred_tree,
                                                                    seg_opts.pred_tree_for_zone_map));

        auto chunk_iter_res = segment->new_iterator(vec_schema, seg_opts);
        ASSERT_OK(chunk_iter_res.status());
        const auto& chunk_iter = chunk_iter_res.value();
        ASSERT_OK(chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));

        auto res_chunk = ChunkHelper::new_chunk(chunk_iter->schema(), 1024);
        ASSERT_OK(chunk_iter->get_next(res_chunk.get()));

        // Should return exactly one row: c0=0, c1="abc"
        ASSERT_EQ(res_chunk->num_rows(), 1);
        auto int_col = ColumnHelper::cast_to_raw<TYPE_INT>(res_chunk->get_column_by_index(0));
        auto varchar_col = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(res_chunk->get_column_by_index(1));
        ASSERT_EQ(int_col->get_data()[0], 0);
        ASSERT_EQ(varchar_col->get_slice(0), Slice("abc"));

        // Should be no more data
        res_chunk->reset();
        ASSERT_TRUE(chunk_iter->get_next(res_chunk.get()).is_end_of_file());
        ASSERT_EQ(0, stats.segment_stats_filtered);
        ASSERT_EQ(0, stats.rows_stats_filtered);
    }

    // Test 2: VARCHAR predicate that should not match any CHAR data which is filtered by segment-level zonemap index
    {
        SegmentReadOptions seg_opts;
        seg_opts.fs = _fs;
        OlapReaderStatistics stats;
        seg_opts.stats = &stats;
        seg_opts.tablet_schema = query_tablet_schema;

        ObjectPool pool;
        auto type_varchar = get_type_info(TYPE_VARCHAR);
        auto predicate = pool.add(new_column_eq_predicate(type_varchar, 1, "xyz"));
        PredicateAndNode pred_root;
        pred_root.add_child(PredicateColumnNode{predicate});
        seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

        // Set up zone map predicate tree for zone map filtering
        ASSERT_OK(ZonemapPredicatesRewriter::rewrite_predicate_tree(&pool, seg_opts.pred_tree,
                                                                    seg_opts.pred_tree_for_zone_map));

        auto chunk_iter_res = segment->new_iterator(vec_schema, seg_opts);
        ASSERT_TRUE(chunk_iter_res.status().is_end_of_file());
        ASSERT_EQ(4, stats.segment_stats_filtered);
        ASSERT_EQ(0, stats.rows_stats_filtered);
    }

    // Test 3: VARCHAR predicate that should not match any CHAR data which is filtered by page-level zonemap
    {
        SegmentReadOptions seg_opts;
        seg_opts.fs = _fs;
        OlapReaderStatistics stats;
        seg_opts.stats = &stats;
        seg_opts.tablet_schema = query_tablet_schema;

        ObjectPool pool;
        auto type_varchar = get_type_info(TYPE_VARCHAR);
        auto predicate = pool.add(new_column_eq_predicate(type_varchar, 1, "aa"));
        PredicateAndNode pred_root;
        pred_root.add_child(PredicateColumnNode{predicate});
        seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

        // Set up zone map predicate tree for zone map filtering
        ASSERT_OK(ZonemapPredicatesRewriter::rewrite_predicate_tree(&pool, seg_opts.pred_tree,
                                                                    seg_opts.pred_tree_for_zone_map));

        config::enable_index_segment_level_zonemap_filter = false;
        DeferOp op([&]() { config::enable_index_segment_level_zonemap_filter = true; });
        auto chunk_iter_res = segment->new_iterator(vec_schema, seg_opts);
        ASSERT_OK(chunk_iter_res.status());
        const auto& chunk_iter = chunk_iter_res.value();
        ASSERT_OK(chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
        auto res_chunk = ChunkHelper::new_chunk(chunk_iter->schema(), 1024);
        auto status = chunk_iter->get_next(res_chunk.get());
        ASSERT_TRUE(status.is_end_of_file());
        ASSERT_EQ(res_chunk->num_rows(), 0);
        ASSERT_EQ(0, stats.segment_stats_filtered);
        ASSERT_EQ(4, stats.rows_stats_filtered);
    }

    // Test 4: VARCHAR range predicate
    {
        SegmentReadOptions seg_opts;
        seg_opts.fs = _fs;
        OlapReaderStatistics stats;
        seg_opts.stats = &stats;
        seg_opts.tablet_schema = query_tablet_schema;

        ObjectPool pool;
        auto type_varchar = get_type_info(TYPE_VARCHAR);
        auto predicate1 = pool.add(new_column_ge_predicate(type_varchar, 1, "def"));
        auto predicate2 = pool.add(new_column_le_predicate(type_varchar, 1, "ghi"));
        PredicateAndNode pred_root;
        pred_root.add_child(PredicateColumnNode{predicate1});
        pred_root.add_child(PredicateColumnNode{predicate2});
        seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

        // Set up zone map predicate tree for zone map filtering
        ASSERT_OK(ZonemapPredicatesRewriter::rewrite_predicate_tree(&pool, seg_opts.pred_tree,
                                                                    seg_opts.pred_tree_for_zone_map));

        auto chunk_iter_res = segment->new_iterator(vec_schema, seg_opts);
        ASSERT_OK(chunk_iter_res.status());
        const auto& chunk_iter = chunk_iter_res.value();
        ASSERT_OK(chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));

        auto res_chunk = ChunkHelper::new_chunk(chunk_iter->schema(), 1024);
        ASSERT_OK(chunk_iter->get_next(res_chunk.get()));

        // Should return two rows: c0=1,2 with c1="def","ghi"
        ASSERT_EQ(res_chunk->num_rows(), 2);
        auto int_col = down_cast<const Int32Column*>(res_chunk->get_column_raw_ptr_by_index(0));
        auto varchar_col = down_cast<const BinaryColumn*>(res_chunk->get_column_raw_ptr_by_index(1));
        ASSERT_EQ(int_col->get_data()[0], 1);
        ASSERT_EQ(int_col->get_data()[1], 2);
        ASSERT_EQ(varchar_col->get_slice(0), Slice("def"));
        ASSERT_EQ(varchar_col->get_slice(1), Slice("ghi"));
        ASSERT_EQ(0, stats.segment_stats_filtered);
        ASSERT_EQ(0, stats.rows_stats_filtered);
    }
}

} // namespace starrocks
