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

#include "compute_env/query/partition_scan_range_pruner.h"

#include "column/column_helper.h"
#include "column/runtime_type_traits.h"
#include "common/logging.h"
#include "common/object_pool.h"
#include "compute_env/runtime_range_pruner.hpp"
#include "exprs/expr.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "runtime/runtime_state.h"
#include "types/date_value.h"
#include "types/logical_type.h"

namespace starrocks {

namespace {
// A partition column range is expanded one value at a time, so the width of the range decides how
// much memory the expansion costs. FE caps what it sends with dynamic_partition_prune_values_limit
// (4096 by default), but that is a tuning knob on the producer and not a bound this side can lean
// on. The value list only ever feeds scan range pruning, which is an optimization, so a range that
// cannot be expanded safely degrades to "this column prunes nothing" instead of failing the query.
constexpr uint64_t kMaxPartitionColumnValues = 1024 * 1024;
} // namespace

StatusOr<ColumnPtr> build_partition_col_values(const SlotDescriptor* slot_desc, const TKeyRange& column_range,
                                               ObjectPool* obj_pool, RuntimeState* state) {
    if (column_range.__isset.list_values && !column_range.list_values.empty()) {
        if (column_range.list_values.size() > kMaxPartitionColumnValues) {
            LOG_EVERY_N(WARNING, 1000) << "partition column range has too many list values to expand, pruning "
                                          "skipped for column: "
                                       << column_range.column_name << ", values: " << column_range.list_values.size();
            return nullptr;
        }
        std::vector<ExprContext*> ctxs;
        for (const auto& obj : column_range.list_values) {
            RETURN_IF_ERROR(ExprFactory::create_expr_tree(obj_pool, obj, &ctxs.emplace_back(), state));
            DCHECK(ctxs.back()->root()->is_constant());
        }
        RETURN_IF_ERROR(ExprExecutor::prepare(ctxs, state));
        RETURN_IF_ERROR(ExprExecutor::open(ctxs, state));

        // The size argument resizes the column, it does not reserve: pass 0 and reserve separately,
        // or every value appended below lands after a run of default-valued rows.
        auto col = ColumnHelper::create_column(slot_desc->type(), true, false, 0, false);
        col->reserve(column_range.list_values.size());
        for (auto* ctx : ctxs) {
            ASSIGN_OR_RETURN(ColumnPtr v, ctx->root()->evaluate_const(ctx));
            if (v->only_null()) {
                col->append_nulls(1);
                continue;
            }
            auto cv = ColumnHelper::unpack_and_duplicate_const_column(1, v);
            col->append(*cv, 0, 1);
        }
        ExprExecutor::close(ctxs, state);
        return col;
    } else if (column_range.__isset.begin_key && column_range.__isset.end_key) {
        if (slot_desc->type().is_date_type()) {
            auto lower_julian = date::from_date_literal(column_range.begin_key);
            auto upper_julian = date::from_date_literal(column_range.end_key);
            if (upper_julian < lower_julian) {
                return nullptr;
            }
            // Count the values instead of walking to the upper bound: JulianDate is an int32, and
            // `date++` past its maximum is undefined, which the counted form cannot reach.
            // JulianDate is an int32, so widen before subtracting: the difference of two extremes
            // does not fit back into one.
            const uint64_t count =
                    static_cast<uint64_t>(static_cast<int64_t>(upper_julian) - static_cast<int64_t>(lower_julian)) + 1;
            if (count > kMaxPartitionColumnValues) {
                LOG_EVERY_N(WARNING, 1000)
                        << "partition column range is too wide to expand, pruning skipped for column: "
                        << column_range.column_name << ", values: " << count;
                return nullptr;
            }

            auto col = ColumnHelper::create_column(slot_desc->type(), true, false, 0, false);
            col->reserve(count + 1);
            for (uint64_t i = 0; i < count; i++) {
                col->append_datum(Datum(DateValue{static_cast<JulianDate>(lower_julian + static_cast<int64_t>(i))}));
            }
            if (column_range.__isset.has_null && column_range.has_null) {
                col->append_nulls(1);
            }
            return col;
        } else if (slot_desc->type().is_integer_type()) {
            if (column_range.end_key < column_range.begin_key) {
                return nullptr;
            }
            // Count the values in unsigned arithmetic, and drive the loop by that count rather than
            // by `v <= end_key`. `end_key - begin_key` overflows int64 for a range that spans the
            // type, and a `v++` walk never terminates when end_key is the int64 maximum: the
            // increment wraps to the minimum and the condition stays true forever.
            const uint64_t width =
                    static_cast<uint64_t>(column_range.end_key) - static_cast<uint64_t>(column_range.begin_key);
            if (width >= kMaxPartitionColumnValues) {
                LOG_EVERY_N(WARNING, 1000)
                        << "partition column range is too wide to expand, pruning skipped for column: "
                        << column_range.column_name << ", width: " << width;
                return nullptr;
            }
            const uint64_t count = width + 1;
            auto col = ColumnHelper::create_column(slot_desc->type(), true, false, 0, false);
            col->reserve(count + 1);
            // begin_key + i stays within [begin_key, end_key] for every i < count, so it cannot
            // overflow even when end_key is the int64 maximum.
#define M(TYPE)                                                                                                   \
    if (slot_desc->type().type == TYPE) {                                                                         \
        for (uint64_t i = 0; i < count; i++) {                                                                    \
            col->append_datum(                                                                                    \
                    Datum((RunTimeTypeTraits<TYPE>::CppType)(column_range.begin_key + static_cast<int64_t>(i)))); \
        }                                                                                                         \
    }
            APPLY_FOR_ALL_INT_TYPE(M)
#undef M
            if (column_range.__isset.has_null && column_range.has_null) {
                col->append_nulls(1);
            }
            return col;
        } else {
            DCHECK(false) << "Unsupported partition column range, column name: " << column_range.column_name;
            return Status::InternalError("Unsupported partition column range");
        }
    } else {
        DCHECK(false) << "Unsupported partition column range, column name: " << column_range.column_name;
        return Status::InternalError("Unsupported partition column range");
    }
}

Status prune_scan_ranges_by_partition_conjuncts(RuntimeState* state, const TupleDescriptor* tuple_desc,
                                                const std::vector<ExprContext*>& partition_conjunct_ctxs,
                                                const std::vector<TScanRangeParams>& scan_ranges,
                                                std::vector<TScanRangeParams>* pruned_scan_ranges) {
    if (partition_conjunct_ctxs.empty() || tuple_desc == nullptr) {
        *pruned_scan_ranges = scan_ranges;
        return Status::OK();
    }

    phmap::flat_hash_map<std::string, SlotDescriptor*> column_name_to_slot;
    for (auto* slot : tuple_desc->slots()) {
        column_name_to_slot[slot->col_name()] = slot;
    }

    ObjectPool obj_pool;
    std::vector<TScanRangeParams> temp;
    temp.reserve(scan_ranges.size());
    for (const auto& scan_range : scan_ranges) {
        const auto& internal_range = scan_range.scan_range.internal_scan_range;
        if (!internal_range.__isset.partition_column_ranges || internal_range.partition_column_ranges.empty()) {
            temp.emplace_back(scan_range);
            continue;
        }

        bool is_pruned = false;
        for (const auto& partition_column_range : internal_range.partition_column_ranges) {
            auto it = column_name_to_slot.find(partition_column_range.column_name);
            if (it == column_name_to_slot.end()) {
                continue;
            }
            auto* slot = it->second;
            ASSIGN_OR_RETURN(auto col, build_partition_col_values(slot, partition_column_range, &obj_pool, state));
            if (col == nullptr) {
                // The range could not be expanded; it contributes no pruning.
                continue;
            }

            Chunk partition_cols_chunk;
            Filter filter(col->size(), 1);
            partition_cols_chunk.append_column(std::move(col), slot->id());

            std::vector<SlotId> slot_ids;
            for (auto* ctx : partition_conjunct_ctxs) {
                slot_ids.clear();
                if (ctx->root()->get_slot_ids(&slot_ids) != 1 || slot_ids[0] != slot->id()) {
                    continue;
                }
                ASSIGN_OR_RETURN(ColumnPtr column, ctx->evaluate(&partition_cols_chunk, filter.data()));
                size_t true_count = ColumnHelper::count_true_with_notnull(column);
                if (true_count == column->size()) {
                    continue;
                } else if (0 == true_count) {
                    is_pruned = true;
                    break;
                } else {
                    bool all_zero = false;
                    ColumnHelper::merge_two_filters(column, &filter, &all_zero);
                    if (all_zero) {
                        is_pruned = true;
                        break;
                    }
                }
            }
            if (is_pruned) {
                break;
            }
        }

        if (!is_pruned) {
            temp.emplace_back(scan_range);
        }
    }
    pruned_scan_ranges->swap(temp);
    return Status::OK();
}

} // namespace starrocks
