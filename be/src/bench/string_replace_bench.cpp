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

// Benchmarks StringFunctions::replace across the shapes that behave differently:
//
//   NO_MATCH        the pattern does not occur. The common case in practice, and the one a
//                   zero-copy path can serve without touching a buffer at all.
//   ONE_MATCH       a single match per row, replacement the same length as the pattern.
//   GROW_MANY       every character matches and the replacement is longer. An in-place
//                   std::string::replace has to shift the unmatched tail on every match, so
//                   this shape is quadratic in the number of matches.
//   SHRINK_MANY     every character matches and the replacement is shorter -- also shifts.
//   PER_ROW_PATTERN pattern and replacement arrive as full columns rather than constants,
//                   so a to_string() per row per argument is visible here.
//
// Row count and row width are separate axes because the per-row cost (one buffer
// allocation) and the per-match cost (tail shifting) scale differently.

#include <benchmark/benchmark.h>
#include <glog/logging.h>

#include <memory>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "exprs/function_context.h"
#include "exprs/string_functions.h"
#include "types/logical_type.h"

namespace starrocks {

enum ReplaceShape {
    NO_MATCH = 0,
    ONE_MATCH = 1,
    GROW_MANY = 2,
    SHRINK_MANY = 3,
    PER_ROW_PATTERN = 4,
};

namespace {

struct ReplaceCase {
    Columns columns;
    std::unique_ptr<FunctionContext> ctx;
};

std::string make_row(ReplaceShape shape, size_t width) {
    switch (shape) {
    case NO_MATCH:
        // no 'z' anywhere, so the pattern below never occurs
        return std::string(width, 'a');
    case ONE_MATCH: {
        std::string s(width, 'a');
        s[width / 2] = 'z';
        return s;
    }
    case GROW_MANY:
    case SHRINK_MANY:
        // every character is a match
        return std::string(width, 'z');
    case PER_ROW_PATTERN: {
        std::string s(width, 'a');
        s[width / 2] = 'z';
        return s;
    }
    }
    return std::string(width, 'a');
}

// pattern / replacement per shape. SHRINK_MANY starts from a two-character pattern so the
// replacement can be shorter than what it replaces.
void pattern_for(ReplaceShape shape, std::string* ptn, std::string* rpl) {
    switch (shape) {
    case NO_MATCH:
        *ptn = "z";
        *rpl = "yy";
        break;
    case ONE_MATCH:
        *ptn = "z";
        *rpl = "y";
        break;
    case GROW_MANY:
        *ptn = "z";
        *rpl = "yy";
        break;
    case SHRINK_MANY:
        *ptn = "zz";
        *rpl = "y";
        break;
    case PER_ROW_PATTERN:
        *ptn = "z";
        *rpl = "yy";
        break;
    }
}

ReplaceCase build_case(ReplaceShape shape, size_t num_rows, size_t width) {
    ReplaceCase c;
    c.ctx.reset(FunctionContext::create_test_context());

    std::string ptn;
    std::string rpl;
    pattern_for(shape, &ptn, &rpl);

    auto str = BinaryColumn::create();
    for (size_t i = 0; i < num_rows; i++) {
        str->append(make_row(shape, width));
    }

    if (shape == PER_ROW_PATTERN) {
        auto ptn_col = BinaryColumn::create();
        auto rpl_col = BinaryColumn::create();
        for (size_t i = 0; i < num_rows; i++) {
            ptn_col->append(ptn);
            rpl_col->append(rpl);
        }
        c.columns = Columns{std::move(str), std::move(ptn_col), std::move(rpl_col)};
    } else {
        auto ptn_data = BinaryColumn::create();
        ptn_data->append(ptn);
        auto rpl_data = BinaryColumn::create();
        rpl_data->append(rpl);
        c.columns = Columns{std::move(str), ConstColumn::create(std::move(ptn_data), num_rows),
                            ConstColumn::create(std::move(rpl_data), num_rows)};
    }

    c.ctx->set_constant_columns(c.columns);
    CHECK(StringFunctions::replace_prepare(c.ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    return c;
}

} // namespace

static void BM_StringReplace(benchmark::State& state) {
    const auto shape = static_cast<ReplaceShape>(state.range(0));
    const size_t num_rows = state.range(1);
    const size_t width = state.range(2);

    ReplaceCase c = build_case(shape, num_rows, width);

    size_t produced = 0;
    for (auto _ : state) {
        auto result = StringFunctions::replace(c.ctx.get(), c.columns);
        CHECK(result.ok());
        produced += result.value()->size();
        benchmark::DoNotOptimize(produced);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(num_rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(num_rows) *
                            static_cast<int64_t>(width));

    CHECK(StringFunctions::replace_close(c.ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
}

static void BM_StringReplace_Args(benchmark::internal::Benchmark* b) {
    // one chunk of typical rows, then the same row count at a wider row, then a small
    // number of very wide rows to separate per-row cost from per-match cost.
    for (int shape = NO_MATCH; shape <= PER_ROW_PATTERN; shape++) {
        b->Args({shape, 4096, 64});
        b->Args({shape, 4096, 512});
        b->Args({shape, 64, 16384});
    }
    b->Unit(benchmark::kMicrosecond);
}

BENCHMARK(BM_StringReplace)->Apply(BM_StringReplace_Args);

} // namespace starrocks

BENCHMARK_MAIN();
