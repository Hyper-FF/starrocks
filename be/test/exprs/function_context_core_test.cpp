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
#include <vector>

#include "column/column_helper.h"
#include "common/ngram_bloom_filter_state.h"
#include "exprs/function_context.h"
#include "runtime/mem_pool.h"
#include "types/logical_type.h"

namespace starrocks {

TEST(FunctionContextCoreTest, CreateTestContextArgAccess) {
    std::vector<FunctionContext::TypeDesc> arg_types = {TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_VARCHAR)};
    auto ctx = std::unique_ptr<FunctionContext>(
            FunctionContext::create_test_context(std::move(arg_types), TypeDescriptor(TYPE_BIGINT)));

    EXPECT_EQ(2, ctx->get_num_args());
    ASSERT_NE(nullptr, ctx->get_arg_type(0));
    EXPECT_EQ(TYPE_INT, ctx->get_arg_type(0)->type);
    ASSERT_NE(nullptr, ctx->get_arg_type(1));
    EXPECT_EQ(TYPE_VARCHAR, ctx->get_arg_type(1)->type);
    EXPECT_EQ(nullptr, ctx->get_arg_type(-1));
    EXPECT_EQ(nullptr, ctx->get_arg_type(2));
    EXPECT_EQ(TYPE_BIGINT, ctx->get_return_type().type);
}

TEST(FunctionContextCoreTest, FunctionStateScopeSetGet) {
    FunctionContext ctx;
    int fragment_local_state = 22;

    ctx.set_function_state(FunctionContext::FRAGMENT_LOCAL, &fragment_local_state);

    EXPECT_EQ(&fragment_local_state, ctx.get_function_state(FunctionContext::FRAGMENT_LOCAL));
}

TEST(FunctionContextCoreTest, ErrorMessageSticky) {
    FunctionContext ctx;
    EXPECT_FALSE(ctx.has_error());

    ctx.set_error("first error");
    EXPECT_TRUE(ctx.has_error());
    ASSERT_NE(nullptr, ctx.error_msg());
    EXPECT_STREQ("first error", ctx.error_msg());

    ctx.set_error("second error");
    ASSERT_NE(nullptr, ctx.error_msg());
    EXPECT_STREQ("first error", ctx.error_msg());
}

namespace {
struct CoreTestThreadState : FunctionThreadState {
    int value = 0;
};
} // namespace

TEST(FunctionContextCoreTest, ThreadStateRegistryCreatesOncePerContext) {
    FunctionContext ctx;
    int created = 0;
    auto* s1 = ctx.get_or_create_thread_state<CoreTestThreadState>([&]() {
        ++created;
        auto s = std::make_unique<CoreTestThreadState>();
        s->value = 42;
        return s;
    });
    ASSERT_NE(nullptr, s1);
    EXPECT_EQ(42, s1->value);

    // A second call for the same (FunctionContext, worker) returns the same instance and does
    // not run the factory again.
    auto* s2 = ctx.get_or_create_thread_state<CoreTestThreadState>(
            [&]() { ++created; return std::make_unique<CoreTestThreadState>(); });
    EXPECT_EQ(s1, s2);
    EXPECT_EQ(1, created);
}

} // namespace starrocks
