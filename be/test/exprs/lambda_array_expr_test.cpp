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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cmath>
#include <memory>

#include "base/testutil/assert.h"
#include "butil/time.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "exprs/arithmetic_expr.h"
#include "exprs/array_expr.h"
#include "exprs/array_map_expr.h"
#include "exprs/cast_expr.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "exprs/function_call_expr.h"
#include "exprs/function_helper.h"
#include "exprs/is_null_predicate.h"
#include "exprs/lambda_function.h"
#include "exprs/literal.h"
#include "exprs/mock_vectorized_expr.h"
#include "runtime/runtime_state.h"

namespace starrocks {

ColumnPtr build_int_column(const std::vector<int>& values) {
    auto data = Int32Column::create();
    data->append_numbers(values.data(), values.size() * sizeof(int32_t));
    return data;
}

class VectorizedLambdaFunctionExprTest : public ::testing::Test {
public:
    void SetUp() override { create_array_expr(); }

    static TExprNode create_expr_node();

    static std::vector<Expr*> create_lambda_expr(ObjectPool* pool);

    void create_array_expr() {
        TypeDescriptor type_arr_int;
        type_arr_int.type = LogicalType::TYPE_ARRAY;
        type_arr_int.children.emplace_back();
        type_arr_int.children.back().type = LogicalType::TYPE_INT;

        // [1,4]
        // [null,null]
        // [null,12]
        auto array = ColumnHelper::create_column(type_arr_int, true);
        array->append_datum(DatumArray{Datum((int32_t)1), Datum((int32_t)4)}); // [1,4]
        array->append_datum(DatumArray{Datum(), Datum()});                     // [NULL, NULL]
        array->append_datum(DatumArray{Datum(), Datum((int32_t)12)});          // [NULL, 12]
        auto* array_values = new_fake_const_expr(std::move(array), type_arr_int);
        _array_expr.push_back(array_values);

        // null
        array = ColumnHelper::create_column(type_arr_int, true);
        array->append_datum(Datum{}); // null
        auto* const_null = new_fake_const_expr(std::move(array), type_arr_int);
        _array_expr.push_back(const_null);

        // [null]
        array = ColumnHelper::create_column(type_arr_int, true);
        array->append_datum(DatumArray{Datum()});
        auto* null_array = new_fake_const_expr(std::move(array), type_arr_int);
        _array_expr.push_back(null_array);

        // []
        array = ColumnHelper::create_column(type_arr_int, true);
        array->append_datum(DatumArray{}); // []
        auto empty_array = new_fake_const_expr(std::move(array), type_arr_int);
        _array_expr.push_back(empty_array);

        // [null]
        // []
        // NULL
        array = ColumnHelper::create_column(type_arr_int, true);
        array->append_datum(DatumArray{Datum()}); // [null]
        array->append_datum(DatumArray{});        // []
        array->append_datum(Datum{});             // NULL
        auto* array_special = new_fake_const_expr(std::move(array), type_arr_int);
        _array_expr.push_back(array_special);

        // const([1,4]...)
        array = ColumnHelper::create_column(type_arr_int, false);
        array->append_datum(DatumArray{Datum((int32_t)1), Datum((int32_t)4)}); // [1,4]
        auto const_col = ConstColumn::create(std::move(array), 3);
        auto* const_array = new_fake_const_expr(std::move(const_col), type_arr_int);
        _array_expr.push_back(const_array);

        // const(null...)
        array = ColumnHelper::create_column(type_arr_int, true);
        array->append_datum(Datum{}); // null...
        const_col = ConstColumn::create(std::move(array), 3);
        const_array = new_fake_const_expr(std::move(const_col), type_arr_int);
        _array_expr.push_back(const_array);

        // const([null]...)
        array = ColumnHelper::create_column(type_arr_int, false);
        array->append_datum(DatumArray{Datum()}); // [null]...
        const_col = ConstColumn::create(std::move(array), 3);
        const_array = new_fake_const_expr(std::move(const_col), type_arr_int);
        _array_expr.push_back(const_array);

        // const([]...)
        array = ColumnHelper::create_column(type_arr_int, false);
        array->append_datum(DatumArray{}); // []...
        const_col = ConstColumn::create(std::move(array), 3);
        const_array = new_fake_const_expr(std::move(const_col), type_arr_int);
        _array_expr.push_back(const_array);
    }

    FakeConstExpr* new_fake_const_expr(MutableColumnPtr&& value, const TypeDescriptor& type) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::INT_LITERAL);
        node.__set_num_children(0);
        node.__set_type(type.to_thrift());
        FakeConstExpr* e = _objpool.add(new FakeConstExpr(node));
        e->_column = std::move(value);
        return e;
    }

    static TExprNode create_int_literal_node(int64_t value_literal) {
        TExprNode lit_node;
        lit_node.__set_node_type(TExprNodeType::INT_LITERAL);
        lit_node.__set_num_children(0);
        lit_node.__set_type(gen_type_desc(TPrimitiveType::INT));
        TIntLiteral lit_value;
        lit_value.__set_value(value_literal);
        lit_node.__set_int_literal(lit_value);
        return lit_node;
    }

    std::vector<Expr*> _array_expr;
    std::vector<Chunk*> _chunks;

protected:
    RuntimeState _runtime_state;
    ObjectPool _objpool;
};

TExprNode VectorizedLambdaFunctionExprTest::create_expr_node() {
    TExprNode expr_node;
    expr_node.opcode = TExprOpcode::ADD;
    expr_node.child_type = TPrimitiveType::INT;
    expr_node.node_type = TExprNodeType::BINARY_PRED;
    expr_node.num_children = 2;
    expr_node.__isset.opcode = true;
    expr_node.__isset.child_type = true;
    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    return expr_node;
}

std::vector<Expr*> VectorizedLambdaFunctionExprTest::create_lambda_expr(ObjectPool* pool) {
    std::vector<Expr*> lambda_funcs;

    // create lambda functions
    TExprNode tlambda_func;
    tlambda_func.opcode = TExprOpcode::ADD;
    tlambda_func.child_type = TPrimitiveType::INT;
    tlambda_func.node_type = TExprNodeType::LAMBDA_FUNCTION_EXPR;
    tlambda_func.num_children = 2;
    tlambda_func.__isset.opcode = true;
    tlambda_func.__isset.child_type = true;
    tlambda_func.type = gen_type_desc(TPrimitiveType::INT);
    LambdaFunction* lambda_func = pool->add(new LambdaFunction(tlambda_func));

    // x -> x
    TExprNode slot_ref;
    slot_ref.node_type = TExprNodeType::SLOT_REF;
    slot_ref.type = gen_type_desc(TPrimitiveType::INT);
    slot_ref.num_children = 0;
    slot_ref.__isset.slot_ref = true;
    slot_ref.slot_ref.slot_id = 100000;
    slot_ref.slot_ref.tuple_id = 0;
    slot_ref.__set_is_nullable(true);

    ColumnRef* col1 = pool->add(new ColumnRef(slot_ref));
    ColumnRef* col2 = pool->add(new ColumnRef(slot_ref));
    lambda_func->add_child(col1);
    lambda_func->add_child(col2);
    lambda_funcs.push_back(lambda_func);

    // x -> x is null
    lambda_func = pool->add(new LambdaFunction(tlambda_func));
    ColumnRef* col3 = pool->add(new ColumnRef(slot_ref));
    ColumnRef* col4 = pool->add(new ColumnRef(slot_ref));
    TExprNode node = create_expr_node();
    node.fn.name.function_name = "is_null_pred";
    auto* is_null = pool->add(VectorizedIsNullPredicateFactory::from_thrift(node));
    is_null->add_child(col4);
    lambda_func->add_child(is_null);
    lambda_func->add_child(col3);
    lambda_funcs.push_back(lambda_func);

    // x -> x + a (captured columns)
    lambda_func = pool->add(new LambdaFunction(tlambda_func));
    ColumnRef* col5 = pool->add(new ColumnRef(slot_ref));
    node = create_expr_node();
    node.opcode = TExprOpcode::ADD;
    node.type = gen_type_desc(TPrimitiveType::INT);
    auto* add_expr = pool->add(VectorizedArithmeticExprFactory::from_thrift(node));
    ColumnRef* col6 = pool->add(new ColumnRef(slot_ref));
    slot_ref.slot_ref.slot_id = 1;
    ColumnRef* col7 = pool->add(new ColumnRef(slot_ref));
    add_expr->add_child(col6);
    add_expr->add_child(col7);
    lambda_func->add_child(add_expr);
    lambda_func->add_child(col5);
    lambda_funcs.push_back(lambda_func);

    // x -> -110
    lambda_func = pool->add(new LambdaFunction(tlambda_func));
    auto tint_literal = create_int_literal_node(-110);
    auto int_literal = pool->add(new VectorizedLiteral(tint_literal));
    slot_ref.slot_ref.slot_id = 100000;
    ColumnRef* col8 = pool->add(new ColumnRef(slot_ref));
    lambda_func->add_child(int_literal);
    lambda_func->add_child(col8);
    lambda_funcs.push_back(lambda_func);
    return lambda_funcs;
}

// just consider one level, not nested
// array_map(lambdaFunction(x<type>, lambdaExpr),array<type>)
TEST_F(VectorizedLambdaFunctionExprTest, array_map_lambda_test_normal_array) {
    auto cur_chunk = std::make_shared<Chunk>();
    std::vector<int> vec_a = {1, 1, 1};
    cur_chunk->append_column(build_int_column(vec_a), 1);
    for (int i = 0; i < 1; ++i) {
        auto lambda_funcs = create_lambda_expr(&_objpool);
        for (int j = 0; j < lambda_funcs.size(); ++j) {
            ArrayMapExpr array_map_expr(array_type(TYPE_INT));
            array_map_expr.clear_children();
            array_map_expr.add_child(lambda_funcs[j]);
            array_map_expr.add_child(_array_expr[i]);
            ExprContext exprContext(&array_map_expr);
            std::vector<ExprContext*> expr_ctxs = {&exprContext};
            ASSERT_OK(ExprExecutor::prepare(expr_ctxs, &_runtime_state));
            ASSERT_OK(ExprExecutor::open(expr_ctxs, &_runtime_state));
            auto lambda = dynamic_cast<LambdaFunction*>(lambda_funcs[j]);

            // check LambdaFunction::prepare()
            std::vector<SlotId> ids, arguments;
            lambda->get_captured_slot_ids(&ids);
            lambda->get_lambda_arguments_ids(&arguments);

            ASSERT_TRUE(arguments.size() == 1 && arguments[0] == 100000); // the x's slot_id = 100000
            if (j == 2) {
                ASSERT_TRUE(ids.size() == 1 && ids[0] == 1); // the slot_id of the captured column is 1
            } else {
                ASSERT_TRUE(ids.empty());
            }

            ColumnPtr result = array_map_expr.evaluate(&exprContext, cur_chunk.get());

            if (i == 0 && j == 0) { // array_map(x -> x, array<int>)
                                    // [1,4]
                                    // [null,null]
                                    // [null,12]
                ASSERT_FALSE(result->is_constant());
                ASSERT_FALSE(result->is_numeric());

                ASSERT_EQ(3, result->size());
                ASSERT_EQ(1, result->get(0).get_array()[0].get_int32());
                ASSERT_EQ(4, result->get(0).get_array()[1].get_int32());
                ASSERT_TRUE(result->get(1).get_array()[0].is_null());
                ASSERT_TRUE(result->get(1).get_array()[1].is_null());
                ASSERT_TRUE(result->get(2).get_array()[0].is_null());
                ASSERT_EQ(12, result->get(2).get_array()[1].get_int32());
            } else if (i == 0 && j == 1) { // array_map(x -> x is null, array<int>)
                ASSERT_EQ(3, result->size());
                ASSERT_EQ(0, result->get(0).get_array()[0].get_int8());
                ASSERT_EQ(0, result->get(0).get_array()[1].get_int8());
                ASSERT_EQ(1, result->get(1).get_array()[0].get_int8());
                ASSERT_EQ(1, result->get(1).get_array()[1].get_int8());
                ASSERT_EQ(1, result->get(2).get_array()[0].get_int8());
                ASSERT_EQ(0, result->get(2).get_array()[1].get_int8());
            } else if (i == 0 && j == 2) { // // array_map(x -> x+a, array<int>)
                ASSERT_EQ(3, result->size());
                ASSERT_EQ(2, result->get(0).get_array()[0].get_int32());
                ASSERT_EQ(5, result->get(0).get_array()[1].get_int32());
                ASSERT_TRUE(result->get(1).get_array()[0].is_null());
                ASSERT_TRUE(result->get(1).get_array()[1].is_null());
                ASSERT_TRUE(result->get(2).get_array()[0].is_null());
                ASSERT_EQ(13, result->get(2).get_array()[1].get_int32());
            } else if (i == 0 && j == 3) {
                ASSERT_EQ(3, result->size());
                ASSERT_EQ(-110, result->get(0).get_array()[0].get_int32());
                ASSERT_EQ(-110, result->get(0).get_array()[1].get_int32());
                ASSERT_EQ(-110, result->get(1).get_array()[0].get_int32());
                ASSERT_EQ(-110, result->get(1).get_array()[1].get_int32());
                ASSERT_EQ(-110, result->get(2).get_array()[0].get_int32());
                ASSERT_EQ(-110, result->get(2).get_array()[1].get_int32());
            }

            ExprExecutor::close(expr_ctxs, &_runtime_state);
        }
    }
}

TEST_F(VectorizedLambdaFunctionExprTest, array_map_lambda_test_special_array) {
    auto cur_chunk = std::make_shared<Chunk>();
    std::vector<int> vec_a = {1, 1, 1};
    cur_chunk->append_column(build_int_column(vec_a), 1);
    for (int i = 1; i < 5; ++i) {
        auto lambda_funcs = create_lambda_expr(&_objpool);
        for (int j = 0; j < lambda_funcs.size(); ++j) {
            ArrayMapExpr array_map_expr(array_type(TYPE_INT));
            array_map_expr.clear_children();
            array_map_expr.add_child(lambda_funcs[j]);
            array_map_expr.add_child(_array_expr[i]);
            ExprContext exprContext(&array_map_expr);
            std::vector<ExprContext*> expr_ctxs = {&exprContext};
            ASSERT_OK(ExprExecutor::prepare(expr_ctxs, &_runtime_state));
            ASSERT_OK(ExprExecutor::open(expr_ctxs, &_runtime_state));
            auto lambda = dynamic_cast<LambdaFunction*>(lambda_funcs[j]);

            // check LambdaFunction::prepare()
            std::vector<SlotId> ids, arguments;
            lambda->get_captured_slot_ids(&ids);
            lambda->get_lambda_arguments_ids(&arguments);

            ASSERT_TRUE(arguments.size() == 1 && arguments[0] == 100000); // the x's slot_id = 100000
            if (j == 2) {
                ASSERT_TRUE(ids.size() == 1 && ids[0] == 1); // the slot_id of the captured column is 1
            } else {
                ASSERT_TRUE(ids.empty());
            }
            ColumnPtr result = array_map_expr.evaluate(&exprContext, cur_chunk.get());

            if (i == 1) { // array_map(x->xxx,null)
                ASSERT_EQ(3, result->size());
                ASSERT_TRUE(result->is_null(0));
            } else if (i == 2 && (j == 0 || j == 2)) { // array_map( x->x || x->x+a, [null])
                ASSERT_EQ(1, result->size());
                ASSERT_TRUE(result->get(0).get_array()[0].is_null());
            } else if (i == 2 && j == 1) { // array_map(x -> x is null,[null])
                ASSERT_EQ(1, result->size());
                ASSERT_EQ(1, result->get(0).get_array()[0].get_int8());
            } else if (i == 2 && j == 3) { // array_map(x -> -110,[null])
                ASSERT_EQ(1, result->size());
                ASSERT_EQ(-110, result->get(0).get_array()[0].get_int32());
            } else if (i == 3) { // array_map(x->xxx,[])
                ASSERT_EQ(3, result->size());
                ASSERT_TRUE(result->get(0).get_array().empty());
            } else if (i == 4 && (j == 0 || j == 2)) { // array_map(x->x || x->x+a, array<special>)
                                                       // [null]
                                                       // []
                                                       // NULL
                ASSERT_EQ(3, result->size());
                ASSERT_TRUE(result->get(0).get_array()[0].is_null());
                ASSERT_TRUE(result->get(1).get_array().empty());
                ASSERT_TRUE(result->is_null(2));
            } else if (i == 4 && j == 1) { // array_map(x->x is null, array<special>)
                ASSERT_EQ(3, result->size());
                ASSERT_EQ(1, result->get(0).get_array()[0].get_int8());
                ASSERT_TRUE(result->get(1).get_array().empty());
                ASSERT_TRUE(result->is_null(2));
            } else if (i == 4 && j == 3) { // array_map(x-> -110, array<special>)
                ASSERT_EQ(3, result->size());
                ASSERT_EQ(-110, result->get(0).get_array()[0].get_int32());
                ASSERT_TRUE(result->get(1).get_array().empty());
                ASSERT_TRUE(result->is_null(2));
            }

            ExprExecutor::close(expr_ctxs, &_runtime_state);
        }
    }
}

TEST_F(VectorizedLambdaFunctionExprTest, array_map_lambda_test_const_array) {
    auto cur_chunk = std::make_shared<Chunk>();
    std::vector<int> vec_a = {1, 1, 1};
    cur_chunk->append_column(build_int_column(vec_a), 1);
    for (int i = 5; i < _array_expr.size(); ++i) {
        auto lambda_funcs = create_lambda_expr(&_objpool);
        for (int j = 0; j < lambda_funcs.size(); ++j) {
            ArrayMapExpr array_map_expr(array_type(j == 1 ? TYPE_BOOLEAN : TYPE_INT));
            array_map_expr.clear_children();
            array_map_expr.add_child(lambda_funcs[j]);
            array_map_expr.add_child(_array_expr[i]);
            ExprContext exprContext(&array_map_expr);
            std::vector<ExprContext*> expr_ctxs = {&exprContext};
            ASSERT_OK(ExprExecutor::prepare(expr_ctxs, &_runtime_state));
            ASSERT_OK(ExprExecutor::open(expr_ctxs, &_runtime_state));
            auto lambda = dynamic_cast<LambdaFunction*>(lambda_funcs[j]);

            // check LambdaFunction::prepare()
            std::vector<SlotId> ids, arguments;
            lambda->get_captured_slot_ids(&ids);
            lambda->get_lambda_arguments_ids(&arguments);

            ASSERT_TRUE(arguments.size() == 1 && arguments[0] == 100000); // the x's slot_id = 100000
            if (j == 2) {
                ASSERT_TRUE(ids.size() == 1 && ids[0] == 1); // the slot_id of the captured column is 1
            } else {
                ASSERT_TRUE(ids.empty());
            }
            ColumnPtr result = array_map_expr.evaluate(&exprContext, cur_chunk.get());
            if (i == 5 && j == 0) { // array_map( x->x, array<const[1,4]...>)
                ASSERT_EQ(3, result->size());
                ASSERT_EQ(1, result->get(0).get_array()[0].get_int32());
                ASSERT_EQ(4, result->get(0).get_array()[1].get_int32());
                ASSERT_EQ(1, result->get(1).get_array()[0].get_int32());
                ASSERT_EQ(4, result->get(1).get_array()[1].get_int32());
                ASSERT_EQ(1, result->get(2).get_array()[0].get_int32());
                ASSERT_EQ(4, result->get(2).get_array()[1].get_int32());
            } else if (i == 5 && j == 1) { // array_map(x->x is null, array<const[1,4]...>)
                ASSERT_EQ(3, result->size());
                ASSERT_EQ(0, result->get(0).get_array()[0].get_int8());
                ASSERT_EQ(0, result->get(0).get_array()[1].get_int8());
                ASSERT_EQ(0, result->get(1).get_array()[0].get_int8());
                ASSERT_EQ(0, result->get(1).get_array()[1].get_int8());
                ASSERT_EQ(0, result->get(2).get_array()[0].get_int8());
                ASSERT_EQ(0, result->get(2).get_array()[1].get_int8());
                LOG(INFO) << "pass";
            } else if (i == 5 && j == 2) { // // array_map( x->x + a, array<const[1,4]...>)
                ASSERT_EQ(3, result->size());
                ASSERT_EQ(2, result->get(0).get_array()[0].get_int32());
                ASSERT_EQ(5, result->get(0).get_array()[1].get_int32());
                ASSERT_EQ(2, result->get(1).get_array()[0].get_int32());
                ASSERT_EQ(5, result->get(1).get_array()[1].get_int32());
                ASSERT_EQ(2, result->get(2).get_array()[0].get_int32());
                ASSERT_EQ(5, result->get(2).get_array()[1].get_int32());
            } else if (i == 5 && j == 3) { // // array_map( x-> -110, array<const[1,4]...>)
                ASSERT_EQ(3, result->size());
                ASSERT_EQ(-110, result->get(0).get_array()[0].get_int32());
                ASSERT_EQ(-110, result->get(0).get_array()[1].get_int32());
                ASSERT_EQ(-110, result->get(1).get_array()[0].get_int32());
                ASSERT_EQ(-110, result->get(1).get_array()[1].get_int32());
                ASSERT_EQ(-110, result->get(2).get_array()[0].get_int32());
                ASSERT_EQ(-110, result->get(2).get_array()[1].get_int32());
            } else if (i == 6) { // array_map(x -> x || x->x is null || x -> x+a, array<const(null...)>)
                ASSERT_EQ(3, result->size());
                ASSERT_TRUE(result->is_null(0));
                ASSERT_TRUE(result->is_null(1));
                ASSERT_TRUE(result->is_null(2));
            } else if (i == 7 && (j == 0 || j == 2)) { // array_map(x -> x || x-> x+a,array<const([null]...)>)
                ASSERT_EQ(3, result->size());
                ASSERT_TRUE(result->get(0).get_array()[0].is_null());
                ASSERT_TRUE(result->get(1).get_array()[0].is_null());
                ASSERT_TRUE(result->get(2).get_array()[0].is_null());

            } else if (i == 7 && j == 1) { // array_map(x -> x is null, array<const([null]...)>)
                ASSERT_EQ(3, result->size());
                ASSERT_EQ(1, result->get(0).get_array()[0].get_int8());
                ASSERT_EQ(1, result->get(1).get_array()[0].get_int8());
                ASSERT_EQ(1, result->get(2).get_array()[0].get_int8());
            } else if (i == 7 && j == 3) { // array_map(x -> -110, array<const([null]...)>)
                ASSERT_EQ(3, result->size());
                ASSERT_EQ(-110, result->get(0).get_array()[0].get_int32());
                ASSERT_EQ(-110, result->get(1).get_array()[0].get_int32());
                ASSERT_EQ(-110, result->get(2).get_array()[0].get_int32());
            } else if (i == 8) { // array_map(x -> x || x -> x is null || x -> x+a || x -> -110, array<const([]...)>)
                ASSERT_EQ(3, result->size());
                ASSERT_TRUE(result->get(0).get_array().empty());
                ASSERT_TRUE(result->get(1).get_array().empty());
                ASSERT_TRUE(result->get(2).get_array().empty());
            }

            if (j == 1) { // array<int> -> array<bool>
                auto data_column = result;
                if (data_column->is_constant()) {
                    data_column = FunctionHelper::get_data_column_of_const(data_column);
                }
                if (data_column->is_nullable()) {
                    data_column = down_cast<const NullableColumn*>(data_column.get())->data_column();
                }
                auto array_col = ArrayColumn::dynamic_pointer_cast(data_column);
                ASSERT_EQ(2, array_col->elements_column()->type_size());
            }
            ExprExecutor::close(expr_ctxs, &_runtime_state);
        }
    }
}

TEST_F(VectorizedLambdaFunctionExprTest, test_lambda_common_expr_slot_conflict) {
    auto cur_chunk = std::make_shared<Chunk>();
    std::vector<int> vec_a = {1, 1, 1};
    cur_chunk->append_column(build_int_column(vec_a), 1);

    // Create a string column for length function
    auto string_col = BinaryColumn::create();
    string_col->append("abc");
    string_col->append("def");
    string_col->append("ghi");
    cur_chunk->append_column(string_col, 2); // slot_id = 2 for captured column z

    auto fake_col = BinaryColumn::create();
    fake_col->append("abc");
    fake_col->append("def");
    fake_col->append("ghi");
    // fake column, which will conflict with the slot id of the common expr extracted by the lambda function
    cur_chunk->append_column(fake_col, 100002);

    // Create two array columns for testing
    TypeDescriptor type_arr_int;
    type_arr_int.type = LogicalType::TYPE_ARRAY;
    type_arr_int.children.emplace_back();
    type_arr_int.children.back().type = LogicalType::TYPE_INT;

    // col1: [1,2], [3,4], [5,6]
    auto array1 = ColumnHelper::create_column(type_arr_int, false);
    array1->append_datum(DatumArray{Datum((int32_t)1), Datum((int32_t)2)});
    array1->append_datum(DatumArray{Datum((int32_t)3), Datum((int32_t)4)});
    array1->append_datum(DatumArray{Datum((int32_t)5), Datum((int32_t)6)});
    auto* col1_expr = new_fake_const_expr(std::move(array1), type_arr_int);

    // col2: [10,20], [30,40], [50,60]
    auto array2 = ColumnHelper::create_column(type_arr_int, false);
    array2->append_datum(DatumArray{Datum((int32_t)10), Datum((int32_t)20)});
    array2->append_datum(DatumArray{Datum((int32_t)30), Datum((int32_t)40)});
    array2->append_datum(DatumArray{Datum((int32_t)50), Datum((int32_t)60)});
    auto* col2_expr = new_fake_const_expr(std::move(array2), type_arr_int);

    // Create lambda function: (x,y) -> x + y + length(z)
    TExprNode tlambda_func;
    tlambda_func.opcode = TExprOpcode::ADD;
    tlambda_func.child_type = TPrimitiveType::INT;
    tlambda_func.node_type = TExprNodeType::LAMBDA_FUNCTION_EXPR;
    tlambda_func.num_children = 3; // lambda_expr + 2 arguments
    tlambda_func.__isset.opcode = true;
    tlambda_func.__isset.child_type = true;
    tlambda_func.type = gen_type_desc(TPrimitiveType::INT);
    LambdaFunction* lambda_func = _objpool.add(new LambdaFunction(tlambda_func));

    // Create lambda arguments: x and y
    TExprNode slot_ref_x, slot_ref_y;
    slot_ref_x.node_type = TExprNodeType::SLOT_REF;
    slot_ref_x.type = gen_type_desc(TPrimitiveType::INT);
    slot_ref_x.num_children = 0;
    slot_ref_x.__isset.slot_ref = true;
    slot_ref_x.slot_ref.slot_id = 100000; // x's slot_id
    slot_ref_x.slot_ref.tuple_id = 0;
    slot_ref_x.__set_is_nullable(true);

    slot_ref_y.node_type = TExprNodeType::SLOT_REF;
    slot_ref_y.type = gen_type_desc(TPrimitiveType::INT);
    slot_ref_y.num_children = 0;
    slot_ref_y.__isset.slot_ref = true;
    slot_ref_y.slot_ref.slot_id = 100001; // y's slot_id
    slot_ref_y.slot_ref.tuple_id = 0;
    slot_ref_y.__set_is_nullable(true);

    ColumnRef* col_x = _objpool.add(new ColumnRef(slot_ref_x));
    ColumnRef* col_y = _objpool.add(new ColumnRef(slot_ref_y));

    // Create captured column reference: z (slot_id = 2)
    TExprNode slot_ref_z;
    slot_ref_z.node_type = TExprNodeType::SLOT_REF;
    slot_ref_z.type = gen_type_desc(TPrimitiveType::VARCHAR);
    slot_ref_z.num_children = 0;
    slot_ref_z.__isset.slot_ref = true;
    slot_ref_z.slot_ref.slot_id = 2; // z's slot_id (captured column)
    slot_ref_z.slot_ref.tuple_id = 0;
    slot_ref_z.__set_is_nullable(true);
    ColumnRef* col_z = _objpool.add(new ColumnRef(slot_ref_z));

    // Create length function call: length(z)
    TExprNode length_node;
    length_node.node_type = TExprNodeType::FUNCTION_CALL;
    length_node.type = gen_type_desc(TPrimitiveType::INT);
    length_node.num_children = 1;
    length_node.__isset.fn = true;
    length_node.fn.name.function_name = "length";
    length_node.fn.fid = 30120;
    length_node.fn.__isset.fid = true;
    length_node.fn.binary_type = TFunctionBinaryType::BUILTIN;
    auto* length_expr = _objpool.add(new VectorizedFunctionCallExpr(length_node));
    length_expr->add_child(col_z);

    // Create arithmetic expression: x + y + length(z)
    TExprNode add_node1;
    add_node1.opcode = TExprOpcode::ADD;
    add_node1.child_type = TPrimitiveType::INT;
    add_node1.node_type = TExprNodeType::BINARY_PRED;
    add_node1.num_children = 2;
    add_node1.__isset.opcode = true;
    add_node1.__isset.child_type = true;
    add_node1.type = gen_type_desc(TPrimitiveType::INT);
    auto* add_expr1 = _objpool.add(VectorizedArithmeticExprFactory::from_thrift(add_node1));
    add_expr1->add_child(col_x);
    add_expr1->add_child(col_y);

    TExprNode add_node2;
    add_node2.opcode = TExprOpcode::ADD;
    add_node2.child_type = TPrimitiveType::INT;
    add_node2.node_type = TExprNodeType::BINARY_PRED;
    add_node2.num_children = 2;
    add_node2.__isset.opcode = true;
    add_node2.__isset.child_type = true;
    add_node2.type = gen_type_desc(TPrimitiveType::INT);
    auto* add_expr2 = _objpool.add(VectorizedArithmeticExprFactory::from_thrift(add_node2));
    add_expr2->add_child(add_expr1);
    add_expr2->add_child(length_expr);

    // Build lambda function: lambda_expr, arg_x, arg_y
    lambda_func->add_child(add_expr2); // lambda expression
    lambda_func->add_child(col_x);     // argument x
    lambda_func->add_child(col_y);     // argument y

    // Create ArrayMapExpr: array_map((x,y)->x + y + length(z), col1, col2)
    ArrayMapExpr array_map_expr(array_type(TYPE_INT));
    array_map_expr.clear_children();
    array_map_expr.add_child(lambda_func);
    array_map_expr.add_child(col1_expr);
    array_map_expr.add_child(col2_expr);

    ExprContext exprContext(&array_map_expr);
    std::vector<ExprContext*> expr_ctxs = {&exprContext};
    ASSERT_OK(ExprExecutor::prepare(expr_ctxs, &_runtime_state));
    ASSERT_OK(ExprExecutor::open(expr_ctxs, &_runtime_state));

    // Check LambdaFunction::prepare()
    std::vector<SlotId> ids, arguments;
    lambda_func->get_captured_slot_ids(&ids);
    lambda_func->get_lambda_arguments_ids(&arguments);

    ASSERT_TRUE(arguments.size() == 2 && arguments[0] == 100000 && arguments[1] == 100001);

    ColumnPtr result = array_map_expr.evaluate(&exprContext, cur_chunk.get());

    // Verify results
    // For each row, the lambda function (x,y)->x + y + length(z) is applied to each element
    // Row 0: x=1, y=10, length("abc")=3, result should be 1+10+3=14
    // Row 0: x=2, y=20, length("abc")=3, result should be 2+20+3=25
    // Row 1: x=3, y=30, length("def")=3, result should be 3+30+3=36
    // Row 1: x=4, y=40, length("def")=3, result should be 4+40+3=47
    // Row 2: x=5, y=50, length("ghi")=3, result should be 5+50+3=58
    // Row 2: x=6, y=60, length("ghi")=3, result should be 6+60+3=69

    ASSERT_EQ(3, result->size());
    ASSERT_EQ(14, result->get(0).get_array()[0].get_int32());
    ASSERT_EQ(25, result->get(0).get_array()[1].get_int32());
    ASSERT_EQ(36, result->get(1).get_array()[0].get_int32());
    ASSERT_EQ(47, result->get(1).get_array()[1].get_int32());
    ASSERT_EQ(58, result->get(2).get_array()[0].get_int32());
    ASSERT_EQ(69, result->get(2).get_array()[1].get_int32());

    ExprExecutor::close(expr_ctxs, &_runtime_state);
}

// Repro attempt for https://github.com/StarRocks/starrocks/issues/68481
// "slot_id xyz already exists BE" regression in 4.0.4.
//
// This stresses extract_outer_common_exprs with:
//   1. Two independent sub-expressions in the same lambda body that both get
//      extracted into `_outer_common_exprs` (length(z) and length(w)), forcing
//      two allocations in the same extract call.
//   2. Fake columns in the input chunk at the slot ids the BE is likely to
//      allocate (100002, 100003), so that if tmp_chunk ever confuses these
//      with the input chunk we'd see the "slot_id already exists" error.
//   3. The same SlotId (100003) referenced by TWO different captured columns
//      (w and a would-be collision) to exercise the dedup path.
TEST_F(VectorizedLambdaFunctionExprTest, test_lambda_two_common_exprs_slot_conflict) {
    auto cur_chunk = std::make_shared<Chunk>();
    std::vector<int> vec_a = {1, 1, 1};
    cur_chunk->append_column(build_int_column(vec_a), 1);

    // z (slot 2) and w (slot 3) - both captured and both used inside length()
    auto z_col = BinaryColumn::create();
    z_col->append("ab");
    z_col->append("cde");
    z_col->append("fghi");
    cur_chunk->append_column(z_col, 2);

    auto w_col = BinaryColumn::create();
    w_col->append("xxxx");
    w_col->append("yyy");
    w_col->append("zz");
    cur_chunk->append_column(w_col, 3);

    // Fake columns at the exact slot ids BE is expected to allocate for the
    // two outer common exprs (100002, 100003). If extract_outer_common_exprs
    // and evaluate_lambda_expr are not carefully isolated, appending these to
    // tmp_chunk would collide.
    auto fake_col_1 = BinaryColumn::create();
    fake_col_1->append("bogus1");
    fake_col_1->append("bogus1");
    fake_col_1->append("bogus1");
    cur_chunk->append_column(fake_col_1, 100002);

    auto fake_col_2 = BinaryColumn::create();
    fake_col_2->append("bogus2");
    fake_col_2->append("bogus2");
    fake_col_2->append("bogus2");
    cur_chunk->append_column(fake_col_2, 100003);

    TypeDescriptor type_arr_int;
    type_arr_int.type = LogicalType::TYPE_ARRAY;
    type_arr_int.children.emplace_back();
    type_arr_int.children.back().type = LogicalType::TYPE_INT;

    // col1: [1,2], [3,4], [5,6]
    auto array1 = ColumnHelper::create_column(type_arr_int, false);
    array1->append_datum(DatumArray{Datum((int32_t)1), Datum((int32_t)2)});
    array1->append_datum(DatumArray{Datum((int32_t)3), Datum((int32_t)4)});
    array1->append_datum(DatumArray{Datum((int32_t)5), Datum((int32_t)6)});
    auto* col1_expr = new_fake_const_expr(std::move(array1), type_arr_int);

    // Lambda: x -> x + length(z) + length(w)
    TExprNode tlambda_func;
    tlambda_func.opcode = TExprOpcode::ADD;
    tlambda_func.child_type = TPrimitiveType::INT;
    tlambda_func.node_type = TExprNodeType::LAMBDA_FUNCTION_EXPR;
    tlambda_func.num_children = 2; // lambda_expr + 1 arg
    tlambda_func.__isset.opcode = true;
    tlambda_func.__isset.child_type = true;
    tlambda_func.type = gen_type_desc(TPrimitiveType::INT);
    LambdaFunction* lambda_func = _objpool.add(new LambdaFunction(tlambda_func));

    TExprNode slot_ref_x;
    slot_ref_x.node_type = TExprNodeType::SLOT_REF;
    slot_ref_x.type = gen_type_desc(TPrimitiveType::INT);
    slot_ref_x.num_children = 0;
    slot_ref_x.__isset.slot_ref = true;
    slot_ref_x.slot_ref.slot_id = 100000;
    slot_ref_x.slot_ref.tuple_id = 0;
    slot_ref_x.__set_is_nullable(true);
    ColumnRef* col_x = _objpool.add(new ColumnRef(slot_ref_x));

    auto make_slot_ref = [&](SlotId slot_id, TPrimitiveType::type pt) {
        TExprNode n;
        n.node_type = TExprNodeType::SLOT_REF;
        n.type = gen_type_desc(pt);
        n.num_children = 0;
        n.__isset.slot_ref = true;
        n.slot_ref.slot_id = slot_id;
        n.slot_ref.tuple_id = 0;
        n.__set_is_nullable(true);
        return _objpool.add(new ColumnRef(n));
    };
    ColumnRef* col_z = make_slot_ref(2, TPrimitiveType::VARCHAR);
    ColumnRef* col_w = make_slot_ref(3, TPrimitiveType::VARCHAR);

    auto make_length_call = [&](Expr* child) {
        TExprNode length_node;
        length_node.node_type = TExprNodeType::FUNCTION_CALL;
        length_node.type = gen_type_desc(TPrimitiveType::INT);
        length_node.num_children = 1;
        length_node.__isset.fn = true;
        length_node.fn.name.function_name = "length";
        length_node.fn.fid = 30120;
        length_node.fn.__isset.fid = true;
        length_node.fn.binary_type = TFunctionBinaryType::BUILTIN;
        auto* length_expr = _objpool.add(new VectorizedFunctionCallExpr(length_node));
        length_expr->add_child(child);
        return length_expr;
    };
    auto* len_z = make_length_call(col_z);
    auto* len_w = make_length_call(col_w);

    auto make_add = [&](Expr* a, Expr* b) {
        TExprNode add_node;
        add_node.opcode = TExprOpcode::ADD;
        add_node.child_type = TPrimitiveType::INT;
        add_node.node_type = TExprNodeType::BINARY_PRED;
        add_node.num_children = 2;
        add_node.__isset.opcode = true;
        add_node.__isset.child_type = true;
        add_node.type = gen_type_desc(TPrimitiveType::INT);
        auto* e = _objpool.add(VectorizedArithmeticExprFactory::from_thrift(add_node));
        e->add_child(a);
        e->add_child(b);
        return e;
    };
    // x + length(z) + length(w)
    auto* body = make_add(make_add(col_x, len_z), len_w);

    lambda_func->add_child(body);
    lambda_func->add_child(col_x);

    ArrayMapExpr array_map_expr(array_type(TYPE_INT));
    array_map_expr.clear_children();
    array_map_expr.add_child(lambda_func);
    array_map_expr.add_child(col1_expr);

    ExprContext exprContext(&array_map_expr);
    std::vector<ExprContext*> expr_ctxs = {&exprContext};
    ASSERT_OK(ExprExecutor::prepare(expr_ctxs, &_runtime_state));
    ASSERT_OK(ExprExecutor::open(expr_ctxs, &_runtime_state));

    // Must not throw "slot_id already exists".
    ColumnPtr result = array_map_expr.evaluate(&exprContext, cur_chunk.get());

    // Row 0: z="ab"(2), w="xxxx"(4) -> +6. elems 1,2 -> 7,8.
    // Row 1: z="cde"(3), w="yyy"(3) -> +6. elems 3,4 -> 9,10.
    // Row 2: z="fghi"(4), w="zz"(2) -> +6. elems 5,6 -> 11,12.
    ASSERT_EQ(3, result->size());
    ASSERT_EQ(7, result->get(0).get_array()[0].get_int32());
    ASSERT_EQ(8, result->get(0).get_array()[1].get_int32());
    ASSERT_EQ(9, result->get(1).get_array()[0].get_int32());
    ASSERT_EQ(10, result->get(1).get_array()[1].get_int32());
    ASSERT_EQ(11, result->get(2).get_array()[0].get_int32());
    ASSERT_EQ(12, result->get(2).get_array()[1].get_int32());

    ExprExecutor::close(expr_ctxs, &_runtime_state);
}

// Repro attempt for #68481: sibling ArrayMap expressions evaluated against the
// same input chunk. Each ArrayMap computes its own `next_slot_id` based on
// `context->root()->max_used_slot_id() + 1`, and we expect the allocations to
// not overlap with each other nor with existing columns in the input chunk.
TEST_F(VectorizedLambdaFunctionExprTest, test_lambda_sibling_array_maps_slot_conflict) {
    auto cur_chunk = std::make_shared<Chunk>();
    std::vector<int> vec_a = {1, 1, 1};
    cur_chunk->append_column(build_int_column(vec_a), 1);

    auto z_col = BinaryColumn::create();
    z_col->append("ab");
    z_col->append("cde");
    z_col->append("fghi");
    cur_chunk->append_column(z_col, 2);

    TypeDescriptor type_arr_int;
    type_arr_int.type = LogicalType::TYPE_ARRAY;
    type_arr_int.children.emplace_back();
    type_arr_int.children.back().type = LogicalType::TYPE_INT;

    auto build_arr = [&]() {
        auto array = ColumnHelper::create_column(type_arr_int, false);
        array->append_datum(DatumArray{Datum((int32_t)1), Datum((int32_t)2)});
        array->append_datum(DatumArray{Datum((int32_t)3), Datum((int32_t)4)});
        array->append_datum(DatumArray{Datum((int32_t)5), Datum((int32_t)6)});
        return new_fake_const_expr(std::move(array), type_arr_int);
    };

    auto make_slot_ref = [&](SlotId slot_id, TPrimitiveType::type pt) {
        TExprNode n;
        n.node_type = TExprNodeType::SLOT_REF;
        n.type = gen_type_desc(pt);
        n.num_children = 0;
        n.__isset.slot_ref = true;
        n.slot_ref.slot_id = slot_id;
        n.slot_ref.tuple_id = 0;
        n.__set_is_nullable(true);
        return _objpool.add(new ColumnRef(n));
    };

    auto make_length_call = [&](Expr* child) {
        TExprNode length_node;
        length_node.node_type = TExprNodeType::FUNCTION_CALL;
        length_node.type = gen_type_desc(TPrimitiveType::INT);
        length_node.num_children = 1;
        length_node.__isset.fn = true;
        length_node.fn.name.function_name = "length";
        length_node.fn.fid = 30120;
        length_node.fn.__isset.fid = true;
        length_node.fn.binary_type = TFunctionBinaryType::BUILTIN;
        auto* length_expr = _objpool.add(new VectorizedFunctionCallExpr(length_node));
        length_expr->add_child(child);
        return length_expr;
    };

    auto make_add = [&](Expr* a, Expr* b) {
        TExprNode add_node;
        add_node.opcode = TExprOpcode::ADD;
        add_node.child_type = TPrimitiveType::INT;
        add_node.node_type = TExprNodeType::BINARY_PRED;
        add_node.num_children = 2;
        add_node.__isset.opcode = true;
        add_node.__isset.child_type = true;
        add_node.type = gen_type_desc(TPrimitiveType::INT);
        auto* e = _objpool.add(VectorizedArithmeticExprFactory::from_thrift(add_node));
        e->add_child(a);
        e->add_child(b);
        return e;
    };

    auto make_array_map = [&](SlotId arg_slot) {
        TExprNode tlambda_func;
        tlambda_func.opcode = TExprOpcode::ADD;
        tlambda_func.child_type = TPrimitiveType::INT;
        tlambda_func.node_type = TExprNodeType::LAMBDA_FUNCTION_EXPR;
        tlambda_func.num_children = 2;
        tlambda_func.__isset.opcode = true;
        tlambda_func.__isset.child_type = true;
        tlambda_func.type = gen_type_desc(TPrimitiveType::INT);
        auto* lambda_func = _objpool.add(new LambdaFunction(tlambda_func));

        auto* col_x = make_slot_ref(arg_slot, TPrimitiveType::INT);
        auto* col_z = make_slot_ref(2, TPrimitiveType::VARCHAR);
        auto* len_z = make_length_call(col_z);
        auto* body = make_add(col_x, len_z);

        lambda_func->add_child(body);
        lambda_func->add_child(col_x);

        auto* am = _objpool.add(new ArrayMapExpr(array_type(TYPE_INT)));
        am->add_child(lambda_func);
        am->add_child(build_arr());
        return am;
    };

    // Two sibling ArrayMapExprs each rooted in their own ExprContext but
    // sharing the same input chunk. Both should allocate non-overlapping
    // tmp_chunk slots without throwing "slot_id already exists".
    // Arguments use different slot ids (100000 and 100001) to avoid trivial dedup.
    auto* am1 = make_array_map(100000);
    auto* am2 = make_array_map(100001);

    ExprContext ctx1(am1);
    ExprContext ctx2(am2);
    std::vector<ExprContext*> expr_ctxs = {&ctx1, &ctx2};
    ASSERT_OK(ExprExecutor::prepare(expr_ctxs, &_runtime_state));
    ASSERT_OK(ExprExecutor::open(expr_ctxs, &_runtime_state));

    (void)am1->evaluate(&ctx1, cur_chunk.get());
    (void)am2->evaluate(&ctx2, cur_chunk.get());

    ExprExecutor::close(expr_ctxs, &_runtime_state);
}

// Nested array_map exercise related to https://github.com/StarRocks/starrocks/issues/68481.
//
// The scenario constructs `array_map(x -> x + array_length(array_map(y -> y + length(w), arr2)), arr1)`
// where `w` is only referenced from inside the INNER array_map body.
//
// The outer `ArrayMapExpr::prepare` extracts the inner `array_length(...)` as a
// single outer common expression. After that, inner's subtree (and the captured
// slot `w`) is only reachable via `outer._outer_common_exprs` - NOT via
// `_children`. When the inner ArrayMapExpr is then prepared (during outer's
// `for (expr : _outer_common_exprs) expr->prepare(...)` loop), it computes
// `max_used_slot_id` on the root which no longer sees `w`'s slot id.
//
// In the current code `max_used_slot_id` still moves monotonically forward
// because the outer's freshly-allocated ColumnRef slot is higher than any
// slot the outer saw before extraction, so the inner's `next_slot_id` never
// collides with its hidden captured slots. This test pins that invariant -
// if it's ever broken by a refactor, the `tmp_chunk->append_column` call in
// `evaluate_lambda_expr` will throw "slot_id X already exists".
TEST_F(VectorizedLambdaFunctionExprTest, test_nested_array_map_hidden_slot_conflict_68481) {
    constexpr SlotId kOuterArgSlot = 100000;
    constexpr SlotId kInnerArgSlot = 100001;
    constexpr SlotId kHiddenCaptureSlot = 100003; // intentionally > outer arg

    auto chunk = std::make_shared<Chunk>();
    std::vector<int> vec_a = {1, 1, 1};
    chunk->append_column(build_int_column(vec_a), 1);

    // slot_W (kHiddenCaptureSlot): only referenced from inside the INNER
    // array_map body. Once the outer extracts the inner into
    // _outer_common_exprs, this slot is invisible to for_each_slot_id on root.
    auto w_col = BinaryColumn::create();
    w_col->append("ab");
    w_col->append("cde");
    w_col->append("fghi");
    chunk->append_column(w_col, kHiddenCaptureSlot);

    TypeDescriptor type_arr_int;
    type_arr_int.type = LogicalType::TYPE_ARRAY;
    type_arr_int.children.emplace_back();
    type_arr_int.children.back().type = LogicalType::TYPE_INT;

    auto build_int_arr = [&](int v1, int v2) {
        auto array = ColumnHelper::create_column(type_arr_int, false);
        array->append_datum(DatumArray{Datum((int32_t)v1), Datum((int32_t)v2)});
        array->append_datum(DatumArray{Datum((int32_t)v1 + 2), Datum((int32_t)v2 + 2)});
        array->append_datum(DatumArray{Datum((int32_t)v1 + 4), Datum((int32_t)v2 + 4)});
        return new_fake_const_expr(std::move(array), type_arr_int);
    };

    auto make_slot_ref = [&](SlotId slot_id, TPrimitiveType::type pt) {
        TExprNode n;
        n.node_type = TExprNodeType::SLOT_REF;
        n.type = gen_type_desc(pt);
        n.num_children = 0;
        n.__isset.slot_ref = true;
        n.slot_ref.slot_id = slot_id;
        n.slot_ref.tuple_id = 0;
        n.__set_is_nullable(true);
        return _objpool.add(new ColumnRef(n));
    };

    auto make_length_call = [&](Expr* child) {
        TExprNode length_node;
        length_node.node_type = TExprNodeType::FUNCTION_CALL;
        length_node.type = gen_type_desc(TPrimitiveType::INT);
        length_node.num_children = 1;
        length_node.__isset.fn = true;
        length_node.fn.name.function_name = "length";
        length_node.fn.fid = 30120;
        length_node.fn.__isset.fid = true;
        length_node.fn.binary_type = TFunctionBinaryType::BUILTIN;
        auto* length_expr = _objpool.add(new VectorizedFunctionCallExpr(length_node));
        length_expr->add_child(child);
        return length_expr;
    };

    auto make_add = [&](Expr* a, Expr* b) {
        TExprNode add_node;
        add_node.opcode = TExprOpcode::ADD;
        add_node.child_type = TPrimitiveType::INT;
        add_node.node_type = TExprNodeType::BINARY_PRED;
        add_node.num_children = 2;
        add_node.__isset.opcode = true;
        add_node.__isset.child_type = true;
        add_node.type = gen_type_desc(TPrimitiveType::INT);
        auto* e = _objpool.add(VectorizedArithmeticExprFactory::from_thrift(add_node));
        e->add_child(a);
        e->add_child(b);
        return e;
    };

    auto make_lambda = [&](Expr* body, Expr* arg) {
        TExprNode tlambda;
        tlambda.opcode = TExprOpcode::ADD;
        tlambda.child_type = TPrimitiveType::INT;
        tlambda.node_type = TExprNodeType::LAMBDA_FUNCTION_EXPR;
        tlambda.num_children = 2;
        tlambda.__isset.opcode = true;
        tlambda.__isset.child_type = true;
        tlambda.type = gen_type_desc(TPrimitiveType::INT);
        auto* lf = _objpool.add(new LambdaFunction(tlambda));
        lf->add_child(body);
        lf->add_child(arg);
        return lf;
    };

    // Inner lambda:  y -> y + length(slot_W)
    auto* inner_arg = make_slot_ref(kInnerArgSlot, TPrimitiveType::INT);
    auto* col_w = make_slot_ref(kHiddenCaptureSlot, TPrimitiveType::VARCHAR);
    auto* inner_body = make_add(inner_arg, make_length_call(col_w));
    auto* inner_lambda = make_lambda(inner_body, inner_arg);

    // Inner array_map(inner_lambda, inner_arr)
    auto* inner_am = _objpool.add(new ArrayMapExpr(array_type(TYPE_INT)));
    inner_am->add_child(inner_lambda);
    inner_am->add_child(build_int_arr(10, 20));

    // Outer lambda:  x -> x + element_at(inner_am, 1)
    // We keep it simple: outer body = x + inner_am_firstelement. To avoid
    // pulling in element_at complexity, use the inner_am directly in an
    // `array_length` call so that outer extract still treats the inner_am
    // subtree as an independent, extractable expression of scalar type.
    //
    // However, the ArrayMap returns array<int> and we need a scalar result
    // inside the outer lambda's arithmetic body. Use `array_length` for that.
    TExprNode len_arr_node;
    len_arr_node.node_type = TExprNodeType::FUNCTION_CALL;
    len_arr_node.type = gen_type_desc(TPrimitiveType::INT);
    len_arr_node.num_children = 1;
    len_arr_node.__isset.fn = true;
    len_arr_node.fn.name.function_name = "array_length";
    len_arr_node.fn.fid = 110000;
    len_arr_node.fn.__isset.fid = true;
    len_arr_node.fn.binary_type = TFunctionBinaryType::BUILTIN;
    auto* array_length_of_inner = _objpool.add(new VectorizedFunctionCallExpr(len_arr_node));
    array_length_of_inner->add_child(inner_am);

    auto* outer_arg = make_slot_ref(kOuterArgSlot, TPrimitiveType::INT);
    auto* outer_body = make_add(outer_arg, array_length_of_inner);
    auto* outer_lambda = make_lambda(outer_body, outer_arg);

    ArrayMapExpr outer_am(array_type(TYPE_INT));
    outer_am.clear_children();
    outer_am.add_child(outer_lambda);
    outer_am.add_child(build_int_arr(1, 2));

    ExprContext ctx(&outer_am);
    std::vector<ExprContext*> expr_ctxs = {&ctx};
    ASSERT_OK(ExprExecutor::prepare(expr_ctxs, &_runtime_state));
    ASSERT_OK(ExprExecutor::open(expr_ctxs, &_runtime_state));

    // The bug surfaces as `throw std::runtime_error("slot_id X already exists")`
    // from chunk.cpp. If the fix is in place, evaluation proceeds normally.
    ColumnPtr result = outer_am.evaluate(&ctx, chunk.get());
    ASSERT_EQ(3, result->size());

    ExprExecutor::close(expr_ctxs, &_runtime_state);
}

} // namespace starrocks
