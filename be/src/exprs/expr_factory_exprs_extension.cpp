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

#include "common/object_pool.h"
#include "exprs/array_map_expr.h"
#include "exprs/array_sort_lambda_expr.h"
#include "exprs/arrow_function_call.h"
#include "exprs/dict_query_expr.h"
#include "exprs/dictionary_get_expr.h"
#include "exprs/dictmapping_expr.h"
#include "exprs/expr_factory.h"
#include "exprs/function_call_expr.h"
#include "exprs/java_function_call_expr.h"
#include "exprs/map_apply_expr.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#ifdef STARROCKS_JIT_ENABLE
#include "exprs/jit/expr_jit_pass.h"
#endif

namespace starrocks {

namespace {

// Get the fragment-level MemPool, but only when |pool| belongs to the same
// fragment. If |pool| is query-level or there is no RuntimeState, fall back
// to heap allocation.
inline MemPool* get_fragment_mem_pool(RuntimeState* state, ObjectPool* pool) {
    return (state != nullptr && pool == state->obj_pool()) ? state->fragment_mem_pool() : nullptr;
}

template <typename T>
void* alloc_from(MemPool* mem_pool) {
    void* ptr = mem_pool->allocate_aligned(sizeof(T), alignof(T));
    DCHECK(ptr != nullptr);
    return ptr;
}

// Placement-new |T| into the fragment MemPool when available; otherwise fall
// back to a heap allocation that is owned by |pool|.
template <typename T, typename... Args>
T* create_expr(ObjectPool* pool, MemPool* mp, Args&&... args) {
    if (mp != nullptr) {
        return pool->emplace<T>(alloc_from<T>(mp), std::forward<Args>(args)...);
    }
    return pool->add(new T(std::forward<Args>(args)...));
}

Status expr_factory_non_core_create_pre_hook(ObjectPool* pool, const TExprNode& texpr_node, Expr** expr,
                                             RuntimeState* state) {
    if (*expr != nullptr) {
        return Status::OK();
    }
    if (texpr_node.node_type != TExprNodeType::FUNCTION_CALL &&
        texpr_node.node_type != TExprNodeType::COMPUTE_FUNCTION_CALL) {
        return Status::OK();
    }

    MemPool* mp = get_fragment_mem_pool(state, pool);
    if (texpr_node.fn.binary_type == TFunctionBinaryType::SRJAR) {
        *expr = create_expr<JavaFunctionCallExpr>(pool, mp, texpr_node);
    } else if (texpr_node.fn.binary_type == TFunctionBinaryType::PYTHON) {
        *expr = create_expr<ArrowFunctionCallExpr>(pool, mp, texpr_node);
    }
    return Status::OK();
}

Status expr_factory_non_core_create_post_hook(ObjectPool* pool, const TExprNode& texpr_node, Expr** expr,
                                              RuntimeState* state) {
    if (*expr != nullptr) {
        return Status::OK();
    }

    MemPool* mp = get_fragment_mem_pool(state, pool);
    switch (texpr_node.node_type) {
    case TExprNodeType::FUNCTION_CALL:
    case TExprNodeType::COMPUTE_FUNCTION_CALL:
        if (texpr_node.fn.name.function_name == "array_map") {
            *expr = create_expr<ArrayMapExpr>(pool, mp, texpr_node);
        } else if (texpr_node.fn.name.function_name == "array_sort_lambda") {
            *expr = create_expr<ArraySortLambdaExpr>(pool, mp, texpr_node);
        } else if (texpr_node.fn.name.function_name == "map_apply") {
            *expr = create_expr<MapApplyExpr>(pool, mp, texpr_node);
        } else {
            *expr = create_expr<VectorizedFunctionCallExpr>(pool, mp, texpr_node);
        }
        break;
    case TExprNodeType::DICT_EXPR:
        *expr = create_expr<DictMappingExpr>(pool, mp, texpr_node);
        break;
    case TExprNodeType::DICT_QUERY_EXPR:
        *expr = create_expr<DictQueryExpr>(pool, mp, texpr_node);
        break;
    case TExprNodeType::DICTIONARY_GET_EXPR:
        *expr = create_expr<DictionaryGetExpr>(pool, mp, texpr_node);
        break;
    default:
        break;
    }
    return Status::OK();
}

Status expr_factory_jit_rewrite_hook(Expr** root_expr, ObjectPool* pool, RuntimeState* state) {
#ifdef STARROCKS_JIT_ENABLE
    return ExprJITPass::rewrite_root(root_expr, pool, state);
#else
    (void)root_expr;
    (void)pool;
    (void)state;
    return Status::OK();
#endif
}

struct ExprFactoryExprsExtensionRegistrar {
    ExprFactoryExprsExtensionRegistrar() {
        ExprFactory::set_non_core_create_pre_hook(expr_factory_non_core_create_pre_hook);
        ExprFactory::set_non_core_create_post_hook(expr_factory_non_core_create_post_hook);
        ExprFactory::set_jit_rewrite_hook(expr_factory_jit_rewrite_hook);
    }
};

ExprFactoryExprsExtensionRegistrar k_expr_factory_exprs_extension_registrar;

} // namespace

} // namespace starrocks
