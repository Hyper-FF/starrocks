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

#include "exprs/java_function_call_expr.h"

#include <any>
#include <memory>
#include <sstream>
#include <tuple>
#include <vector>

#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/expr_context.h"
#include "exprs/function_context.h"
#include "gutil/casts.h"
#include "jni.h"
#include "runtime/user_function_cache.h"
#include "types/type_descriptor.h"
#include "udf/java/java_data_converter.h"
#include "udf/java/java_udf.h"
#include "udf/java/utils.h"

namespace starrocks {

struct UDFFunctionCallHelper {
    JavaUDFContext* fn_desc;
    JavaMethodDescriptor* call_desc;

    StatusOr<ColumnPtr> call(FunctionContext* ctx, Columns& columns, size_t size) {
        auto& helper = JVMFunctionHelper::getInstance();
        JNIEnv* env = helper.getEnv();
        int num_cols = ctx->get_num_args();
        std::vector<const Column*> input_cols;

        for (const auto& col : columns) {
            input_cols.emplace_back(col.get());
        }
        // each input arguments as three local references (nullcolumn, offsetcolumn, bytescolumn)
        // result column as a ref
        env->PushLocalFrame((num_cols + 1) * 3 + 1);
        auto defer = DeferOp([env]() { env->PopLocalFrame(nullptr); });

        // Build the per-arg jclass vector from the cached evaluate_arg_classes. Only
        // STRUCT slots are populated; other entries are nullptr and the converter
        // ignores them.
        std::vector<jclass> arg_classes;
        arg_classes.reserve(fn_desc->evaluate_arg_classes.size());
        for (const auto& gref : fn_desc->evaluate_arg_classes) {
            arg_classes.emplace_back(reinterpret_cast<jclass>(gref.handle()));
        }

        // convert input columns to object columns
        std::vector<jobject> input_col_objs;
        auto st = JavaDataTypeConverter::convert_to_boxed_array(ctx, input_cols.data(), num_cols, size,
                                                                &input_col_objs, &arg_classes);
        RETURN_IF_ERROR(st);

        // call UDF method
        ASSIGN_OR_RETURN(auto res, helper.batch_call(fn_desc->call_stub.get(), input_col_objs.data(),
                                                     input_col_objs.size(), size));
        // get result
        auto result_cols = get_boxed_result(ctx, res, size);
        return result_cols;
    }

    // Materialize the parent NullableColumn / inner StructColumn / per-field NullableColumn
    // chain that StarRocks expects for a STRUCT result, then drive per-subfield writes:
    // the Java helper extracts record components into per-field Object[] arrays (and writes
    // the parent null bitmap as a side effect); we then dispatch each subfield to the
    // per-type writer, recursing into get_struct_boxed_result for STRUCT subfields.
    Status get_struct_boxed_result(JVMFunctionHelper& helper, const TypeDescriptor& return_type, jobject result,
                                   size_t num_rows, NullableColumn* nullable_outer) {
        auto* struct_col = down_cast<StructColumn*>(nullable_outer->data_column_raw_ptr());
        int num_fields = static_cast<int>(struct_col->fields_size());

        // Resize parent + each subfield column to numRows so addresses line up with the
        // boxed-array writes performed on the Java side. Subfield resize is recursive:
        // a STRUCT subfield resizing only its top NullableColumn would leave its inner
        // children sized 0 when extract_struct_field_arrays writes the bitmap.
        nullable_outer->resize(num_rows);
        std::vector<jint> sub_field_types;
        sub_field_types.reserve(num_fields);
        for (int f = 0; f < num_fields; ++f) {
            struct_col->field_column_raw_ptr(f)->resize(num_rows);
            sub_field_types.emplace_back(static_cast<jint>(return_type.children[f].type));
        }

        jlong parent_addr = reinterpret_cast<jlong>(nullable_outer);
        ASSIGN_OR_RETURN(jobject per_field_arrays_obj,
                         helper.extract_struct_field_arrays(result, static_cast<int>(num_rows), parent_addr,
                                                             sub_field_types));
        if (per_field_arrays_obj == nullptr) {
            return Status::InternalError("extract_struct_field_arrays returned null");
        }
        auto per_field_arrays = reinterpret_cast<jobjectArray>(per_field_arrays_obj);
        DeferOp drop_per_field([&]() {
            JVMFunctionHelper::getInstance().getEnv()->DeleteLocalRef(per_field_arrays_obj);
        });

        JNIEnv* env = helper.getEnv();
        for (int f = 0; f < num_fields; ++f) {
            jobject field_array = env->GetObjectArrayElement(per_field_arrays, f);
            DeferOp drop_field([&]() {
                if (field_array) env->DeleteLocalRef(field_array);
            });
            const TypeDescriptor& field_type = return_type.children[f];
            Column* field_col = struct_col->field_column_raw_ptr(f);

            if (field_type.type == TYPE_STRUCT) {
                // Subfield columns are themselves NullableColumn(StructColumn) — strip the
                // outer NullableColumn to recurse into the inner struct.
                auto* sub_nullable = down_cast<NullableColumn*>(field_col);
                RETURN_IF_ERROR(get_struct_boxed_result(helper, field_type, field_array, num_rows, sub_nullable));
            } else if (is_decimalv3_field_type(field_type.type)) {
                RETURN_IF_ERROR(helper.get_decimal_result_from_boxed_array(
                        field_type.type, field_type.precision, field_type.scale, field_col, field_array,
                        static_cast<int>(num_rows), /*error_if_overflow=*/false));
            } else {
                RETURN_IF_ERROR(helper.get_result_from_boxed_array(field_type.type, field_col, field_array,
                                                                    static_cast<int>(num_rows)));
            }
        }
        return Status::OK();
    }

    StatusOr<ColumnPtr> get_boxed_result(FunctionContext* ctx, jobject result, size_t num_rows) {
        if (result == nullptr) {
            return ColumnHelper::create_const_null_column(num_rows);
        }
        auto& helper = JVMFunctionHelper::getInstance();
        DCHECK(call_desc->method_desc[0].is_box);
        const auto& return_type = ctx->get_return_type();
        auto res = ColumnHelper::create_column(return_type, true);
        if (is_decimalv3_field_type(return_type.type)) {
            RETURN_IF_ERROR(helper.get_decimal_result_from_boxed_array(return_type.type, return_type.precision,
                                                                       return_type.scale, res.get(), result, num_rows,
                                                                       ctx->error_if_overflow()));
        } else if (return_type.type == TYPE_STRUCT) {
            auto* nullable_outer = down_cast<NullableColumn*>(res.get());
            RETURN_IF_ERROR(get_struct_boxed_result(helper, return_type, result, num_rows, nullable_outer));
        } else {
            RETURN_IF_ERROR(helper.get_result_from_boxed_array(return_type.type, res.get(), result, num_rows));
        }
        RETURN_IF_ERROR(ColumnHelper::update_nested_has_null(res.get()));
        down_cast<NullableColumn*>(res.get())->update_has_null();
        return res;
    }
};

JavaFunctionCallExpr::JavaFunctionCallExpr(const TExprNode& node) : Expr(node) {}

StatusOr<ColumnPtr> JavaFunctionCallExpr::evaluate_checked(ExprContext* context, Chunk* ptr) {
    Columns columns(children().size());

    for (int i = 0; i < _children.size(); ++i) {
        ASSIGN_OR_RETURN(columns[i], _children[i]->evaluate_checked(context, ptr));
    }
    StatusOr<ColumnPtr> res;
    auto call_udf = [&]() {
        res = _call_helper->call(context->fn_context(_fn_context_index), columns, ptr != nullptr ? ptr->num_rows() : 1);
        return Status::OK();
    };
    (void)call_function_in_pthread(_runtime_state, call_udf)->get_future().get();
    return res;
}

JavaFunctionCallExpr::~JavaFunctionCallExpr() {
    // nothing to do if JavaFunctionCallExpr has not been prepared
    if (_runtime_state == nullptr) return;
    auto promise = call_function_in_pthread(_runtime_state, [this]() {
        this->_func_desc.reset();
        this->_call_helper.reset();
        return Status::OK();
    });
    (void)promise->get_future().get();
}

// TODO support prepare UDF
Status JavaFunctionCallExpr::prepare(RuntimeState* state, ExprContext* context) {
    _runtime_state = state;
    // init Expr::prepare
    RETURN_IF_ERROR(Expr::prepare(state, context));

    if (!_fn.__isset.fid) {
        return Status::InternalError("Not Found function id for " + _fn.name.function_name);
    }

    FunctionContext::TypeDesc return_type = _type;
    std::vector<FunctionContext::TypeDesc> args_types;

    args_types.reserve(_children.size());
    for (Expr* child : _children) {
        args_types.push_back(child->type());
    }

    // todo: varargs use for allocate slice memory, need compute buffer size
    //  for varargs in vectorized engine?
    _fn_context_index = context->register_func(state, return_type, args_types);
    context->fn_context(_fn_context_index)->set_is_udf(true);

    _func_desc = std::make_shared<JavaUDFContext>();
    // TODO:
    _is_returning_random_value = false;
    return Status::OK();
}

bool JavaFunctionCallExpr::is_constant() const {
    if (_is_returning_random_value) {
        return false;
    }
    return Expr::is_constant();
}

StatusOr<std::shared_ptr<JavaUDFContext>> JavaFunctionCallExpr::_build_udf_func_desc(
        FunctionContext::FunctionStateScope scope, const std::string& libpath) {
    auto desc = std::make_shared<JavaUDFContext>();
    // init class loader and analyzer
    desc->udf_classloader = std::make_unique<ClassLoader>(libpath);
    RETURN_IF_ERROR(desc->udf_classloader->init());
    desc->analyzer = std::make_unique<ClassAnalyzer>();

    ASSIGN_OR_RETURN(desc->udf_class, desc->udf_classloader->getClass(_fn.scalar_fn.symbol));

    auto add_method = [&](const std::string& name, std::unique_ptr<JavaMethodDescriptor>* res) {
        bool has_method = false;
        std::string method_name = name;
        std::string signature;
        std::vector<MethodTypeDescriptor> mtdesc;
        RETURN_IF_ERROR(desc->analyzer->has_method(desc->udf_class.clazz(), method_name, &has_method));
        if (has_method) {
            RETURN_IF_ERROR(desc->analyzer->get_signature(desc->udf_class.clazz(), method_name, &signature));
            RETURN_IF_ERROR(desc->analyzer->get_method_desc(signature, &mtdesc));
            *res = std::make_unique<JavaMethodDescriptor>();
            (*res)->name = std::move(method_name);
            (*res)->signature = std::move(signature);
            (*res)->method_desc = std::move(mtdesc);
            ASSIGN_OR_RETURN((*res)->method, desc->analyzer->get_method_object(desc->udf_class.clazz(), name));
        }
        return Status::OK();
    };

    // Now we don't support prepare/close for UDF
    // RETURN_IF_ERROR(add_method("prepare", &desc->prepare));
    // RETURN_IF_ERROR(add_method("method_close", &desc->close));
    RETURN_IF_ERROR(add_method("evaluate", &desc->evaluate));

    // Cache the formal parameter / return Class<?> refs for STRUCT-typed UDF args
    // (and STRUCT return type). Only populated for STRUCT slots; non-STRUCT slots
    // get an empty JavaGlobalRef and the converter falls through to the existing
    // boxing path.
    {
        auto& helper = JVMFunctionHelper::getInstance();
        JNIEnv* env = helper.getEnv();
        jobject method_obj = desc->evaluate->method.handle();

        jclass method_class = env->FindClass("java/lang/reflect/Method");
        DCHECK(method_class != nullptr);
        DeferOp drop_method_class([&]() { env->DeleteLocalRef(method_class); });

        jmethodID get_param_types =
                env->GetMethodID(method_class, "getParameterTypes", "()[Ljava/lang/Class;");
        jmethodID get_return_type = env->GetMethodID(method_class, "getReturnType", "()Ljava/lang/Class;");
        DCHECK(get_param_types != nullptr && get_return_type != nullptr);

        // STRUCT args: cache jclass for each slot whose declared SQL type is STRUCT.
        int num_args = static_cast<int>(_children.size());
        desc->evaluate_arg_classes.reserve(num_args);
        bool has_struct = false;
        for (int i = 0; i < num_args; ++i) {
            if (_children[i]->type().type == TYPE_STRUCT) {
                has_struct = true;
                break;
            }
        }
        if (_type.type == TYPE_STRUCT) {
            has_struct = true;
        }

        if (has_struct) {
            jobjectArray param_types =
                    (jobjectArray)env->CallObjectMethod(method_obj, get_param_types);
            if (env->ExceptionCheck()) {
                env->ExceptionClear();
                return Status::InternalError("failed to introspect UDF evaluate parameter types");
            }
            DeferOp drop_param_types([&]() {
                if (param_types) env->DeleteLocalRef(param_types);
            });

            for (int i = 0; i < num_args; ++i) {
                if (_children[i]->type().type != TYPE_STRUCT) {
                    desc->evaluate_arg_classes.emplace_back(nullptr);
                    continue;
                }
                jobject cls = env->GetObjectArrayElement(param_types, i);
                if (cls == nullptr) {
                    return Status::InternalError(
                            fmt::format("UDF evaluate parameter {} class is null", i));
                }
                jobject gref = env->NewGlobalRef(cls);
                env->DeleteLocalRef(cls);
                desc->evaluate_arg_classes.emplace_back(gref);
            }

            if (_type.type == TYPE_STRUCT) {
                jobject ret_cls = env->CallObjectMethod(method_obj, get_return_type);
                if (env->ExceptionCheck() || ret_cls == nullptr) {
                    env->ExceptionClear();
                    return Status::InternalError("failed to introspect UDF evaluate return type");
                }
                desc->evaluate_return_class = JavaGlobalRef(env->NewGlobalRef(ret_cls));
                env->DeleteLocalRef(ret_cls);
            }
        }
    }

    // create UDF function instance
    ASSIGN_OR_RETURN(desc->udf_handle, desc->udf_class.newInstance());
    // BatchEvaluateStub
    auto* stub_clazz = BatchEvaluateStub::stub_clazz_name;
    auto* stub_method_name = BatchEvaluateStub::batch_evaluate_method_name;
    auto udf_clazz = desc->udf_class.clazz();
    auto update_method = desc->evaluate->method.handle();

    // For varargs UDFs, pass the actual number of varargs input columns (excluding fixed params)
    // so that the stub generator produces the correct signature.
    // method_desc layout: [return, fixedParam1, ..., fixedParamF, varargs_elem] → size = F + 2
    // so numFixedParams = method_desc.size() - 2.
    int num_fixed_params = (_fn.has_var_args && desc->evaluate)
                                   ? std::max(0, static_cast<int>(desc->evaluate->method_desc.size()) - 2)
                                   : 0;
    int num_actual_var_args = _fn.has_var_args ? std::max(0, static_cast<int>(_children.size()) - num_fixed_params) : 0;
    ASSIGN_OR_RETURN(auto update_stub_clazz,
                     desc->udf_classloader->genCallStub(stub_clazz, udf_clazz, update_method,
                                                        ClassLoader::BATCH_EVALUATE, num_actual_var_args));
    ASSIGN_OR_RETURN(auto method, desc->analyzer->get_method_object(update_stub_clazz.clazz(), stub_method_name));
    desc->call_stub = std::make_unique<BatchEvaluateStub>(desc->udf_handle.handle(), std::move(update_stub_clazz),
                                                          JavaGlobalRef(method));

    if (desc->prepare != nullptr) {
        // we only support fragment local scope to call prepare
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            // TODO: handle prepare function
        }
    }

    return desc;
}

Status JavaFunctionCallExpr::open(RuntimeState* state, ExprContext* context,
                                  FunctionContext::FunctionStateScope scope) {
    // init parent open
    RETURN_IF_ERROR(Expr::open(state, context, scope));
    RETURN_IF_ERROR(detect_java_runtime());
    // init function context
    Columns const_columns;
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        const_columns.reserve(_children.size());
        for (const auto& child : _children) {
            ASSIGN_OR_RETURN(auto&& child_col, child->evaluate_const(context))
            const_columns.emplace_back(std::move(child_col));
        }
    }

    UserFunctionCache::FunctionCacheDesc func_cache_desc(_fn.fid, _fn.hdfs_location, _fn.checksum,
                                                         TFunctionBinaryType::SRJAR, _fn.cloud_configuration);
    // cacheable
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto get_func_desc = [this, scope, state](const std::string& lib) -> StatusOr<std::any> {
            std::any func_desc;
            auto call = [&]() {
                ASSIGN_OR_RETURN(func_desc, _build_udf_func_desc(scope, lib));
                return Status::OK();
            };
            RETURN_IF_ERROR(call_function_in_pthread(state, call)->get_future().get());
            return func_desc;
        };

        auto function_cache = UserFunctionCache::instance();
        if (_fn.__isset.isolated && !_fn.isolated) {
            ASSIGN_OR_RETURN(auto desc, function_cache->load_cacheable_java_udf(func_cache_desc, get_func_desc));
            _func_desc = std::any_cast<std::shared_ptr<JavaUDFContext>>(desc.second);
        } else {
            std::string libpath;
            RETURN_IF_ERROR(function_cache->get_libpath(func_cache_desc, &libpath));
            ASSIGN_OR_RETURN(auto desc, get_func_desc(libpath));
            _func_desc = std::any_cast<std::shared_ptr<JavaUDFContext>>(desc);
        }

        _call_helper = std::make_shared<UDFFunctionCallHelper>();
        _call_helper->fn_desc = _func_desc.get();
        _call_helper->call_desc = _func_desc->evaluate.get();
    }
    return Status::OK();
}

void JavaFunctionCallExpr::close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) {
    auto function_close = [this, scope]() {
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            if (_func_desc && _func_desc->close) {
                _call_udf_close();
            }
            _func_desc.reset();
            _call_helper.reset();
        }
        return Status::OK();
    };
    (void)call_function_in_pthread(state, function_close)->get_future().get();
    Expr::close(state, context, scope);
}

void JavaFunctionCallExpr::_call_udf_close() {}

} // namespace starrocks
