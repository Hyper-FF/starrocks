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

package com.starrocks.sql.optimizer.rule.mv;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.type.BitmapType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.HLLType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PercentileType;
import com.starrocks.type.Type;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.catalog.Function.CompareMode.IS_IDENTICAL;

public class MaterializedViewRewriter extends OptExpressionVisitor<OptExpression, MaterializedViewRule.RewriteContext> {

    public MaterializedViewRewriter() {
    }

    public OptExpression rewrite(OptExpression optExpression, MaterializedViewRule.RewriteContext context) {
        return optExpression.getOp().accept(this, optExpression, context);
    }

    public static boolean isCaseWhenScalarOperator(ScalarOperator operator) {
        if (operator instanceof CaseWhenOperator) {
            return true;
        }

        return operator instanceof CallOperator &&
                FunctionSet.IF.equalsIgnoreCase(((CallOperator) operator).getFnName());
    }

    @Override
    public OptExpression visit(OptExpression optExpression, MaterializedViewRule.RewriteContext context) {
        for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
            optExpression.setChild(childIdx, rewrite(optExpression.inputAt(childIdx), context));
        }

        return OptExpression.create(optExpression.getOp(), optExpression.getInputs());
    }

    @Override
    public OptExpression visitLogicalProject(OptExpression optExpression, MaterializedViewRule.RewriteContext context) {
        for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
            optExpression.setChild(childIdx, rewrite(optExpression.inputAt(childIdx), context));
        }

        LogicalProjectOperator projectOperator = (LogicalProjectOperator) optExpression.getOp();

        Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : projectOperator.getColumnRefMap().entrySet()) {
            ColumnRefOperator queryColRef = kv.getKey();
            ScalarOperator queryScalarOperator = kv.getValue();
            if (queryScalarOperator.getUsedColumns().contains(context.queryColumnRef)) {
                if (queryScalarOperator instanceof ColumnRefOperator) {
                    newProjectMap.put(context.mvColumnRef, context.mvColumnRef);
                } else if (isCaseWhenScalarOperator(queryScalarOperator)) {
                    // rewrite query column ref into mv agg column ref,
                    // eg: sum(case when a > 1 then b else 0 end), rewrite to sum(case when mv_column > 1 then b else 0 end)
                    Map<ColumnRefOperator, ScalarOperator> replaceMap = new HashMap<>();
                    replaceMap.put(context.queryColumnRef, context.mvColumnRef);
                    ReplaceColumnRefRewriter replaceColumnRefRewriter = new ReplaceColumnRefRewriter(replaceMap);
                    ScalarOperator rewritten = replaceColumnRefRewriter.rewrite(kv.getValue());
                    // The substitution may widen a value-producing child's type
                    // (e.g. SMALLINT -> BIGINT for a sum-MV column). Re-derive
                    // the wrapping IF / CASE WHEN / COALESCE result type so that
                    // children remain consistent with the call signature, and
                    // also propagate the new type to the projection's output
                    // column ref so upstream operators (e.g. HashAgg consuming
                    // this slot) see the widened input type.
                    rewritten = retypeConditionalExpressions(rewritten);
                    if (!rewritten.getType().matchesType(queryColRef.getType())) {
                        queryColRef.setType(rewritten.getType());
                    }
                    newProjectMap.put(queryColRef, rewritten);
                } else {
                    // eg: bitmap_union(to_bitmap(a)), still rewrite to bitmap_union(to_bitmap(a))
                    newProjectMap.put(queryColRef, context.mvColumnRef);
                }
            } else {
                newProjectMap.put(queryColRef, queryScalarOperator);
            }
        }
        return OptExpression.create(new LogicalProjectOperator(newProjectMap), optExpression.getInputs());
    }

    @Override
    public OptExpression visitLogicalTableScan(OptExpression optExpression,
                                               MaterializedViewRule.RewriteContext context) {
        if (!OperatorType.LOGICAL_OLAP_SCAN.equals(optExpression.getOp().getOpType())) {
            return optExpression;
        }

        LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) optExpression.getOp();

        if (olapScanOperator.getColRefToColumnMetaMap().containsKey(context.queryColumnRef)) {
            Map<ColumnRefOperator, Column> columnRefOperatorColumnMap =
                    new HashMap<>(olapScanOperator.getColRefToColumnMetaMap());
            columnRefOperatorColumnMap.remove(context.queryColumnRef);
            columnRefOperatorColumnMap.put(context.mvColumnRef, context.mvColumn);

            LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
            LogicalOlapScanOperator newScanOperator = builder.withOperator(olapScanOperator)
                    .setColRefToColumnMetaMap(columnRefOperatorColumnMap).build();
            optExpression = OptExpression.create(newScanOperator, optExpression.getInputs());
        }
        return optExpression;
    }

    private CallOperator rewriteAggregateFunc(ReplaceColumnRefRewriter replaceColumnRefRewriter,
                                              Column mvColumn,
                                              CallOperator queryAggFunc) {
        String functionName = queryAggFunc.getFnName();
        if (functionName.equals(FunctionSet.COUNT) && !queryAggFunc.isDistinct()) {
            CallOperator callOperator = new CallOperator(FunctionSet.SUM,
                    queryAggFunc.getType(),
                    queryAggFunc.getChildren(),
                    ExprUtils.getBuiltinFunction(FunctionSet.SUM, new Type[] {IntegerType.BIGINT}, IS_IDENTICAL));
            return finalizeAgg(replaceColumnRefRewriter, callOperator);
        } else if (functionName.equals(FunctionSet.SUM) && !queryAggFunc.isDistinct()) {
            CallOperator callOperator = new CallOperator(FunctionSet.SUM,
                    queryAggFunc.getType(),
                    queryAggFunc.getChildren(),
                    ScalarOperatorUtil.findSumFn(new Type[] {mvColumn.getType()}));
            return finalizeAgg(replaceColumnRefRewriter, callOperator);
        } else if (((functionName.equals(FunctionSet.COUNT) && queryAggFunc.isDistinct())
                || functionName.equals(FunctionSet.MULTI_DISTINCT_COUNT)) &&
                mvColumn.getAggregationType() == AggregateType.BITMAP_UNION) {
            CallOperator callOperator = new CallOperator(FunctionSet.BITMAP_UNION_COUNT,
                    queryAggFunc.getType(),
                    queryAggFunc.getChildren(),
                    ExprUtils.getBuiltinFunction(FunctionSet.BITMAP_UNION_COUNT, new Type[] {BitmapType.BITMAP},
                            IS_IDENTICAL));
            return finalizeAgg(replaceColumnRefRewriter, callOperator);
        } else if (functionName.equals(FunctionSet.BITMAP_AGG) &&
                mvColumn.getAggregationType() == AggregateType.BITMAP_UNION) {
            CallOperator callOperator = new CallOperator(FunctionSet.BITMAP_UNION,
                    queryAggFunc.getType(),
                    queryAggFunc.getChildren(),
                    ExprUtils.getBuiltinFunction(FunctionSet.BITMAP_UNION, new Type[] {BitmapType.BITMAP},
                            IS_IDENTICAL));
            return finalizeAgg(replaceColumnRefRewriter, callOperator);
        } else if (
                (functionName.equals(FunctionSet.NDV) || functionName.equals(FunctionSet.APPROX_COUNT_DISTINCT))
                        && mvColumn.getAggregationType() == AggregateType.HLL_UNION) {
            CallOperator callOperator = new CallOperator(FunctionSet.HLL_UNION_AGG,
                    queryAggFunc.getType(),
                    queryAggFunc.getChildren(),
                    ExprUtils.getBuiltinFunction(FunctionSet.HLL_UNION_AGG, new Type[] {HLLType.HLL}, IS_IDENTICAL));
            return finalizeAgg(replaceColumnRefRewriter, callOperator);
        } else if (functionName.equals(FunctionSet.PERCENTILE_APPROX) &&
                mvColumn.getAggregationType() == AggregateType.PERCENTILE_UNION) {

            ScalarOperator child = queryAggFunc.getChildren().get(0);
            if (child instanceof CastOperator) {
                child = child.getChild(0);
            }
            Preconditions.checkState(child instanceof ColumnRefOperator);
            CallOperator callOperator = new CallOperator(FunctionSet.PERCENTILE_UNION,
                    queryAggFunc.getType(),
                    Lists.newArrayList(child),
                    ExprUtils.getBuiltinFunction(FunctionSet.PERCENTILE_UNION,
                            new Type[] {PercentileType.PERCENTILE}, IS_IDENTICAL));
            return finalizeAgg(replaceColumnRefRewriter, callOperator);
        } else {
            return finalizeAgg(replaceColumnRefRewriter, queryAggFunc);
        }
    }

    /**
     * Apply {@link #replaceColumnRefRewriter} (column-ref substitution) and then
     * re-derive the result type of any IF / IFNULL / COALESCE / CASE WHEN node
     * found inside the rewritten expression. The substitution may widen a child
     * type (typical: SMALLINT -> BIGINT for a sum-MV column), and we must
     * propagate that change through the wrapping conditional so the call
     * signature stays consistent with its children.
     */
    private CallOperator finalizeAgg(ReplaceColumnRefRewriter replaceColumnRefRewriter, CallOperator call) {
        ScalarOperator rewritten = replaceColumnRefRewriter.rewrite(call);
        return (CallOperator) retypeConditionalExpressions(rewritten);
    }

    private static ScalarOperator retypeConditionalExpressions(ScalarOperator op) {
        if (op == null) {
            return null;
        }
        for (int i = 0; i < op.getChildren().size(); i++) {
            op.setChild(i, retypeConditionalExpressions(op.getChild(i)));
        }
        if (op instanceof CaseWhenOperator) {
            return retypeCaseWhen((CaseWhenOperator) op);
        }
        if (op instanceof CallOperator) {
            CallOperator call = (CallOperator) op;
            String name = call.getFnName();
            if (FunctionSet.IF.equalsIgnoreCase(name)) {
                return retypeIf(call);
            }
            if (FunctionSet.COALESCE.equalsIgnoreCase(name)
                    || FunctionSet.IFNULL.equalsIgnoreCase(name)) {
                return retypeCoalesceLike(call);
            }
        }
        return op;
    }

    private static ScalarOperator retypeCaseWhen(CaseWhenOperator op) {
        List<Type> valueTypes = Lists.newArrayList();
        for (int i = 0; i < op.getWhenClauseSize(); i++) {
            valueTypes.add(op.getThenClause(i).getType());
        }
        if (op.hasElse()) {
            valueTypes.add(op.getElseClause().getType());
        }
        if (valueTypes.isEmpty()) {
            return op;
        }
        Type newCommon = TypeManager.getCommonSuperType(valueTypes);
        if (newCommon == null || !newCommon.isValid() || newCommon.matchesType(op.getType())) {
            return op;
        }
        ScalarOperator caseClause = op.hasCase() ? op.getCaseClause() : null;
        List<ScalarOperator> whenThens = Lists.newArrayList();
        for (int i = 0; i < op.getWhenClauseSize(); i++) {
            whenThens.add(op.getWhenClause(i));
            whenThens.add(castIfNeeded(op.getThenClause(i), newCommon));
        }
        ScalarOperator elseClause = op.hasElse() ? castIfNeeded(op.getElseClause(), newCommon) : null;
        return new CaseWhenOperator(newCommon, caseClause, elseClause, whenThens);
    }

    private static ScalarOperator retypeIf(CallOperator call) {
        if (call.getArguments().size() != 3) {
            return call;
        }
        ScalarOperator cond = call.getArguments().get(0);
        ScalarOperator thenOp = call.getArguments().get(1);
        ScalarOperator elseOp = call.getArguments().get(2);
        Type newCommon = TypeManager.getCommonSuperType(thenOp.getType(), elseOp.getType());
        if (newCommon == null || !newCommon.isValid() || newCommon.matchesType(call.getType())) {
            return call;
        }
        List<ScalarOperator> args = Lists.newArrayList(
                cond, castIfNeeded(thenOp, newCommon), castIfNeeded(elseOp, newCommon));
        Function fn = ExprUtils.getBuiltinFunction(FunctionSet.IF,
                new Type[] {BooleanType.BOOLEAN, newCommon, newCommon},
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator(FunctionSet.IF, newCommon, args, fn);
    }

    private static ScalarOperator retypeCoalesceLike(CallOperator call) {
        if (call.getArguments().isEmpty()) {
            return call;
        }
        List<Type> argTypes = call.getArguments().stream()
                .map(ScalarOperator::getType)
                .collect(Collectors.toList());
        Type newCommon = TypeManager.getCommonSuperType(argTypes);
        if (newCommon == null || !newCommon.isValid() || newCommon.matchesType(call.getType())) {
            return call;
        }
        List<ScalarOperator> args = call.getArguments().stream()
                .map(a -> castIfNeeded(a, newCommon))
                .collect(Collectors.toList());
        Type[] sigArgs = new Type[args.size()];
        Arrays.fill(sigArgs, newCommon);
        Function fn = ExprUtils.getBuiltinFunction(call.getFnName(), sigArgs,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator(call.getFnName(), newCommon, args, fn);
    }

    /**
     * If {@code aggCall}'s actual argument types no longer match its declared
     * function signature (because an upstream projection widened a child
     * ColumnRef type), re-resolve the builtin function so the BE picks the
     * specialization matching the actual chunk column types.  Without this,
     * BE would still see e.g. {@code sum(SMALLINT)} while being fed a BIGINT
     * column, triggering an aggregator down_cast assertion / SIGABRT.
     */
    private static CallOperator resyncAggCallToArgTypes(CallOperator aggCall) {
        Function fn = aggCall.getFunction();
        if (fn == null) {
            return aggCall;
        }
        Type[] declaredArgs = fn.getArgs();
        List<ScalarOperator> actualArgs = aggCall.getArguments();
        if (declaredArgs.length != actualArgs.size()) {
            return aggCall;
        }
        Type[] actualTypes = new Type[actualArgs.size()];
        boolean mismatch = false;
        for (int i = 0; i < actualArgs.size(); i++) {
            actualTypes[i] = actualArgs.get(i).getType();
            if (!declaredArgs[i].matchesType(actualTypes[i])) {
                mismatch = true;
            }
        }
        if (!mismatch) {
            return aggCall;
        }
        Function newFn;
        if (FunctionSet.SUM.equalsIgnoreCase(aggCall.getFnName()) && actualTypes.length == 1) {
            newFn = ScalarOperatorUtil.findSumFn(actualTypes);
        } else {
            newFn = ExprUtils.getBuiltinFunction(aggCall.getFnName(), actualTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        }
        if (newFn == null) {
            return aggCall;
        }
        return new CallOperator(aggCall.getFnName(), aggCall.getType(),
                actualArgs, newFn, aggCall.isDistinct(), aggCall.isRemovedDistinct());
    }

    private static ScalarOperator castIfNeeded(ScalarOperator op, Type target) {
        if (target.matchesType(op.getType())) {
            return op;
        }
        // Fold cast(literal as T) into a typed literal so the plan dump stays
        // tidy even if no constant-folding pass runs after this rewrite.
        if (op instanceof ConstantOperator) {
            Optional<ConstantOperator> folded = ((ConstantOperator) op).castTo(target);
            if (folded.isPresent()) {
                return folded.get();
            }
        }
        return new CastOperator(target, op, true);
    }

    @Override
    public OptExpression visitLogicalAggregate(OptExpression optExpression,
                                               MaterializedViewRule.RewriteContext context) {
        for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
            optExpression.setChild(childIdx, rewrite(optExpression.inputAt(childIdx), context));
        }

        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) optExpression.getOp();

        Map<ColumnRefOperator, ScalarOperator> replaceMap = new HashMap<>();
        replaceMap.put(context.queryColumnRef, context.mvColumnRef);
        ReplaceColumnRefRewriter replaceColumnRefRewriter = new ReplaceColumnRefRewriter(replaceMap);

        Map<ColumnRefOperator, CallOperator> newAggMap = new HashMap<>(aggregationOperator.getAggregations());
        for (Map.Entry<ColumnRefOperator, CallOperator> kv : aggregationOperator.getAggregations().entrySet()) {
            CallOperator queryAggFunc = kv.getValue();
            if (queryAggFunc.getUsedColumns().isEmpty()) {
                break;
            }

            String functionName = queryAggFunc.getFnName();
            if (functionName.equals(context.aggCall.getFnName())
                    && queryAggFunc.getUsedColumns().getFirstId() == context.queryColumnRef.getId()) {
                CallOperator newAggFunc = rewriteAggregateFunc(replaceColumnRefRewriter, context.mvColumn, queryAggFunc);
                if (newAggFunc != null) {
                    newAggMap.put(kv.getKey(), newAggFunc);
                    break;
                }
            }
        }
        // visitLogicalProject may have widened a projection's output ColumnRef
        // type (e.g. from SMALLINT to BIGINT for the sum-MV column substitution
        // inside an IF/CASE WHEN).  Any aggregate function consuming such a
        // column still carries the old (narrower) function signature here, so
        // re-resolve its builtin function to the actual argument type.
        newAggMap.replaceAll((out, aggCall) -> resyncAggCallToArgTypes(aggCall));
        return OptExpression.create(new LogicalAggregationOperator(
                aggregationOperator.getType(),
                aggregationOperator.getGroupingKeys(),
                aggregationOperator.getPartitionByColumns(),
                newAggMap,
                aggregationOperator.isSplit(),
                aggregationOperator.getLimit(),
                aggregationOperator.getPredicate()), optExpression.getInputs());
    }

    @Override
    public OptExpression visitLogicalTableFunction(OptExpression optExpression,
                                                   MaterializedViewRule.RewriteContext context) {
        for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
            optExpression.setChild(childIdx, rewrite(optExpression.inputAt(childIdx), context));
        }

        List<ColumnRefOperator> newOuterColumns = Lists.newArrayList();
        LogicalTableFunctionOperator tableFunctionOperator = (LogicalTableFunctionOperator) optExpression.getOp();
        for (ColumnRefOperator col : tableFunctionOperator.getOuterColRefs()) {
            if (col.equals(context.queryColumnRef)) {
                newOuterColumns.add(context.mvColumnRef);
            } else {
                newOuterColumns.add(col);
            }
        }

        LogicalTableFunctionOperator newOperator = (new LogicalTableFunctionOperator.Builder())
                .withOperator(tableFunctionOperator)
                .setOuterColRefs(newOuterColumns)
                .build();

        return OptExpression.create(newOperator, optExpression.getInputs());
    }

    @Override
    public OptExpression visitLogicalJoin(OptExpression optExpression, MaterializedViewRule.RewriteContext context) {
        for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
            optExpression.setChild(childIdx, rewrite(optExpression.inputAt(childIdx), context));
        }

        LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.getOp();
        return OptExpression.create(joinOperator, optExpression.getInputs());
    }
}
