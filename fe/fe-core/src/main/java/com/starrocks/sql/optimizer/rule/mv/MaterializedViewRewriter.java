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
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.type.BitmapType;
import com.starrocks.type.HLLType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PercentileType;
import com.starrocks.type.Type;

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
                    ScalarOperator rewritten = widenConditional(replaceColumnRefRewriter.rewrite(kv.getValue()));
                    // Propagate the (possibly widened) result type onto the slot
                    // ColumnRefOperator. ColumnRefOperator equality is by id only,
                    // so this updates every consumer that holds the same ref --
                    // notably the upstream aggregate's input arg, which we then
                    // re-resolve in visitLogicalAggregate.
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
     * Apply the column-ref substitution and propagate any widened child types
     * up through wrapping IF / IFNULL / COALESCE / CASE WHEN nodes.
     */
    private CallOperator finalizeAgg(ReplaceColumnRefRewriter replaceColumnRefRewriter, CallOperator call) {
        return (CallOperator) widenConditional(replaceColumnRefRewriter.rewrite(call));
    }

    /**
     * Retype every IF / IFNULL / COALESCE / CASE WHEN inside {@code op} to the
     * common supertype of its value-producing branches.
     *
     * <p>An MV column substitution can widen a value child (e.g. SMALLINT k3
     * is replaced by BIGINT mv_sum_k3). Without retyping, the wrapping
     * conditional keeps its original narrow result type and the BE
     * instantiates the Expr at the wrong type, crashing in down_cast.
     */
    private static ScalarOperator widenConditional(ScalarOperator op) {
        return op == null ? null : op.accept(WIDEN_CONDITIONAL_SHUTTLE, null);
    }

    private static final BaseScalarOperatorShuttle WIDEN_CONDITIONAL_SHUTTLE = new BaseScalarOperatorShuttle() {
        @Override
        public ScalarOperator visitCall(CallOperator call, Void context) {
            CallOperator c = (CallOperator) super.visitCall(call, context);
            String name = c.getFnName();
            if (FunctionSet.IF.equalsIgnoreCase(name) && c.getChildren().size() == 3) {
                return tryWiden(c, Lists.newArrayList(c.getChild(1), c.getChild(2)));
            }
            if (FunctionSet.COALESCE.equalsIgnoreCase(name) || FunctionSet.IFNULL.equalsIgnoreCase(name)) {
                return tryWiden(c, c.getChildren());
            }
            return c;
        }

        @Override
        public ScalarOperator visitCaseWhenOperator(CaseWhenOperator op, Void context) {
            CaseWhenOperator cw = (CaseWhenOperator) super.visitCaseWhenOperator(op, context);
            List<ScalarOperator> branches = Lists.newArrayList();
            for (int i = 0; i < cw.getWhenClauseSize(); i++) {
                branches.add(cw.getThenClause(i));
            }
            if (cw.hasElse()) {
                branches.add(cw.getElseClause());
            }
            return tryWiden(cw, branches);
        }

        private ScalarOperator tryWiden(ScalarOperator op, List<ScalarOperator> branches) {
            if (branches.isEmpty()) {
                return op;
            }
            Type newType = TypeManager.getCommonSuperType(
                    branches.stream().map(ScalarOperator::getType).collect(Collectors.toList()));
            if (newType == null || !newType.isValid() || newType.matchesType(op.getType())) {
                return op;
            }
            return rebuildWidened(op, newType);
        }
    };

    /**
     * Cast value branches of {@code op} to {@code newType} and rebuild it.
     * For CallOperator conditionals also re-resolves the Function so the
     * declared signature matches the new child types.
     */
    private static ScalarOperator rebuildWidened(ScalarOperator op, Type newType) {
        if (op instanceof CaseWhenOperator) {
            CaseWhenOperator cw = (CaseWhenOperator) op;
            ScalarOperator caseClause = cw.hasCase() ? cw.getCaseClause() : null;
            List<ScalarOperator> whenThens = Lists.newArrayList();
            for (int i = 0; i < cw.getWhenClauseSize(); i++) {
                whenThens.add(cw.getWhenClause(i));
                whenThens.add(castIfNeeded(cw.getThenClause(i), newType));
            }
            ScalarOperator elseClause = cw.hasElse() ? castIfNeeded(cw.getElseClause(), newType) : null;
            return new CaseWhenOperator(newType, caseClause, elseClause, whenThens);
        }
        CallOperator call = (CallOperator) op;
        String name = call.getFnName();
        // IF reserves arg 0 for the condition; COALESCE/IFNULL widen all args.
        int firstValueIdx = FunctionSet.IF.equalsIgnoreCase(name) ? 1 : 0;
        List<ScalarOperator> args = Lists.newArrayList(call.getChildren());
        for (int i = firstValueIdx; i < args.size(); i++) {
            args.set(i, castIfNeeded(args.get(i), newType));
        }
        Type[] argTypes = args.stream().map(ScalarOperator::getType).toArray(Type[]::new);
        Function fn = ExprUtils.getBuiltinFunction(name, argTypes,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator(name, newType, args, fn != null ? fn : call.getFunction(), call.isDistinct());
    }

    private static ScalarOperator castIfNeeded(ScalarOperator op, Type target) {
        if (target.matchesType(op.getType())) {
            return op;
        }
        // Fold cast(literal as T) so the rendered plan stays tidy.
        if (op instanceof ConstantOperator) {
            Optional<ConstantOperator> folded = ((ConstantOperator) op).castTo(target);
            if (folded.isPresent()) {
                return folded.get();
            }
        }
        return new CastOperator(target, op, true);
    }

    /**
     * Re-resolve {@code call}'s Function when its actual children types no
     * longer match the resolved signature, returning the original call when
     * no change is needed.
     */
    private static CallOperator reResolveSignature(CallOperator call) {
        Function fn = call.getFunction();
        if (fn == null || call.getChildren().isEmpty()) {
            return call;
        }
        Type[] expected = fn.getArgs();
        Type[] actual = call.getChildren().stream().map(ScalarOperator::getType).toArray(Type[]::new);
        boolean changed = false;
        for (int i = 0; i < actual.length && i < expected.length; i++) {
            if (!actual[i].matchesType(expected[i])) {
                changed = true;
                break;
            }
        }
        if (!changed) {
            return call;
        }
        Function newFn = ExprUtils.getBuiltinFunction(call.getFnName(), actual,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        if (newFn == null) {
            return call;
        }
        return new CallOperator(call.getFnName(), newFn.getReturnType(),
                Lists.newArrayList(call.getChildren()), newFn, call.isDistinct());
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
        // The child Project may have widened a slot type via setType above; any
        // aggregate whose argument is that slot still has its Function resolved
        // against the narrow type and would make the BE pick the wrong
        // SumAggregateFunction template. Re-resolve signatures whose actual
        // child types no longer match the previously-resolved Function.
        newAggMap.replaceAll((k, v) -> reResolveSignature(v));
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
