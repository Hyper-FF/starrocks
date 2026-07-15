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

package com.starrocks.sql.plan;

import com.starrocks.catalog.Function;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.tree.exprreuse.ScalarOperatorsReuse;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.thrift.TFunctionBinaryType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

// Splits a PhysicalProjectOperator that contains a UDF into three stacked project operators:
//
//   pre-Project  -> UDF-Project -> post-Project
//
//   - pre-Project computes each UDF argument that is not already a column into an intermediate
//     column (skipped when every argument is already a column).
//   - UDF-Project's columnRefMap contains ONLY the UDF calls (each reading columns), so the BE
//     routes just this node onto the async, non-pipeline-blocking operator and the background thread
//     does essentially only the Flight RPC.
//   - post-Project reproduces the original outputs, with each UDF call subtree replaced by its output
//     column (handles UDFs nested inside larger expressions).
//
// Applied during fragment building (PlanFragmentBuilder.visitPhysicalProject), which then emits one
// ProjectNode per stage by recursing -- so the optimizer's physical tree (and plan tests) are not
// perturbed. v1 leaves a UDF nested inside another UDF's arguments unsplit.
public class AsyncUdfProjectionSplitter {
    private AsyncUdfProjectionSplitter() {
    }

    public static boolean containsArrowUdf(Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
        for (ScalarOperator value : columnRefMap.values()) {
            if (containsArrowUdf(value)) {
                return true;
            }
        }
        return false;
    }

    // A UDF may hide in the common-sub-expression map (e.g. a UDF call reused across output columns),
    // so check both maps.
    public static boolean containsArrowUdf(Map<ColumnRefOperator, ScalarOperator> columnRefMap,
                                           Map<ColumnRefOperator, ScalarOperator> commonSubMap) {
        return containsArrowUdf(columnRefMap) || (commonSubMap != null && containsArrowUdf(commonSubMap));
    }

    // A projection is already in the isolated UDF-only form (the udf stage this splitter produces):
    // every value is a passthrough column, or a bare UDF call whose arguments are all columns.
    public static boolean isAlreadyIsolated(Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
        boolean anyUdf = false;
        for (ScalarOperator value : columnRefMap.values()) {
            if (value instanceof ColumnRefOperator) {
                continue;
            }
            if (value instanceof CallOperator && isArrowUdf((CallOperator) value)) {
                anyUdf = true;
                for (ScalarOperator arg : value.getChildren()) {
                    if (!(arg instanceof ColumnRefOperator)) {
                        return false;
                    }
                }
                continue;
            }
            return false;
        }
        return anyUdf;
    }

    // Splits a projection's columnRefMap into the stage maps [pre?, udf, post] (bottom-to-top), or
    // null when no split applies (no UDF, already isolated, or a UDF nested inside another UDF's args).
    // pre is omitted when every UDF argument is already a column. Common sub-expressions are inlined
    // back into the columnRefMap first (the split's UDF de-dup keeps a repeated UDF computed once).
    public static List<Map<ColumnRefOperator, ScalarOperator>> splitMaps(
            Map<ColumnRefOperator, ScalarOperator> columnRefMap,
            Map<ColumnRefOperator, ScalarOperator> commonSubMap, ColumnRefFactory factory) {
        if (commonSubMap != null && !commonSubMap.isEmpty()) {
            ReplaceColumnRefRewriter inliner = new ReplaceColumnRefRewriter(commonSubMap, true);
            Map<ColumnRefOperator, ScalarOperator> inlined = new HashMap<>();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : columnRefMap.entrySet()) {
                inlined.put(entry.getKey(), inliner.rewrite(entry.getValue()));
            }
            columnRefMap = inlined;
        }
        if (isAlreadyIsolated(columnRefMap)) {
            return null;
        }

        LinkedHashSet<CallOperator> udfCalls = new LinkedHashSet<>();
        boolean[] nested = new boolean[] {false};
        for (ScalarOperator value : columnRefMap.values()) {
            collectUdfCalls(value, false, udfCalls, nested);
        }
        if (udfCalls.isEmpty() || nested[0]) {
            return null;
        }

        Map<ColumnRefOperator, ScalarOperator> preMap = new HashMap<>();
        Map<ColumnRefOperator, ScalarOperator> udfMap = new HashMap<>();
        Map<ColumnRefOperator, ScalarOperator> postMap = new HashMap<>();
        Map<ScalarOperator, ColumnRefOperator> udfToOut = new HashMap<>();
        boolean preHasComputed = false;

        for (CallOperator udf : udfCalls) {
            List<ScalarOperator> newArgs = new ArrayList<>();
            for (ScalarOperator arg : udf.getChildren()) {
                if (arg instanceof ColumnRefOperator) {
                    newArgs.add(arg);
                } else {
                    ColumnRefOperator inCol = factory.create(arg, arg.getType(), arg.isNullable());
                    preMap.put(inCol, arg);
                    newArgs.add(inCol);
                    preHasComputed = true;
                }
            }
            CallOperator rewrittenUdf = new CallOperator(udf.getFnName(), udf.getType(), newArgs, udf.getFunction(),
                    udf.isDistinct(), udf.isRemovedDistinct());
            ColumnRefOperator outCol = factory.create(udf, udf.getType(), udf.isNullable());
            udfMap.put(outCol, rewrittenUdf);
            udfToOut.put(udf, outCol);
        }

        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : columnRefMap.entrySet()) {
            postMap.put(entry.getKey(),
                    ScalarOperatorsReuse.rewriteOperatorWithCommonOperator(entry.getValue(), udfToOut));
        }

        // udf passes through original columns post still needs (post's child is udf).
        ColumnRefSet postUsed = new ColumnRefSet();
        for (ScalarOperator value : postMap.values()) {
            postUsed.union(value.getUsedColumns());
        }
        postUsed.getStream().map(factory::getColumnRef).forEach(col -> {
            if (!udfMap.containsKey(col)) {
                udfMap.put(col, col);
            }
        });

        // pre outputs everything udf reads except the intermediate columns pre itself computes.
        ColumnRefSet udfInput = new ColumnRefSet();
        for (ScalarOperator value : udfMap.values()) {
            udfInput.union(value.getUsedColumns());
        }
        udfInput.getStream().map(factory::getColumnRef).forEach(col -> {
            if (!preMap.containsKey(col)) {
                preMap.put(col, col);
            }
        });

        List<Map<ColumnRefOperator, ScalarOperator>> stages = new ArrayList<>();
        if (preHasComputed) {
            stages.add(preMap);
        }
        stages.add(udfMap);
        stages.add(postMap);
        return stages;
    }

    // Returns base -> [pre] -> udf -> post as a stack of PhysicalProjectOperators, or null when no
    // split applies. Used for a standalone PhysicalProjectOperator (visitPhysicalProject).
    public static OptExpression trySplit(OptExpression projectExpr, ColumnRefFactory factory) {
        PhysicalProjectOperator project = (PhysicalProjectOperator) projectExpr.getOp();
        List<Map<ColumnRefOperator, ScalarOperator>> stages =
                splitMaps(project.getColumnRefMap(), project.getCommonSubOperatorMap(), factory);
        if (stages == null) {
            return null;
        }
        Statistics statistics = projectExpr.getStatistics();
        OptExpression chain = projectExpr.inputAt(0);
        for (Map<ColumnRefOperator, ScalarOperator> stage : stages) {
            chain = newProject(stage, chain, statistics);
        }
        return chain;
    }

    private static OptExpression newProject(Map<ColumnRefOperator, ScalarOperator> map, OptExpression child,
                                            Statistics statistics) {
        OptExpression opt = OptExpression.create(new PhysicalProjectOperator(map, new HashMap<>()), child);
        if (statistics != null) {
            opt.setStatistics(statistics);
        }
        return opt;
    }

    // UDFs the BE evaluates through ArrowFunctionCallExpr: Python UDFs and arrow-input Java UDFs.
    // These are the ones routed onto the async operator, so only they are worth splitting out.
    private static boolean isArrowUdf(CallOperator call) {
        Function fn = call.getFunction();
        if (fn == null) {
            return false;
        }
        TFunctionBinaryType binaryType = fn.getBinaryType();
        return binaryType == TFunctionBinaryType.PYTHON ||
                (binaryType == TFunctionBinaryType.SRJAR && "arrow".equalsIgnoreCase(fn.getInputType()));
    }

    private static boolean containsArrowUdf(ScalarOperator op) {
        if (op instanceof CallOperator && isArrowUdf((CallOperator) op)) {
            return true;
        }
        for (ScalarOperator child : op.getChildren()) {
            if (containsArrowUdf(child)) {
                return true;
            }
        }
        return false;
    }

    private static void collectUdfCalls(ScalarOperator op, boolean insideUdf, LinkedHashSet<CallOperator> udfs,
                                        boolean[] nested) {
        if (op instanceof CallOperator && isArrowUdf((CallOperator) op)) {
            if (insideUdf) {
                nested[0] = true;
            }
            udfs.add((CallOperator) op);
            for (ScalarOperator child : op.getChildren()) {
                collectUdfCalls(child, true, udfs, nested);
            }
            return;
        }
        for (ScalarOperator child : op.getChildren()) {
            collectUdfCalls(child, insideUdf, udfs, nested);
        }
    }
}
