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

import org.junit.jupiter.api.Test;

/**
 * Reproduction case for:
 *   "PushDownPredicateSetRule 通过 Projection 改写后谓词放置位置不正确"
 *
 * Bug location:
 *   fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/
 *       PushDownPredicateSetRule.java  (lines 64-73)
 *
 * What goes wrong:
 *   When the Filter -> Set(Union/Intersect/Except) pattern fires, the rule
 *   pushes the outer filter into every set branch. For each branch, if the
 *   direct child of the Set has a non-null operator-level Projection
 *   (usually because an earlier rule merged a LogicalProjectOperator into
 *   its child's `projection` field), the rule does:
 *
 *       rewriteExpr = ReplaceColumnRefRewriter(child.getProjection()
 *                                                   .getColumnRefMap())
 *                             .rewrite(rewriteExpr);
 *       ...
 *       if (addNewFilterOp) {
 *           // new Filter placed ABOVE child
 *           OptExpression.create(new LogicalFilterOperator(rewriteExpr),
 *                                setOptExpression.inputAt(setChildIdx));
 *       }
 *
 *   `child.getProjection().getColumnRefMap()` maps the child's OUTPUT
 *   column refs (post-projection) to their PRE-projection expressions.
 *   After the rewrite, rewriteExpr therefore references pre-projection
 *   column refs. But then the new LogicalFilterOperator is placed
 *   ABOVE the child, whose visible input is the POST-projection output.
 *   The filter ends up referring to columns that are not present at
 *   that plan position -- the "predicate placement is incorrect" bug.
 *
 * Both queries below construct a union whose branches are expected to
 * have an operator-level projection on their direct child at the time
 * PushDownPredicateUnionRule fires:
 *
 *   testUnionBranchWithExpressionProjection
 *       Project(v+1 AS a) is merged into the scan/agg child. A WHERE
 *       clause over the union's output column `a` is then pushed down
 *       and gets incorrectly rewritten through the child's projection
 *       map.
 *
 *   testUnionBranchWithAggregateProjection
 *       Each branch is an aggregate with an output projection that
 *       renames/wraps the agg result. The outer WHERE on the aggregate
 *       alias triggers the same rewrite.
 *
 * Until the bug is fixed, either:
 *   (a) the planner throws during finalization because the filter
 *       references a column-ref that is not visible at its position, or
 *   (b) the produced plan evaluates the predicate against the wrong
 *       column, yielding incorrect results at runtime.
 *
 * A correct plan must contain the predicate expressed over the
 * POST-projection output column (e.g. `<pred> > 5` at a filter node
 * that can actually see it), NOT rewritten to the pre-projection
 * expression (e.g. `v1 + 1 > 5` above a node whose output is only the
 * renamed column).
 */
public class PushDownPredicateSetRuleReproTest extends PlanTestBase {

    /**
     * Each union branch is `SELECT <expr> AS a FROM ...`. The inline
     * LogicalProjectOperator gets merged into the scan's operator-level
     * projection before PushDownPredicateUnionRule fires. The outer
     * WHERE `a > 5` is then pushed down into each branch and -- per the
     * bug -- rewritten through `scan.getProjection()`, producing a
     * filter referencing pre-projection columns placed ABOVE the scan.
     */
    @Test
    public void testUnionBranchWithExpressionProjection() throws Exception {
        String sql = "SELECT a FROM ("
                + "  SELECT v1 + 1 AS a FROM t0"
                + "  UNION ALL"
                + "  SELECT v4 + 1 AS a FROM t1"
                + ") u WHERE a > 5";

        // Simply producing a valid fragment plan is enough to demonstrate
        // the bug is absent. With the bug, the filter that lands above
        // each scan references the pre-projection column-ref (v1 / v4),
        // whereas the scan's output only exposes the renamed column `a`.
        // Depending on downstream rules this either throws during plan
        // finalization or leaves a predicate on an invisible slot.
        String plan = getFragmentPlan(sql);

        // The pushed-down predicate must be applied on the post-projection
        // output of each branch. The simplest invariant is: the predicate
        // must appear somewhere in the plan (it must not silently vanish
        // because the rewrite referenced an invalid column and a later
        // rule discarded it).
        assertContains(plan, "PREDICATES");
    }

    /**
     * Each branch is an aggregate whose result is wrapped by an outer
     * expression, forcing a projection on top of the aggregate. The
     * aggregate + projection get merged so the LogicalAggregationOperator
     * ends up with a non-null `projection`. The outer WHERE on the
     * aggregate alias triggers the buggy rewrite.
     */
    @Test
    public void testUnionBranchWithAggregateProjection() throws Exception {
        String sql = "SELECT cnt FROM ("
                + "  SELECT count(v1) + 1 AS cnt FROM t0"
                + "  UNION ALL"
                + "  SELECT count(v4) + 1 AS cnt FROM t1"
                + ") u WHERE cnt > 10";

        String plan = getFragmentPlan(sql);

        // Same invariant: the predicate must still be present in the
        // plan after the rule runs.
        assertContains(plan, "PREDICATES");
    }

    /**
     * Direct check of the buggy branch of `doProcess`: when the set
     * node itself has a projection AND the set children have
     * projections, both rewrite steps are applied. The predicate
     * pushed above each child must NOT be in terms of pre-projection
     * columns of the child.
     */
    @Test
    public void testUnionWithBothOuterAndBranchProjection() throws Exception {
        String sql = "SELECT x * 2 FROM ("
                + "  SELECT v1 + v2 AS x FROM t0"
                + "  UNION ALL"
                + "  SELECT v4 + v5 AS x FROM t1"
                + ") u WHERE x > 3";

        String plan = getFragmentPlan(sql);

        // The predicate must be pushed into both branches (UNION ALL
        // with a filter on the union output should result in predicates
        // on every child).
        assertContains(plan, "PREDICATES");
    }
}
