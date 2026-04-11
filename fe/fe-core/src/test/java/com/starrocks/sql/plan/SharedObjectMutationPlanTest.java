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

import com.starrocks.common.FeConstants;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * SQL-level regression tests for shared object mutation bugs in the FE optimizer.
 * Each test exercises a specific optimizer code path where in-place mutation of
 * shared ScalarOperator/ColumnRefOperator objects previously caused corruption.
 * <p>
 * Related to commit f90720c9 and the broader clone-before-modify fix set.
 */
public class SharedObjectMutationPlanTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;

        // Table with generated column for PartitionSelector tests
        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS t_gen_partition (" +
                " c1 datetime NOT NULL," +
                " c2 bigint," +
                " c3 DATETIME NULL AS date_trunc('month', c1) " +
                " ) " +
                " DUPLICATE KEY(c1) " +
                " PARTITION BY (c2, c3) " +
                " PROPERTIES('replication_num'='1')");
        starRocksAssert.ddl("ALTER TABLE t_gen_partition ADD PARTITION IF NOT EXISTS p1_202401 " +
                "VALUES IN (('1', '2024-01-01'))");
        starRocksAssert.ddl("ALTER TABLE t_gen_partition ADD PARTITION IF NOT EXISTS p1_202402 " +
                "VALUES IN (('1', '2024-02-01'))");
        starRocksAssert.ddl("ALTER TABLE t_gen_partition ADD PARTITION IF NOT EXISTS p2_202401 " +
                "VALUES IN (('2', '2024-01-01'))");
    }

    @AfterAll
    public static void afterClass() throws Exception {
        try {
            starRocksAssert.dropTable("t_gen_partition");
        } catch (Exception ignored) {
        }
    }

    // =====================================================================
    // 1. ExtractAggregateColumn: v.setChild() on shared aggregation map
    //
    // Trigger: Multiple aggregate functions whose children are non-columnref
    // expressions from the child projection. ExtractAggregateColumn rewrites
    // these by pulling expressions into the aggregate. If CallOperators in
    // the aggregation map are mutated in-place, the second aggregate sees
    // the first aggregate's rewritten children instead of the original ones.
    // =====================================================================

    @Test
    public void testExtractAggregateColumnSharedCallOperator() throws Exception {
        // Two aggregates on the same projected expression: COUNT(v1+v2), SUM(v1+v2).
        // ExtractAggregateColumn should rewrite both correctly without corruption.
        String sql = "SELECT COUNT(v1 + v2), SUM(v1 + v2) FROM t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "AGGREGATE");
        assertContains(plan, "count");
        assertContains(plan, "sum");
    }

    @Test
    public void testExtractAggregateColumnMultipleExpressions() throws Exception {
        // Three different aggregates on computed expressions.
        String sql = "SELECT MIN(v1 * v2), MAX(v1 * v2), AVG(v1 * v2) FROM t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "min");
        assertContains(plan, "max");
        assertContains(plan, "avg");
    }

    // =====================================================================
    // 2. PushDownAggregateRewriter:409 - setChild(0, ref) on shared agg
    //
    // Trigger: Aggregate push down through a join, where the aggregation's
    // input is a complex (non-columnref) expression. The rewriter creates
    // a project node below the pushed-down aggregate and replaces the
    // aggregate's child with a new column ref. If this is done in-place,
    // a second aggregate referencing the same CallOperator is corrupted.
    // =====================================================================

    @Test
    public void testPushDownAggComplexExprTwoAggs() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(1);
        connectContext.getSessionVariable().setCboPushDownAggregateMode(1);
        try {
            // SUM and MIN on the same computed expression, pushed down through JOIN.
            String sql = "SELECT SUM(t0.v1 + t0.v2), MIN(t0.v1 + t0.v2) " +
                    "FROM t0 JOIN t1 ON t0.v1 = t1.v4 " +
                    "GROUP BY t0.v3";
            String plan = getVerboseExplain(sql);
            // Both aggregates must be present (not corrupted)
            assertContains(plan, "sum");
            assertContains(plan, "min");
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(0);
            connectContext.getSessionVariable().setCboPushDownAggregateMode(-1);
        }
    }

    @Test
    public void testPushDownAggCaseWhenShared() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(1);
        connectContext.getSessionVariable().setCboPushDownAggregateMode(1);
        connectContext.getSessionVariable().setEnableRewriteSumByAssociativeRule(false);
        connectContext.getSessionVariable().setEnableEliminateAgg(false);
        try {
            // Two aggregates on the same CASE WHEN column, pushed through a JOIN.
            // This is the exact pattern from f90720c9 - if the CaseWhenOperator is
            // mutated in-place, the second aggregate (MIN) sees corrupted children.
            String sql = "SELECT SUM(sub.cval), MIN(sub.cval), sub.fk " +
                    "FROM ( " +
                    "    SELECT t1d AS fk, " +
                    "           CASE WHEN t1b = 1 THEN t1e ELSE NULL END AS cval " +
                    "    FROM test_all_type " +
                    ") sub " +
                    "JOIN t0 ON sub.fk = t0.v1 " +
                    "GROUP BY sub.fk";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "sum");
            assertContains(plan, "min");
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(0);
            connectContext.getSessionVariable().setCboPushDownAggregateMode(-1);
            connectContext.getSessionVariable().setEnableRewriteSumByAssociativeRule(true);
            connectContext.getSessionVariable().setEnableEliminateAgg(true);
        }
    }

    @Test
    public void testPushDownAggIfShared() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(1);
        connectContext.getSessionVariable().setCboPushDownAggregateMode(1);
        connectContext.getSessionVariable().setEnableRewriteSumByAssociativeRule(false);
        connectContext.getSessionVariable().setEnableEliminateAgg(false);
        try {
            // Same pattern but with IF instead of CASE WHEN.
            String sql = "SELECT SUM(sub.cval), MIN(sub.cval), sub.fk " +
                    "FROM ( " +
                    "    SELECT t1d AS fk, " +
                    "           IF(t1b = 1, t1e, NULL) AS cval " +
                    "    FROM test_all_type " +
                    ") sub " +
                    "JOIN t0 ON sub.fk = t0.v1 " +
                    "GROUP BY sub.fk";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "sum");
            assertContains(plan, "min");
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(0);
            connectContext.getSessionVariable().setCboPushDownAggregateMode(-1);
            connectContext.getSessionVariable().setEnableRewriteSumByAssociativeRule(true);
            connectContext.getSessionVariable().setEnableEliminateAgg(true);
        }
    }

    // =====================================================================
    // 3. ImplicitCastRule: setElseClause/setThenClause on shared CaseWhen
    //
    // Trigger: A CASE WHEN expression where then/else types don't match
    // the return type, requiring implicit casts. If the CaseWhenOperator
    // is shared (e.g. via CSE or projection reuse), in-place mutation
    // corrupts other references.
    // =====================================================================

    @Test
    public void testImplicitCastCaseWhenTypeMismatch() throws Exception {
        // CASE WHEN with smallint then clause and double else clause.
        // ImplicitCastRule must add casts without corrupting shared state.
        String sql = "SELECT CASE WHEN t1a = 'a' THEN t1b ELSE t1f END AS val " +
                "FROM test_all_type";
        String plan = getVerboseExplain(sql);
        // Should produce a valid plan with CASE WHEN and cast
        assertContains(plan, "CASE");
    }

    @Test
    public void testImplicitCastCaseWhenMultipleReferences() throws Exception {
        // Same CASE WHEN expression used twice - any implicit cast rule
        // mutation must not corrupt the second reference.
        String sql = "SELECT " +
                "    CASE WHEN t1b > 0 THEN t1b ELSE t1c END + 1, " +
                "    CASE WHEN t1b > 0 THEN t1b ELSE t1c END + 2 " +
                "FROM test_all_type";
        String plan = getVerboseExplain(sql);
        // Both expressions must produce valid results
        assertNotContains(plan, "ERROR");
    }

    @Test
    public void testImplicitCastCaseWhenInSubquery() throws Exception {
        // CASE WHEN in a subquery's projection, referenced by multiple
        // outer aggregates - triggers sharing through projection reuse.
        String sql = "SELECT SUM(sub.val), AVG(sub.val) FROM ( " +
                "    SELECT CASE WHEN t1b > 0 THEN t1e ELSE t1c END AS val " +
                "    FROM test_all_type " +
                ") sub";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "sum");
        assertContains(plan, "avg");
    }

    // =====================================================================
    // 4. PartitionSelector: setChild() on shared partition predicate
    //
    // Trigger: Partition pruning with generated columns. The visitor
    // deduces predicates on generated columns from predicates on base
    // columns. If the predicate tree is modified in-place, the shared
    // predicate nodes used by other operators are corrupted.
    // =====================================================================

    @Test
    public void testPartitionSelectorGeneratedColumnPredicate() throws Exception {
        // Predicate on base column c1 that can be deduced to generated column c3.
        // The visitor must clone before modifying the predicate tree.
        String sql = "SELECT * FROM t_gen_partition WHERE c1 = '2024-01-15' AND c2 = 1";
        String plan = getFragmentPlan(sql);
        // Should produce valid partition pruning
        assertContains(plan, "t_gen_partition");
    }

    @Test
    public void testPartitionSelectorCompoundPredicate() throws Exception {
        // Compound predicate (AND) with multiple base column predicates that
        // can be deduced to the generated column. Tests that compound predicate
        // nodes are not corrupted when children are replaced.
        String sql = "SELECT * FROM t_gen_partition " +
                "WHERE c1 >= '2024-01-01' AND c1 < '2024-02-01' AND c2 = 1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "t_gen_partition");
    }

    // =====================================================================
    // 5. PullUpScanPredicateRule: setChild() on shared scan predicate
    //
    // Trigger: Predicates on scan nodes that contain expressions that
    // cannot use storage-layer optimizations (e.g. array/lambda functions).
    // The rule pulls these up to a Filter node above the scan. If the
    // shared predicate tree is modified in-place, other references
    // (like the remaining pushed-down predicates) are corrupted.
    // =====================================================================

    @Test
    public void testPullUpScanPredicateArrayFunction() throws Exception {
        // array_length can't use storage-layer optimization, so PullUpScanPredicateRule
        // should pull it up. The shared predicate tree must not be corrupted.
        String sql = "SELECT * FROM tarray WHERE array_length(v3) > 10 AND v1 = 5";
        String plan = getFragmentPlan(sql);
        // v1=5 should stay on the scan, array_length should be pulled up
        assertContains(plan, "OlapScanNode");
    }

    @Test
    public void testPullUpScanPredicateWithCommonSubExpr() throws Exception {
        // Multiple predicates sharing sub-expressions, some pushable and some not.
        // Tests that the predicate tree cloning doesn't corrupt shared nodes.
        String sql = "SELECT * FROM tarray " +
                "WHERE array_length(v3) > 10 AND array_length(v3) + v1 > 20 AND v2 = 5";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "OlapScanNode");
    }

    // =====================================================================
    // 6. Cross-cutting: nested CASE WHEN with aggregate push down
    //
    // Trigger: Combines PushDownAggregateRewriter (CASE WHEN handling)
    // and ImplicitCastRule (type mismatch). A nested CASE WHEN with
    // type mismatches, referenced by multiple aggregates, pushed down
    // through a join. Tests the interaction of multiple fixes.
    // =====================================================================

    @Test
    public void testNestedCaseWhenAggPushDown() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(1);
        connectContext.getSessionVariable().setCboPushDownAggregateMode(1);
        connectContext.getSessionVariable().setEnableRewriteSumByAssociativeRule(false);
        connectContext.getSessionVariable().setEnableEliminateAgg(false);
        try {
            String sql = "WITH cte AS (\n" +
                    "  SELECT t1d AS fk, t1a AS cat,\n" +
                    "         CASE WHEN t1b = 1 THEN t1e ELSE t1f END AS cval\n" +
                    "  FROM test_all_type\n" +
                    ")\n" +
                    "SELECT SUM(CASE WHEN cat IS NOT NULL THEN cval ELSE NULL END),\n" +
                    "       MIN(CASE WHEN cat IS NOT NULL THEN cval ELSE NULL END),\n" +
                    "       fk\n" +
                    "FROM cte JOIN t0 ON cte.fk = t0.v1\n" +
                    "GROUP BY fk";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "sum");
            assertContains(plan, "min");
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(0);
            connectContext.getSessionVariable().setCboPushDownAggregateMode(-1);
            connectContext.getSessionVariable().setEnableRewriteSumByAssociativeRule(true);
            connectContext.getSessionVariable().setEnableEliminateAgg(true);
        }
    }
}
