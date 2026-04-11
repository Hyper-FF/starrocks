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
 * SQL plan-level regression tests for shared-object mutation bugs in the FE optimizer.
 *
 * <h2>How to run</h2>
 * <pre>
 *   ./run-fe-ut.sh --test com.starrocks.sql.plan.SharedObjectMutationPlanTest
 * </pre>
 *
 * <h2>How to reproduce the original bugs</h2>
 * Revert the corresponding fix (see each method's Javadoc), then run this test class.
 * The test that exercises the reverted path should fail with one of:
 * <ul>
 *   <li>An assertion error (plan contains wrong column refs / missing aggregates)</li>
 *   <li>A {@code Preconditions} / {@code IllegalStateException} during planning</li>
 *   <li>A {@code NullPointerException} because the shared operator's child was replaced with an unrelated ref</li>
 * </ul>
 *
 * <h2>Related files and commits</h2>
 * <ul>
 *   <li>Reference fix: {@code f90720c9} (PushDownAggregateRewriter case-when/if)</li>
 *   <li>Root fix: ArgsScalarOperator.clone() copies the arguments list</li>
 * </ul>
 */
public class SharedObjectMutationPlanTest extends PlanTestBase {

    private static int savedAggStage;
    private static int savedPushDownMode;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;

        // -- Tables for PartitionSelector tests --
        starRocksAssert.withTable(
                "CREATE TABLE IF NOT EXISTS t_gen_partition (" +
                        " c1 datetime NOT NULL," +
                        " c2 bigint," +
                        " c3 DATETIME NULL AS date_trunc('month', c1)" +
                        ")" +
                        " DUPLICATE KEY(c1)" +
                        " PARTITION BY (c2, c3)" +
                        " PROPERTIES('replication_num'='1')");
        starRocksAssert.ddl(
                "ALTER TABLE t_gen_partition ADD PARTITION IF NOT EXISTS p1_202401 VALUES IN (('1','2024-01-01'))");
        starRocksAssert.ddl(
                "ALTER TABLE t_gen_partition ADD PARTITION IF NOT EXISTS p1_202402 VALUES IN (('1','2024-02-01'))");
        starRocksAssert.ddl(
                "ALTER TABLE t_gen_partition ADD PARTITION IF NOT EXISTS p2_202401 VALUES IN (('2','2024-01-01'))");
        starRocksAssert.ddl(
                "ALTER TABLE t_gen_partition ADD PARTITION IF NOT EXISTS p2_202402 VALUES IN (('2','2024-02-01'))");

        // Save original settings
        savedAggStage = connectContext.getSessionVariable().getNewPlannerAggStage();
        savedPushDownMode = connectContext.getSessionVariable().getCboPushDownAggregateMode();
    }

    @AfterAll
    public static void afterClass() {
        // Restore settings
        connectContext.getSessionVariable().setNewPlanerAggStage(savedAggStage);
        connectContext.getSessionVariable().setCboPushDownAggregateMode(savedPushDownMode);
        try {
            starRocksAssert.dropTable("t_gen_partition");
        } catch (Exception ignored) {
        }
    }

    /** Enable aggregate-push-down for a block of tests. */
    private void withAggPushDown(SQLRunnable r) throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(1);
        connectContext.getSessionVariable().setCboPushDownAggregateMode(1);
        connectContext.getSessionVariable().setEnableRewriteSumByAssociativeRule(false);
        connectContext.getSessionVariable().setEnableEliminateAgg(false);
        try {
            r.run();
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(savedAggStage);
            connectContext.getSessionVariable().setCboPushDownAggregateMode(savedPushDownMode);
            connectContext.getSessionVariable().setEnableRewriteSumByAssociativeRule(true);
            connectContext.getSessionVariable().setEnableEliminateAgg(true);
        }
    }

    @FunctionalInterface
    private interface SQLRunnable {
        void run() throws Exception;
    }

    // =====================================================================
    //  1. ExtractAggregateColumn  (rule/tree/ExtractAggregateColumn.java)
    //
    //  BUG:  aggregateOperator.getAggregations().forEach((k,v) -> v.setChild(...))
    //        mutates shared CallOperator in the aggregation map.
    //  REPRO: revert the clone+new-map in ExtractAggregateColumn.rewriteAggregateOperator
    //  SYMPTOM: second aggregate (SUM) sees wrong/corrupted children -> plan error or
    //           wrong column references in AGGREGATE node.
    // =====================================================================

    @Test
    public void testExtractAggColumn_twoAggsOnSameExpr() throws Exception {
        // COUNT and SUM both reference projection expression (v1+v2).
        // ExtractAggregateColumn pulls the expression into the aggregate.
        // Without clone, the second agg's CallOperator gets the first agg's rewritten child.
        String sql = "SELECT COUNT(v1 + v2), SUM(v1 + v2) FROM t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "count");
        assertContains(plan, "sum");
    }

    @Test
    public void testExtractAggColumn_threeAggsOnSameExpr() throws Exception {
        String sql = "SELECT MIN(v1 * v2), MAX(v1 * v2), AVG(v1 * v2) FROM t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "min");
        assertContains(plan, "max");
        assertContains(plan, "avg");
    }

    // =====================================================================
    //  2. PushDownAggregateRewriter line ~409  (rule/tree/pdagg/PushDownAggregateRewriter.java)
    //
    //  BUG:  entry.getValue().setChild(0, ref) mutates shared CallOperator
    //        when the aggregate's input is a complex (non-columnref) expression.
    //  REPRO: revert the clone in PushDownAggregateRewriter.rewrite (~line 409)
    //  SYMPTOM: the second aggregate's CallOperator child is overwritten with
    //           the first aggregate's new ColumnRef -> wrong plan / NPE.
    // =====================================================================

    @Test
    public void testPushDownAgg_complexExprTwoAggs() throws Exception {
        withAggPushDown(() -> {
            // Two aggregates on (v1+v2) pushed through a join.
            // Without clone, SUM's CallOperator.child(0) is replaced with a new ColRef,
            // then MIN reuses the same CallOperator and sees that ColRef instead of (v1+v2).
            String sql =
                    "SELECT SUM(t0.v1 + t0.v2), MIN(t0.v1 + t0.v2) " +
                    "FROM t0 JOIN t1 ON t0.v1 = t1.v4 " +
                    "GROUP BY t0.v3";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "sum");
            assertContains(plan, "min");
        });
    }

    // =====================================================================
    //  3. PushDownAggregateRewriter CASE-WHEN / IF  (same file, rewriteProject)
    //
    //  BUG:  setThenClause / setChild on the shared CaseWhenOperator / CallOperator(IF)
    //        from originProjectMap.  (fixed in f90720c9, but also needs the line-409 fix)
    //  REPRO: revert the clone lines in PushDownAggregateRewriter.rewriteProject
    //  SYMPTOM: second aggregate (MIN) sees pushed-down ColumnRefs instead of original
    //           columns -> wrong plan / assert.
    // =====================================================================

    @Test
    public void testPushDownAgg_caseWhenShared() throws Exception {
        withAggPushDown(() -> {
            // SUM + MIN on the same CASE WHEN column through a join.
            String sql =
                    "SELECT SUM(sub.cval), MIN(sub.cval), sub.fk " +
                    "FROM (" +
                    "  SELECT t1d AS fk," +
                    "         CASE WHEN t1b = 1 THEN t1e ELSE NULL END AS cval" +
                    "  FROM test_all_type" +
                    ") sub " +
                    "JOIN t0 ON sub.fk = t0.v1 " +
                    "GROUP BY sub.fk";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "sum");
            assertContains(plan, "min");
        });
    }

    @Test
    public void testPushDownAgg_ifShared() throws Exception {
        withAggPushDown(() -> {
            // Same bug path but the IF variant.
            String sql =
                    "SELECT SUM(sub.cval), MIN(sub.cval), sub.fk " +
                    "FROM (" +
                    "  SELECT t1d AS fk," +
                    "         IF(t1b = 1, t1e, NULL) AS cval" +
                    "  FROM test_all_type" +
                    ") sub " +
                    "JOIN t0 ON sub.fk = t0.v1 " +
                    "GROUP BY sub.fk";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "sum");
            assertContains(plan, "min");
        });
    }

    @Test
    public void testPushDownAgg_nestedCaseWhen() throws Exception {
        withAggPushDown(() -> {
            // Nested CASE WHEN: outer CASE WHEN wraps a column that is itself
            // a CASE WHEN from a CTE. Exercises both the collector and the rewriter.
            String sql =
                    "WITH cte AS (" +
                    "  SELECT t1d AS fk, t1a AS cat," +
                    "         CASE WHEN t1b = 1 THEN t1e ELSE t1f END AS cval" +
                    "  FROM test_all_type" +
                    ")" +
                    "SELECT SUM(CASE WHEN cat IS NOT NULL THEN cval ELSE NULL END)," +
                    "       MIN(CASE WHEN cat IS NOT NULL THEN cval ELSE NULL END)," +
                    "       fk " +
                    "FROM cte JOIN t0 ON cte.fk = t0.v1 " +
                    "GROUP BY fk";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "sum");
            assertContains(plan, "min");
        });
    }

    // =====================================================================
    //  4. ImplicitCastRule  (rewrite/scalar/ImplicitCastRule.java)
    //
    //  BUG:  visitCaseWhenOperator modifies CaseWhenOperator in-place via
    //        setElseClause / setThenClause / setCaseClause / setWhenClause.
    //  REPRO: revert the clone() line at the top of visitCaseWhenOperator
    //  SYMPTOM: if the same CaseWhenOperator instance is referenced from
    //           two places, the second reference gets unexpected CastOperators
    //           injected -> type mismatch or wrong results.
    // =====================================================================

    @Test
    public void testImplicitCast_caseWhenTypeMismatch() throws Exception {
        // CASE with smallint-then and double-else  ->  ImplicitCastRule adds casts.
        String sql = "SELECT CASE WHEN t1a = 'a' THEN t1b ELSE t1f END FROM test_all_type";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "CASE");
    }

    @Test
    public void testImplicitCast_caseWhenUsedByTwoAggs() throws Exception {
        // The CASE WHEN expression feeds into SUM and AVG.  During optimisation
        // the implicit-cast rewrite operates on the CaseWhenOperator; if it
        // mutates the shared instance, the second aggregate's tree is corrupted.
        String sql =
                "SELECT SUM(sub.val), AVG(sub.val) FROM (" +
                "  SELECT CASE WHEN t1b > 0 THEN t1e ELSE t1c END AS val" +
                "  FROM test_all_type" +
                ") sub";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "sum");
        assertContains(plan, "avg");
    }

    @Test
    public void testImplicitCast_caseWhenWithCaseClause() throws Exception {
        // Exercises the setCaseClause / setWhenClause path.
        // CASE t1b WHEN 1 THEN ... WHEN 2 THEN ... END
        String sql =
                "SELECT CASE t1b WHEN 1 THEN t1e WHEN 2 THEN t1f ELSE 0 END " +
                "FROM test_all_type";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "CASE");
    }

    // =====================================================================
    //  5. PartitionSelector  (rule/transformation/partition/PartitionSelector.java)
    //
    //  BUG:  scalarOperator.setChild(i, result) in the generated-column deduction
    //        visitor mutates the shared predicate tree.
    //  REPRO: revert the clone-on-first-write in PartitionSelector's visitor
    //  SYMPTOM: subsequent predicate evaluation uses corrupted tree -> wrong
    //           partition pruning or NullPointerException.
    // =====================================================================

    @Test
    public void testPartitionSelector_generatedColumnSinglePred() throws Exception {
        // Predicate on base column c1 deduced to generated column c3=date_trunc('month',c1).
        String sql = "SELECT * FROM t_gen_partition WHERE c1 = '2024-01-15' AND c2 = 1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "t_gen_partition");
        // Should prune down to 1 partition
        assertContains(plan, "partitions=1/4");
    }

    @Test
    public void testPartitionSelector_generatedColumnRangePred() throws Exception {
        // Compound range predicate => visitor replaces children of AND node.
        String sql =
                "SELECT * FROM t_gen_partition " +
                "WHERE c1 >= '2024-01-01' AND c1 < '2024-02-01' AND c2 = 1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "t_gen_partition");
        assertContains(plan, "partitions=1/4");
    }

    @Test
    public void testPartitionSelector_generatedColumnOrPred() throws Exception {
        // OR predicate: each branch triggers visitor child replacement.
        String sql =
                "SELECT * FROM t_gen_partition " +
                "WHERE (c1 = '2024-01-15' OR c1 = '2024-02-15') AND c2 = 1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "t_gen_partition");
        assertContains(plan, "partitions=2/4");
    }

    // =====================================================================
    //  6. PullUpScanPredicateRule  (rule/transformation/PullUpScanPredicateRule.java)
    //
    //  BUG:  root.setChild(i, replaceScalarOperator(...)) mutates the predicate
    //        tree in-place; shared nodes are corrupted.
    //  REPRO: revert the clone-on-first-write in replaceScalarOperator
    //  SYMPTOM: the pushed-down predicate on the scan node references the wrong
    //           child after the pull-up -> wrong results or plan error.
    // =====================================================================

    @Test
    public void testPullUpScanPredicate_arrayLambdaMixed() throws Exception {
        // array lambda predicate can't be pushed to storage, gets pulled up.
        // v1 = 10 stays on scan.  Shared tree must not be corrupted.
        String sql = "SELECT * FROM tarray WHERE v1 = 10 AND array_max(array_map(x -> x + v2, v3)) > 1";
        String plan = getFragmentPlan(sql);
        // Pulled-up predicate on top, pushed predicate on scan
        assertContains(plan, "SELECT");
        assertContains(plan, "PREDICATES: 1: v1 = 10");
    }

    @Test
    public void testPullUpScanPredicate_commonSubExpr() throws Exception {
        // Two predicates share the sub-expression (v1+v2).  The second predicate
        // adds v3.  PullUpScanPredicateRule pulls both up, with common sub-expression.
        // v1=5 stays on the scan.
        String sql = "SELECT * FROM t0 WHERE v1 + v2 > 10 AND v1 + v2 + v3 > 20 AND v1 = 5";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SELECT");
        assertContains(plan, "common sub expr");
        assertContains(plan, "PREDICATES: 1: v1 = 5");
    }

    // =====================================================================
    //  7. MVProjectAggProjectScanRewrite  (rule/mv/MVProjectAggProjectScanRewrite.java)
    //     AggregatedMaterializedViewRewriter  (rule/transformation/materialization/...)
    //
    //  These two require a running MV rewrite context which needs PseudoCluster
    //  or full MV setup (MVTestBase).  The mutation is in ColumnRefOperator
    //  type/nullable; the unit test in SharedObjectMutationTest.java covers
    //  the clone semantics directly.  A plan-level test for MV rewrite belongs
    //  in the dedicated MV test suite (PushDownAggregationWithMVTest) where
    //  createAndRefreshMv() is available.
    //
    //  Here we add a minimal smoke-test that at least plans an aggregate query
    //  on test_agg (AGGREGATE KEY table with PERCENTILE_UNION) to catch any
    //  NPE / assert from the rewrite path.
    // =====================================================================

    @Test
    public void testAggTable_percentileQuery() throws Exception {
        // percentile_approx_raw on an AGGREGATE KEY table with PERCENTILE_UNION column.
        // Exercises MVProjectAggProjectScanRewrite if an MV is present, or at least
        // verifies the aggregate table planning does not crash.
        String sql = "SELECT k1, percentile_approx_raw(p1, 0.5) FROM test_agg GROUP BY k1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "test_agg");
        assertContains(plan, "AGGREGATE");
    }
}
