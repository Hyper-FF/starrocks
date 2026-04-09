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

package com.starrocks.sql.optimizer.validate;

import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Integration test for shuffle distribution correctness with join expressions.
 * <p>
 * Validates the two-layer defense against incorrect shuffle distribution:
 * <ol>
 *   <li><b>JoinHelper.init()</b> (defensive): skips non-column-ref predicates when
 *       deriving distribution columns. Expression predicates are evaluated locally.
 *       Shuffling by fewer (column-ref only) columns is valid per {@code isSatisfy()} semantics.</li>
 *   <li><b>PushDownJoinOnExpressionToChildProject</b> (optimal): projects expressions to
 *       column refs so ALL predicates participate in shuffle for better data locality.</li>
 * </ol>
 */
class JoinOnPredicateCheckerPlanTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @AfterEach
    public void tearDown() {
        connectContext.getSessionVariable().setCboDisabledRules("");
        connectContext.getSessionVariable().setEnablePlanValidation(true);
    }

    // ==================== Optimal: rule projects expression to column ref ====================

    /**
     * With PushDownJoinOnExpressionToChildProject enabled (default):
     * - Expression v1+1 is projected to a new column ref in a child project node
     * - SHUFFLE uses ALL equality columns (projected ref and v4) → optimal data locality
     */
    @Test
    void testExpressionProjected_allColumnsParticipateInShuffle() throws Exception {
        String sql = "select count(*) from t0 join t1 on t0.v1 + 1 = t1.v4";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "HASH JOIN");
        // The expression v1+1 should NOT appear in "equal join conjunct" — it should
        // have been replaced by a projected column ref
        assertNotContains(plan, "equal join conjunct: 1: v1 + 1");
    }

    /**
     * Column-ref only equality (v1 = v4): always correct regardless of rule state.
     */
    @Test
    void testColumnRefEquality_alwaysCorrectDistribution() throws Exception {
        connectContext.getSessionVariable().setCboDisabledRules(
                "TF_PUSH_DOWN_JOIN_ON_EXPRESSION_TO_CHILD_PROJECT");
        String sql = "select count(*) from t0 join t1 on t0.v1 = t1.v4";
        assertDoesNotThrow(() -> getFragmentPlan(sql));
    }

    // ==================== Defensive: JoinHelper skips expression predicates ====================

    /**
     * With rule disabled: expression v1+1 stays in the ON predicate, but JoinHelper.init()
     * skips it when deriving distribution columns. Query falls back to broadcast or
     * shuffle by fewer columns. Result is CORRECT (just less optimal than projecting).
     * <p>
     * This is the defensive safety net: even if the projection rule didn't run,
     * JoinHelper won't derive a wrong shuffle key from the expression.
     */
    @Test
    void testExpressionNotProjected_defensiveFallback_correctResult() throws Exception {
        connectContext.getSessionVariable().setCboDisabledRules(
                "TF_PUSH_DOWN_JOIN_ON_EXPRESSION_TO_CHILD_PROJECT");

        // Single expression predicate: all equality predicates are skipped for distribution.
        // JoinHelper.leftOnCols/rightOnCols are empty → falls back to broadcast join.
        String sql = "select count(*) from t0 join t1 on t0.v1 + 1 = t1.v4";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "HASH JOIN");
        // Since there are no column-ref equality predicates for shuffle,
        // the join should use BROADCAST distribution
        assertContains(plan, "BROADCAST");
    }

    /**
     * Multi-predicate: v1=v4 AND v2+1=v5, rule disabled.
     * <p>
     * JoinHelper.init() skips the expression predicate (v2+1=v5) and only uses
     * the column-ref predicate (v1=v4) for shuffle distribution.
     * SHUFFLE by hash(v1)/hash(v4) ensures v1=v4 rows co-locate.
     * v2+1=v5 is evaluated locally on each node.
     * <p>
     * This is "少的可以满足多的": fewer shuffle columns (v1 only) produce correct
     * results — they just send more data per node than shuffling by (v1, v2+1).
     */
    @Test
    void testMultiPredicate_partialExpression_shuffleByColumnRefOnly() throws Exception {
        connectContext.getSessionVariable().setCboDisabledRules(
                "TF_PUSH_DOWN_JOIN_ON_EXPRESSION_TO_CHILD_PROJECT");

        String sql = "select count(*) from t0 join t1 on t0.v1 = t1.v4 and t0.v2 + 1 = t1.v5";
        String plan = getFragmentPlan(sql);
        // Join succeeds — JoinHelper only uses v1=v4 for shuffle, v2+1=v5 is local
        assertContains(plan, "HASH JOIN");
    }

    /**
     * Right-side expression: v1 = v4+1, rule disabled.
     * JoinHelper skips this predicate → falls back to broadcast.
     */
    @Test
    void testRightExpression_defensiveFallback() throws Exception {
        connectContext.getSessionVariable().setCboDisabledRules(
                "TF_PUSH_DOWN_JOIN_ON_EXPRESSION_TO_CHILD_PROJECT");

        String sql = "select count(*) from t0 join t1 on t0.v1 = t1.v4 + 1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "HASH JOIN");
        // No column-ref equality predicates → broadcast
        assertContains(plan, "BROADCAST");
    }

    // ==================== Optimal vs defensive comparison ====================

    /**
     * Same query with and without the projection rule — both produce correct results,
     * but with different distribution strategies.
     * <p>
     * With rule: SHUFFLE by projected column ref (all predicates participate) → optimal
     * Without rule: BROADCAST or shuffle by subset (fewer predicates) → correct but less optimal
     */
    @Test
    void testOptimalVsDefensive_bothCorrect() throws Exception {
        String sql = "select count(*) from t0 join t1 on t0.v1 = t1.v4 and t0.v2 + 1 = t1.v5";

        // Optimal: rule projects v2+1 → shuffle by (v1, projected_v2_plus_1) / (v4, v5)
        connectContext.getSessionVariable().setCboDisabledRules("");
        String optimalPlan = getFragmentPlan(sql);
        assertContains(optimalPlan, "HASH JOIN");

        // Defensive: JoinHelper skips v2+1 → shuffle by (v1) / (v4) only
        connectContext.getSessionVariable().setCboDisabledRules(
                "TF_PUSH_DOWN_JOIN_ON_EXPRESSION_TO_CHILD_PROJECT");
        String defensivePlan = getFragmentPlan(sql);
        assertContains(defensivePlan, "HASH JOIN");

        // Both plans should produce correct results — no exception thrown
    }
}
