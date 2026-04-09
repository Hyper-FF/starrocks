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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for {@link JoinOnPredicateChecker}.
 * <p>
 * Validates that the checker detects incorrect shuffle distribution caused by non-column-ref
 * expressions in hash join equality predicates. This reproduces the class of bugs described
 * in issue #71058:
 * <ul>
 *   <li>A join ON predicate like {@code t0.v1 + 1 = t1.v4} has expression {@code v1 + 1}</li>
 *   <li>{@code JoinHelper.init()} extracts distribution col via {@code getUsedColumns().getFirstId()},
 *       getting base column {@code v1} instead of expression {@code v1 + 1}</li>
 *   <li>SHUFFLE distributes by {@code hash(v1)} on left and {@code hash(v4)} on right</li>
 *   <li>A left row with v1=5 goes to hash(5), right row with v4=6 goes to hash(6),
 *       but 5+1=6 means they should match — they end up on different nodes → WRONG RESULT</li>
 * </ul>
 * {@code PushDownJoinOnExpressionToChildProject} fixes this by projecting expressions to
 * column refs before distribution derivation.
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

    // ==================== Distribution correctness: rule enabled ====================

    /**
     * With PushDownJoinOnExpressionToChildProject enabled (default):
     * - Expression {@code v1 + 1} is projected to a new column ref in a child project node
     * - SHUFFLE distribution uses the projected column ref (correct)
     * - Validation passes
     */
    @Test
    void testExpressionProjected_correctShuffleDistribution() throws Exception {
        String sql = "select count(*) from t0 join t1 on t0.v1 + 1 = t1.v4";
        String plan = getFragmentPlan(sql);
        // Plan should contain HASH JOIN with PARTITIONED (shuffle) distribution
        // and equal join conjunct using a projected column ref, not the raw expression
        assertContains(plan, "HASH JOIN");
        // The expression v1+1 should NOT appear in "equal join conjunct" — it should
        // have been replaced by a projected column ref
        assertNotContains(plan, "equal join conjunct: 1: v1 + 1");
    }

    /**
     * Column-ref only equality (v1 = v4): shuffle uses hash(v1) and hash(v4) directly.
     * Always correct regardless of rule state.
     */
    @Test
    void testColumnRefEquality_alwaysCorrectDistribution() throws Exception {
        connectContext.getSessionVariable().setCboDisabledRules(
                "TF_PUSH_DOWN_JOIN_ON_EXPRESSION_TO_CHILD_PROJECT");
        String sql = "select count(*) from t0 join t1 on t0.v1 = t1.v4";
        // Should pass even with the rule disabled — no expression to project
        assertDoesNotThrow(() -> getFragmentPlan(sql));
    }

    // ==================== Distribution mismatch: rule disabled ====================

    /**
     * With rule disabled: expression v1+1 stays in the ON predicate.
     * JoinHelper would derive shuffle key as v1 (base column) instead of v1+1 (expression).
     * Checker detects this distribution mismatch.
     */
    @Test
    void testExpressionNotProjected_shuffleDistributionMismatch() {
        connectContext.getSessionVariable().setCboDisabledRules(
                "TF_PUSH_DOWN_JOIN_ON_EXPRESSION_TO_CHILD_PROJECT");
        connectContext.getSessionVariable().setEnablePlanValidation(true);

        String sql = "select count(*) from t0 join t1 on t0.v1 + 1 = t1.v4";
        Exception ex = assertThrows(Exception.class, () -> getFragmentPlan(sql));
        assertTrue(ex.getMessage().contains("Shuffle distribution check failed"),
                "Expected shuffle distribution error, got: " + ex.getMessage());
    }

    /**
     * Multi-predicate: v1=v4 AND v2+1=v5.
     * <p>
     * SHUFFLE_JOIN uses ALL equality columns: left hash(v1, v2), right hash(v4, v5).
     * Even though v1=v4 is correct, v2+1=v5 with shuffle by v2 (not v2+1) is wrong.
     * In SHUFFLE_JOIN semantics, fewer correct columns do NOT compensate for one
     * incorrect column — the combined hash is wrong.
     * <p>
     * Example: left(v1=1, v2=5) → hash(1,5), right(v4=1, v5=6) → hash(1,6).
     * Should match (v1=v4, v2+1=v5) but hash(1,5)≠hash(1,6) → different nodes.
     */
    @Test
    void testMultiPredicate_partialExpression_shuffleDistributionMismatch() {
        connectContext.getSessionVariable().setCboDisabledRules(
                "TF_PUSH_DOWN_JOIN_ON_EXPRESSION_TO_CHILD_PROJECT");
        connectContext.getSessionVariable().setEnablePlanValidation(true);

        String sql = "select count(*) from t0 join t1 on t0.v1 = t1.v4 and t0.v2 + 1 = t1.v5";
        Exception ex = assertThrows(Exception.class, () -> getFragmentPlan(sql));
        assertTrue(ex.getMessage().contains("Shuffle distribution check failed"),
                "Expected shuffle distribution error, got: " + ex.getMessage());
    }

    /**
     * Right-side expression (v4 + 1): same distribution mismatch.
     * Shuffle would use hash(v4) but equality needs hash(v4 + 1).
     */
    @Test
    void testRightExpressionNotProjected_shuffleDistributionMismatch() {
        connectContext.getSessionVariable().setCboDisabledRules(
                "TF_PUSH_DOWN_JOIN_ON_EXPRESSION_TO_CHILD_PROJECT");
        connectContext.getSessionVariable().setEnablePlanValidation(true);

        String sql = "select count(*) from t0 join t1 on t0.v1 = t1.v4 + 1";
        Exception ex = assertThrows(Exception.class, () -> getFragmentPlan(sql));
        assertTrue(ex.getMessage().contains("Shuffle distribution check failed"),
                "Expected shuffle distribution error, got: " + ex.getMessage());
    }

    /**
     * LEFT JOIN with expression: forces SHUFFLE (not broadcast) distribution since
     * right join requires shuffle. Same distribution mismatch applies.
     */
    @Test
    void testLeftJoinExpression_shuffleDistributionMismatch() {
        connectContext.getSessionVariable().setCboDisabledRules(
                "TF_PUSH_DOWN_JOIN_ON_EXPRESSION_TO_CHILD_PROJECT");
        connectContext.getSessionVariable().setEnablePlanValidation(true);

        String sql = "select count(*) from t0 left join t1 on t0.v1 + t0.v2 = t1.v4";
        Exception ex = assertThrows(Exception.class, () -> getFragmentPlan(sql));
        assertTrue(ex.getMessage().contains("Shuffle distribution check failed"),
                "Expected shuffle distribution error, got: " + ex.getMessage());
    }

    // ==================== Validation toggle ====================

    /**
     * With plan validation disabled, checker is skipped. The query produces a plan
     * with incorrect shuffle distribution (wrong result in production) but no error
     * during planning.
     */
    @Test
    void testValidationDisabled_incorrectDistributionSilentlyAllowed() {
        connectContext.getSessionVariable().setCboDisabledRules(
                "TF_PUSH_DOWN_JOIN_ON_EXPRESSION_TO_CHILD_PROJECT");
        connectContext.getSessionVariable().setEnablePlanValidation(false);

        String sql = "select count(*) from t0 join t1 on t0.v1 + 1 = t1.v4";
        // Query plans successfully, but the shuffle distribution is incorrect.
        // Without the checker, this would silently produce wrong results at runtime.
        assertDoesNotThrow(() -> getFragmentPlan(sql));
    }
}
