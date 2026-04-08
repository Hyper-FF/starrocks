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
 * Validates that the checker detects non-column-ref expressions in hash join equality
 * predicates when the {@code PushDownJoinOnExpressionToChildProject} rule is skipped.
 * This reproduces the class of bugs described in issue #71058 where MV rewrite introduces
 * new join expressions that don't get projected, leading to incorrect shuffle distribution.
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

    /**
     * When PushDownJoinOnExpressionToChildProject is disabled, a query with an expression
     * in the join ON clause (v1 + 1 = v4) should fail validation because the hash join
     * equality predicate contains a non-column-ref expression.
     */
    @Test
    void testExpressionInJoinOnClauseDetected() {
        connectContext.getSessionVariable().setCboDisabledRules(
                "TF_PUSH_DOWN_JOIN_ON_EXPRESSION_TO_CHILD_PROJECT");
        connectContext.getSessionVariable().setEnablePlanValidation(true);

        String sql = "select count(*) from t0 join t1 on t0.v1 + 1 = t1.v4";
        Exception ex = assertThrows(Exception.class, () -> getFragmentPlan(sql));
        assertTrue(ex.getMessage().contains("Join on-predicate check failed")
                        || ex.getMessage().contains("non-column-ref"),
                "Expected JoinOnPredicateChecker error, got: " + ex.getMessage());
    }

    /**
     * With the rule enabled (default), the same query should pass validation because
     * PushDownJoinOnExpressionToChildProject pushes expressions into child project nodes.
     */
    @Test
    void testExpressionInJoinOnClausePassesWithRule() {
        connectContext.getSessionVariable().setCboDisabledRules("");
        connectContext.getSessionVariable().setEnablePlanValidation(true);

        String sql = "select count(*) from t0 join t1 on t0.v1 + 1 = t1.v4";
        assertDoesNotThrow(() -> getFragmentPlan(sql));
    }

    /**
     * When plan validation is disabled, the checker should not block the query even if
     * the rule is disabled and expressions remain un-projected.
     */
    @Test
    void testCheckerSkippedWhenValidationDisabled() {
        connectContext.getSessionVariable().setCboDisabledRules(
                "TF_PUSH_DOWN_JOIN_ON_EXPRESSION_TO_CHILD_PROJECT");
        connectContext.getSessionVariable().setEnablePlanValidation(false);

        String sql = "select count(*) from t0 join t1 on t0.v1 + 1 = t1.v4";
        assertDoesNotThrow(() -> getFragmentPlan(sql));
    }

    /**
     * Plain column-ref equality predicates (v1 = v4) should always pass validation,
     * regardless of whether the rule is enabled or disabled.
     */
    @Test
    void testColumnRefOnlyPredicateAlwaysPasses() {
        connectContext.getSessionVariable().setCboDisabledRules(
                "TF_PUSH_DOWN_JOIN_ON_EXPRESSION_TO_CHILD_PROJECT");
        connectContext.getSessionVariable().setEnablePlanValidation(true);

        String sql = "select count(*) from t0 join t1 on t0.v1 = t1.v4";
        assertDoesNotThrow(() -> getFragmentPlan(sql));
    }

    /**
     * Left join with expression in ON clause should also be detected.
     */
    @Test
    void testLeftJoinExpressionDetected() {
        connectContext.getSessionVariable().setCboDisabledRules(
                "TF_PUSH_DOWN_JOIN_ON_EXPRESSION_TO_CHILD_PROJECT");
        connectContext.getSessionVariable().setEnablePlanValidation(true);

        String sql = "select count(*) from t0 left join t1 on t0.v1 + t0.v2 = t1.v4";
        Exception ex = assertThrows(Exception.class, () -> getFragmentPlan(sql));
        assertTrue(ex.getMessage().contains("Join on-predicate check failed")
                        || ex.getMessage().contains("non-column-ref"),
                "Expected JoinOnPredicateChecker error, got: " + ex.getMessage());
    }

    /**
     * Right side expression in ON clause should also be detected.
     */
    @Test
    void testRightSideExpressionDetected() {
        connectContext.getSessionVariable().setCboDisabledRules(
                "TF_PUSH_DOWN_JOIN_ON_EXPRESSION_TO_CHILD_PROJECT");
        connectContext.getSessionVariable().setEnablePlanValidation(true);

        String sql = "select count(*) from t0 join t1 on t0.v1 = t1.v4 + 1";
        Exception ex = assertThrows(Exception.class, () -> getFragmentPlan(sql));
        assertTrue(ex.getMessage().contains("Join on-predicate check failed")
                        || ex.getMessage().contains("non-column-ref"),
                "Expected JoinOnPredicateChecker error, got: " + ex.getMessage());
    }
}
