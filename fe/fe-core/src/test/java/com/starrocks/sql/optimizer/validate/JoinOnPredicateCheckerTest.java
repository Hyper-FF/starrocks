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

import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.MockOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinOnPredicateCheckerTest {

    /**
     * Column-ref equality (col_a = col_b): shuffle by hash(col_a) and hash(col_b).
     * JoinHelper.getUsedColumns().getFirstId() returns the correct column for both sides,
     * so shuffle distribution is correct.
     */
    @Test
    void testColumnRefEquality_correctShuffleDistribution() {
        ColumnRefOperator leftCol = new ColumnRefOperator(1, IntegerType.INT, "l", true);
        ColumnRefOperator rightCol = new ColumnRefOperator(2, IntegerType.INT, "r", true);
        OptExpression root = buildHashJoinExpr(leftCol, rightCol, leftCol, rightCol);
        assertDoesNotThrow(() -> JoinOnPredicateChecker.getInstance().validate(root, null));
    }

    /**
     * Expression equality (col_a + 1 = col_b): JoinHelper would extract col_a
     * via getUsedColumns().getFirstId() and shuffle by hash(col_a), but the actual
     * join condition requires hash(col_a + 1). This produces wrong data co-location.
     * <p>
     * Example: left row col_a=5 → hash(5) → nodeA, right row col_b=6 → hash(6) → nodeB.
     * But 5+1=6, so they should match — they end up on different nodes → WRONG RESULT.
     */
    @Test
    void testLeftExpressionEquality_incorrectShuffleDistribution() {
        ColumnRefOperator leftCol = new ColumnRefOperator(1, IntegerType.INT, "l", true);
        ColumnRefOperator rightCol = new ColumnRefOperator(2, IntegerType.INT, "r", true);
        // Simulates: l + 1 = r (expression on left side)
        ScalarOperator addExpr = new CallOperator("add", IntegerType.INT,
                List.of(leftCol, ConstantOperator.createInt(1)));
        OptExpression root = buildHashJoinExpr(addExpr, rightCol, leftCol, rightCol);

        StarRocksPlannerException ex = assertThrows(StarRocksPlannerException.class,
                () -> JoinOnPredicateChecker.getInstance().validate(root, null));
        assertTrue(ex.getMessage().contains("Shuffle distribution check failed"),
                "Unexpected error: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("not projected to a column ref"),
                "Should mention the unprojected expression: " + ex.getMessage());
    }

    /**
     * Expression on right side (col_a = col_b + 1): same distribution mismatch problem.
     * Shuffle uses hash(col_b) but equality needs hash(col_b + 1).
     */
    @Test
    void testRightExpressionEquality_incorrectShuffleDistribution() {
        ColumnRefOperator leftCol = new ColumnRefOperator(1, IntegerType.INT, "l", true);
        ColumnRefOperator rightCol = new ColumnRefOperator(2, IntegerType.INT, "r", true);
        // Simulates: l = r + 1 (expression on right side)
        ScalarOperator addExpr = new CallOperator("add", IntegerType.INT,
                List.of(rightCol, ConstantOperator.createInt(1)));
        OptExpression root = buildHashJoinExpr(leftCol, addExpr, leftCol, rightCol);

        StarRocksPlannerException ex = assertThrows(StarRocksPlannerException.class,
                () -> JoinOnPredicateChecker.getInstance().validate(root, null));
        assertTrue(ex.getMessage().contains("Shuffle distribution check failed"),
                "Unexpected error: " + ex.getMessage());
    }

    /**
     * No on-predicate (cross join): no equality predicates → no shuffle distribution
     * requirement → always valid.
     */
    @Test
    void testCrossJoin_noShuffleDistribution() {
        PhysicalHashJoinOperator join = new PhysicalHashJoinOperator(
                JoinOperator.CROSS_JOIN,
                null,
                "",
                Operator.DEFAULT_LIMIT,
                null,
                null,
                null,
                null);

        ColumnRefOperator leftCol = new ColumnRefOperator(1, IntegerType.INT, "l", true);
        ColumnRefOperator rightCol = new ColumnRefOperator(2, IntegerType.INT, "r", true);

        OptExpression left = new OptExpression(new MockOperator(OperatorType.LOGICAL_VALUES));
        left.setLogicalProperty(new LogicalProperty(ColumnRefSet.of(leftCol)));
        OptExpression right = new OptExpression(new MockOperator(OperatorType.LOGICAL_VALUES));
        right.setLogicalProperty(new LogicalProperty(ColumnRefSet.of(rightCol)));

        OptExpression root = OptExpression.create(join, left, right);
        assertDoesNotThrow(() -> JoinOnPredicateChecker.getInstance().validate(root, null));
    }

    private static OptExpression buildHashJoinExpr(ScalarOperator predicateLeft, ScalarOperator predicateRight,
                                                   ColumnRefOperator leftOutputCol, ColumnRefOperator rightOutputCol) {
        BinaryPredicateOperator predicate = new BinaryPredicateOperator(
                BinaryType.EQ, predicateLeft, predicateRight);

        PhysicalHashJoinOperator join = new PhysicalHashJoinOperator(
                JoinOperator.INNER_JOIN,
                predicate,
                "",
                Operator.DEFAULT_LIMIT,
                null,
                null,
                null,
                null);

        OptExpression left = new OptExpression(new MockOperator(OperatorType.LOGICAL_VALUES));
        left.setLogicalProperty(new LogicalProperty(ColumnRefSet.of(leftOutputCol)));

        OptExpression right = new OptExpression(new MockOperator(OperatorType.LOGICAL_VALUES));
        right.setLogicalProperty(new LogicalProperty(ColumnRefSet.of(rightOutputCol)));

        return OptExpression.create(join, left, right);
    }
}
