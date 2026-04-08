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

    @Test
    void testColumnRefPredicatesPasses() {
        // Both sides are column refs — should pass
        ColumnRefOperator leftCol = new ColumnRefOperator(1, IntegerType.INT, "l", true);
        ColumnRefOperator rightCol = new ColumnRefOperator(2, IntegerType.INT, "r", true);
        OptExpression root = buildHashJoinExpr(leftCol, rightCol, leftCol, rightCol);
        assertDoesNotThrow(() -> JoinOnPredicateChecker.getInstance().validate(root, null));
    }

    @Test
    void testExpressionInLeftSideFails() {
        // Left side is an expression (add function), right side is column ref — should fail
        ColumnRefOperator leftCol = new ColumnRefOperator(1, IntegerType.INT, "l", true);
        ColumnRefOperator rightCol = new ColumnRefOperator(2, IntegerType.INT, "r", true);
        ScalarOperator addExpr = new CallOperator("add", IntegerType.INT,
                List.of(leftCol, ConstantOperator.createInt(1)));
        OptExpression root = buildHashJoinExpr(addExpr, rightCol, leftCol, rightCol);

        StarRocksPlannerException ex = assertThrows(StarRocksPlannerException.class,
                () -> JoinOnPredicateChecker.getInstance().validate(root, null));
        assertTrue(ex.getMessage().contains("non-column-ref"),
                "Unexpected error: " + ex.getMessage());
    }

    @Test
    void testExpressionInRightSideFails() {
        // Left side is column ref, right side is an expression — should fail
        ColumnRefOperator leftCol = new ColumnRefOperator(1, IntegerType.INT, "l", true);
        ColumnRefOperator rightCol = new ColumnRefOperator(2, IntegerType.INT, "r", true);
        ScalarOperator addExpr = new CallOperator("add", IntegerType.INT,
                List.of(rightCol, ConstantOperator.createInt(1)));
        OptExpression root = buildHashJoinExpr(leftCol, addExpr, leftCol, rightCol);

        StarRocksPlannerException ex = assertThrows(StarRocksPlannerException.class,
                () -> JoinOnPredicateChecker.getInstance().validate(root, null));
        assertTrue(ex.getMessage().contains("non-column-ref"),
                "Unexpected error: " + ex.getMessage());
    }

    @Test
    void testNullOnPredicatePasses() {
        // No on-predicate (cross join) — should pass
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
