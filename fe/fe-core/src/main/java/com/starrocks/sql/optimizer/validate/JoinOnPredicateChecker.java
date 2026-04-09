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

import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;

/**
 * Validates that hash join equality predicates used for shuffle distribution only reference
 * column refs, not complex expressions.
 * <p>
 * Hash joins derive shuffle distribution columns from equality predicates via
 * {@code JoinHelper.init()}, which extracts the underlying column ID using
 * {@code getUsedColumns().getFirstId()}. When a predicate operand is an expression
 * (e.g. {@code t1.i2 + 1}), this extraction returns the <b>base column</b> ({@code i2}),
 * not the expression value — causing data to be shuffled by {@code hash(i2)} instead
 * of {@code hash(i2 + 1)}. Rows that should match under the equality condition end up
 * on different nodes, producing <b>incorrect query results</b>.
 * <p>
 * The {@code PushDownJoinOnExpressionToChildProject} rule prevents this by pushing
 * expressions into child project nodes and replacing them with column refs before
 * distribution derivation. This checker detects cases where that rule was not applied.
 *
 * @see com.starrocks.sql.optimizer.rule.transformation.PushDownJoinOnExpressionToChildProject
 * @see com.starrocks.sql.optimizer.JoinHelper#init()
 */
public class JoinOnPredicateChecker implements PlanValidator.Checker {

    private static final String PREFIX = "Shuffle distribution check failed.";
    private static final JoinOnPredicateChecker INSTANCE = new JoinOnPredicateChecker();

    private JoinOnPredicateChecker() {}

    public static JoinOnPredicateChecker getInstance() {
        return INSTANCE;
    }

    @Override
    public void validate(OptExpression physicalPlan, TaskContext taskContext) {
        Visitor visitor = new Visitor();
        physicalPlan.getOp().accept(visitor, physicalPlan, null);
    }

    private static class Visitor extends OptExpressionVisitor<Void, Void> {

        @Override
        public Void visit(OptExpression optExpression, Void context) {
            for (OptExpression input : optExpression.getInputs()) {
                Operator operator = input.getOp();
                operator.accept(this, input, null);
            }
            return null;
        }

        @Override
        public Void visitPhysicalHashJoin(OptExpression optExpression, Void context) {
            PhysicalHashJoinOperator joinOp = (PhysicalHashJoinOperator) optExpression.getOp();
            ScalarOperator onPredicate = joinOp.getOnPredicate();
            if (onPredicate != null) {
                ColumnRefSet leftColumns = optExpression.inputAt(0).getOutputColumns();
                ColumnRefSet rightColumns = optExpression.inputAt(1).getOutputColumns();
                List<BinaryPredicateOperator> eqPredicates =
                        JoinHelper.getEqualsPredicate(leftColumns, rightColumns,
                                Utils.extractConjuncts(onPredicate));

                for (BinaryPredicateOperator predicate : eqPredicates) {
                    ScalarOperator left = predicate.getChild(0);
                    ScalarOperator right = predicate.getChild(1);

                    // Both sides of the equality must be column refs so that
                    // JoinHelper.init() derives correct shuffle distribution columns.
                    // If a side is an expression like (col + 1), getUsedColumns().getFirstId()
                    // returns the base column id, producing a wrong shuffle key.
                    if (!left.isColumnRef() || !right.isColumnRef()) {
                        throw new StarRocksPlannerException(
                                String.format("%s Hash join equality predicate contains expression '%s' " +
                                                "that was not projected to a column ref. " +
                                                "Shuffle distribution would use the base column instead of " +
                                                "the expression value, causing incorrect data co-location. " +
                                                "predicate: %s",
                                        PREFIX,
                                        left.isColumnRef() ? right : left,
                                        predicate),
                                ErrorType.INTERNAL_ERROR);
                    }
                }
            }
            visit(optExpression, context);
            return null;
        }
    }
}
