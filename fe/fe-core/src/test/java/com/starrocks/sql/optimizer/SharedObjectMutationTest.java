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

package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.scalar.ImplicitCastRule;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.BooleanType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests that optimizer rules clone shared objects before mutating them,
 * rather than modifying shared instances in place.
 * <p>
 * This is a regression test suite for the class of bugs fixed alongside f90720c9.
 */
public class SharedObjectMutationTest {

    // ---------------------------------------------------------------
    // ArgsScalarOperator.clone() — root fix
    // ---------------------------------------------------------------

    @Test
    public void testCompoundPredicateCloneHasIndependentChildren() {
        // CompoundPredicateOperator extends PredicateOperator extends ArgsScalarOperator.
        // Before the fix, clone() shared the same arguments list, so setChild() on the
        // clone would corrupt the original.
        ColumnRefOperator col1 = new ColumnRefOperator(1, BooleanType.BOOLEAN, "c1", true);
        ColumnRefOperator col2 = new ColumnRefOperator(2, BooleanType.BOOLEAN, "c2", true);
        CompoundPredicateOperator original = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, col1, col2);

        ScalarOperator cloned = original.clone();

        // Mutate the clone's first child
        ColumnRefOperator col3 = new ColumnRefOperator(3, BooleanType.BOOLEAN, "c3", true);
        cloned.setChild(0, col3);

        // Original must be unaffected
        assertSame(col1, original.getChild(0), "Original child 0 was corrupted by clone mutation");
        assertSame(col2, original.getChild(1), "Original child 1 was corrupted by clone mutation");
        // Clone has the new child
        assertSame(col3, cloned.getChild(0));
    }

    @Test
    public void testBinaryPredicateCloneHasIndependentChildren() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "c1", true);
        ConstantOperator val = ConstantOperator.createInt(42);
        BinaryPredicateOperator original = new BinaryPredicateOperator(BinaryType.EQ, col1, val);

        ScalarOperator cloned = original.clone();

        ConstantOperator newVal = ConstantOperator.createInt(99);
        cloned.setChild(1, newVal);

        // Original must be unaffected
        assertSame(val, original.getChild(1), "Original child was corrupted by clone mutation");
        assertSame(newVal, cloned.getChild(1));
    }

    @Test
    public void testInPredicateCloneHasIndependentChildren() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "c1", true);
        ConstantOperator v1 = ConstantOperator.createInt(1);
        ConstantOperator v2 = ConstantOperator.createInt(2);
        InPredicateOperator original = new InPredicateOperator(false, col1, v1, v2);

        ScalarOperator cloned = original.clone();

        ConstantOperator v3 = ConstantOperator.createInt(3);
        cloned.setChild(1, v3);

        assertSame(v1, original.getChild(1), "Original IN-predicate child was corrupted by clone mutation");
        assertSame(v3, cloned.getChild(1));
    }

    // ---------------------------------------------------------------
    // ImplicitCastRule — clone CaseWhenOperator before mutation
    // ---------------------------------------------------------------

    @Test
    public void testImplicitCastRuleDoesNotMutateSharedCaseWhen() {
        // Build: CASE WHEN true THEN 1(smallint) ELSE 2(smallint) END  with return type BIGINT.
        // ImplicitCastRule should add CastOperators for the then/else clauses to match BIGINT,
        // but must NOT mutate the original CaseWhenOperator.

        ConstantOperator whenClause = ConstantOperator.createBoolean(true);
        ConstantOperator thenClause = ConstantOperator.createSmallInt((short) 1);
        ConstantOperator elseClause = ConstantOperator.createSmallInt((short) 2);

        CaseWhenOperator original = new CaseWhenOperator(IntegerType.BIGINT, null, elseClause,
                Lists.newArrayList(whenClause, thenClause));

        // Capture original children for later comparison
        ScalarOperator origThen = original.getThenClause(0);
        ScalarOperator origElse = original.getElseClause();

        ImplicitCastRule rule = new ImplicitCastRule();
        ScalarOperator result = rule.visitCaseWhenOperator(original, null);

        // The result should be a different object (the clone)
        assertNotSame(original, result, "ImplicitCastRule should return a cloned operator");

        // Original then/else must be unchanged (no CastOperator injected)
        assertSame(origThen, original.getThenClause(0),
                "Original thenClause was mutated in place by ImplicitCastRule");
        assertSame(origElse, original.getElseClause(),
                "Original elseClause was mutated in place by ImplicitCastRule");

        // Result should have casts where types didn't match
        CaseWhenOperator resultCaseWhen = (CaseWhenOperator) result;
        assertEquals(true, resultCaseWhen.getThenClause(0) instanceof CastOperator,
                "Result thenClause should be wrapped in CastOperator");
        assertEquals(true, resultCaseWhen.getElseClause() instanceof CastOperator,
                "Result elseClause should be wrapped in CastOperator");
    }

    @Test
    public void testImplicitCastRuleSharedCaseWhenTwoReferences() {
        // Simulate two expressions sharing the same CaseWhenOperator.
        // After applying ImplicitCastRule to one, the other must be unchanged.

        ConstantOperator whenClause = ConstantOperator.createBoolean(true);
        ConstantOperator thenClause = ConstantOperator.createSmallInt((short) 10);
        ConstantOperator elseClause = ConstantOperator.createSmallInt((short) 20);

        CaseWhenOperator shared = new CaseWhenOperator(IntegerType.BIGINT, null, elseClause,
                Lists.newArrayList(whenClause, thenClause));

        // Two expressions reference the same CaseWhenOperator
        ScalarOperator ref1 = shared;
        ScalarOperator ref2 = shared;

        // Apply ImplicitCastRule to ref1
        ImplicitCastRule rule = new ImplicitCastRule();
        ScalarOperator result1 = rule.visitCaseWhenOperator((CaseWhenOperator) ref1, null);

        // ref2 (the shared original) must still have its original children
        CaseWhenOperator ref2CaseWhen = (CaseWhenOperator) ref2;
        assertSame(thenClause, ref2CaseWhen.getThenClause(0),
                "Shared CaseWhenOperator was corrupted: thenClause was mutated");
        assertSame(elseClause, ref2CaseWhen.getElseClause(),
                "Shared CaseWhenOperator was corrupted: elseClause was mutated");
    }

    // ---------------------------------------------------------------
    // CallOperator clone — deep clone preserves independence
    // ---------------------------------------------------------------

    @Test
    public void testCallOperatorCloneIsIndependent() {
        // CallOperator.clone() already does deep clone. Verify this still holds
        // and serves as a baseline for the other fixes.
        ColumnRefOperator col = new ColumnRefOperator(1, IntegerType.INT, "c1", true);
        CallOperator original = new CallOperator("sum", IntegerType.BIGINT, Lists.newArrayList(col));

        CallOperator cloned = (CallOperator) original.clone();
        ColumnRefOperator newCol = new ColumnRefOperator(2, IntegerType.INT, "c2", true);
        cloned.setChild(0, newCol);

        // Original's child is still the original col reference (clone doesn't affect the original)
        assertSame(col, original.getChild(0), "Original CallOperator child was corrupted by clone");
        assertEquals(1, ((ColumnRefOperator) original.getChild(0)).getId());
        assertEquals(2, ((ColumnRefOperator) cloned.getChild(0)).getId());
    }

    // ---------------------------------------------------------------
    // ColumnRefOperator clone — type/nullable independence
    // ---------------------------------------------------------------

    @Test
    public void testColumnRefOperatorCloneTypeIndependence() {
        // Verifies that mutating type/nullable on a cloned ColumnRefOperator
        // does not affect the original. This validates the fix for
        // MVProjectAggProjectScanRewrite and AggregatedMaterializedViewRewriter.
        ColumnRefOperator original = new ColumnRefOperator(1, IntegerType.INT, "col", false);

        ColumnRefOperator cloned = (ColumnRefOperator) original.clone();
        cloned.setType(IntegerType.BIGINT);
        cloned.setNullable(true);

        // Original must be unaffected
        assertEquals(IntegerType.INT, original.getType(),
                "Original ColumnRefOperator type was corrupted by clone mutation");
        assertEquals(false, original.isNullable(),
                "Original ColumnRefOperator nullable was corrupted by clone mutation");

        // Clone has updated values
        assertEquals(IntegerType.BIGINT, cloned.getType());
        assertEquals(true, cloned.isNullable());

        // Same ID (identity-based equality still works)
        assertEquals(original.getId(), cloned.getId());
    }

    // ---------------------------------------------------------------
    // Predicate tree clone — verifies PartitionSelector / PullUpScanPredicateRule pattern
    // ---------------------------------------------------------------

    @Test
    public void testPredicateTreeCloneBeforeChildReplacement() {
        // Simulates the pattern used in PartitionSelector and PullUpScanPredicateRule:
        // traverse a predicate tree, and when replacing a child, clone the parent first.

        ColumnRefOperator c1 = new ColumnRefOperator(1, IntegerType.INT, "c1", true);
        ColumnRefOperator c2 = new ColumnRefOperator(2, IntegerType.INT, "c2", true);
        ConstantOperator v1 = ConstantOperator.createInt(10);
        ConstantOperator v2 = ConstantOperator.createInt(20);

        BinaryPredicateOperator pred1 = new BinaryPredicateOperator(BinaryType.GT, c1, v1);
        BinaryPredicateOperator pred2 = new BinaryPredicateOperator(BinaryType.LT, c2, v2);
        CompoundPredicateOperator andPred = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, pred1, pred2);

        // Simulate a "shared" reference
        CompoundPredicateOperator sharedRef = andPred;

        // Clone-before-modify pattern (what the fix does)
        ScalarOperator cloned = andPred.clone();
        ConstantOperator newV1 = ConstantOperator.createInt(99);
        BinaryPredicateOperator newPred1 = new BinaryPredicateOperator(BinaryType.GT, c1, newV1);
        cloned.setChild(0, newPred1);

        // Original and shared reference must be unaffected
        assertSame(pred1, sharedRef.getChild(0),
                "Shared predicate tree was corrupted by clone-and-modify");
        assertSame(pred2, sharedRef.getChild(1),
                "Shared predicate tree second child was corrupted");
    }
}
