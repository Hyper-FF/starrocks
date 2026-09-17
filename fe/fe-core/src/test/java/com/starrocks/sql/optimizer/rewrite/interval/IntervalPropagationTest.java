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


package com.starrocks.sql.optimizer.rewrite.interval;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The paths that only exist because the propagation carries an interval rather than a boolean. A
 * boolean cannot say which way a function runs, cannot notice that an endpoint overflowed, and cannot
 * distinguish a bound that stays strict from one that has to open up.
 */
public class IntervalPropagationTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        // the propagator evaluates the function at the interval's endpoints, and the constant folder
        // will not touch a call whose Function is unbound -- which is what binding needs a catalog for
        UtFrameUtils.createMinStarRocksCluster();
    }

    private static final ColumnRefOperator C1 = new ColumnRefOperator(1, IntegerType.BIGINT, "c1", true);
    private static final ColumnRefOperator DT = new ColumnRefOperator(2, DateType.DATETIME, "dt", true);

    /** Binds the real builtin, so the endpoints actually fold instead of silently yielding no image. */
    private static CallOperator call(String name, Type type, ScalarOperator... args) {
        Type[] argTypes = new Type[args.length];
        for (int i = 0; i < args.length; i++) {
            argTypes[i] = args[i].getType();
        }
        Function fn = ExprUtils.getBuiltinFunction(name, argTypes, Function.CompareMode.IS_IDENTICAL);
        return new CallOperator(name, fn == null ? type : fn.getReturnType(),
                ImmutableList.copyOf(args), fn);
    }

    /** A call with no builtin behind it, for the undeclared-function case. */
    private static CallOperator unboundCall(String name, Type type, ScalarOperator... args) {
        return new CallOperator(name, type, ImmutableList.copyOf(args));
    }

    private static ConstantOperator bigint(long v) {
        return ConstantOperator.createBigint(v);
    }

    private static ConstantOperator datetime(int y, int m, int d) {
        return ConstantOperator.createDatetime(LocalDateTime.of(y, m, d, 0, 0));
    }

    /** The question the boolean could not be asked: which way does this run. */
    @Test
    public void testDirectionIsPerArgument() {
        // 100 - c1 decreases; c1 - 100 increases
        assertEquals(Direction.DECREASING, ExpressionIntervalAnalyzer.analyze(
                call(FunctionSet.SUBTRACT, IntegerType.BIGINT, bigint(100), C1)).direction());
        assertEquals(Direction.INCREASING, ExpressionIntervalAnalyzer.analyze(
                call(FunctionSet.SUBTRACT, IntegerType.BIGINT, C1, bigint(100))).direction());
    }

    /**
     * Two decreases make an increase. The old boolean had no way to say this, so a caller either
     * refused the whole expression or accepted it with the direction unknown; here the shapes compose
     * and `100 - (100 - c1)` comes out usable.
     */
    @Test
    public void testDirectionsCompose() {
        ScalarOperator inner = call(FunctionSet.SUBTRACT, IntegerType.BIGINT, bigint(100), C1);
        ScalarOperator outer = call(FunctionSet.SUBTRACT, IntegerType.BIGINT, bigint(100), inner);
        assertEquals(Direction.INCREASING, ExpressionIntervalAnalyzer.analyze(outer).direction());
        assertTrue(ExpressionIntervalAnalyzer.analyze(outer).isIncreasing());
    }

    /**
     * An undeclared function propagates nothing, so the caller declines. That is the inversion this
     * design is for: the cost of an omission is a lost optimization, where the boolean's default made
     * it lost rows.
     */
    @Test
    public void testUndeclaredFunctionIsUnknownRatherThanMonotonic() {
        assertFalse(IntervalPropagators.isDescribed("some_function_nobody_described"));
        IntervalMapping mapping = ExpressionIntervalAnalyzer.analyze(
                unboundCall("some_function_nobody_described", IntegerType.BIGINT, C1, bigint(1)));
        assertEquals(Direction.UNKNOWN, mapping.direction());
        assertFalse(mapping.isKnown());
    }

    /**
     * The bound types of the image are the strictness answer. from_unixtime() renders one datetime per
     * second, so a strict input bound stays strict; from_unixtime_ms() divides by 1000, so a thousand
     * inputs share an output and the bound has to close or the partition holding the match is pruned.
     */
    @Test
    public void testPlateauClosesAStrictBound() {
        IntervalMapping strict = ExpressionIntervalAnalyzer.analyze(
                call(FunctionSet.FROM_UNIXTIME, VarcharType.VARCHAR, C1),
                Range.lessThan(bigint(1609689600L)));
        assertTrue(strict.isIncreasing());
        assertFalse(strict.plateau());
        assertTrue(strict.outRange().isPresent(), "no image means the assertion below proves nothing");
        assertEquals(BoundType.OPEN, strict.outRange().get().upperBoundType());

        IntervalMapping flat = ExpressionIntervalAnalyzer.analyze(
                call(FunctionSet.FROM_UNIXTIME_MS, VarcharType.VARCHAR, C1),
                Range.lessThan(bigint(1609689600500L)));
        assertTrue(flat.isIncreasing());
        assertTrue(flat.plateau());
        assertTrue(flat.outRange().isPresent(), "no image means the assertion below proves nothing");
        assertEquals(BoundType.CLOSED, flat.outRange().get().upperBoundType());
    }

    /**
     * The bound type and the plateau flag answer two different questions, and an aligned endpoint is
     * where they disagree.
     * <p>
     * Whether a strict bound survives is about the value just INSIDE the interval: for `x < c` the
     * image's top is attained only if f(c-1) already equals f(c). Whether the rewritten predicate may
     * be dropped afterwards is about EITHER side: if anything adjacent to c shares its output then
     * `f(a) OP f(c)` stops implying `a OP c`, and the kept partitions no longer imply the predicate.
     * <p>
     * from_unixtime_ms() divides by 1000, so at a whole second the value below renders the previous
     * second -- the bound stays strict and keeps its pruning -- while the value above renders the same
     * second, so the mapping is still not injective there and the predicate has to stay. Collapsing
     * the two questions into one flag loses pruning in one direction and rows in the other.
     */
    @Test
    public void testAlignedEndpointKeepsItsStrictBoundButStillPlateaus() {
        CallOperator ms = call(FunctionSet.FROM_UNIXTIME_MS, VarcharType.VARCHAR, C1);

        // 1609689600000 is a whole second: 1609689599999 renders the second before it, so the top of
        // the image is not attained from inside and the bound stays open
        IntervalMapping aligned = ExpressionIntervalAnalyzer.analyze(ms, Range.lessThan(bigint(1609689600000L)));
        assertTrue(aligned.outRange().isPresent());
        assertEquals(BoundType.OPEN, aligned.outRange().get().upperBoundType());
        // ... and 1609689600001 renders the same second, so the mapping still collapses here
        assertTrue(aligned.plateau());

        // half a second in, the value below renders the same second and the bound has to close
        IntervalMapping midway = ExpressionIntervalAnalyzer.analyze(ms, Range.lessThan(bigint(1609689600500L)));
        assertTrue(midway.outRange().isPresent());
        assertEquals(BoundType.CLOSED, midway.outRange().get().upperBoundType());
        assertTrue(midway.plateau());

        // a strictly injective rendering collapses nowhere, so neither question is affected
        IntervalMapping seconds = ExpressionIntervalAnalyzer.analyze(
                call(FunctionSet.FROM_UNIXTIME, VarcharType.VARCHAR, C1),
                Range.lessThan(bigint(1609689600L)));
        assertTrue(seconds.outRange().isPresent());
        assertEquals(BoundType.OPEN, seconds.outRange().get().upperBoundType());
        assertFalse(seconds.plateau());
    }

    /**
     * Mapping the endpoints is also the domain check. A boolean flag is blind to an argument the
     * function cannot render: to_datetime(ts, scale) only renders scales 0, 3 and 6, so an image was
     * never available for scale 5 and the shape must not be handed out with a range attached.
     */
    @Test
    public void testUnrenderableConstantYieldsNoImage() {
        IntervalMapping bad = ExpressionIntervalAnalyzer.analyze(
                call(FunctionSet.TO_DATETIME, DateType.DATETIME, C1, ConstantOperator.createInt(5)),
                Range.closed(bigint(1), bigint(1000)));
        assertFalse(bad.outRange().isPresent());
    }

    /**
     * Date arithmetic is partial at the far end of the calendar: years_add('9999-12-31', 1) is NULL.
     * The endpoint evaluation notices, so no image is published for an interval reaching that far.
     */
    @Test
    public void testOverflowingEndpointYieldsNoImage() {
        IntervalMapping overflowing = ExpressionIntervalAnalyzer.analyze(
                call(FunctionSet.YEARS_ADD, DateType.DATETIME, DT, ConstantOperator.createInt(1)),
                Range.closed(datetime(2021, 1, 1), datetime(9999, 12, 31)));
        assertTrue(overflowing.isIncreasing());
        assertFalse(overflowing.outRange().isPresent());

        IntervalMapping ordinary = ExpressionIntervalAnalyzer.analyze(
                call(FunctionSet.YEARS_ADD, DateType.DATETIME, DT, ConstantOperator.createInt(1)),
                Range.closed(datetime(2021, 1, 1), datetime(2021, 12, 31)));
        assertTrue(ordinary.outRange().isPresent());
    }

    /** A decreasing function hands back the interval with its endpoints the right way round. */
    @Test
    public void testDecreasingImageIsReordered() {
        IntervalMapping mapping = ExpressionIntervalAnalyzer.analyze(
                call(FunctionSet.SUBTRACT, IntegerType.BIGINT, bigint(100), C1),
                Range.closed(bigint(10), bigint(30)));
        assertEquals(Direction.DECREASING, mapping.direction());
        assertTrue(mapping.outRange().isPresent(), "no image means the assertions below prove nothing");
        assertEquals(bigint(70), mapping.outRange().get().lowerEndpoint());
        assertEquals(bigint(90), mapping.outRange().get().upperEndpoint());
    }

    /** date_trunc() takes the unit first, so the ordered argument is the second one. */
    @Test
    public void testOrderedArgumentIsNotAlwaysTheFirst() {
        assertTrue(ExpressionIntervalAnalyzer.analyze(
                call(FunctionSet.DATE_TRUNC, DateType.DATETIME,
                        ConstantOperator.createVarchar("day"), DT)).isIncreasing());
        // a column in the unit argument orders results by how the unit names sort
        assertFalse(ExpressionIntervalAnalyzer.analyze(
                call(FunctionSet.DATE_TRUNC, DateType.DATETIME,
                        new ColumnRefOperator(3, VarcharType.VARCHAR, "u", true),
                        datetime(2021, 1, 1))).isKnown());
    }

    /**
     * A cast is a function too, and the image has to survive it.
     * <p>
     * This is the shape every expression-partitioned table actually has: RANGE(from_unixtime_ms(ts))
     * is spelled cast(from_unixtime_ms(ts) as datetime). Letting the cast contribute only a shape
     * drops the interval the call below it computed, and with it the bound types that say whether a
     * strict predicate may keep its strict bound -- which is exactly the row loss #79111 fixed.
     */
    @Test
    public void testImageSurvivesAnEnclosingCast() {
        CallOperator ms = call(FunctionSet.FROM_UNIXTIME_MS, VarcharType.VARCHAR, C1);
        CastOperator cast = new CastOperator(DateType.DATETIME, ms);

        IntervalMapping midway = ExpressionIntervalAnalyzer.analyze(cast, Range.lessThan(bigint(1609689600500L)));
        assertTrue(midway.isIncreasing());
        assertTrue(midway.outRange().isPresent(), "the cast must map the interval, not just declare a shape");
        assertEquals(BoundType.CLOSED, midway.outRange().get().upperBoundType());

        // and an aligned endpoint still keeps its strict bound through the same cast
        IntervalMapping aligned = ExpressionIntervalAnalyzer.analyze(cast, Range.lessThan(bigint(1609689600000L)));
        assertTrue(aligned.outRange().isPresent());
        assertEquals(BoundType.OPEN, aligned.outRange().get().upperBoundType());
    }

    /** Two columns is not a function of one interval, so there is nothing to substitute into. */
    @Test
    public void testTwoColumnsIsUnknown() {
        assertFalse(ExpressionIntervalAnalyzer.analyze(
                call(FunctionSet.ADD, IntegerType.BIGINT, C1,
                        new ColumnRefOperator(4, IntegerType.BIGINT, "c2", true))).isKnown());
    }
}
