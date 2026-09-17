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

import com.google.common.collect.Range;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;

import java.util.Optional;

/**
 * What a function does to an interval: where the inputs land, how tightly that is known, which way the
 * function runs, and whether it collapses distinct inputs onto one output.
 * <p>
 * This replaces the boolean that {@code @ConstantFunction(isMonotonic = true)} used to hand to the
 * pruning consumers. The boolean answered only "does this preserve order", and every caller read it as
 * the stronger claim that the expression increases with its column -- which silently dropped rows for
 * an expression running the other way. More importantly the boolean's default was the unsafe one: a
 * function marked monotonic by mistake, or monotonic in one argument and arbitrary in another, loses
 * data. Here the default is {@link #unknown()}, so a function nobody has described yet costs a
 * pruning opportunity and nothing else.
 * <p>
 * <b>outRange and strictness.</b> The bound types of {@link #outRange()} carry the answer the old code
 * had to probe for. For a strictly increasing f, {@code (-inf, c)} maps to {@code (-inf, f(c))} -- an
 * open bound, so a {@code <} predicate keeps its {@code <}. For an f that plateaus at c, some x &lt; c
 * also maps to f(c), so the image is {@code (-inf, f(c)]} -- a closed bound, and the predicate has to
 * be relaxed to {@code <=} or the partition holding the matching rows is pruned away. {@link
 * #plateau()} is the same fact summarized over the whole range, for consumers asking the static
 * question rather than mapping a concrete interval.
 * <p>
 * <b>Exactness.</b> {@link Exactness#EXACT} means the bounds are tight, not that every value inside
 * is attained -- a monotone function over the integers skips values. Pruning stays correct either
 * way, since a looser range only keeps partitions it could have dropped. Dropping the original
 * predicate after the rewrite is a question about injectivity rather than tightness, and {@link
 * #plateau()} is what answers it.
 */
public final class IntervalMapping {
    private static final IntervalMapping UNKNOWN =
            new IntervalMapping(null, Exactness.OVER, Direction.UNKNOWN, true);

    private final Range<ConstantOperator> outRange;
    private final Exactness exactness;
    private final Direction direction;
    private final boolean plateau;

    private IntervalMapping(Range<ConstantOperator> outRange, Exactness exactness, Direction direction,
                            boolean plateau) {
        this.outRange = outRange;
        this.exactness = exactness;
        this.direction = direction;
        this.plateau = plateau;
    }

    /** No claim: the caller must not rewrite anything across this function. */
    public static IntervalMapping unknown() {
        return UNKNOWN;
    }

    public static IntervalMapping of(Range<ConstantOperator> outRange, Exactness exactness,
                                     Direction direction, boolean plateau) {
        if (direction == Direction.UNKNOWN) {
            return UNKNOWN;
        }
        return new IntervalMapping(outRange, exactness, direction, plateau);
    }

    /**
     * A claim about shape alone, with no image computed -- what a consumer gets when it asks the
     * static question "may I rewrite across this" over the whole domain.
     */
    public static IntervalMapping shape(Direction direction, boolean plateau) {
        return of(null, Exactness.OVER, direction, plateau);
    }

    /** Absent when the image was not computed; never treat absence as the empty range. */
    public Optional<Range<ConstantOperator>> outRange() {
        return Optional.ofNullable(outRange);
    }

    public Exactness exactness() {
        return exactness;
    }

    public Direction direction() {
        return direction;
    }

    /** Whether two distinct inputs may share an output, which is what forces a strict bound open. */
    public boolean plateau() {
        return plateau;
    }

    public boolean isKnown() {
        return direction != Direction.UNKNOWN;
    }

    /** True when the result grows with the argument, which is what an operator-preserving rewrite needs. */
    public boolean isIncreasing() {
        return direction == Direction.INCREASING;
    }

    /**
     * The mapping of {@code outer(inner(x))}. Exactness and plateau are the pessimistic combination:
     * an over-approximation anywhere makes the whole thing over-approximate, and a plateau anywhere
     * collapses inputs.
     */
    public IntervalMapping compose(IntervalMapping inner) {
        if (!isKnown() || !inner.isKnown()) {
            return UNKNOWN;
        }
        Exactness combined = exactness == Exactness.EXACT && inner.exactness == Exactness.EXACT
                ? Exactness.EXACT : Exactness.OVER;
        return of(outRange, combined, direction.compose(inner.direction), plateau || inner.plateau);
    }

    @Override
    public String toString() {
        return "IntervalMapping{" + direction + ", " + exactness + (plateau ? ", plateau" : "")
                + (outRange == null ? "" : ", " + outRange) + "}";
    }
}
