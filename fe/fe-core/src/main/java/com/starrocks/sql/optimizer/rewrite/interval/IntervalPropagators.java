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

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;

import java.util.Map;

/**
 * What each function does to each of its arguments.
 * <p>
 * This is the replacement for {@code @ConstantFunction(isMonotonic = true)} as the thing pruning
 * consults. The annotation's boolean said only "order is preserved somewhere", which left three
 * questions unanswered -- in which direction, over which argument, and whether distinct inputs
 * collapse -- and answered them all, implicitly and wrongly, as "increasing, every argument, no
 * collapsing". Each of those has cost rows: a decreasing expression deduced a bound pointing the
 * wrong way (#79187), a column in a format or day-of-week argument reordered results arbitrarily,
 * and a plateauing expression kept a strict bound that pruned the partition holding the match
 * (#79111).
 * <p>
 * A function absent from this table propagates nothing, so every consumer declines to rewrite across
 * it. That inverts the old failure mode: forgetting to describe a function now costs an optimization
 * instead of losing rows, and describing it wrongly is a much smaller surface than the single boolean
 * was, because the declaration says which argument it is talking about.
 */
public final class IntervalPropagators {
    /** FunctionSet has no constants for these two. */
    private static final String JODATIME_FORMAT = "jodatime_format";
    private static final String TO_ISO8601 = "to_iso8601";

    /** Shape of one argument: how the result moves as that argument grows, and whether it collapses. */
    private static final class Shape {
        final Direction direction;
        final boolean plateau;

        Shape(Direction direction, boolean plateau) {
            this.direction = direction;
            this.plateau = plateau;
        }
    }

    /**
     * Strictly increasing: distinct inputs stay distinct, whatever the other arguments say. Reserve
     * this for functions that are injective by construction -- adding and subtracting keep every
     * value apart -- because it tells the propagator not to bother probing, and a wrong claim here
     * keeps a strict bound that prunes the partition holding the matching rows.
     */
    private static final Shape UP = new Shape(Direction.INCREASING, false);
    /**
     * Increasing, and MAY collapse distinct inputs onto one output. Whether it actually collapses at
     * a given endpoint is decided by probing the neighbour, not by this flag: from_unixtime() renders
     * one datetime per second under a full-precision format and one per DAY under '%Y-%m-%d', so the
     * answer belongs to the constant, not to the name. Declaring the possibility costs nothing when
     * it does not happen -- the probe finds no collapse and the strict bound survives.
     */
    private static final Shape UP_FLAT = new Shape(Direction.INCREASING, true);
    /** Strictly decreasing. */
    private static final Shape DOWN = new Shape(Direction.DECREASING, false);
    /** No claim for this argument. */
    private static final Shape NONE = new Shape(Direction.UNKNOWN, true);

    /**
     * Keyed by function name; the value lists one shape per argument position, in declaration order.
     * A position past the end of its list, like the time zone of a three-argument from_unixtime, is
     * {@link #NONE}.
     */
    private static final Map<String, Shape[]> SHAPES = ImmutableMap.<String, Shape[]>builder()
            // ---- arithmetic -------------------------------------------------------------------
            // a + b grows with both sides; a - b grows with a and shrinks with b. Both are exact and
            // injective in each argument -- and the endpoint evaluation catches the wrap-around that
            // makes that false at the extremes of the integer types.
            .put(FunctionSet.ADD, new Shape[] {UP, UP})
            .put(FunctionSet.SUBTRACT, new Shape[] {UP, DOWN})

            // ---- date arithmetic --------------------------------------------------------------
            // date + n and date - n: injective in both, and the interval math is what catches the
            // overflow past the maximum date that renders NULL.
            .put(FunctionSet.ADD_MONTHS, new Shape[] {UP, UP})
            .put(FunctionSet.ADDDATE, new Shape[] {UP, UP})
            .put(FunctionSet.DATE_ADD, new Shape[] {UP, UP})
            .put(FunctionSet.DAYS_ADD, new Shape[] {UP, UP})
            .put(FunctionSet.HOURS_ADD, new Shape[] {UP, UP})
            .put(FunctionSet.MILLISECONDS_ADD, new Shape[] {UP, UP})
            .put(FunctionSet.MINUTES_ADD, new Shape[] {UP, UP})
            .put(FunctionSet.MONTHS_ADD, new Shape[] {UP, UP})
            .put(FunctionSet.QUARTERS_ADD, new Shape[] {UP, UP})
            .put(FunctionSet.SECONDS_ADD, new Shape[] {UP, UP})
            .put(FunctionSet.WEEKS_ADD, new Shape[] {UP, UP})
            .put(FunctionSet.YEARS_ADD, new Shape[] {UP, UP})
            .put(FunctionSet.DATE_SUB, new Shape[] {UP, DOWN})
            .put(FunctionSet.SUBDATE, new Shape[] {UP, DOWN})
            .put(FunctionSet.DAYS_SUB, new Shape[] {UP, DOWN})
            .put(FunctionSet.HOURS_SUB, new Shape[] {UP, DOWN})
            .put(FunctionSet.MILLISECONDS_SUB, new Shape[] {UP, DOWN})
            .put(FunctionSet.MINUTES_SUB, new Shape[] {UP, DOWN})
            .put(FunctionSet.MONTHS_SUB, new Shape[] {UP, DOWN})
            .put(FunctionSet.QUARTERS_SUB, new Shape[] {UP, DOWN})
            .put(FunctionSet.SECONDS_SUB, new Shape[] {UP, DOWN})
            .put(FunctionSet.WEEKS_SUB, new Shape[] {UP, DOWN})
            .put(FunctionSet.YEARS_SUB, new Shape[] {UP, DOWN})

            // ---- differences ------------------------------------------------------------------
            // datediff(a, b) and timediff(a, b) are "a minus b" in disguise: later a, larger result;
            // later b, smaller. They also quantize -- datediff counts whole days -- so two instants
            // in the same day share a result and a strict bound has to open up.
            .put(FunctionSet.DATEDIFF, new Shape[] {UP_FLAT, new Shape(Direction.DECREASING, true)})
            .put(FunctionSet.TIMEDIFF, new Shape[] {UP_FLAT, new Shape(Direction.DECREASING, true)})

            // ---- truncation and extraction ----------------------------------------------------
            // date_trunc takes the unit FIRST, so the ordered argument is the second one; a unit
            // column orders results by however the unit names sort, which is not the time order.
            .put(FunctionSet.DATE_TRUNC, new Shape[] {NONE, UP_FLAT})
            // time_slice is not even signed in its interval: the bucket floor jumps around as the
            // interval grows (a duration of 100 floors to 96 at interval 6, 98 at 7, 0 at 101).
            .put(FunctionSet.TIME_SLICE, new Shape[] {UP_FLAT, NONE, NONE, NONE})
            .put(FunctionSet.TO_DAYS, new Shape[] {UP_FLAT})
            .put(FunctionSet.YEAR, new Shape[] {UP_FLAT})
            .put(FunctionSet.TO_DATE, new Shape[] {UP_FLAT})
            .put(FunctionSet.UNIX_TIMESTAMP, new Shape[] {UP_FLAT})
            .put(TO_ISO8601, new Shape[] {UP_FLAT})
            // last_day(dt, unit): every instant in the same month maps to that month's last day, and
            // the unit names order results arbitrarily.
            .put(FunctionSet.LAST_DAY, new Shape[] {UP_FLAT, NONE})
            // next_day/previous_day(dt, dow): the day-of-week argument orders results by how the
            // strings sort -- 'Monday' sorts below 'Sunday' while next_day sends it above.
            .put(FunctionSet.NEXT_DAY, new Shape[] {UP_FLAT, NONE})
            .put(FunctionSet.PREVIOUS_DAY, new Shape[] {UP_FLAT, NONE})

            // ---- epoch rendering --------------------------------------------------------------
            // Whether these collapse depends on the format they are handed: from_unixtime() renders
            // one string per second under a full-precision format and one per day under '%Y-%m-%d',
            // which the format check accepts as order-preserving because it still leads with the
            // year. So the possibility is declared and the probe settles it per endpoint.
            // from_unixtime_ms divides by 1000 and collapses regardless. The format and time zone
            // arguments reorder the output arbitrarily, and a format that leads with the month makes
            // even the leading argument non-monotonic -- which the format validation rejects before
            // any shape here is trusted.
            .put(FunctionSet.FROM_UNIXTIME, new Shape[] {UP_FLAT, NONE, NONE})
            .put(FunctionSet.FROM_UNIXTIME_MS, new Shape[] {UP_FLAT, NONE, NONE})
            // to_datetime(unixtime, scale) divides by 10^scale, so a LARGER scale renders an EARLIER
            // instant -- and only 0, 3 and 6 render at all, the rest being NULL.
            .put(FunctionSet.TO_DATETIME, new Shape[] {UP_FLAT, NONE})

            // ---- text rendering ---------------------------------------------------------------
            // The format argument decides everything about the output's order, so only a constant
            // format can be reasoned about, and only after it has been checked.
            .put(FunctionSet.DATE_FORMAT, new Shape[] {UP_FLAT, NONE})
            .put(JODATIME_FORMAT, new Shape[] {UP_FLAT, NONE})
            .put(FunctionSet.STR2DATE, new Shape[] {UP_FLAT, NONE})
            .put(FunctionSet.STR_TO_DATE, new Shape[] {UP_FLAT, NONE})
            .build();

    private IntervalPropagators() {
    }

    /**
     * How {@code call} moves the interval of its {@code varIndex}-th argument. Returns a propagator
     * that yields {@link IntervalMapping#unknown()} when the function, or that position of it, has
     * not been described.
     */
    public static IntervalPropagator of(CallOperator call, int varIndex) {
        Shape[] shapes = SHAPES.get(call.getFnName().toLowerCase());
        if (shapes == null || varIndex < 0 || varIndex >= shapes.length) {
            return (in, args) -> IntervalMapping.unknown();
        }
        Shape shape = shapes[varIndex];
        return new EndpointPropagator(call, varIndex, shape.direction, shape.plateau);
    }

    /** Whether anything at all is known about this function, for callers reporting why they declined. */
    public static boolean isDescribed(String fnName) {
        return SHAPES.containsKey(fnName.toLowerCase());
    }

    /** Every described function name, so the declarations can be checked against the functions. */
    static java.util.Set<String> describedNames() {
        return SHAPES.keySet();
    }

    /** The declared direction for one argument, or UNKNOWN when that position carries no claim. */
    static Direction declaredDirection(String fnName, int argIndex) {
        Shape[] shapes = SHAPES.get(fnName.toLowerCase());
        return shapes == null || argIndex < 0 || argIndex >= shapes.length
                ? Direction.UNKNOWN : shapes[argIndex].direction;
    }

    /**
     * Whether the declaration allows this argument to collapse distinct inputs. A declaration of
     * false is a promise of injectivity, and a wrong one keeps a strict bound that prunes away the
     * partition holding the matching rows -- so it is the claim most worth checking against reality.
     */
    static boolean declaredMayCollapse(String fnName, int argIndex) {
        Shape[] shapes = SHAPES.get(fnName.toLowerCase());
        return shapes == null || argIndex < 0 || argIndex >= shapes.length || shapes[argIndex].plateau;
    }
}
