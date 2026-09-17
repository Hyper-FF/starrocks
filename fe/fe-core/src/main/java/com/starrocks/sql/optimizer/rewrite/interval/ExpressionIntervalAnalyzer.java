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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorEvaluator;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * What a whole expression does to the interval of the one column it reads.
 * <p>
 * The expression is treated as a function of that column: each node contributes its own shape and the
 * shapes compose, so {@code 100 - (100 - c1)} comes out INCREASING rather than being refused for
 * containing a subtraction, and {@code date_trunc('day', from_unixtime_ms(ts))} comes out increasing
 * with a plateau because one of the two steps collapses inputs.
 * <p>
 * An expression reading more than one column has no single interval to talk about and is UNKNOWN: the
 * rewrites this serves substitute one column's constant into the expression, which is meaningless when
 * two columns vary independently.
 */
public final class ExpressionIntervalAnalyzer {
    /** Names whose output text orders the way the instant does, for a format that leads with the year. */
    private static final ImmutableSet<String> DATETIME_TEXT_FUNCTIONS = ImmutableSet.of(
            FunctionSet.FROM_UNIXTIME, FunctionSet.FROM_UNIXTIME_MS);

    private ExpressionIntervalAnalyzer() {
    }

    /** The shape of {@code expr} as a function of its column, with no interval mapped through it. */
    public static IntervalMapping analyze(ScalarOperator expr) {
        return analyze(expr, null);
    }

    /**
     * Whether every expression inside a predicate satisfies {@code accept}.
     * <p>
     * The consumers hand in whole predicates -- `date_trunc('day', dt) <= date_sub(current_date(), 2)`
     * -- not bare expressions, and a predicate is not a function of an interval: it is a boolean
     * combination of expressions that are. So predicate and compound nodes are descended through and
     * the interval analysis is applied to the maximal expression subtrees underneath, which is the
     * contract the callers have always had.
     *
     * @return the name of the first function that failed, or empty when everything passed
     */
    public static Optional<String> firstUnacceptable(ScalarOperator expr,
                                                     java.util.function.Predicate<IntervalMapping> accept) {
        if (expr == null) {
            return Optional.empty();
        }
        if (expr instanceof PredicateOperator || expr instanceof CompoundPredicateOperator) {
            for (ScalarOperator child : expr.getChildren()) {
                Optional<String> failed = firstUnacceptable(child, accept);
                if (failed.isPresent()) {
                    return failed;
                }
            }
            return Optional.empty();
        }
        // A bare column or constant is trivially fine and carries no function to blame.
        if (expr instanceof ColumnRefOperator || expr instanceof ConstantOperator) {
            return Optional.empty();
        }
        if (accept.test(analyze(expr))) {
            return Optional.empty();
        }
        return Optional.of(blame(expr));
    }

    /** The deepest call in a rejected subtree, which is the useful half of an error message. */
    private static String blame(ScalarOperator expr) {
        for (ScalarOperator child : expr.getChildren()) {
            String name = blame(child);
            if (!name.isEmpty()) {
                return name;
            }
        }
        return expr instanceof CallOperator call ? call.getFnName() : "";
    }

    /**
     * @param in the interval the column ranges over, or null to ask only about the shape
     */
    public static IntervalMapping analyze(ScalarOperator expr, Range<ConstantOperator> in) {
        if (expr == null) {
            return IntervalMapping.unknown();
        }
        if (Utils.extractColumnRef(expr).size() > 1) {
            return IntervalMapping.unknown();
        }
        return visit(expr, in);
    }

    private static IntervalMapping visit(ScalarOperator expr, Range<ConstantOperator> in) {
        if (expr instanceof ColumnRefOperator) {
            // the identity: the column's own interval, unchanged
            return IntervalMapping.of(in, Exactness.EXACT, Direction.INCREASING, false);
        }
        if (expr instanceof ConstantOperator) {
            return IntervalMapping.shape(Direction.CONSTANT, true);
        }
        if (expr instanceof CastOperator cast) {
            return visitCast(cast, in);
        }
        if (expr instanceof CallOperator call) {
            return visitCall(call, in);
        }
        return IntervalMapping.unknown();
    }

    private static IntervalMapping visitCast(CastOperator cast, Range<ConstantOperator> in) {
        IntervalMapping inner = visit(cast.getChild(0), in);
        if (!inner.isKnown()) {
            return IntervalMapping.unknown();
        }
        Type from = cast.fromType();
        Type to = cast.getType();
        if (from.equals(to)) {
            return inner;
        }
        // A widening integer cast keeps every value and its order; DATETIME -> DATE keeps the order
        // but throws the time away, so it collapses inputs.
        boolean widening = isIntegerWidening(from, to);
        boolean dateTruncating = from.isDatetime() && to.isDate();
        boolean dateWidening = from.isDate() && to.isDatetime();
        if (!widening && !dateTruncating && !dateWidening) {
            // Crossing between strings and numbers or dates reorders values: '99845' sorts after
            // '998425506019' while 99845 is far below it. The exception is the canonical datetime text
            // a from_unixtime()/date_format() renders, which an expression partition on a unix
            // timestamp is spelled with: RANGE(from_unixtime(ts)) translates to
            // cast(from_unixtime(ts) as datetime), and refusing that costs those tables their pruning.
            if (!(from.isStringType() && (to.isDate() || to.isDatetime())
                    && producesOrderedDatetimeText(cast.getChild(0)))) {
                return IntervalMapping.unknown();
            }
        }
        // A cast is a function like any other, so it has to map the interval rather than just declare a
        // shape: composing a shape-only mapping would drop the image the subtree below already
        // computed, and the bound types that carry the strictness answer along with it.
        Range<ConstantOperator> image = castRange(inner.outRange().orElse(null), to, dateTruncating);
        IntervalMapping self = image == null
                ? IntervalMapping.shape(Direction.INCREASING, dateTruncating)
                : IntervalMapping.of(image, Exactness.EXACT, Direction.INCREASING, dateTruncating);
        return self.compose(inner);
    }

    /**
     * The interval a cast lands the values in. Null when an endpoint will not convert, which leaves
     * the shape standing without an image rather than publishing a range the caller would prune with.
     */
    private static Range<ConstantOperator> castRange(Range<ConstantOperator> in, Type to, boolean truncating) {
        if (in == null) {
            return null;
        }
        try {
            ConstantOperator low = in.hasLowerBound() ? in.lowerEndpoint().castTo(to).orElse(null) : null;
            ConstantOperator high = in.hasUpperBound() ? in.upperEndpoint().castTo(to).orElse(null) : null;
            if ((in.hasLowerBound() && low == null) || (in.hasUpperBound() && high == null)) {
                return null;
            }
            // Truncating to a coarser type collapses inputs, so whatever was open has to close: values
            // outside the interval now share the endpoint's converted value.
            BoundType lowType = !in.hasLowerBound() ? null
                    : truncating ? BoundType.CLOSED : in.lowerBoundType();
            BoundType highType = !in.hasUpperBound() ? null
                    : truncating ? BoundType.CLOSED : in.upperBoundType();
            if (low == null && high == null) {
                return Range.all();
            }
            if (low == null) {
                return Range.upTo(high, highType);
            }
            if (high == null) {
                return Range.downTo(low, lowType);
            }
            return low.compareTo(high) > 0 ? null : Range.range(low, lowType, high, highType);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * These render an instant as text whose order matches the instant's, but only for a format that
     * leads with the year. The format was already vetted when the call itself was analyzed, so a call
     * that got this far has an accepted one.
     */
    private static boolean producesOrderedDatetimeText(ScalarOperator operator) {
        if (!(operator instanceof CallOperator call)) {
            return false;
        }
        return DATETIME_TEXT_FUNCTIONS.contains(call.getFnName().toLowerCase())
                && call.getChildren().size() <= 3
                && ScalarOperatorEvaluator.INSTANCE.isMonotonicFunction(call);
    }

    private static boolean isIntegerWidening(Type from, Type to) {
        int fromRank = integerRank(from);
        int toRank = integerRank(to);
        return fromRank > 0 && toRank > 0 && toRank >= fromRank;
    }

    private static int integerRank(Type type) {
        if (type.isTinyint()) {
            return 1;
        } else if (type.isSmallint()) {
            return 2;
        } else if (type.isInt()) {
            return 3;
        } else if (type.isBigint()) {
            return 4;
        } else if (type.isLargeint()) {
            return 5;
        }
        return 0;
    }

    private static IntervalMapping visitCall(CallOperator call, Range<ConstantOperator> in) {
        // Which argument carries the column? The others have to fold to constants, because the
        // propagator evaluates the function at the interval's endpoints with them held fixed.
        int varIndex = -1;
        List<ConstantOperator> args = new ArrayList<>();
        for (int i = 0; i < call.getChildren().size(); i++) {
            ScalarOperator child = call.getChild(i);
            if (Utils.extractColumnRef(child).isEmpty()) {
                ScalarOperator folded = child instanceof CallOperator childCall
                        ? ScalarOperatorEvaluator.INSTANCE.evaluation(childCall) : child;
                if (!(folded instanceof ConstantOperator constant)) {
                    return IntervalMapping.unknown();
                }
                args.add(constant);
            } else {
                if (varIndex >= 0) {
                    // the same column in two arguments: the expression is not a function of one
                    // interval any more
                    return IntervalMapping.unknown();
                }
                varIndex = i;
                args.add(null);
            }
        }
        if (varIndex < 0) {
            return IntervalMapping.shape(Direction.CONSTANT, true);
        }
        // The registry is keyed by function NAME, which adds the per-argument and directional knowledge
        // a name-level flag cannot carry -- but it must not throw away the per-SIGNATURE gate that was
        // there before it. add() and subtract() are declared monotonic for the integer types and
        // deliberately not for DOUBLE or any DECIMAL, and unix_timestamp() is declared over a DATETIME
        // while its zero-argument form returns the current time. Consulting only the name would quietly
        // start pruning on all three. This also carries the format validation, which decides whether
        // the leading argument of a rendering function keeps the order at all.
        if (!ScalarOperatorEvaluator.INSTANCE.isMonotonicFunction(call)) {
            return IntervalMapping.unknown();
        }
        IntervalMapping inner = visit(call.getChild(varIndex), in);
        if (!inner.isKnown()) {
            return IntervalMapping.unknown();
        }
        // The propagator is asked about the interval the ARGUMENT sees, which is what the subtree
        // below it produced -- not about the column's own interval.
        Range<ConstantOperator> argRange = inner.outRange().orElse(null);
        IntervalMapping self = IntervalPropagators.of(call, varIndex).propagate(argRange, args);
        return self.compose(inner);
    }
}
