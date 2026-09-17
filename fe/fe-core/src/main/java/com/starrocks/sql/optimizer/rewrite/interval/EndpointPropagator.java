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
import com.google.common.collect.Range;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorEvaluator;

import java.util.List;

/**
 * Builds an {@link IntervalPropagator} out of a per-argument shape declaration.
 * <p>
 * Nearly every function here needs the same propagator: map the two endpoints of the input interval
 * through the function, swap them if the function decreases, and hand back the result. Only the shape
 * -- which way this argument runs and whether the function collapses distinct inputs -- differs, so
 * that is all a registration has to state, and the image is computed by the constant folder that
 * already knows how to evaluate the function.
 * <p>
 * Endpoint evaluation is also where partiality is caught. {@code years_add('9999-12-31', 1)} is NULL,
 * {@code subtract(INT_MIN, 1)} wraps around, and {@code to_datetime(ts, 5)} is NULL for a scale the
 * function does not render. An endpoint that folds to NULL, to a non-constant, or out of the declared
 * type means the image is not what the shape promised, so the mapping degrades to
 * {@link IntervalMapping#unknown()} rather than handing back a range the caller would prune with.
 */
final class EndpointPropagator implements IntervalPropagator {
    private final CallOperator template;
    private final int varIndex;
    private final Direction direction;
    private final boolean plateau;

    /**
     * @param template the call as it appears in the tree, cloned per endpoint with the variable child
     *                 replaced -- which is what keeps the function binding and the return type right,
     *                 since the constant folder looks its invoker up by the full signature
     */
    EndpointPropagator(CallOperator template, int varIndex, Direction direction, boolean plateau) {
        this.template = template;
        this.varIndex = varIndex;
        this.direction = direction;
        this.plateau = plateau;
    }

    /**
     * One end of the input interval, already carrying everything the image needs from it: where the
     * function sends it, whether the bound was strict, and whether the plateau it sits on reaches
     * past the interval on either side.
     * <p>
     * Collecting this per endpoint is what keeps the two coordinate systems apart. Everything here is
     * in INPUT space; orienting to output space is then a single swap of two of these, with no
     * second round of direction arithmetic.
     */
    private static final class Endpoint {
        final ConstantOperator mapped;
        final BoundType bound;
        /** Some value one step INTO the interval renders the same output. */
        final boolean collapsesInward;
        /** Some value one step OUT of the interval renders the same output. */
        final boolean collapsesOutward;

        Endpoint(ConstantOperator mapped, BoundType bound, boolean collapsesInward, boolean collapsesOutward) {
            this.mapped = mapped;
            this.bound = bound;
            this.collapsesInward = collapsesInward;
            this.collapsesOutward = collapsesOutward;
        }

        /** The image closes here when something outside the interval shares this output. */
        BoundType imageBound() {
            return collapsesInward ? BoundType.CLOSED : bound;
        }
    }

    @Override
    public IntervalMapping propagate(Range<ConstantOperator> in, List<ConstantOperator> args) {
        if (direction == Direction.UNKNOWN) {
            return IntervalMapping.unknown();
        }
        // The static question: no interval to map, only the shape is being asked about.
        if (in == null || !(in.hasLowerBound() || in.hasUpperBound())) {
            return IntervalMapping.shape(direction, plateau);
        }

        // "inward" is +1 from the bottom of the interval and -1 from the top
        Endpoint low = in.hasLowerBound() ? at(in.lowerEndpoint(), in.lowerBoundType(), 1, args) : null;
        Endpoint high = in.hasUpperBound() ? at(in.upperEndpoint(), in.upperBoundType(), -1, args) : null;
        if ((in.hasLowerBound() && low == null) || (in.hasUpperBound() && high == null)) {
            // partial, overflowing or unfoldable at an endpoint: the shape still holds but the image
            // does not, and a caller handed a wrong image prunes away rows
            return IntervalMapping.shape(direction, plateau);
        }

        Endpoint imageLow = direction == Direction.DECREASING ? high : low;
        Endpoint imageHigh = direction == Direction.DECREASING ? low : high;
        Range<ConstantOperator> out = build(imageLow, imageHigh);
        if (out == null) {
            return IntervalMapping.shape(direction, plateau);
        }
        // Collapsing on EITHER side is what stops f(a) OP f(c) implying a OP c, so the flag the
        // predicate-elimination caller reads is the union; the bound types above use only the inward
        // side, because only a value inside the interval can attain the image's endpoint.
        boolean collapsed = (low != null && (low.collapsesInward || low.collapsesOutward))
                || (high != null && (high.collapsesInward || high.collapsesOutward));
        return IntervalMapping.of(out, Exactness.EXACT, direction, collapsed);
    }

    /**
     * Maps one endpoint and, when the function can collapse at all, probes the neighbour on each side
     * to find out whether it collapses HERE. Null when the function does not produce a value there.
     */
    private Endpoint at(ConstantOperator value, BoundType bound, long inward, List<ConstantOperator> args) {
        ConstantOperator mapped = apply(value, args);
        if (mapped == null) {
            return null;
        }
        if (!plateau) {
            return new Endpoint(mapped, bound, false, false);
        }
        return new Endpoint(mapped, bound,
                sameOutput(mapped, value, inward, args), sameOutput(mapped, value, -inward, args));
    }

    /** Whether the value one step away in this direction renders the same output. */
    private boolean sameOutput(ConstantOperator here, ConstantOperator value, long delta,
                               List<ConstantOperator> args) {
        return AdjacentValues.step(value, delta)
                .map(neighbour -> apply(neighbour, args))
                .filter(here::equals)
                .isPresent();
    }

    private static Range<ConstantOperator> build(Endpoint low, Endpoint high) {
        try {
            if (low == null && high == null) {
                return Range.all();
            }
            if (low == null) {
                return Range.upTo(high.mapped, high.imageBound());
            }
            if (high == null) {
                return Range.downTo(low.mapped, low.imageBound());
            }
            if (low.mapped.compareTo(high.mapped) > 0) {
                return null;
            }
            return Range.range(low.mapped, low.imageBound(), high.mapped, high.imageBound());
        } catch (Exception e) {
            return null;
        }
    }




    private ConstantOperator apply(ConstantOperator value, List<ConstantOperator> args) {
        try {
            CallOperator call = (CallOperator) template.clone();
            for (int i = 0; i < call.getChildren().size(); i++) {
                call.setChild(i, i == varIndex ? value : args.get(i));
            }
            ScalarOperator folded = ScalarOperatorEvaluator.INSTANCE.evaluation(call);
            if (folded instanceof ConstantOperator result && !result.isNull()) {
                return result;
            }
        } catch (Exception e) {
            // an argument the function rejects is indistinguishable from one it maps to NULL
        }
        return null;
    }
}
