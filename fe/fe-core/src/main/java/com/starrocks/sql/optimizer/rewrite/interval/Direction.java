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

/**
 * Which way a function's result moves as one argument grows, holding the others fixed.
 * <p>
 * A boolean "is monotonic" cannot express this, and every consumer that carries a comparison operator
 * across an expression needs it: rewriting {@code col < c} into {@code f(col) < f(c)} is sound only
 * while f increases, and deduces a bound pointing the wrong way when it decreases.
 * <p>
 * {@link #UNKNOWN} is the default for anything undeclared. It is not "not monotonic" -- it is "no
 * claim was made" -- and consumers must decline to rewrite, which costs an optimization rather than
 * rows.
 */
public enum Direction {
    /** a &lt;= b implies f(a) &lt;= f(b). */
    INCREASING,
    /** a &lt;= b implies f(a) &gt;= f(b). */
    DECREASING,
    /** f ignores this argument, so every input maps to one output. */
    CONSTANT,
    /** No claim. Consumers must not rewrite across this function. */
    UNKNOWN;

    /**
     * Direction of {@code outer(inner(x))} given the direction of each. Composition is why a nested
     * expression such as {@code 100 - (100 - c1)} can still be recognized as increasing instead of
     * being refused because one node in the tree decreases.
     */
    public Direction compose(Direction inner) {
        if (this == UNKNOWN || inner == UNKNOWN) {
            return UNKNOWN;
        }
        if (this == CONSTANT || inner == CONSTANT) {
            return CONSTANT;
        }
        return this == inner ? INCREASING : DECREASING;
    }

    /** The direction seen from the other side of a comparison, used when an operator is flipped. */
    public Direction reverse() {
        switch (this) {
            case INCREASING:
                return DECREASING;
            case DECREASING:
                return INCREASING;
            default:
                return this;
        }
    }
}
