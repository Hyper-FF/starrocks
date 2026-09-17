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

import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.Type;

import java.util.Optional;

/**
 * The value one step from a constant, for deciding whether a function collapses AT a given endpoint
 * rather than merely somewhere.
 * <p>
 * The difference matters: from_unixtime_ms() divides by 1000, so it collapses a thousand inputs onto
 * each output -- but at an endpoint that lands on a whole second the value one below renders the
 * previous second, so nothing outside the interval shares that output and a strict bound stays
 * strict. Treating the function's ability to collapse as if it collapsed everywhere costs pruning at
 * every endpoint that happens to be aligned.
 */
final class AdjacentValues {
    private AdjacentValues() {
    }

    /** Empty when there is no next value to name: a non-integer constant, or one at its type's edge. */
    static Optional<ConstantOperator> step(ConstantOperator constant, long delta) {
        if (constant == null || constant.isNull()) {
            return Optional.empty();
        }
        Type type = constant.getType();
        try {
            if (type.isTinyint()) {
                long value = Math.addExact(constant.getTinyInt(), delta);
                return value < Byte.MIN_VALUE || value > Byte.MAX_VALUE
                        ? Optional.empty() : Optional.of(ConstantOperator.createTinyInt((byte) value));
            } else if (type.isSmallint()) {
                long value = Math.addExact(constant.getSmallint(), delta);
                return value < Short.MIN_VALUE || value > Short.MAX_VALUE
                        ? Optional.empty() : Optional.of(ConstantOperator.createSmallInt((short) value));
            } else if (type.isInt()) {
                long value = Math.addExact(constant.getInt(), delta);
                return value < Integer.MIN_VALUE || value > Integer.MAX_VALUE
                        ? Optional.empty() : Optional.of(ConstantOperator.createInt((int) value));
            } else if (type.isBigint()) {
                return Optional.of(ConstantOperator.createBigint(Math.addExact(constant.getBigint(), delta)));
            }
        } catch (ArithmeticException e) {
            return Optional.empty();
        }
        return Optional.empty();
    }
}
