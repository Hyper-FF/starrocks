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

import java.util.List;

/**
 * How one function moves an interval of one of its arguments.
 * <p>
 * The variable argument's position is conveyed by {@code args}: it holds every argument in
 * declaration order with {@code null} at the variable's slot. That is what lets a single function
 * answer differently per position -- {@code datediff(a, b)} increases in a and decreases in b,
 * {@code date_trunc(unit, value)} carries its order in the second argument and not the first, and
 * {@code next_day(dt, dow)} carries none at all in its second.
 * <p>
 * An implementation that cannot describe a position returns {@link IntervalMapping#unknown()} for it.
 * Doing so costs pruning, never rows, which is the whole point of the default.
 */
@FunctionalInterface
public interface IntervalPropagator {
    /**
     * @param in   the interval the variable argument ranges over; {@link Range#all()} asks the static
     *             question "what is true over the whole domain" rather than for a concrete image
     * @param args every argument in declaration order, {@code null} at the variable argument's index
     */
    IntervalMapping propagate(Range<ConstantOperator> in, List<ConstantOperator> args);
}
