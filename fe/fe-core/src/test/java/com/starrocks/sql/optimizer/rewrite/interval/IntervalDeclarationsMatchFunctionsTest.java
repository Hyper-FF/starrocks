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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Function;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorEvaluator;
import com.starrocks.type.Type;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks the declarations against the functions they describe, rather than against a query plan.
 * <p>
 * The table in IntervalPropagators is the part of this design most likely to be wrong and least
 * likely to be covered: a plan test only exercises the handful of functions someone happened to
 * partition a table by, so a declaration can be wrong for years without a single test going red. It
 * happened while this was being written -- from_unixtime() was declared strictly increasing, which
 * holds for a full-precision format and fails for '%Y-%m-%d', a format the validation accepts. No
 * behaviour test could have caught it; only asking the function itself does.
 * <p>
 * The claim checked here is the one whose failure loses rows: a declaration of "cannot collapse" is a
 * promise of injectivity, and a false promise keeps a strict bound that prunes away the partition
 * holding the matching rows. The direction claim is checked alongside it.
 */
public class IntervalDeclarationsMatchFunctionsTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
    }

    /** A handful of consecutive values, so a collapse between neighbours cannot hide. */
    private static List<ConstantOperator> samples(Type type) {
        List<ConstantOperator> values = new ArrayList<>();
        if (type.isBigint()) {
            for (long v : new long[] {1609689600L, 1609689601L, 1609689602L, 1609689660L, 1609776000L}) {
                values.add(ConstantOperator.createBigint(v));
            }
        } else if (type.isInt()) {
            for (int v : new int[] {1, 2, 3, 10, 100}) {
                values.add(ConstantOperator.createInt(v));
            }
        } else if (type.isDatetime()) {
            for (LocalDateTime t : new LocalDateTime[] {
                    LocalDateTime.of(2021, 1, 4, 0, 0, 0),
                    LocalDateTime.of(2021, 1, 4, 0, 0, 1),
                    LocalDateTime.of(2021, 1, 4, 0, 1, 0),
                    LocalDateTime.of(2021, 1, 4, 5, 0, 0),
                    LocalDateTime.of(2021, 1, 5, 0, 0, 0)}) {
                values.add(ConstantOperator.createDatetime(t));
            }
        } else if (type.isDate()) {
            for (LocalDateTime t : new LocalDateTime[] {
                    LocalDateTime.of(2021, 1, 4, 0, 0),
                    LocalDateTime.of(2021, 1, 5, 0, 0),
                    LocalDateTime.of(2021, 2, 4, 0, 0),
                    LocalDateTime.of(2022, 1, 4, 0, 0)}) {
                values.add(ConstantOperator.createDate(t));
            }
        }
        return values;
    }

    /** A value the other arguments can be held at, chosen so the call folds rather than erroring. */
    private static ConstantOperator hold(Type type) {
        if (type.isInt()) {
            return ConstantOperator.createInt(1);
        } else if (type.isBigint()) {
            return ConstantOperator.createBigint(1609689600L);
        } else if (type.isDatetime()) {
            return ConstantOperator.createDatetime(LocalDateTime.of(2021, 1, 4, 0, 0));
        } else if (type.isDate()) {
            return ConstantOperator.createDate(LocalDateTime.of(2021, 1, 4, 0, 0));
        }
        return null;
    }

    /** Every builtin overload registered under this name. */
    private static List<Function> builtinsNamed(String name) {
        List<Function> found = new ArrayList<>();
        for (Function fn : GlobalStateMgr.getCurrentState().getBuiltinFunctions()) {
            if (fn.functionName().equalsIgnoreCase(name)) {
                found.add(fn);
            }
        }
        return found;
    }

    private static ConstantOperator fold(Function fn, List<ConstantOperator> args) {
        try {
            CallOperator call = new CallOperator(fn.functionName(), fn.getReturnType(),
                    ImmutableList.<ScalarOperator>copyOf(args), fn);
            ScalarOperator folded = ScalarOperatorEvaluator.INSTANCE.evaluation(call);
            return folded instanceof ConstantOperator result && !result.isNull() ? result : null;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * For every described position that promises injectivity, evaluate the function over consecutive
     * inputs and assert that no two of them share an output, and that the outputs move the declared
     * way. A position holding a varchar -- a format, a unit, a day-of-week name -- carries no claim
     * and is skipped, since there is no meaningful value to sweep it over.
     */
    @Test
    public void testDeclarationsHoldForTheFunctionsTheyDescribe() {
        List<String> problems = new ArrayList<>();
        // A check that silently stops checking passes just as quietly as one that holds, and this one
        // has three ways to go quiet: no builtin found under a name, no sample for an argument type,
        // no other argument that can be held fixed. Counting the comparisons actually made is what
        // keeps "green" meaning something.
        int[] comparisons = new int[1];
        for (String name : IntervalPropagators.describedNames()) {
            for (Function fn : builtinsNamed(name)) {
                Type[] argTypes = fn.getArgs();
                for (int i = 0; i < argTypes.length; i++) {
                    Direction declared = IntervalPropagators.declaredDirection(name, i);
                    if (declared != Direction.INCREASING && declared != Direction.DECREASING) {
                        continue;
                    }
                    List<ConstantOperator> sweep = samples(argTypes[i]);
                    if (sweep.isEmpty()) {
                        continue;
                    }
                    List<ConstantOperator> args = new ArrayList<>();
                    boolean holdable = true;
                    for (int j = 0; j < argTypes.length; j++) {
                        if (j == i) {
                            args.add(null);
                            continue;
                        }
                        ConstantOperator held = hold(argTypes[j]);
                        if (held == null) {
                            holdable = false;
                            break;
                        }
                        args.add(held);
                    }
                    if (!holdable) {
                        continue;
                    }
                    check(fn, name, i, declared, sweep, args, problems, comparisons);
                }
            }
        }
        assertTrue(problems.isEmpty(), "declarations disagree with the functions:\n" + String.join("\n", problems));
        assertTrue(comparisons[0] >= 100,
                "only " + comparisons[0] + " declarations were actually exercised -- the sweep has gone "
                        + "quiet and this test is no longer checking the table");
    }

    private static void check(Function fn, String name, int index, Direction declared,
                              List<ConstantOperator> sweep, List<ConstantOperator> args,
                              List<String> problems, int[] comparisons) {
        ConstantOperator previous = null;
        ConstantOperator previousInput = null;
        for (ConstantOperator input : sweep) {
            List<ConstantOperator> call = new ArrayList<>(args);
            call.set(index, input);
            ConstantOperator out = fold(fn, call);
            if (out == null) {
                continue;
            }
            if (previous != null) {
                comparisons[0]++;
                int moved = out.compareTo(previous);
                if (declared == Direction.INCREASING && moved < 0) {
                    problems.add(name + " arg " + index + " declared INCREASING but " + previousInput
                            + " -> " + previous + " and " + input + " -> " + out);
                }
                if (declared == Direction.DECREASING && moved > 0) {
                    problems.add(name + " arg " + index + " declared DECREASING but " + previousInput
                            + " -> " + previous + " and " + input + " -> " + out);
                }
                if (moved == 0 && !IntervalPropagators.declaredMayCollapse(name, index)) {
                    problems.add(name + " arg " + index + " promises injectivity but " + previousInput
                            + " and " + input + " both render " + out);
                }
            }
            previous = out;
            previousInput = input;
        }
    }
}
