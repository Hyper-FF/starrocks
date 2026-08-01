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

package com.starrocks.sql.optimizer.rule.tree.lowcardinality;

import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.ArrayType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/*
 * stringRefToDefineExprMap is a graph, not a tree, and nothing in the collector guarantees it is acyclic:
 * the merge stage of a split aggregation records `k -> array_agg(k)`, and CTE / union / table-function
 * mappings chain column refs onto one another. checkDependOnExpr used to guard only against a define
 * expression that directly names the column being resolved, and only in its generic branch, so any cycle
 * reaching it through the array_agg / struct / subfield branches - or any cycle longer than one hop -
 * recursed until the FE threw StackOverflowError. The error was swallowed further up and only ever
 * appeared in fe.warn.log.
 */
public class DecodeCollectorCyclicDefineTest {

    private static final Type ARRAY_VARCHAR = new ArrayType(VarcharType.VARCHAR);

    private static ColumnRefOperator ref(int id, Type type) {
        return new ColumnRefOperator(id, type, "c" + id, true);
    }

    private static CallOperator arrayAgg(ScalarOperator arg) {
        return new CallOperator(FunctionSet.ARRAY_AGG, ARRAY_VARCHAR, List.of(arg));
    }

    private static OptExpression trivialPlan() {
        return OptExpression.create(
                new PhysicalValuesOperator(Lists.newArrayList(), Lists.newArrayList(), -1, null, null));
    }

    /*
     * Runs the collector on a thread with a small stack so a runaway recursion fails fast and cannot take
     * the whole surefire JVM with it. Returns whatever the collector threw, or null when it terminated.
     */
    private static Throwable runCollect(Consumer<DecodeCollector> seed) throws InterruptedException {
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Runnable body = () -> {
            try {
                SessionVariable session = new SessionVariable();
                session.setEnableStructLowCardinalityOptimize(true);
                DecodeCollector collector = new DecodeCollector(session, true);
                seed.accept(collector);
                collector.collect(trivialPlan(), new DecodeContext(new ColumnRefFactory()));
            } catch (Throwable t) {
                thrown.set(t);
            }
        };
        Thread thread = new Thread(null, body, "decode-collector-cyclic-define", 1 << 18);
        thread.start();
        thread.join(60_000);
        Assertions.assertFalse(thread.isAlive(), "collector did not terminate");
        return thrown.get();
    }

    private static void assertTerminates(String what, Consumer<DecodeCollector> seed) throws InterruptedException {
        Throwable t = runCollect(seed);
        if (t instanceof StackOverflowError) {
            Assertions.fail("checkDependOnExpr recursed until the stack died on " + what
                    + "; the define graph must be walked with a cycle guard, StackOverflowError: " + t);
        }
        Assertions.assertNull(t, "unexpected failure on " + what + ": " + t);
    }

    /*
     * `k -> array_agg(k)`: exactly what the merge stage of a split array_agg records when the local stage
     * did not register a define for the same key first.
     */
    @Test
    public void testSelfCycleThroughArrayAgg() throws InterruptedException {
        assertTerminates("a self-referencing array_agg define", collector -> {
            ColumnRefOperator k = ref(7, ARRAY_VARCHAR);
            collector.setDefineExpr(k, arrayAgg(k), 1);
        });
    }

    /*
     * Two array_agg defines pointing at each other. The old direct-self-reference guard could not see this
     * one even in principle: neither define names the column it defines.
     */
    @Test
    public void testTwoHopCycleThroughArrayAgg() throws InterruptedException {
        assertTerminates("a two-hop array_agg define cycle", collector -> {
            ColumnRefOperator a = ref(7, ARRAY_VARCHAR);
            ColumnRefOperator b = ref(8, ARRAY_VARCHAR);
            collector.setDefineExpr(a, arrayAgg(b), 1);
            collector.setDefineExpr(b, arrayAgg(a), 1);
        });
    }

    /*
     * A cycle that alternates between the array_agg branch and the generic column-ref branch, i.e. the
     * shape a projection over a re-aggregated column produces.
     */
    @Test
    public void testMixedCycleThroughArrayAggAndProjection() throws InterruptedException {
        assertTerminates("an array_agg/projection define cycle", collector -> {
            ColumnRefOperator a = ref(7, ARRAY_VARCHAR);
            ColumnRefOperator b = ref(8, ARRAY_VARCHAR);
            collector.setDefineExpr(a, arrayAgg(b), 1);
            collector.setDefineExpr(b, new CallOperator(FunctionSet.UPPER, ARRAY_VARCHAR, List.of(a)), 1);
        });
    }

    /*
     * A longer cycle made only of plain column-ref defines - the union / CTE-consume mapping shape.
     */
    @Test
    public void testThreeHopCycleThroughColumnRefDefines() throws InterruptedException {
        assertTerminates("a three-hop column-ref define cycle", collector -> {
            ColumnRefOperator a = ref(7, VarcharType.VARCHAR);
            ColumnRefOperator b = ref(8, VarcharType.VARCHAR);
            ColumnRefOperator c = ref(9, VarcharType.VARCHAR);
            collector.setDefineExpr(a, b, 1);
            collector.setDefineExpr(b, c, 1);
            collector.setDefineExpr(c, a, 1);
        });
    }

    /*
     * An acyclic chain of the same shape must still resolve, so the guard cannot be a depth bound that
     * gives up on legal input. Nothing here is a known string column, so nothing is registered, but the
     * walk has to finish on its own terms.
     */
    @Test
    public void testAcyclicChainStillResolves() throws InterruptedException {
        Throwable t = runCollect(collector -> {
            // c100 has no define of its own, so the chain bottoms out there
            ColumnRefOperator prev = ref(100, ARRAY_VARCHAR);
            for (int i = 101; i < 200; i++) {
                ColumnRefOperator cur = ref(i, ARRAY_VARCHAR);
                collector.setDefineExpr(cur, arrayAgg(prev), 1);
                prev = cur;
            }
        });
        Assertions.assertNull(t, "a long acyclic define chain must resolve: " + t);
    }
}
