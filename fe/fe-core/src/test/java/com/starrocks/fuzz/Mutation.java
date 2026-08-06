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

package com.starrocks.fuzz;

import com.starrocks.sql.ast.QueryStatement;

import java.util.Collections;
import java.util.Random;
import java.util.Set;

/**
 * One mutation operator, as enumerated in SQL_AST_FUZZER_PLAN.md §2.
 *
 * <p>An operator edits the tree <b>in place</b> and returns a human-readable description of what it
 * did, or {@code null} when it did not apply to this statement. The description is not decoration: a
 * mutant that cannot be deparsed leaves the report with no SQL to show, and a finding nobody can
 * reproduce is worth nothing, so the description has to be enough to rebuild the mutant from the seed.
 *
 * <p>Contract every operator must honour:
 * <ul>
 *   <li><b>The tree is unanalyzed.</b> Node types are populated but {@code getType()} is not, so decide
 *       what to inject from the node class, never from its type.</li>
 *   <li><b>Never share an Expr between two trees.</b> Injected fragments are kept as text and reparsed
 *       at injection time. Reusing a node from the pool aliases it into two statements and the second
 *       analysis corrupts the first.</li>
 *   <li><b>Returning null is normal</b> and cheap. Prefer it over forcing a mutation that does not fit;
 *       the driver simply moves on to the next attempt.</li>
 *   <li><b>Do not worry about producing an invalid tree.</b> Every mutant is serialized and reparsed
 *       before it is evaluated ({@code AstMutationFuzzerTest#reparseThroughGrammar}), so a tree the
 *       grammar cannot express is dropped rather than reported. Do worry about producing a tree that
 *       deparses to something the parser accepts but that means something different -- that is a false
 *       finding, and it is the failure mode to design against.</li>
 * </ul>
 */
public interface Mutation {

    /** Short stable id used in the report, e.g. {@code "M5-clause"}. */
    String name();

    /**
     * Applies the operator to {@code stmt} in place.
     *
     * @return a description of the edit, or null when the operator does not apply
     */
    String apply(QueryStatement stmt, AstMutationFuzzerTest.Pool pool, Random rnd);

    /**
     * The {@link SqlFeatures} elements this operator can MANUFACTURE, for coverage steering.
     *
     * <p>Declared per operator rather than in one table beside the scheduler, because the answer
     * changes whenever an operator gains a shape -- and at that moment only the operator's own file
     * is open. A central table would be correct on the day it was written and quietly wrong after.
     *
     * <p>Read as a capability claim, not a guarantee: an operator that CAN produce {@code F:cte}
     * still returns null on a tree with nowhere to put one. Steering raises an operator's draw
     * order; whether the edit lands is still decided by the tree.
     *
     * <p>Only claim what the operator really creates. Claiming an element it cannot produce makes
     * steering chase a target it can never reach, which is worse than not steering: the weight is
     * spent every round and the deficit never closes.
     *
     * @return element keys spelled as {@link SqlFeatures} emits them, or empty for an operator that
     *     edits expressions only and reaches no structural feature
     */
    default Set<String> coverageTargets() {
        return Collections.emptySet();
    }
}
