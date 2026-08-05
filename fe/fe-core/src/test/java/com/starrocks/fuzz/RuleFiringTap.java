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

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleType;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;

import java.util.BitSet;
import java.util.List;

/**
 * Records every rule the optimizer runs, in every phase, without touching product code.
 *
 * <p>Reading {@link com.starrocks.sql.optimizer.GroupExpression#getAppliedRuleMasks()} -- which is
 * what {@link RuleCoverage} does -- only sees the memo exploration phase. The optimizer runs three:
 *
 * <pre>
 *   logicalRuleRewrite (RBO, rewrites the OptExpression TREE)
 *        -> memoOptimize (the only phase that builds GroupExpressions)
 *        -> physicalRuleRewrite
 * </pre>
 *
 * {@code RewriteTreeTask} applies rules directly to the tree and never creates a GroupExpression, so
 * the mask-based signal cannot see predicate pushdown, partition pruning or aggregate splitting at
 * all. Measured consequence: those rules reported as "never fired" across 816 production queries,
 * which is impossible -- they run on nearly every query with a WHERE clause. The blind spot was in
 * the measurement, not the corpus.
 *
 * <p>{@link Rule#transform} is the one method every rule in every phase goes through, and
 * {@link Rule#type()} names it, so a single interception yields complete coverage. JMockit is
 * already a test dependency here (597 test files use it).
 *
 * <p>The tap DELEGATES: {@code invocation.proceed()} runs the real rule, so the plan produced under
 * measurement is the plan produced without it. A tap that changed the result would make every
 * finding suspect.
 */
public final class RuleFiringTap {

    private static final BitSet FIRED = new BitSet(RuleType.NUM_RULES.ordinal() + 1);
    private static final BitSet PRODUCED = new BitSet(RuleType.NUM_RULES.ordinal() + 1);
    private static volatile boolean installed;

    private RuleFiringTap() {
    }

    /** Idempotent: MockUp installs process-wide and re-installing would stack interceptors. */
    public static synchronized void install() {
        if (installed) {
            return;
        }
        new MockUp<Rule>() {
            @Mock
            public List<OptExpression> transform(Invocation invocation, OptExpression input,
                                                 OptimizerContext context) {
                Rule self = invocation.getInvokedInstance();
                List<OptExpression> out = invocation.proceed(input, context);
                int ordinal = self.type().ordinal();
                synchronized (FIRED) {
                    FIRED.set(ordinal);
                    // A rule that matched and returned nothing changed no plan. Tracking the two
                    // separately keeps "the optimizer considered this" apart from "this rewrote
                    // something", which are different claims about coverage.
                    if (out != null && !out.isEmpty()) {
                        PRODUCED.set(ordinal);
                    }
                }
                return out;
            }
        };
        installed = true;
    }

    public static synchronized void reset() {
        synchronized (FIRED) {
            FIRED.clear();
            PRODUCED.clear();
        }
    }

    /** Rules whose transform ran. */
    public static BitSet fired() {
        synchronized (FIRED) {
            return (BitSet) FIRED.clone();
        }
    }

    /** Rules whose transform returned at least one expression. */
    public static BitSet produced() {
        synchronized (FIRED) {
            return (BitSet) PRODUCED.clone();
        }
    }
}
