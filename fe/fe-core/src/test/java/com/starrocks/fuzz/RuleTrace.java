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

import com.starrocks.common.profile.Tracers;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Which rules the RBO phase actually applied to one query, read out of the product's own tracer.
 *
 * <p>This closes the hole {@link RuleCoverage} leaves. The mask signal reads
 * {@code GroupExpression#getAppliedRuleMasks()}, and only the memo phase ever builds a
 * GroupExpression, so predicate pushdown, partition pruning and aggregate splitting -- all of which
 * live in {@code logicalRuleRewrite} -- are invisible to it.
 *
 * <p>{@link RuleFiringTap} was written to cover that phase and does not: it intercepts
 * {@code RewriteTreeTask#applyRules} and sets a bit for every rule in the {@code rules} argument,
 * but the product only reaches {@code transform} after four guards (rule disabled, rule exhausted,
 * pattern match, {@code Rule#check} -- {@code RewriteTreeTask.java:100-108}). So the tap reports
 * rules CONSIDERED, which is the phase's static rule list and therefore very nearly the same set for
 * every query. As a coverage signal it is a constant; as a feedback key it carries nothing.
 *
 * <p>The correct hook was already in the product, one line inside those guards:
 *
 * <pre>
 *   RewriteTreeTask.java:117  try (Timer ignore = Tracers.watchScope(Module.OPTIMIZER, rule.toString()))
 * </pre>
 *
 * and {@link com.starrocks.sql.optimizer.rule.Rule#toString()} returns {@code type().name()}, so the
 * scope name IS the {@link RuleType} name. Arm the tracer for the OPTIMIZER module and every rule
 * that genuinely transformed leaves a named scope behind. No instrumentation, no JMockit, no agent.
 *
 * <p>Scope: the RBO phase. {@code ApplyRuleTask} (the memo phase) wraps the same call with
 * {@code rule.getClass().getSimpleName()} instead, which is a different namespace and does not map
 * back to RuleType. That phase is what the masks already measure correctly, so the union of
 * {@link RuleCoverage} and this class is the real per-query rule vector, and neither half is
 * redundant.
 *
 * <p>Cost: one ScopedTimer allocation per fired rule, plus {@link RuleType#values()} hash lookups on
 * read. Nothing re-runs the optimizer -- unlike {@code RuleCoverage.collect}, which optimizes a
 * second time. The tracer is a ThreadLocal and {@code StatementPlanner} never calls
 * {@code Tracers.init} or {@code Tracers.register} itself, so the mask armed here survives the whole
 * plan.
 */
public final class RuleTrace {

    private static final RuleType[] RULES = RuleType.values();
    private static final int NUM_RULES = RuleType.NUM_RULES.ordinal();

    private RuleTrace() {
    }

    /**
     * Starts a fresh recording. Call once per query, BEFORE planning it.
     *
     * <p>{@code register()} installs a new TracerImpl with its own TimeWatcher, which is what makes
     * the reading per-query rather than cumulative. {@code init} then enables TIMER mode for the
     * OPTIMIZER module; without both, {@code watchScope} hands back the shared empty timer and every
     * query reads back as zero rules -- a silent zero, indistinguishable from a corpus that fires
     * nothing.
     */
    public static void arm() {
        Tracers.register();
        Tracers.init(Tracers.Mode.TIMER, Tracers.Module.OPTIMIZER, false, false);
    }

    /** Whether the tracer is actually armed, so a caller can fail loudly instead of reading zeros. */
    public static boolean isArmed() {
        return Tracers.isSetTraceMode(Tracers.Mode.TIMER)
                && Tracers.isSetTraceModule(Tracers.Module.OPTIMIZER);
    }

    /**
     * Rules the RBO phase applied since the last {@link #arm()}, indexed by {@link RuleType} ordinal
     * so it ORs directly with a {@link RuleCoverage} vector.
     */
    public static BitSet fired() {
        BitSet fired = new BitSet(NUM_RULES + 1);
        for (int i = 0; i < NUM_RULES; i++) {
            if (Tracers.getSpecifiedTimer(RULES[i].name()).isPresent()) {
                fired.set(i);
            }
        }
        return fired;
    }

    /** The same set as names, for reporting and for use as coverage-map element keys. */
    public static List<String> firedNames() {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < NUM_RULES; i++) {
            String name = RULES[i].name();
            if (Tracers.getSpecifiedTimer(name).isPresent()) {
                names.add(name);
            }
        }
        return names;
    }

    /** Drops the recording. Optional -- {@link #arm()} replaces it -- but keeps the ThreadLocal tidy. */
    public static void disarm() {
        Tracers.close();
    }
}
