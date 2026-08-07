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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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

    /**
     * Rules the MEMO phase applied, by class name.
     *
     * <p>The other half of the optimizer, and the reason a run reports 115 of 265 rules and calls
     * 161 "never traced" with the join-reorder family at the top of the list. {@code ApplyRuleTask}
     * wraps its {@code transform} in the same tracer scope as the RBO phase, but names it
     * {@code rule.getClass().getSimpleName()} instead of {@code rule.toString()}, so probing
     * {@link RuleType} names -- which is all {@link #fired()} can do -- never matches one of them.
     * They were recorded all along and nobody was reading them.
     *
     * <p>Recovered by parsing {@code Tracers.printScopeTimer()}, because {@code Tracers} exposes
     * {@code getSpecifiedTimer(name)} and no way to enumerate. Parsing a human-readable format is
     * the weaker choice and it is taken deliberately: the alternative is an accessor in product
     * code, and this whole signal's value is that it needs no product change. {@link #parseFailed()}
     * makes the weakness loud -- see the validation in {@link #firedScopes}.
     *
     * <p>Class names, not RuleTypes. Mapping one to the other needs a rule INSTANCE and the memo
     * builds its own; as a coverage element the class name is exactly as good, since what matters
     * is that two runs agree on whether the same rule fired.
     */
    public static Set<String> memoRuleClasses() {
        Set<String> out = new LinkedHashSet<>();
        for (String scope : firedScopes()) {
            // StarRocks names every rule class *Rule, and the non-rule scopes in this module are
            // phase timers put there by StatementPlanner -- Analyzer, Transformer, Optimizer,
            // ExecPlanBuild, Lock. RuleType names are excluded because fired() already has them and
            // counting a rule under both namespaces would inflate the map with duplicates.
            if (scope.endsWith("Rule") && !RULE_TYPE_NAMES.contains(scope)) {
                out.add(scope);
            }
        }
        return out;
    }

    /**
     * Every scope name the tracer recorded, parsed out of the printed form.
     *
     * <p>{@code ScopedTimer.toString()} is {@code <indent>-- <name>[<count>] <time>} and
     * {@code printScopeTimer} prefixes each line with {@code <ms>ms|}, so the name is what sits
     * between {@code "-- "} and the last {@code '['}.
     */
    static Set<String> firedScopes() {
        Set<String> names = new LinkedHashSet<>();
        String printed;
        try {
            printed = Tracers.printScopeTimer();
        } catch (Throwable t) {
            parseFailures++;
            return names;
        }
        for (String line : printed.split("\n")) {
            int start = line.indexOf("-- ");
            if (start < 0) {
                continue;
            }
            int end = line.lastIndexOf('[');
            if (end <= start + 3) {
                continue;
            }
            names.add(line.substring(start + 3, end).trim());
        }
        // Self-check. The probe form is known good -- RuleTraceProbeTest pins it -- so every rule it
        // finds must also appear here. If the printed format ever changes, this is what says so;
        // without it a broken parser would report zero memo rules, which is indistinguishable from
        // an optimizer that ran none.
        for (int i = 0; i < NUM_RULES; i++) {
            String name = RULES[i].name();
            if (Tracers.getSpecifiedTimer(name).isPresent() && !names.contains(name)) {
                parseFailures++;
                break;
            }
        }
        return names;
    }

    /**
     * How many times the scope parse disagreed with the probe, across the process.
     *
     * <p>Reported rather than thrown: a measurement defect must not fail a mutant. But it must not
     * be silent either -- a zero here and a zero from {@link #memoRuleClasses()} mean opposite
     * things and only this number tells them apart.
     */
    public static int parseFailed() {
        return parseFailures;
    }

    private static int parseFailures;

    private static final Set<String> RULE_TYPE_NAMES = ruleTypeNames();

    private static Set<String> ruleTypeNames() {
        Set<String> names = new LinkedHashSet<>();
        for (RuleType t : RULES) {
            names.add(t.name());
        }
        return names;
    }

    /** Drops the recording. Optional -- {@link #arm()} replaces it -- but keeps the ThreadLocal tidy. */
    public static void disarm() {
        Tracers.close();
    }
}
