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

import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * M9 of SQL_AST_FUZZER_PLAN.md §2 — session flag perturbation.
 *
 * <p>Not a tree edit, so it does not implement {@link Mutation}: it changes the environment the same
 * tree is parsed and analyzed in. The driver flips a flag, runs the usual analyze / deparse / reparse
 * / reanalyze chain, and restores the flag afterwards.
 *
 * <p><b>Almost every session flag is inert against this oracle, by construction.</b> The round-trip
 * oracle compares SQL <i>text</i>. A flag that only changes inferred types, or only changes the plan,
 * produces identical text and so cannot be observed here no matter how much it changes the query.
 * Flipping one burns an iteration while looking like coverage.
 *
 * <p>The set below is therefore not the CBO list from the plan, and not even the list of flags the
 * analyzer reads. It is the list that was <b>measured</b> to change the deparsed analyzed tree:
 * <ul>
 *   <li>{@code enable_groupby_use_output_alias} — decides whether GROUP BY binds to a select-list
 *       alias or to a table column, so it changes name resolution and can flip a statement between
 *       accepted and rejected.</li>
 *   <li>{@code sql_mode} — parser level, so it changes the tree outright: with PIPES_AS_CONCAT,
 *       {@code 'a' || 'b'} parses as {@code concat}; without it, as {@code OR}.</li>
 * </ul>
 *
 * <p>Measured and rejected, so they are not silently re-added: {@code sql_select_limit} (applied in
 * RelationTransformer, i.e. the optimizer, not the analyzer), {@code enable_strict_type},
 * {@code decimal_overflow_to_double}, {@code large_in_predicate_threshold} (type- or plan-only, and
 * types are not printed), {@code enable_strict_order_by} (no observable text difference in either
 * direction). {@code count_distinct_implementation} is unusable for a different reason: its getter
 * returns an enum while its setter takes a String, so the previous value cannot be handed back.
 *
 * <p>These become worthwhile once the plan-differential oracle (O4) exists, because a plan is exactly
 * what they do change. Add them here then, gated on the oracle in use.
 *
 * <h3>That condition is now met for plan-visible flags</h3>
 *
 * <p>Every round-tripped mutant is planned, and {@link PlanShape#elements} reads the plan the
 * optimizer chose. A flag that changes only the plan is no longer invisible: it moves {@code OP:}
 * and {@code EDGE:} elements in the coverage map even when the deparsed text is byte-identical.
 *
 * <p>The low-cardinality family is added on exactly that basis, and it is the family to add first.
 * The global dictionary gates a whole subsystem -- dictionary-encoded scans, the rewrites these
 * flags name, and the dictionary MERGE that runs when a UNION ALL puts two dictionary columns in one
 * slot, which is where a confirmed BE crash lives. OFF is the informative direction: every one of
 * them defaults to ON, so the unexplored plans are the ones without the rewrite.
 *
 * <p><b>They do not reach the text oracle and are not meant to.</b> Judge them by whether the
 * coverage map moves, not by round-trip findings. And note the limit of what the FE arm can do here:
 * it has no data, so no dictionary ever materialises: what these reach is the FE-side rewrite
 * gating. The crash behind it needs the cluster arm, real rows, and a column the data generator kept
 * narrow.
 */
public final class SessionFlagPerturbation {

    /** One flag, the values worth trying, and how to read and write it. */
    private static final class Knob {
        final String name;
        final List<Object> values;
        final Function<SessionVariable, Object> getter;
        final BiConsumer<SessionVariable, Object> setter;

        Knob(String name, Function<SessionVariable, Object> getter,
                BiConsumer<SessionVariable, Object> setter, Object... values) {
            this.name = name;
            this.getter = getter;
            this.setter = setter;
            this.values = Arrays.asList(values);
        }
    }

    private static final List<Knob> KNOBS = buildKnobs();

    private static List<Knob> buildKnobs() {
        List<Knob> knobs = new ArrayList<>();
        knobs.add(new Knob("enable_groupby_use_output_alias",
                SessionVariable::getEnableGroupbyUseOutputAlias,
                (sv, v) -> sv.setEnableGroupbyUseOutputAlias((Boolean) v),
                true, false));
        knobs.add(new Knob("sql_mode",
                SessionVariable::getSqlMode,
                (sv, v) -> sv.setSqlMode((Long) v),
                SqlModeHelper.MODE_DEFAULT,
                SqlModeHelper.MODE_DEFAULT | SqlModeHelper.MODE_PIPES_AS_CONCAT,
                SqlModeHelper.MODE_DEFAULT | SqlModeHelper.MODE_ONLY_FULL_GROUP_BY,
                SqlModeHelper.MODE_DEFAULT | SqlModeHelper.MODE_FORBID_INVALID_DATE,
                SqlModeHelper.MODE_DEFAULT | SqlModeHelper.MODE_DOUBLE_LITERAL,
                SqlModeHelper.MODE_DEFAULT | SqlModeHelper.MODE_ALLOW_THROW_EXCEPTION));

        // The low-cardinality family. Plan-only, so inert against the text oracle and deliberately
        // included anyway -- see the class comment. Only false is offered as the second value
        // because true is the product default and re-applying it perturbs nothing.
        knobs.add(new Knob("cbo_enable_low_cardinality_optimize",
                SessionVariable::isEnableLowCardinalityOptimize,
                (sv, v) -> sv.setEnableLowCardinalityOptimize((Boolean) v),
                false, true));
        // cbo_enable_low_cardinality_optimize_for_join is deliberately absent: SessionVariable has
        // isEnableLowCardinalityOptimizeForJoin() and no matching setter, so the previous value
        // cannot be handed back and a perturbation would leak into every later mutant. Same reason
        // count_distinct_implementation is excluded above. The cluster arm reaches it instead --
        // there it is a `set` statement, not a Java accessor.
        // The one that gates the UNION ALL dictionary merge, which is the path a confirmed BE crash
        // sits on -- a partial merge across branches feeding VARCHAR into an INT slot.
        knobs.add(new Knob("enable_low_cardinality_optimize_for_union_all",
                SessionVariable::isEnableLowCardinalityOptimizeForUnionAll,
                (sv, v) -> sv.setEnableLowCardinalityOptimizeForUnionAll((Boolean) v),
                false, true));
        knobs.add(new Knob("low_cardinality_optimize_v2",
                SessionVariable::isUseLowCardinalityOptimizeV2,
                (sv, v) -> sv.setUseLowCardinalityOptimizeV2((Boolean) v),
                false, true));
        knobs.add(new Knob("array_low_cardinality_optimize",
                SessionVariable::isEnableArrayLowCardinalityOptimize,
                (sv, v) -> sv.setEnableArrayLowCardinalityOptimize((Boolean) v),
                false, true));
        knobs.add(new Knob("struct_low_cardinality_optimize",
                SessionVariable::isEnableStructLowCardinalityOptimize,
                (sv, v) -> sv.setEnableStructLowCardinalityOptimize((Boolean) v),
                false, true));
        return knobs;
    }

    /** A flag set to a value, with the previous value captured so it can be put back. */
    public static final class Perturbation implements AutoCloseable {
        private final SessionVariable sv;
        private final Knob knob;
        private final Object previous;
        private final Object applied;

        private Perturbation(SessionVariable sv, Knob knob, Object previous, Object applied) {
            this.sv = sv;
            this.knob = knob;
            this.previous = previous;
            this.applied = applied;
        }

        /** Description for the report, e.g. {@code "M9-session: sql_mode=34"}. */
        public String description() {
            return "M9-session: " + knob.name + "=" + applied;
        }

        @Override
        public void close() {
            knob.setter.accept(sv, previous);
        }
    }

    private SessionFlagPerturbation() {
    }

    /**
     * Picks one flag and sets it. Always restore via {@link Perturbation#close()} -- the session
     * variable is shared with every later seed, so a leaked flag silently reinterprets the rest of the
     * run and every finding after that point is untrustworthy.
     *
     * @return the applied perturbation, or null when the chosen value already equals the current one
     */
    public static Perturbation apply(SessionVariable sv, Random rnd) {
        Knob knob = KNOBS.get(rnd.nextInt(KNOBS.size()));
        Object previous = knob.getter.apply(sv);
        Object next = knob.values.get(rnd.nextInt(knob.values.size()));
        if (next.equals(previous)) {
            return null;
        }
        knob.setter.accept(sv, next);
        return new Perturbation(sv, knob, previous, next);
    }

    /** Number of flags available, so a test can assert the set did not silently shrink. */
    public static int knobCount() {
        return KNOBS.size();
    }
}
