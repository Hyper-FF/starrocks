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

import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleSet;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.CombinationRule;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Maps a rule's class name back to its {@link RuleType}, so the two phases can be counted together.
 *
 * <p>The optimizer names its tracer scopes differently in each phase: {@code rule.toString()}, which
 * is the RuleType name, in the RBO rewrite, and {@code rule.getClass().getSimpleName()} in the memo.
 * Without a bridge the two are separate vocabularies, and the consequence is not cosmetic -- a rule
 * that runs only in the memo shows up in "never traced" forever, so the blind-spot list is padded
 * with rules that fire on nearly every query. The join reorder family sat at the top of that list
 * for exactly this reason.
 *
 * <p>The mapping only exists at runtime: a Rule carries its type as instance state, set by its
 * constructor, so nothing static can be read off the class. This walks the product's own rule sets
 * for instances rather than guessing from names -- a name-based mapping
 * ({@code JoinCommutativityRule} → {@code TF_JOIN_COMMUTATIVITY}) is right often enough to be
 * dangerous and wrong silently.
 *
 * <p>Reflection, because {@code RuleSet} keeps its lists private and exposes no accessor. The
 * alternative is adding one to product code, which this whole signal exists to avoid. It is
 * contained: two field reads and a walk over public static fields, all inside a test.
 *
 * <p><b>Not necessarily complete.</b> Rules reachable only through paths this walk does not cover
 * simply stay unmapped, and an unmapped class name is reported as itself rather than silently
 * dropped -- an incomplete index must not look like a complete one.
 */
public final class RuleTypeIndex {

    private static final Map<String, Set<RuleType>> BY_CLASS_NAME = build();

    /** Every RuleType carried by an instance the walk reached. */
    private static final Set<RuleType> REGISTERED = collectRegistered();

    private RuleTypeIndex() {
    }

    /**
     * The RuleType for a class name, but ONLY when the class carries exactly one.
     *
     * <p>Some classes carry several: {@code JoinAssociativityRule} has an INNER and an OUTER static
     * instance with different types, built from the same class. The memo names its scopes by class,
     * so when one of those fires the trace cannot say which type it was -- and picking either would
     * be a silent fabrication in a number whose whole purpose is to be trustworthy. The first
     * version of this index did exactly that, by overwriting: last instance written wins, and the
     * other type reads as never traced forever.
     *
     * @return the type when unambiguous, null when the class is unknown OR shared
     */
    public static RuleType typeOf(String classSimpleName) {
        Set<RuleType> types = BY_CLASS_NAME.get(classSimpleName);
        return types != null && types.size() == 1 ? types.iterator().next() : null;
    }

    /** Every type a class name may stand for, empty when the walk never reached it. */
    public static Set<RuleType> typesOf(String classSimpleName) {
        Set<RuleType> types = BY_CLASS_NAME.get(classSimpleName);
        return types == null ? java.util.Collections.emptySet() : types;
    }

    /**
     * Whether any rule set the walk covered actually holds an instance of this type.
     *
     * <p>The ones that answer false are the interesting ones. {@code TF_MULTI_JOIN_ORDER} is carried
     * by {@code ReorderJoinRule}, which {@code QueryOptimizer} builds with {@code new} and calls
     * directly rather than registering -- so it runs through neither {@code RewriteTreeTask} nor
     * {@code ApplyRuleTask}, the only two places a tracer scope is opened. It can fire on every
     * query and still read as never traced, which is what it did while two rounds of work went into
     * generating multi-way joins to "reach" it.
     *
     * <p>Reported separately for that reason: a blind spot that no corpus can close is not a gap,
     * and listing it beside real gaps sends the next person down the same road.
     */
    public static boolean isRegistered(RuleType type) {
        return REGISTERED.contains(type);
    }

    /** How many distinct rule classes the index resolved, for reporting how complete it is. */
    public static int size() {
        return BY_CLASS_NAME.size();
    }

    /**
     * Translates traced memo class names into RuleType names, keeping the untranslatable ones.
     *
     * <p>An unmapped name is passed through unchanged. It is still a real rule that really fired --
     * dropping it would understate coverage to make a report tidier.
     */
    public static Set<String> toRuleTypeNames(Collection<String> classNames) {
        Set<String> out = new LinkedHashSet<>();
        for (String cls : classNames) {
            RuleType t = typeOf(cls);
            out.add(t == null ? cls : t.name());
        }
        return out;
    }

    private static Set<RuleType> collectRegistered() {
        Set<RuleType> all = new LinkedHashSet<>();
        BY_CLASS_NAME.values().forEach(all::addAll);
        return all;
    }

    private static Map<String, Set<RuleType>> build() {
        Map<String, Set<RuleType>> map = new HashMap<>();
        Deque<Rule> queue = new ArrayDeque<>();
        Set<Rule> seen = java.util.Collections.newSetFromMap(new IdentityHashMap<>());

        try {
            RuleSet ruleSet = new RuleSet();
            // The opt-in registrations. The constructor adds the rules every query needs; the join
            // families are added later by the optimizer, per query, depending on what the plan
            // contains -- addJoinTransformationRules, addHashJoinImplementationRule and their
            // siblings. Skipping them left exactly seven classes unresolved, and they were the ones
            // that mattered: JoinCommutativityRule, JoinAssociativityRule, OuterJoinEliminationRule
            // and the join implementations, i.e. the whole reorder family that had been sitting at
            // the top of the never-traced list while firing on most queries with a join.
            //
            // Called by name and individually caught: this list is the product's, not ours, and a
            // renamed method should cost one family's worth of index rather than the whole thing.
            for (Method m : RuleSet.class.getDeclaredMethods()) {
                if (Modifier.isPublic(m.getModifiers()) && m.getParameterCount() == 0
                        && m.getName().startsWith("add") && m.getName().endsWith("Rule")
                        || Modifier.isPublic(m.getModifiers()) && m.getParameterCount() == 0
                        && m.getName().startsWith("add") && m.getName().endsWith("Rules")) {
                    try {
                        m.invoke(ruleSet);
                    } catch (Throwable ignored) {
                        // A registration that needs context it does not have here contributes
                        // nothing; every other one still does.
                    }
                }
            }
            // The instance lists: implementation rules are assigned at construction, transformation
            // rules are added by the constructor and by the registrations above.
            for (String field : new String[] {"implementRules", "transformRules"}) {
                collectField(RuleSet.class, field, ruleSet, queue);
            }
            // The CombinationRule constants, which are public static and hold most of the RBO rules.
            for (Field f : RuleSet.class.getDeclaredFields()) {
                if (Modifier.isStatic(f.getModifiers()) && Rule.class.isAssignableFrom(f.getType())) {
                    f.setAccessible(true);
                    Object v = f.get(null);
                    if (v instanceof Rule) {
                        queue.add((Rule) v);
                    }
                }
            }
        } catch (Throwable ignored) {
            // A partial index is useful and a missing one is survivable: callers pass unmapped names
            // through, so the worst case is the reporting this class improves stays as it was.
        }

        while (!queue.isEmpty()) {
            Rule rule = queue.poll();
            if (rule == null || !seen.add(rule)) {
                continue;
            }
            try {
                map.computeIfAbsent(rule.getClass().getSimpleName(), k -> new LinkedHashSet<>())
                        .add(rule.type());
            } catch (Throwable ignored) {
                continue;
            }
            // A CombinationRule is a bag of other rules and the children are what actually fire, so
            // an index that stopped at the bag would miss most of the vocabulary.
            if (rule instanceof CombinationRule) {
                collectField(CombinationRule.class, "rules", rule, queue);
            }
            try {
                queue.addAll(rule.predecessorRules());
                queue.addAll(rule.successorRules());
            } catch (Throwable ignored) {
                // Some rules build these lazily from context; skipping them costs coverage of the
                // index, not correctness of it.
            }
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static void collectField(Class<?> owner, String name, Object instance, Deque<Rule> into) {
        try {
            Field f = owner.getDeclaredField(name);
            f.setAccessible(true);
            Object value = f.get(instance);
            if (value instanceof Collection) {
                for (Object o : (Collection<Object>) value) {
                    if (o instanceof Rule) {
                        into.add((Rule) o);
                    }
                }
            }
        } catch (Throwable ignored) {
            // Field renamed or access denied: the index is smaller, and says so through size().
        }
    }
}
