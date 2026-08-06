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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * How often each coverage ELEMENT has been reached, and whether a mutant reached somewhere worth
 * spending more budget on.
 *
 * <p>This replaces the previous feedback key, which was the whole {@link PlanShape} fingerprint used
 * as a set member: a mutant counted as interesting exactly when its complete plan shape string had
 * never been seen. That has two failures a coverage-guided fuzzer cannot live with.
 *
 * <ul>
 *   <li><b>It saturates.</b> Novelty is all-or-nothing, so once the common shapes are enumerated
 *       almost every mutant reads as uninteresting and the feedback channel goes quiet -- exactly
 *       when the run is long enough for feedback to matter.</li>
 *   <li><b>There is no partial credit.</b> A mutant that produced a plan differing from a known one
 *       by a single new operator is scored the same as one that reproduced a known plan exactly.
 *       Decomposing the plan into elements is what makes novelty compositional, and it is the same
 *       move edge coverage makes over whole-path coverage.</li>
 * </ul>
 *
 * <p>Elements are namespaced strings so several signals share one map and one budget:
 * {@code R:<RuleType>} for an RBO rule ({@link RuleTrace}), {@code M:<RuleType>} for a memo-phase
 * rule ({@link RuleCoverage}), {@code OP:} / {@code EDGE:} for the chosen plan
 * ({@link PlanShape#elements}), and {@code F:} for a static SQL feature ({@link SqlFeatures}).
 * Namespacing matters: a rule and an operator can share a name, and silently merging them would
 * make both counts wrong.
 *
 * <p>Not thread-safe, deliberately. Shards are processes, not threads -- the catalog is a singleton
 * and two threads mutating different corpus files would race on table names -- so a lock here would
 * cost something and protect nothing.
 */
public final class CoverageMap {

    /**
     * How often the run has reached each element. Insertion-ordered so a report reads in discovery
     * order, which is what makes "when did we first reach X" answerable after the fact.
     */
    private final Map<String, long[]> hits = new LinkedHashMap<>();

    /** Observations since the last rarity recount, and how often to recount. */
    private long observations;
    private long lastRecount;
    private static final long RECOUNT_EVERY = 1000;

    /**
     * Count at or below which an element is treated as rare, refreshed periodically.
     *
     * <p>Rarity has to be RELATIVE. A fixed threshold ("fewer than 8 hits is rare") calls everything
     * rare in the first thousand mutants and nothing rare after a million, so the feedback would
     * fire constantly and then never -- which is the saturation problem again, one level down.
     * Recomputing a low percentile of the live count distribution keeps the fraction of elements
     * considered rare roughly stable however long the run goes.
     */
    private long rareThreshold = 1;

    /** The fraction of elements the rare threshold aims to cover. */
    private static final double RARE_PERCENTILE = 0.15;

    /** What one observation reached, and what that is worth. */
    public static final class Gain {
        /** Elements no earlier observation had reached. */
        public final int newElements;
        /** Elements that exist but sit in the rare tail. */
        public final int rareElements;
        /** The rarest element's hit count BEFORE this observation, or 0 if something was new. */
        public final long rarestCount;

        Gain(int newElements, int rareElements, long rarestCount) {
            this.newElements = newElements;
            this.rareElements = rareElements;
            this.rarestCount = rarestCount;
        }

        /**
         * Whether this mutant earns a place in the queue.
         *
         * <p>New coverage always qualifies. Rare coverage qualifies too, and that is the part the
         * old key could not express: reaching a rule that has fired twice in a million mutants is
         * evidence the input space around this mutant is under-explored, even though nothing about
         * it is strictly new.
         */
        public boolean isInteresting() {
            return newElements > 0 || rareElements > 0;
        }

        /**
         * Mutation budget multiplier, 1..8.
         *
         * <p>Capped on purpose. An uncapped schedule pours the whole round into whichever mutant
         * happened to reach the rarest element, and a fuzzer that spends a round on one input is
         * not exploring -- it is reducing.
         */
        public int energy() {
            if (newElements <= 0 && rareElements <= 0) {
                return 1;
            }
            int e = 1 + 2 * newElements + rareElements;
            return Math.min(8, e);
        }
    }

    /**
     * Records what one mutant reached and reports what it gained.
     *
     * <p>The gain is computed against the map as it was BEFORE this call, then the counts are
     * updated. Doing it the other way round would make every element look at least once-seen and
     * {@code newElements} would be permanently zero -- a silent zero, and those are the ones that
     * look like a working measurement.
     */
    public Gain observe(Collection<String> elements) {
        int newElements = 0;
        int rareElements = 0;
        long rarest = Long.MAX_VALUE;

        for (String e : elements) {
            long[] slot = hits.get(e);
            if (slot == null) {
                newElements++;
                rarest = 0;
                hits.put(e, new long[] {1});
            } else {
                if (slot[0] <= rareThreshold) {
                    rareElements++;
                }
                rarest = Math.min(rarest, slot[0]);
                slot[0]++;
            }
        }

        observations++;
        if (observations - lastRecount >= RECOUNT_EVERY) {
            recountRarity();
        }
        return new Gain(newElements, rareElements, rarest == Long.MAX_VALUE ? 0 : rarest);
    }

    /**
     * Moves the rare threshold to the {@value #RARE_PERCENTILE} percentile of the current counts.
     *
     * <p>Sorting every element is O(n log n), but n is the number of distinct elements -- a few
     * thousand at most, since the rule vocabulary is bounded by {@code RuleType} and plan operators
     * by the operator set -- and it happens once per {@value #RECOUNT_EVERY} observations.
     */
    private void recountRarity() {
        lastRecount = observations;
        if (hits.isEmpty()) {
            return;
        }
        List<Long> counts = new ArrayList<>(hits.size());
        for (long[] slot : hits.values()) {
            counts.add(slot[0]);
        }
        counts.sort(Comparator.naturalOrder());
        int idx = (int) (counts.size() * RARE_PERCENTILE);
        rareThreshold = counts.get(Math.min(idx, counts.size() - 1));
    }

    /** Distinct elements reached so far. */
    public int size() {
        return hits.size();
    }

    /** Total observations recorded, so a caller can report a rate rather than a bare count. */
    public long observations() {
        return observations;
    }

    /** The current rare cutoff, for reports that need to say what "rare" meant at the time. */
    public long rareThreshold() {
        return rareThreshold;
    }

    /** Hit count for one element, 0 if never reached. */
    public long hits(String element) {
        long[] slot = hits.get(element);
        return slot == null ? 0 : slot[0];
    }

    /** Distinct elements reached within one namespace, e.g. {@code "R:"} for RBO rules. */
    public int sizeOf(String prefix) {
        int n = 0;
        for (String e : hits.keySet()) {
            if (e.startsWith(prefix)) {
                n++;
            }
        }
        return n;
    }

    /**
     * The least-reached elements, which is what mutation steering aims at.
     *
     * <p>Note what this cannot tell you: an element reached zero times does not appear here at all,
     * because a map only knows what it has seen. Blind spots need a vocabulary to subtract from --
     * {@link RuleTrace} has one ({@code RuleType.values()}), plan operators and SQL features do not.
     */
    public List<String> rarest(int limit) {
        List<Map.Entry<String, long[]>> entries = new ArrayList<>(hits.entrySet());
        entries.sort(Comparator.comparingLong(e -> e.getValue()[0]));
        List<String> out = new ArrayList<>();
        for (int i = 0; i < entries.size() && i < limit; i++) {
            out.add(entries.get(i).getKey() + "(" + entries.get(i).getValue()[0] + ")");
        }
        return out;
    }

    /** A copy of the counts, for writing a run's coverage out next to its findings. */
    public Map<String, Long> snapshot() {
        Map<String, Long> out = new HashMap<>(hits.size());
        hits.forEach((k, v) -> out.put(k, v[0]));
        return out;
    }
}
