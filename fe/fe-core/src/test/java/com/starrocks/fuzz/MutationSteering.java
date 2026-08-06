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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Draws mutation operators in an order weighted by what the run has NOT covered.
 *
 * <p>Without this the operator mix is "uniform x applicability" and cannot be steered at all: the
 * list is shuffled evenly and the first applicable operator wins, so an operator that is the only
 * way to reach some feature gets no more budget when that feature is missing than when it is
 * saturated. Measured over 6154 rejections, the effective mix was 48% expression edits, 27% M7,
 * 14% M5 and 11% M6 -- and no amount of running changes it, because nothing in the loop knows what
 * is missing.
 *
 * <p>The weight is a deficit count: for each element an operator DECLARES it can manufacture
 * ({@link Mutation#coverageTargets()}), score 2 if the run has never reached it and 1 if it is in
 * the rare tail. So the only operator that can produce a CTE gets pulled forward exactly while CTEs
 * are missing, and drifts back to baseline once they are not.
 *
 * <h3>What this deliberately does not do</h3>
 *
 * <p><b>It never drops an operator to zero.</b> Every operator keeps a base weight, so a saturated
 * one still runs. Coverage is a proxy for where defects are, not a measure of it -- a defect can sit
 * behind an element reached ten thousand times, and an operator starved to zero can never find it.
 *
 * <p><b>It does not steer towards rules or plan shapes,</b> only towards {@code F:} features. An
 * operator can promise to add a CTE; it cannot promise to make the optimizer fire
 * {@code TF_PUSH_CTE_PRODUCE}, because that depends on statistics, the surrounding tree and cost.
 * Steering on an outcome no operator controls would spend weight on a target it cannot move, and
 * that failure is invisible -- the deficit simply never closes.
 */
public final class MutationSteering {

    /** Kept for a never-reached element: worth more than rare, which is worth more than covered. */
    private static final int WEIGHT_MISSING = 2;
    private static final int WEIGHT_RARE = 1;

    /**
     * Floor under every operator's weight, so steering biases the draw rather than replacing it.
     *
     * <p>At 1 with deficits of a few points the bias is strong but not absolute: an operator with
     * three missing targets is drawn about four times as often as a saturated one, not exclusively.
     */
    private static final int WEIGHT_BASE = 1;

    private MutationSteering() {
    }

    /**
     * Orders {@code operators} for one mutation attempt: weighted draw without replacement.
     *
     * <p>Without replacement because the caller walks the list until something applies, and an
     * operator that did not apply to this tree will not apply to it on a second draw either.
     */
    public static List<Mutation> order(List<Mutation> operators, CoverageMap coverage, Random rnd) {
        if (coverage == null || operators.size() <= 1) {
            List<Mutation> copy = new ArrayList<>(operators);
            java.util.Collections.shuffle(copy, rnd);
            return copy;
        }

        List<Mutation> pool = new ArrayList<>(operators);
        List<Integer> weights = new ArrayList<>(pool.size());
        int total = 0;
        for (Mutation op : pool) {
            int w = weightOf(op, coverage);
            weights.add(w);
            total += w;
        }

        List<Mutation> out = new ArrayList<>(pool.size());
        while (!pool.isEmpty()) {
            int pick = rnd.nextInt(Math.max(1, total));
            int i = 0;
            for (; i < pool.size() - 1; i++) {
                pick -= weights.get(i);
                if (pick < 0) {
                    break;
                }
            }
            out.add(pool.remove(i));
            total -= weights.remove(i);
        }
        return out;
    }

    /** Base weight plus the operator's coverage deficit over the elements it claims to reach. */
    public static int weightOf(Mutation op, CoverageMap coverage) {
        int weight = WEIGHT_BASE;
        for (String target : op.coverageTargets()) {
            long hits = coverage.hits(target);
            if (hits == 0) {
                weight += WEIGHT_MISSING;
            } else if (hits <= coverage.rareThreshold()) {
                weight += WEIGHT_RARE;
            }
        }
        return weight;
    }

    /**
     * The current weights, for the run report.
     *
     * <p>Worth printing: a steering layer whose weights are all equal is doing nothing, and that is
     * indistinguishable from a working one unless the numbers are shown. It also names the case
     * where every target of every operator is already covered -- at which point the honest reading
     * is that the corpus, not the scheduler, is the ceiling.
     */
    public static Map<String, Integer> report(List<Mutation> operators, CoverageMap coverage) {
        Map<String, Integer> out = new LinkedHashMap<>();
        for (Mutation op : operators) {
            out.put(op.name(), weightOf(op, coverage));
        }
        return out;
    }
}
