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
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A fingerprint of the plan the optimizer CHOSE, as opposed to the rules it considered.
 *
 * Rule coverage ({@link RuleCoverage}) turned out to be blind to everything cost-driven: with
 * synthetic statistics installed and consulted 90,240 times, the fired-rule set did not move by a
 * single bit. That is not a bug in the injection -- exploration applies every rule that MATCHES a
 * shape, and cost only decides which of the generated alternatives wins. So the rule vector measures
 * the syntactic reach of a corpus, and nothing about which plan came out.
 *
 * <p>This fingerprint measures the other half: the physical operators, join implementations and
 * distribution modes in the winning plan. It is exactly the output of the cost model, so it responds
 * to statistics, session knobs and data size where rule coverage cannot.
 *
 * <p>Deliberately coarse. Column ids, expressions and cardinality estimates are excluded: they make
 * every plan unique, which would make the signal useless for saying two runs chose "the same plan".
 */
public final class PlanShape {

    private PlanShape() {
    }

    /**
     * A stable, order-insensitive summary: the multiset of physical operators, with join type and
     * distribution mode attached where they exist.
     *
     * Order-insensitive because two plans that differ only in which side of a join was built first
     * are the same shape for this purpose, and treating them as different would drown the signal.
     */
    public static String fingerprint(OptExpression plan) {
        List<String> parts = new ArrayList<>();
        walk(plan, parts);
        Collections.sort(parts);
        return String.join(",", parts);
    }

    private static void walk(OptExpression node, List<String> out) {
        if (node == null || node.getOp() == null) {
            return;
        }
        StringBuilder sb = new StringBuilder(node.getOp().getOpType().name());
        if (node.getOp() instanceof PhysicalJoinOperator) {
            // Broadcast vs shuffle shows up as a sibling Distribution node, but the join TYPE
            // (inner/outer/semi) is a plan choice in its own right and is worth separating.
            sb.append('[').append(((PhysicalJoinOperator) node.getOp()).getJoinType()).append(']');
        } else if (node.getOp() instanceof PhysicalDistributionOperator) {
            // The single most cost-sensitive choice in the plan: broadcast a small side or shuffle
            // both. If statistics move anything, they move this.
            sb.append('[').append(((PhysicalDistributionOperator) node.getOp()).getDistributionSpec()
                    .getType()).append(']');
        }
        out.add(sb.toString());
        for (OptExpression child : node.getInputs()) {
            walk(child, out);
        }
    }
}
