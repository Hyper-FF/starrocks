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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.MVTransformerContext;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.optimizer.transformer.TransformerContext;

import java.util.BitSet;

/**
 * Which optimizer rules a query's optimization actually applied.
 *
 * The optimizer already records this per memo group expression
 * ({@link GroupExpression#getAppliedRuleMasks()}), sized by {@link RuleType}, so reading it costs a
 * few bitset ORs and needs no instrumentation, no agent and no coverage build.
 *
 * <p>What this is NOT: proof that a rule changed the plan. A rule can match, run and return the
 * expression it was given. So the vector is a LOWER BOUND on what the optimizer explored -- good for
 * finding what a corpus never reaches, unsuitable as a quality score.
 *
 * <p>Cost: this re-runs transform + optimize. Callers that already planned the same statement pay
 * for the optimizer twice, which is why the fuzzer samples rather than measuring every mutant.
 */
public final class RuleCoverage {

    public static final int NUM_RULES = RuleType.NUM_RULES.ordinal();

    private RuleCoverage() {
    }

    /**
     * Returns the rules applied while optimizing {@code query}, or an empty set if the optimizer
     * took a path that builds no memo.
     *
     * <p>StatementPlanner.plan() cannot be used here: it does not hand back the OptimizerContext,
     * and the memo it leaves behind is the only place the applied-rule bitsets live. The sequence
     * below is the one StatementPlanner itself runs.
     */
    public static BitSet collect(ConnectContext ctx, QueryStatement query) {
        BitSet fired = new BitSet(NUM_RULES + 1);
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        TransformerContext tc =
                new TransformerContext(columnRefFactory, ctx, MVTransformerContext.of(ctx, true));
        LogicalPlan logicalPlan =
                new RelationTransformer(tc).transformWithSelectLimit(query.getQueryRelation());

        OptimizerContext oc = OptimizerFactory.initContext(ctx, columnRefFactory);
        oc.setStatement(query);
        Optimizer optimizer = OptimizerFactory.create(oc);
        optimizer.optimize(logicalPlan.getRoot(), new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()));

        Memo memo = optimizer.getContext().getMemo();
        if (memo == null) {
            // Short-circuit and rule-less paths build no memo. Reporting that as zero coverage would
            // understate the corpus, so callers should treat an empty result as "no contribution".
            return fired;
        }
        for (Group g : memo.getGroups()) {
            for (GroupExpression ge : g.getLogicalExpressions()) {
                fired.or(ge.getAppliedRuleMasks());
            }
            for (GroupExpression ge : g.getPhysicalExpressions()) {
                fired.or(ge.getAppliedRuleMasks());
            }
        }
        return fired;
    }

    /** Rule vector plus the fingerprint of the plan that was chosen. */
    public static final class Result {
        public final BitSet fired;
        public final String planShape;

        Result(BitSet fired, String planShape) {
            this.fired = fired;
            this.planShape = planShape;
        }
    }

    /**
     * Both signals from one optimize pass.
     *
     * They answer different questions and neither substitutes for the other: the rule vector says
     * what the query's SHAPE could reach, the fingerprint says what the cost model actually CHOSE.
     * Rule coverage is provably blind to statistics; the fingerprint is where that shows up.
     */
    public static Result collectBoth(ConnectContext ctx, QueryStatement query) {
        BitSet fired = new BitSet(NUM_RULES + 1);
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        TransformerContext tc =
                new TransformerContext(columnRefFactory, ctx, MVTransformerContext.of(ctx, true));
        LogicalPlan logicalPlan =
                new RelationTransformer(tc).transformWithSelectLimit(query.getQueryRelation());

        OptimizerContext oc = OptimizerFactory.initContext(ctx, columnRefFactory);
        oc.setStatement(query);
        Optimizer optimizer = OptimizerFactory.create(oc);
        OptExpression best = optimizer.optimize(logicalPlan.getRoot(), new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()));

        Memo memo = optimizer.getContext().getMemo();
        if (memo != null) {
            for (Group g : memo.getGroups()) {
                for (GroupExpression ge : g.getLogicalExpressions()) {
                    fired.or(ge.getAppliedRuleMasks());
                }
                for (GroupExpression ge : g.getPhysicalExpressions()) {
                    fired.or(ge.getAppliedRuleMasks());
                }
            }
        }
        return new Result(fired, best == null ? "" : PlanShape.fingerprint(best));
    }

    /** Names of the rules not present in {@code fired}, for reporting a corpus's blind spots. */
    public static String describeNeverFired(BitSet fired, int limit) {
        RuleType[] rules = RuleType.values();
        StringBuilder sb = new StringBuilder();
        int shown = 0;
        for (int i = 0; i < NUM_RULES && shown < limit; i++) {
            if (!fired.get(i)) {
                sb.append(shown++ == 0 ? "" : ", ").append(rules[i].name());
            }
        }
        return sb.toString();
    }
}
