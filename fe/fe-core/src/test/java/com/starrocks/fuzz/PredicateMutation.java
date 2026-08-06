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

import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.expression.AnalyticExpr;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.sql.parser.SqlParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * M11 — predicate mutation. Target: the rules that only fire when a scan carries a predicate.
 *
 * <p>No other operator generates a predicate. M5 can move a WHERE between "present" and "absent" and
 * M10 can add an EXISTS, but every actual condition in the fuzzer's output was written by a human in
 * the corpus. Measured over the SQL-Tester tree that is 0.36 {@code AND} per query — most WHEREs are a
 * single condition — and HAVING sits at 1.8% and never moves. That is the structural reason this
 * campaign has found almost nothing in predicate pushdown, partition pruning, range extraction, index
 * selection or runtime filters: those rules need a predicate to fire on, and there was never one to
 * give them.
 *
 * <p>This operator adds conditions in the four places the optimizer treats differently:
 * <ul>
 *   <li><b>WHERE</b>, conjoined, disjoined or negated. Conjunction is what raises the AND count and
 *       feeds pushdown; disjunction is a different rule family entirely (OR-to-UNION rewriting, range
 *       merging) and is where the shallow-looking bugs live; negation reaches the De Morgan and
 *       null-rejection paths.</li>
 *   <li><b>HAVING</b>, which barely exists in the corpus and is where aggregate pushdown decides
 *       whether it may push at all.</li>
 *   <li><b>A join's ON clause</b>. An extra conjunct on an outer join is the classic trigger for
 *       outer-join simplification: a null-rejecting condition on the null-supplying side is supposed
 *       to turn the join inner, and that rewrite has to preserve the answer.</li>
 * </ul>
 *
 * <p>Design notes, in the order they matter:
 *
 * <ul>
 *   <li><b>Bias towards weak predicates.</b> A condition that selects almost nothing shrinks the
 *       cluster-side differential silently, because that oracle skips any statement whose baseline is
 *       empty — it cannot compare two plans over zero rows. So the generator prefers
 *       {@code IS NOT NULL}, wide {@code BETWEEN} and outright tautologies over equality against a
 *       random literal. A predicate is here to make a rule fire, not to filter.</li>
 *   <li><b>Tautologies are a deliberate form, not a wasted draw.</b> {@code (c IS NULL OR c IS NOT
 *       NULL)} is true for every row of every type, so it must not change the answer — but it does
 *       change the plan, and it references a real column so it cannot simply be folded away at parse
 *       time. That makes it the cheapest possible plan perturbation with a known-correct expected
 *       result.</li>
 *   <li><b>Constants come from the pool, not from a random number generator.</b> The pool's scalar
 *       texts were harvested from the seeds themselves, so they sit inside the value ranges the
 *       corpus actually uses. A literal drawn out of thin air matches every row or none of them, and
 *       either way no pruning decision depends on it.</li>
 *   <li><b>Column references come from the block's own SlotRefs.</b> Those are the columns already
 *       resolving at this point in the tree, written the way the tree wrote them. Anything else —
 *       a bare name, or a column harvested from another block — reintroduces the ambiguity and
 *       "Column cannot be resolved" rejections that {@link AstMutationFuzzerTest.Material} exists to
 *       avoid, and name resolution is already the largest single category of analyzer rejection.</li>
 *   <li><b>The existing predicate is re-parented, never re-rendered.</b> Building
 *       {@code "(" + deparse(old) + ") AND " + new} would put the deparser in the middle of a
 *       mutation, so a lossy render would silently change what the mutant means and the round-trip
 *       oracle would report the difference as a defect in StarRocks. Re-parenting the node that is
 *       already in this tree is what {@code TreeNode.setChild} does everywhere else; the contract in
 *       {@link Mutation} forbids sharing an Expr between two trees, not moving one within its own.</li>
 * </ul>
 */
public class PredicateMutation implements Mutation {

    private static final long SQL_MODE = SqlModeHelper.MODE_DEFAULT;

    private static final int MAX_TARGETS = 64;
    private static final int MAX_DEPTH = 32;
    private static final int MAX_COLUMNS = 24;

    /** Comparison operators, as text, because the predicate is built as text and reparsed. */
    private static final String[] COMPARISONS = {"=", "<>", "<", "<=", ">", ">="};

    /**
     * Fallback constants for when the pool harvested no scalars at all. Deliberately boring: the
     * boundary-value work belongs to M3, and a boundary literal here would make the predicate select
     * nothing, which is the one thing this operator is trying not to do.
     */
    private static final String[] FALLBACK_SCALARS = {"0", "1", "2", "100", "'a'", "''"};

    /** Aggregate conditions for HAVING. All are legal wherever an aggregation exists, whatever the types. */
    private static final String[] HAVING_FORMS = {
            "count(*) > 0", "count(*) >= 1", "count(*) < 1000000000", "count(*) <> -1",
    };

    /** Same list as ClauseMutation's, and for the same reason: the tree is unanalyzed, so this is
     * name-based. A miss only costs a mutant the analyzer then rejects. */
    private static final Set<String> AGGREGATE_NAMES = new HashSet<>(Arrays.asList(
            "count", "sum", "avg", "min", "max", "stddev", "stddev_samp", "stddev_pop", "std",
            "variance", "variance_samp", "variance_pop", "var_samp", "var_pop", "group_concat",
            "array_agg", "array_agg_distinct", "any_value", "bitmap_union", "bitmap_union_count",
            "bitmap_agg", "bitmap_intersect", "hll_union", "hll_union_agg", "hll_raw_agg",
            "percentile_approx", "percentile_union", "percentile_cont", "percentile_disc",
            "multi_distinct_count", "multi_distinct_sum", "approx_count_distinct", "ndv",
            "retention", "window_funnel", "corr", "covar_pop", "covar_samp", "grouping",
            "grouping_id", "sum0", "max_by", "min_by", "histogram"));

    @Override
    public String name() {
        return "M11-predicate";
    }

    /**
     * Conditions: WHERE and HAVING themselves, the boolean connectives, and the predicate forms the
     * generator emits. Deliberately does NOT claim any join feature -- attaching a predicate to a
     * join without an ON clause would turn a cross join into an inner one, which is a shape change
     * and belongs to M6.
     */
    @Override
    public Set<String> coverageTargets() {
        return TARGETS;
    }

    private static final Set<String> TARGETS = Set.of(
            "F:where", "F:having", "F:compound:AND", "F:compound:OR", "F:compound:NOT",
            "F:isnull", "F:between", "F:like", "F:in");

    @Override
    public String apply(QueryStatement stmt, AstMutationFuzzerTest.Pool pool, Random rnd) {
        if (stmt == null || stmt.getQueryRelation() == null) {
            return null;
        }
        List<SelectRelation> blocks = new ArrayList<>();
        List<JoinRelation> joins = new ArrayList<>();
        collect(stmt.getQueryRelation(), blocks, joins, 0);
        if (blocks.isEmpty() && joins.isEmpty()) {
            return null;
        }

        List<Op> ops = new ArrayList<>();
        for (int i = 0; i < blocks.size(); i++) {
            offerBlockOps(blocks.get(i), "select#" + i, pool, rnd, ops);
        }
        for (int i = 0; i < joins.size(); i++) {
            offerJoinOps(joins.get(i), "join#" + i, pool, rnd, ops);
        }
        if (ops.isEmpty()) {
            return null;
        }

        // Shuffle and walk rather than draw once: an op can still decline at injection time, when the
        // predicate it built turns out not to parse on its own. Giving up on the whole statement after
        // one failed draw would bias the operator towards the seeds whose first draw happened to work,
        // and that bias would be invisible in the report.
        Collections.shuffle(ops, rnd);
        for (Op op : ops) {
            String description = op.run();
            if (description != null) {
                return description;
            }
        }
        return null;
    }

    // --------------------------------------------------------------- targets

    /**
     * The same descent the other operators use — CTEs, FROM operands, join sides, set-operation
     * branches, and subqueries reached through expressions. A block this walk does not reach is a
     * block that never gets a predicate, and the gap would be silent.
     */
    private static void collect(Relation relation, List<SelectRelation> blocks, List<JoinRelation> joins,
                                int depth) {
        if (relation == null || depth > MAX_DEPTH || blocks.size() + joins.size() >= MAX_TARGETS) {
            return;
        }
        if (relation instanceof QueryRelation && ((QueryRelation) relation).getCteRelations() != null) {
            for (CTERelation cte : ((QueryRelation) relation).getCteRelations()) {
                collect(cte, blocks, joins, depth + 1);
            }
        }

        if (relation instanceof SelectRelation) {
            SelectRelation select = (SelectRelation) relation;
            blocks.add(select);
            collect(select.getRelation(), blocks, joins, depth + 1);
            for (Expr root : ownExprRoots(select)) {
                collectFromSubqueries(root, blocks, joins, depth + 1);
            }
        } else if (relation instanceof SubqueryRelation) {
            QueryStatement inner = ((SubqueryRelation) relation).getQueryStatement();
            if (inner != null) {
                collect(inner.getQueryRelation(), blocks, joins, depth + 1);
            }
        } else if (relation instanceof SetOperationRelation) {
            for (Relation child : ((SetOperationRelation) relation).getRelations()) {
                collect(child, blocks, joins, depth + 1);
            }
        } else if (relation instanceof JoinRelation) {
            JoinRelation join = (JoinRelation) relation;
            joins.add(join);
            collect(join.getLeft(), blocks, joins, depth + 1);
            collect(join.getRight(), blocks, joins, depth + 1);
        } else if (relation instanceof CTERelation) {
            QueryStatement inner = ((CTERelation) relation).getCteQueryStatement();
            if (inner != null) {
                collect(inner.getQueryRelation(), blocks, joins, depth + 1);
            }
        }
    }

    private static List<Expr> ownExprRoots(SelectRelation select) {
        List<Expr> roots = new ArrayList<>();
        if (select.getSelectList() != null && select.getSelectList().getItems() != null) {
            for (SelectListItem item : select.getSelectList().getItems()) {
                if (!item.isStar() && item.getExpr() != null) {
                    roots.add(item.getExpr());
                }
            }
        }
        if (select.getWhereClause() != null) {
            roots.add(select.getWhereClause());
        }
        if (select.getHavingClause() != null) {
            roots.add(select.getHavingClause());
        }
        // getOriGroupingExprs, not getGroupingExprs: the latter lazily rewrites the clause's own state.
        if (select.getGroupByClause() != null && select.getGroupByClause().getOriGroupingExprs() != null) {
            roots.addAll(select.getGroupByClause().getOriGroupingExprs());
        }
        if (select.hasOrderByClause()) {
            for (OrderByElement element : select.getOrderBy()) {
                if (element != null && element.getExpr() != null) {
                    roots.add(element.getExpr());
                }
            }
        }
        return roots;
    }

    private static void collectFromSubqueries(Expr expr, List<SelectRelation> blocks, List<JoinRelation> joins,
                                              int depth) {
        if (expr == null || depth > MAX_DEPTH || blocks.size() + joins.size() >= MAX_TARGETS) {
            return;
        }
        if (expr instanceof Subquery) {
            QueryStatement inner = ((Subquery) expr).getQueryStatement();
            if (inner != null) {
                collect(inner.getQueryRelation(), blocks, joins, depth + 1);
            }
        }
        for (Expr child : expr.getChildren()) {
            collectFromSubqueries(child, blocks, joins, depth + 1);
        }
    }

    // ------------------------------------------------------------------- ops

    private interface Op {
        String run();
    }

    private static void offerBlockOps(SelectRelation select, String at, AstMutationFuzzerTest.Pool pool,
                                      Random rnd, List<Op> ops) {
        List<String> columns = columnTexts(ownExprRoots(select), pool);

        if (!columns.isEmpty()) {
            Expr where = select.getWhereClause();
            if (where == null) {
                ops.add(() -> {
                    String text = buildPredicate(columns, pool, rnd);
                    Expr predicate = parseExpr(text);
                    if (predicate == null) {
                        return null;
                    }
                    select.setWhereClause(predicate);
                    return at + ": add WHERE " + text;
                });
            } else {
                // Conjunction is offered twice and disjunction once, so the shuffle above lands on AND
                // about half the time. Conjunction is what actually raises the AND-per-query count this
                // operator exists to move; OR is rarer in real queries and rarer here.
                ops.add(conjoinWhere(select, at, columns, pool, rnd));
                ops.add(conjoinWhere(select, at, columns, pool, rnd));
                ops.add(() -> {
                    String text = buildPredicate(columns, pool, rnd);
                    Expr addition = parseExpr(text);
                    if (addition == null) {
                        return null;
                    }
                    select.setWhereClause(new CompoundPredicate(CompoundPredicate.Operator.OR,
                            select.getWhereClause(), addition));
                    return at + ": WHERE ... OR " + text;
                });
                ops.add(() -> {
                    select.setWhereClause(new CompoundPredicate(CompoundPredicate.Operator.NOT,
                            select.getWhereClause(), null));
                    return at + ": negate WHERE";
                });
            }
        }

        offerHaving(select, at, columns, rnd, ops);
    }

    private static Op conjoinWhere(SelectRelation select, String at, List<String> columns,
                                   AstMutationFuzzerTest.Pool pool, Random rnd) {
        return () -> {
            String text = buildPredicate(columns, pool, rnd);
            Expr addition = parseExpr(text);
            if (addition == null) {
                return null;
            }
            select.setWhereClause(new CompoundPredicate(CompoundPredicate.Operator.AND,
                    select.getWhereClause(), addition));
            return at + ": WHERE ... AND " + text;
        };
    }

    /**
     * HAVING is only offered where an aggregation already exists. Everywhere else it is legal SQL that
     * this analyzer rejects for every select item that is not itself aggregated, which is nearly always
     * — the mutant would be spent on a rejection rather than on a rule.
     */
    private static void offerHaving(SelectRelation select, String at, List<String> columns, Random rnd,
                                    List<Op> ops) {
        SelectList selectList = select.getSelectList();
        if (!select.hasGroupByClause() && !hasAggregateItem(selectList)) {
            return;
        }
        ops.add(() -> {
            String text = buildHavingPredicate(columns, rnd);
            Expr addition = parseExpr(text);
            if (addition == null) {
                return null;
            }
            Expr having = select.getHavingClause();
            if (having == null) {
                select.setHaving(addition);
                return at + ": add HAVING " + text;
            }
            select.setHaving(new CompoundPredicate(CompoundPredicate.Operator.AND, having, addition));
            return at + ": HAVING ... AND " + text;
        });
    }

    /**
     * An extra conjunct on a join condition, built only from columns the ON clause already names.
     *
     * <p>Restricting the material to the ON clause's own SlotRefs is what makes this safe without
     * knowing the join's scope: those columns resolve at this point by construction, because the tree
     * already resolved them here. Drawing from the pool instead would name columns from either side of
     * a join that may not be in scope on the side the predicate lands on.
     *
     * <p>A join with no ON clause is skipped rather than given one. CROSS JOIN and comma joins reach
     * here, and turning one into an inner join by attaching a condition is M6's job, not a predicate
     * edit — doing it here would make the operator's own report misleading about what it changed.
     */
    private static void offerJoinOps(JoinRelation join, String at, AstMutationFuzzerTest.Pool pool, Random rnd,
                                     List<Op> ops) {
        Expr on = join.getOnPredicate();
        if (on == null) {
            return;
        }
        List<String> columns = columnTexts(Collections.singletonList(on), pool);
        if (columns.isEmpty()) {
            return;
        }
        ops.add(() -> {
            String text = buildPredicate(columns, pool, rnd);
            Expr addition = parseExpr(text);
            if (addition == null) {
                return null;
            }
            join.setOnPredicate(new CompoundPredicate(CompoundPredicate.Operator.AND,
                    join.getOnPredicate(), addition));
            return at + " (" + join.getJoinOp() + "): ON ... AND " + text;
        });
    }

    // ------------------------------------------------------- predicate texts

    /**
     * Builds one predicate, weighted towards forms that keep most rows.
     *
     * <p>The weighting is the whole point of the generator. The cluster-side differential compares two
     * plans over the rows a statement returns, and skips any statement returning none, so a fuzzer that
     * bolts a selective condition onto every query would quietly convert differential coverage into
     * empty baselines while every counter still looked healthy.
     */
    private static String buildPredicate(List<String> columns, AstMutationFuzzerTest.Pool pool, Random rnd) {
        String column = columns.get(rnd.nextInt(columns.size()));
        int roll = rnd.nextInt(100);

        if (roll < 22) {
            return column + " IS NOT NULL";
        }
        if (roll < 30) {
            return "NOT (" + column + " IS NULL)";
        }
        if (roll < 45) {
            // True for every row of every type, so the answer may not change -- only the plan.
            return "(" + column + " IS NULL OR " + column + " IS NOT NULL)";
        }
        if (roll < 75) {
            return column + " " + COMPARISONS[rnd.nextInt(COMPARISONS.length)] + " " + scalar(pool, rnd);
        }
        if (roll < 85) {
            return column + " IN (" + scalar(pool, rnd) + ", " + scalar(pool, rnd) + ")";
        }
        if (roll < 95) {
            // Two independently drawn bounds, so this is frequently an empty range and frequently a
            // wide one. Ordering them would need types the unanalyzed tree does not carry.
            return column + " BETWEEN " + scalar(pool, rnd) + " AND " + scalar(pool, rnd);
        }
        List<String> booleans = pool.booleanTexts;
        if (booleans.isEmpty()) {
            return column + " IS NOT NULL";
        }
        return "(" + booleans.get(rnd.nextInt(booleans.size())) + ")";
    }

    /**
     * A HAVING condition. Aggregates are legal in HAVING whatever the GROUP BY says, so the count(*)
     * forms always analyze; the aggregate-over-a-column form is offered too because pushdown decisions
     * differ between "the condition is on the group size" and "the condition is on an aggregate value".
     */
    private static String buildHavingPredicate(List<String> columns, Random rnd) {
        if (columns.isEmpty() || rnd.nextInt(100) < 60) {
            return HAVING_FORMS[rnd.nextInt(HAVING_FORMS.length)];
        }
        String column = columns.get(rnd.nextInt(columns.size()));
        String aggregate = rnd.nextBoolean() ? "max" : "min";
        return aggregate + "(" + column + ") IS NOT NULL";
    }

    private static String scalar(AstMutationFuzzerTest.Pool pool, Random rnd) {
        List<String> scalars = pool.scalarTexts;
        if (scalars.isEmpty()) {
            return FALLBACK_SCALARS[rnd.nextInt(FALLBACK_SCALARS.length)];
        }
        // A harvested scalar can be an arbitrarily large expression; parenthesise so it composes as one
        // operand however it was written.
        return "(" + scalars.get(rnd.nextInt(scalars.size())) + ")";
    }

    // --------------------------------------------------------------- helpers

    /**
     * The column references already present in these expression roots, rendered the way the tree wrote
     * them, falling back to the pool only when the roots name none.
     */
    private static List<String> columnTexts(List<Expr> roots, AstMutationFuzzerTest.Pool pool) {
        List<String> texts = new ArrayList<>();
        for (Expr root : roots) {
            collectSlotRefs(root, texts, 0);
        }
        if (texts.isEmpty()) {
            for (String qualified : pool.qualifiedColumns) {
                if (texts.size() >= MAX_COLUMNS) {
                    break;
                }
                if (!texts.contains(qualified)) {
                    texts.add(qualified);
                }
            }
        }
        return texts;
    }

    private static void collectSlotRefs(Expr expr, List<String> out, int depth) {
        if (expr == null || depth > MAX_DEPTH || out.size() >= MAX_COLUMNS) {
            return;
        }
        // Do not descend into a subquery: its columns resolve in its own scope, not in the one the
        // predicate being built will land in.
        if (expr instanceof Subquery) {
            return;
        }
        if (expr instanceof SlotRef) {
            String text = render(expr);
            if (isRenderable(text) && !out.contains(text)) {
                out.add(text);
            }
            return;
        }
        for (Expr child : expr.getChildren()) {
            collectSlotRefs(child, out, depth + 1);
        }
    }

    private static boolean hasAggregateItem(SelectList selectList) {
        if (selectList == null || selectList.getItems() == null) {
            return false;
        }
        return selectList.getItems().stream()
                .anyMatch(item -> !item.isStar() && item.getExpr() != null
                        && containsAggregateAnywhere(item.getExpr()));
    }

    /** An AnalyticExpr wraps an aggregate but is not one, so a window item does not qualify a HAVING. */
    private static boolean containsAggregateAnywhere(Expr expr) {
        if (expr == null || expr instanceof AnalyticExpr) {
            return false;
        }
        if (expr instanceof FunctionCallExpr) {
            String name = ((FunctionCallExpr) expr).getFunctionName();
            if (name != null && AGGREGATE_NAMES.contains(name.toLowerCase())) {
                return true;
            }
        }
        for (Expr child : expr.getChildren()) {
            if (containsAggregateAnywhere(child)) {
                return true;
            }
        }
        return false;
    }

    private static Expr parseExpr(String text) {
        if (text == null || text.trim().isEmpty()) {
            return null;
        }
        try {
            return SqlParser.parseSqlToExpr(text, SQL_MODE);
        } catch (Throwable t) {
            return null;
        }
    }

    private static boolean isRenderable(String text) {
        return text != null && !text.trim().isEmpty() && !text.startsWith("<") && text.length() < 400;
    }

    private static String render(Expr expr) {
        if (expr == null) {
            return "<null>";
        }
        try {
            String sql = AstToSQLBuilder.toSQL(expr);
            return sql == null ? "<null>" : sql.replace('\n', ' ');
        } catch (Throwable t) {
            return "<" + expr.getClass().getSimpleName() + ">";
        }
    }
}
