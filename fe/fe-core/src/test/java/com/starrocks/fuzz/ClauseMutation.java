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
import com.starrocks.sql.ast.GroupByClause;
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
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.LimitElement;
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
 * M5 — clause add/remove, as enumerated in srfuzz/docs/SQL_AST_FUZZER_PLAN.md §2. Target: optimizer rules.
 *
 * <p>M1–M4 all rewrite one {@code Expr} in place; they never change the <i>shape</i> of a query block, so
 * whole families of optimizer rules are unreachable from them. A seed without a GROUP BY never reaches
 * aggregate pushdown, a seed without a LIMIT never reaches the TopN rules, and a seed with a WHERE always
 * has one. M5 is the operator that moves a query block between those shapes: it adds or removes WHERE,
 * GROUP BY, HAVING, ORDER BY, LIMIT, DISTINCT and window (OVER) clauses.
 *
 * <p>Design notes, in the order they cost time to discover:
 *
 * <ul>
 *   <li><b>Every query block is a candidate, not just the outermost.</b> The descent mirrors
 *       {@code AstMutationFuzzerTest#collectRootExprs} (CTEs, FROM operands, join sides, set-operation
 *       branches) and additionally follows {@code Subquery} expressions, so {@code WHERE x IN (SELECT ...)}
 *       is a mutation site too — subquery rewriting is one of the rule families this operator exists for.</li>
 *   <li><b>ORDER BY and LIMIT are skipped on a direct set-operation branch.</b> They live on
 *       {@code QueryRelation}, but the deparser only prints them from {@code visitQueryStatement};
 *       {@code processSetOp} visits the branch relation directly, so a LIMIT put on a bare UNION branch is
 *       silently dropped on the way out and the "mutant" is textually the seed. Wasted budget, not a
 *       finding — but wasted budget that looks like coverage.</li>
 *   <li><b>DISTINCT lives on the SelectList, not on the SelectRelation.</b> {@code SelectRelation.isDistinct}
 *       is an analyzer output filled by {@code fillResolvedAST}; both the deparser and
 *       {@code SelectAnalyzer} read {@code getSelectList().isDistinct()}. Setting the SelectRelation flag on
 *       an unanalyzed tree does nothing at all.</li>
 *   <li><b>Removal writes the shape the parser itself produces</b> — {@code null} for WHERE/GROUP BY/HAVING
 *       /LIMIT, an <i>empty list</i> for ORDER BY (as {@code QueryRelation.clearOrder} does). Nulling the
 *       sort clause instead would hand the analyzer a shape no parse can produce, and any NPE that came
 *       back would be reported as an internal error the fuzzer had manufactured itself.</li>
 *   <li><b>Injected fragments are text, reparsed here.</b> Never an Expr borrowed from the pool or from
 *       elsewhere in the same tree — see {@link Mutation}.</li>
 * </ul>
 *
 * <p>Where there is a choice, the operator prefers the variant likelier to survive analysis: GROUP BY is
 * built from the query's own non-aggregate select items, HAVING is only offered where an aggregation
 * already exists, and a window function is only offered to a block that has neither an aggregate nor a
 * GROUP BY. A mutant rejected by the analyzer costs a full parse/analyze cycle and reaches no rule at all.
 */
public class ClauseMutation implements Mutation {

    private static final long SQL_MODE = SqlModeHelper.MODE_DEFAULT;

    /** Always legal wherever an aggregation exists, and it exercises the HAVING rules rather than the parser. */
    private static final String[] HAVING_TEXTS = {
            "count(*) > 0", "count(*) > 1", "count(*) >= 2", "count(*) < 10",
    };

    /** {@code -1} is LimitElement's "no limit" sentinel and deparses to nothing, so it is not offered. */
    private static final long[] LIMIT_VALUES = {0, 1, 2, 5, 100};
    private static final long[] OFFSET_VALUES = {0, 0, 0, 1, 7};

    private static final String[] WINDOW_AGGREGATES = {"sum", "count", "max", "min", "avg"};
    private static final String[] WINDOW_RANKERS = {"row_number", "rank", "dense_rank"};

    private static final int MAX_GROUP_BY_KEYS = 3;
    private static final int MAX_TARGETS = 64;
    private static final int MAX_DEPTH = 32;

    /**
     * Name-based, because the tree is unanalyzed: {@code FunctionCallExpr#isAggregateFunction} needs the
     * resolved {@code Function} and returns false for every node here. A miss only costs a mutant that the
     * analyzer then rejects, so the list is deliberately conservative rather than exhaustive.
     */
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
        return "M5-clause";
    }

    /**
     * GROUP BY, HAVING, ORDER BY, LIMIT, DISTINCT and window clauses -- the clause-level features
     * whose absence makes whole rule families unreachable, which is the reason this operator exists.
     */
    @Override
    public Set<String> coverageTargets() {
        return TARGETS;
    }

    private static final Set<String> TARGETS = Set.of(
            "F:groupby", "F:having", "F:orderby", "F:limit", "F:distinct", "F:window");

    @Override
    public String apply(QueryStatement stmt, AstMutationFuzzerTest.Pool pool, Random rnd) {
        if (stmt == null || stmt.getQueryRelation() == null) {
            return null;
        }
        List<Target> targets = new ArrayList<>();
        collectTargets(stmt.getQueryRelation(), true, targets, 0);
        if (targets.isEmpty()) {
            return null;
        }

        List<Op> ops = new ArrayList<>();
        for (Target target : targets) {
            offerOps(target, pool, rnd, ops);
        }
        if (ops.isEmpty()) {
            return null;
        }

        // Shuffle and walk, rather than draw once: an op can still decline at injection time (a pooled
        // fragment that no longer parses on its own, a limit that renders to nothing). Giving up on the
        // whole statement after one failed draw would quietly bias the operator towards the seeds whose
        // first draw happened to work, and that bias is invisible in the report.
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

    /** One mutable query block, plus whether the deparser would print an ORDER BY / LIMIT put on it. */
    private static final class Target {
        final SelectRelation select;
        final boolean orderLimitPrintable;
        final int index;

        Target(SelectRelation select, boolean orderLimitPrintable, int index) {
            this.select = select;
            this.orderLimitPrintable = orderLimitPrintable;
            this.index = index;
        }

        String label() {
            return "select#" + index;
        }
    }

    private static void collectTargets(Relation relation, boolean orderLimitPrintable, List<Target> out, int depth) {
        if (relation == null || depth > MAX_DEPTH || out.size() >= MAX_TARGETS) {
            return;
        }
        if (relation instanceof QueryRelation && ((QueryRelation) relation).getCteRelations() != null) {
            for (CTERelation cte : ((QueryRelation) relation).getCteRelations()) {
                collectTargets(cte, true, out, depth + 1);
            }
        }

        if (relation instanceof SelectRelation) {
            SelectRelation select = (SelectRelation) relation;
            out.add(new Target(select, orderLimitPrintable, out.size()));
            // Anything below the block sits inside its own QueryStatement again, so the printability
            // restriction never propagates downwards.
            collectTargets(select.getRelation(), true, out, depth + 1);
            for (Expr root : ownExprRoots(select)) {
                collectFromSubqueries(root, out, depth + 1);
            }
        } else if (relation instanceof SubqueryRelation) {
            QueryStatement inner = ((SubqueryRelation) relation).getQueryStatement();
            if (inner != null) {
                collectTargets(inner.getQueryRelation(), true, out, depth + 1);
            }
        } else if (relation instanceof SetOperationRelation) {
            for (Relation child : ((SetOperationRelation) relation).getRelations()) {
                collectTargets(child, false, out, depth + 1);
            }
        } else if (relation instanceof JoinRelation) {
            JoinRelation join = (JoinRelation) relation;
            collectTargets(join.getLeft(), true, out, depth + 1);
            collectTargets(join.getRight(), true, out, depth + 1);
        } else if (relation instanceof CTERelation) {
            QueryStatement inner = ((CTERelation) relation).getCteQueryStatement();
            if (inner != null) {
                collectTargets(inner.getQueryRelation(), true, out, depth + 1);
            }
        }
    }

    /** The expression roots that belong to this block itself, used only to find nested subqueries. */
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

    private static void collectFromSubqueries(Expr expr, List<Target> out, int depth) {
        if (expr == null || depth > MAX_DEPTH || out.size() >= MAX_TARGETS) {
            return;
        }
        if (expr instanceof Subquery) {
            QueryStatement inner = ((Subquery) expr).getQueryStatement();
            if (inner != null) {
                collectTargets(inner.getQueryRelation(), true, out, depth + 1);
            }
        }
        for (Expr child : expr.getChildren()) {
            collectFromSubqueries(child, out, depth + 1);
        }
    }

    // ------------------------------------------------------------------- ops

    /** One candidate edit. Returns its description, or null when it declined after all. */
    private interface Op {
        String run();
    }

    private static void offerOps(Target target, AstMutationFuzzerTest.Pool pool, Random rnd, List<Op> ops) {
        SelectRelation select = target.select;
        SelectList selectList = select.getSelectList();
        String at = target.label();

        offerWhere(select, pool, rnd, ops, at);
        offerGroupBy(select, selectList, pool, rnd, ops, at);
        offerHaving(select, selectList, rnd, ops, at);
        if (target.orderLimitPrintable) {
            offerOrderBy(select, selectList, pool, rnd, ops, at);
            offerLimit(select, rnd, ops, at);
        }
        offerDistinct(selectList, ops, at);
        offerWindow(select, selectList, pool, rnd, ops, at);
    }

    private static void offerWhere(SelectRelation select, AstMutationFuzzerTest.Pool pool, Random rnd,
                                   List<Op> ops, String at) {
        if (select.hasWhereClause()) {
            ops.add(() -> {
                String before = render(select.getWhereClause());
                select.setWhereClause(null);
                return at + ": remove WHERE " + before;
            });
            return;
        }
        List<String> bucket = pool.bucketFor(true);
        if (bucket.isEmpty()) {
            return;
        }
        ops.add(() -> {
            String text = bucket.get(rnd.nextInt(bucket.size()));
            Expr predicate = parseExpr(text);
            if (predicate == null) {
                return null;
            }
            select.setWhereClause(predicate);
            return at + ": add WHERE " + text;
        });
    }

    private static void offerGroupBy(SelectRelation select, SelectList selectList, AstMutationFuzzerTest.Pool pool,
                                     Random rnd, List<Op> ops, String at) {
        if (select.hasGroupByClause()) {
            ops.add(() -> {
                String before = renderGroupBy(select.getGroupByClause());
                select.setGroupByClause(null);
                return at + ": remove GROUP BY " + before;
            });
            return;
        }
        // A star item cannot be grouped, and grouping a block whose select list is already an aggregate
        // over nothing produces a query the analyzer rejects on sight.
        if (selectList == null || selectList.getItems() == null || selectList.getItems().isEmpty()
                || hasStarItem(selectList)) {
            return;
        }
        List<String> keys = groupableTexts(selectList, pool);
        if (keys.isEmpty()) {
            return;
        }
        ops.add(() -> {
            ArrayList<Expr> exprs = new ArrayList<>();
            List<String> kept = new ArrayList<>();
            for (String key : keys) {
                Expr parsed = parseExpr(key);
                if (parsed != null) {
                    exprs.add(parsed);
                    kept.add(key);
                }
            }
            if (exprs.isEmpty()) {
                return null;
            }
            select.setGroupByClause(new GroupByClause(exprs, GroupByClause.GroupingType.GROUP_BY));
            return at + ": add GROUP BY " + String.join(", ", kept);
        });
    }

    private static void offerHaving(SelectRelation select, SelectList selectList, Random rnd,
                                    List<Op> ops, String at) {
        if (select.hasHavingClause()) {
            ops.add(() -> {
                String before = render(select.getHavingClause());
                select.setHaving(null);
                return at + ": remove HAVING " + before;
            });
            return;
        }
        // HAVING without an aggregation is legal SQL but is rejected here for every select item that is
        // not itself aggregated, which is nearly always. Only offer it where one already exists.
        if (!select.hasGroupByClause() && !hasAggregateItem(selectList)) {
            return;
        }
        ops.add(() -> {
            String text = HAVING_TEXTS[rnd.nextInt(HAVING_TEXTS.length)];
            Expr having = parseExpr(text);
            if (having == null) {
                return null;
            }
            select.setHaving(having);
            return at + ": add HAVING " + text;
        });
    }

    private static void offerOrderBy(SelectRelation select, SelectList selectList, AstMutationFuzzerTest.Pool pool,
                                     Random rnd, List<Op> ops, String at) {
        if (select.hasOrderByClause()) {
            ops.add(() -> {
                String before = renderOrderBy(select.getOrderBy());
                // clearOrder()'s shape, i.e. one the parser also produces. See the class javadoc.
                select.setOrderBy(new ArrayList<>());
                return at + ": remove ORDER BY " + before;
            });
            return;
        }
        List<String> candidates = sortableTexts(selectList, pool);
        if (candidates.isEmpty()) {
            return;
        }
        ops.add(() -> {
            String text = candidates.get(rnd.nextInt(candidates.size()));
            Expr sortExpr = parseExpr(text);
            if (sortExpr == null) {
                return null;
            }
            boolean asc = rnd.nextBoolean();
            Boolean nullsFirst = rnd.nextInt(3) == 0 ? Boolean.valueOf(rnd.nextBoolean()) : null;
            List<OrderByElement> sortClause = new ArrayList<>();
            sortClause.add(new OrderByElement(sortExpr, asc, nullsFirst));
            select.setOrderBy(sortClause);
            return at + ": add ORDER BY " + text + (asc ? " ASC" : " DESC")
                    + (nullsFirst == null ? "" : nullsFirst ? " NULLS FIRST" : " NULLS LAST");
        });
    }

    private static void offerLimit(SelectRelation select, Random rnd, List<Op> ops, String at) {
        if (select.getLimit() != null) {
            String rendered = renderLimit(select.getLimit());
            // A LimitElement that renders to nothing (the -1 sentinel, or a non-literal bound the
            // deparser cannot print) would make removal a textual no-op.
            if (rendered != null && !rendered.trim().isEmpty()) {
                ops.add(() -> {
                    select.setLimit(null);
                    return at + ": remove" + rendered;
                });
            }
            return;
        }
        ops.add(() -> {
            long limit = LIMIT_VALUES[rnd.nextInt(LIMIT_VALUES.length)];
            long offset = OFFSET_VALUES[rnd.nextInt(OFFSET_VALUES.length)];
            select.setLimit(new LimitElement(offset, limit));
            return at + ": add LIMIT " + (offset == 0 ? "" : offset + ", ") + limit;
        });
    }

    private static void offerDistinct(SelectList selectList, List<Op> ops, String at) {
        if (selectList == null) {
            return;
        }
        if (selectList.isDistinct()) {
            ops.add(() -> {
                selectList.setIsDistinct(false);
                return at + ": remove DISTINCT";
            });
        } else {
            ops.add(() -> {
                selectList.setIsDistinct(true);
                return at + ": add DISTINCT";
            });
        }
    }

    private static void offerWindow(SelectRelation select, SelectList selectList, AstMutationFuzzerTest.Pool pool,
                                    Random rnd, List<Op> ops, String at) {
        if (selectList == null || selectList.getItems() == null) {
            return;
        }
        List<Integer> analytic = new ArrayList<>();
        List<Integer> plain = new ArrayList<>();
        for (int i = 0; i < selectList.getItems().size(); i++) {
            SelectListItem item = selectList.getItems().get(i);
            if (item.isStar() || item.getExpr() == null) {
                continue;
            }
            if (item.getExpr() instanceof AnalyticExpr) {
                analytic.add(i);
            } else if (!containsAggregateOrWindow(item.getExpr())) {
                plain.add(i);
            }
        }

        if (!analytic.isEmpty()) {
            // Dropping the OVER only works where the bare call is still an expression. The grammar's
            // `windowFunction over` production makes OVER mandatory for the rank family (ROW_NUMBER,
            // RANK, DENSE_RANK, NTILE, LEAD, LAG, FIRST_VALUE, ...), so `row_number()` on its own is not
            // parseable at all and this op simply declines on such an item. Left as a reparse check
            // rather than a name list: the grammar is where that rule lives, and it moves.
            ops.add(() -> {
                int index = analytic.get(rnd.nextInt(analytic.size()));
                SelectListItem item = selectList.getItems().get(index);
                AnalyticExpr window = (AnalyticExpr) item.getExpr();
                String before = render(window);
                if (window.getFnCall() == null) {
                    return null;
                }
                String text = render(window.getFnCall());
                Expr replacement = parseExpr(text);
                if (replacement == null) {
                    return null;
                }
                item.setExpr(replacement);
                return at + ": remove OVER clause from item " + index + " (" + before + " -> " + text + ")";
            });
        }

        // A window function alongside an aggregation or a GROUP BY is rejected before any rule runs.
        if (plain.isEmpty() || select.hasGroupByClause() || hasAggregateItem(selectList)) {
            return;
        }
        ops.add(() -> {
            int index = plain.get(rnd.nextInt(plain.size()));
            SelectListItem item = selectList.getItems().get(index);
            String argument = render(item.getExpr());
            String text = buildWindowText(argument, partitionText(selectList, pool, rnd, index), rnd);
            Expr replacement = parseExpr(text);
            if (replacement == null) {
                return null;
            }
            item.setExpr(replacement);
            return at + ": add window over item " + index + " (" + argument + " -> " + text + ")";
        });
    }

    private static String buildWindowText(String argument, String partition, Random rnd) {
        StringBuilder sb = new StringBuilder();
        if (rnd.nextInt(3) == 0) {
            sb.append(WINDOW_RANKERS[rnd.nextInt(WINDOW_RANKERS.length)]).append("() OVER (");
            if (partition != null) {
                sb.append("PARTITION BY ").append(partition).append(" ");
            }
            sb.append("ORDER BY ").append(argument).append(")");
        } else {
            sb.append(WINDOW_AGGREGATES[rnd.nextInt(WINDOW_AGGREGATES.length)])
                    .append("(").append(argument).append(") OVER (");
            if (partition != null) {
                sb.append("PARTITION BY ").append(partition);
            }
            sb.append(")");
        }
        return sb.toString();
    }

    // --------------------------------------------------------------- helpers

    /** Grouping keys taken from the block's own non-aggregate select items, so the mutant can analyze. */
    private static List<String> groupableTexts(SelectList selectList, AstMutationFuzzerTest.Pool pool) {
        List<String> texts = new ArrayList<>();
        for (SelectListItem item : selectList.getItems()) {
            if (item.isStar() || item.getExpr() == null || containsAggregateOrWindow(item.getExpr())) {
                continue;
            }
            String text = render(item.getExpr());
            if (isRenderable(text) && !texts.contains(text) && texts.size() < MAX_GROUP_BY_KEYS) {
                texts.add(text);
            }
        }
        if (texts.isEmpty() && !pool.columnNames.isEmpty()) {
            texts.add("`" + pool.columnNames.get(0) + "`");
        }
        return texts;
    }

    private static List<String> sortableTexts(SelectList selectList, AstMutationFuzzerTest.Pool pool) {
        List<String> texts = new ArrayList<>();
        if (selectList != null && selectList.getItems() != null) {
            for (SelectListItem item : selectList.getItems()) {
                if (item.isStar() || item.getExpr() == null) {
                    continue;
                }
                String text = render(item.getExpr());
                if (isRenderable(text) && !texts.contains(text)) {
                    texts.add(text);
                }
            }
        }
        if (texts.isEmpty()) {
            for (String column : pool.columnNames) {
                texts.add("`" + column + "`");
            }
        }
        return texts;
    }

    private static String partitionText(SelectList selectList, AstMutationFuzzerTest.Pool pool, Random rnd,
                                        int skipIndex) {
        if (rnd.nextBoolean()) {
            return null;
        }
        List<String> texts = new ArrayList<>();
        for (int i = 0; i < selectList.getItems().size(); i++) {
            SelectListItem item = selectList.getItems().get(i);
            if (i == skipIndex || item.isStar() || item.getExpr() == null
                    || containsAggregateOrWindow(item.getExpr())) {
                continue;
            }
            String text = render(item.getExpr());
            if (isRenderable(text)) {
                texts.add(text);
            }
        }
        if (texts.isEmpty() && !pool.columnNames.isEmpty()) {
            texts.add("`" + pool.columnNames.get(rnd.nextInt(pool.columnNames.size())) + "`");
        }
        return texts.isEmpty() ? null : texts.get(rnd.nextInt(texts.size()));
    }

    private static boolean hasStarItem(SelectList selectList) {
        return selectList.getItems().stream().anyMatch(SelectListItem::isStar);
    }

    private static boolean hasAggregateItem(SelectList selectList) {
        if (selectList == null || selectList.getItems() == null) {
            return false;
        }
        return selectList.getItems().stream()
                .anyMatch(item -> !item.isStar() && item.getExpr() != null
                        && containsAggregateAnywhere(item.getExpr()));
    }

    /** An AnalyticExpr wraps an aggregate but is not one, so window items do not qualify a HAVING. */
    private static boolean containsAggregateAnywhere(Expr expr) {
        if (expr == null || expr instanceof AnalyticExpr) {
            return false;
        }
        if (isAggregateCall(expr)) {
            return true;
        }
        for (Expr child : expr.getChildren()) {
            if (containsAggregateAnywhere(child)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsAggregateOrWindow(Expr expr) {
        if (expr == null) {
            return false;
        }
        if (expr instanceof AnalyticExpr || isAggregateCall(expr)) {
            return true;
        }
        for (Expr child : expr.getChildren()) {
            if (containsAggregateOrWindow(child)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isAggregateCall(Expr expr) {
        if (!(expr instanceof FunctionCallExpr)) {
            return false;
        }
        String name = ((FunctionCallExpr) expr).getFunctionName();
        return name != null && AGGREGATE_NAMES.contains(name.toLowerCase());
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

    private static String renderGroupBy(GroupByClause clause) {
        if (clause == null) {
            return "<null>";
        }
        try {
            String sql = AstToSQLBuilder.toSQL(clause);
            if (sql != null && !sql.trim().isEmpty()) {
                return sql.replace('\n', ' ');
            }
        } catch (Throwable ignored) {
            // fall through to the clause's own renderer
        }
        try {
            return clause.toSql().replace('\n', ' ');
        } catch (Throwable t) {
            return "<GroupByClause>";
        }
    }

    private static String renderOrderBy(List<OrderByElement> sortClause) {
        List<String> parts = new ArrayList<>();
        for (OrderByElement element : sortClause) {
            parts.add(render(element == null ? null : element.getExpr()));
        }
        return String.join(", ", parts);
    }

    /** @return the deparsed LIMIT (leading space included), or null when it cannot be rendered */
    private static String renderLimit(LimitElement limit) {
        try {
            return AstToSQLBuilder.toSQL(limit);
        } catch (Throwable t) {
            return null;
        }
    }
}
