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

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.GroupByClause;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.ast.expression.AnalyticExpr;
import com.starrocks.sql.ast.expression.ArrowExpr;
import com.starrocks.sql.ast.expression.BetweenPredicate;
import com.starrocks.sql.ast.expression.CaseExpr;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.CollectionElementExpr;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.ExistsPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.IsNullPredicate;
import com.starrocks.sql.ast.expression.LambdaFunctionExpr;
import com.starrocks.sql.ast.expression.LikePredicate;
import com.starrocks.sql.ast.expression.SubfieldExpr;
import com.starrocks.sql.ast.expression.Subquery;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * What a statement CONTAINS, read straight off the AST, as coverage-map elements.
 *
 * <p>This is the third signal, and it is the only one available before the optimizer runs. That
 * matters more than it sounds: {@link RuleTrace} and {@link PlanShape} both require a plan, so a
 * mutant the planner rejects produces no coverage at all and the fuzzer learns nothing from it --
 * even though the shape it was reaching for is exactly the kind that finds planner defects. Feature
 * elements are computable from any mutant that PARSES, so a rejected mutant still says "this run
 * has now tried a correlated EXISTS under a LATERAL join", and rarity accounting still works.
 *
 * <p>It is also the vocabulary the corpus is measured in. "89% of seeds have no join" and "96.5%
 * have no CTE" are statements about features, not about rules or plans, and steering the mutator
 * needs the axis those numbers live on.
 *
 * <p><b>Bounded on purpose.</b> Scalar function names are NOT emitted. There are hundreds, they
 * would dominate the element vocabulary, and rarity is a share of a fixed budget: a thousand
 * one-off function names would make every genuinely rare structural element look common by
 * comparison. Aggregate and window function names ARE emitted, because which aggregate appears
 * changes what the optimizer may do with it (split, push down, rewrite to a meta scan) rather than
 * just what the expression evaluates to.
 */
public final class SqlFeatures {

    /** Guards against a pathological AST turning feature extraction into the expensive step. */
    private static final int MAX_ELEMENTS = 256;
    private static final int MAX_DEPTH = 32;

    /**
     * The aggregates worth naming: the ones the optimizer treats differently, not the ones users
     * type most.
     *
     * <p>Each of these reaches a distinct rewrite -- split aggregation, the meta-scan rewrite for
     * min/max/count, the multi-distinct rewrites, the bitmap and HLL union paths. A scalar function
     * generally reaches none, which is why the vocabulary stops here.
     */
    private static final Set<String> AGGREGATES = new HashSet<>(Arrays.asList(
            FunctionSet.COUNT, FunctionSet.SUM, FunctionSet.AVG, FunctionSet.MIN, FunctionSet.MAX,
            FunctionSet.GROUP_CONCAT, FunctionSet.ARRAY_AGG, FunctionSet.ANY_VALUE,
            FunctionSet.STDDEV, FunctionSet.VARIANCE, FunctionSet.MULTI_DISTINCT_COUNT,
            FunctionSet.HLL_UNION_AGG, FunctionSet.BITMAP_UNION, FunctionSet.PERCENTILE_APPROX,
            FunctionSet.WINDOW_FUNNEL));

    private SqlFeatures() {
    }

    /**
     * Features of one statement, de-duplicated.
     *
     * <p>A set, not a list: the coverage map counts how many MUTANTS reached an element, and a
     * statement with eight joins should raise the count for {@code join} once, not eight times.
     * Volume is captured by the bucketed counts below instead, which is the honest way to say "this
     * one had many" without letting one statement dominate the map.
     */
    public static Set<String> of(QueryStatement stmt) {
        Set<String> out = new LinkedHashSet<>();
        if (stmt == null) {
            return out;
        }
        Counters c = new Counters();
        collect(stmt.getQueryRelation(), out, c, 0);

        add(out, "depth:" + bucket(c.maxDepth));
        add(out, "joins:" + bucket(c.joins));
        add(out, "subqueries:" + bucket(c.subqueries));
        if (c.aggregates > 0) {
            add(out, "aggs:" + bucket(c.aggregates));
        }
        return out;
    }

    /** Mutable tallies threaded through the walk, for the bucketed volume features. */
    private static final class Counters {
        int joins;
        int subqueries;
        int aggregates;
        int maxDepth;
    }

    private static void collect(Relation relation, Set<String> out, Counters c, int depth) {
        if (relation == null || depth > MAX_DEPTH || out.size() > MAX_ELEMENTS) {
            return;
        }
        c.maxDepth = Math.max(c.maxDepth, depth);

        if (relation instanceof QueryRelation) {
            QueryRelation query = (QueryRelation) relation;
            for (CTERelation cte : query.getCteRelations()) {
                add(out, "cte");
                collect(cte.getCteQueryStatement().getQueryRelation(), out, c, depth + 1);
            }
            if (query.hasOrderByClause()) {
                add(out, "orderby");
            }
            if (query.hasLimit()) {
                add(out, "limit");
            }
            if (query.hasOffset()) {
                add(out, "offset");
            }
        }

        if (relation instanceof SelectRelation) {
            SelectRelation select = (SelectRelation) relation;
            if (select.isDistinct()) {
                add(out, "distinct");
            }
            if (select.hasWhereClause()) {
                add(out, "where");
                collectExpr(select.getWhereClause(), out, c, 0);
            }
            if (select.hasGroupByClause()) {
                add(out, "groupby");
                GroupByClause groupBy = select.getGroupByClause();
                if (groupBy.getGroupingType() != null
                        && groupBy.getGroupingType() != GroupByClause.GroupingType.GROUP_BY) {
                    // ROLLUP/CUBE/GROUPING SETS each expand to a different number of aggregation
                    // branches, which is a distinct optimizer path rather than a spelling.
                    add(out, "groupby:" + groupBy.getGroupingType().name());
                }
                if (groupBy.getGroupingExprs() != null) {
                    for (Expr e : groupBy.getGroupingExprs()) {
                        collectExpr(e, out, c, 0);
                    }
                }
            }
            if (select.hasHavingClause()) {
                add(out, "having");
                collectExpr(select.getHavingClause(), out, c, 0);
            }
            // The select list, NOT getOutputExpression(): output expressions, the aggregate list and
            // hasAnalyticInfo() are all filled in by the analyzer, and this runs on a tree that has
            // only been parsed. Reading them returned null and took the whole corpus file down with
            // it -- the same trap the mutation operators carry a rule about.
            if (select.getSelectList() != null && select.getSelectList().getItems() != null) {
                for (SelectListItem item : select.getSelectList().getItems()) {
                    if (!item.isStar()) {
                        collectExpr(item.getExpr(), out, c, 0);
                    }
                }
            }
            collect(select.getRelation(), out, c, depth + 1);
        } else if (relation instanceof JoinRelation) {
            JoinRelation join = (JoinRelation) relation;
            c.joins++;
            add(out, "join:" + join.getJoinOp().name());
            if (join.isLateral()) {
                add(out, "lateral");
            }
            if (join.getOnPredicate() == null) {
                // A join with no ON is a cross join however it was spelled, and it reaches a
                // different part of the optimizer than one the join-order rules can reassociate.
                add(out, "join:noon");
            } else {
                collectExpr(join.getOnPredicate(), out, c, 0);
            }
            if (join.getJoinHint() != null && !join.getJoinHint().isEmpty()) {
                add(out, "joinhint:" + join.getJoinHint());
            }
            collect(join.getLeft(), out, c, depth + 1);
            collect(join.getRight(), out, c, depth + 1);
        } else if (relation instanceof SubqueryRelation) {
            c.subqueries++;
            add(out, "derived");
            collect(((SubqueryRelation) relation).getQueryStatement().getQueryRelation(), out, c, depth + 1);
        } else if (relation instanceof SetOperationRelation) {
            SetOperationRelation setop = (SetOperationRelation) relation;
            add(out, "setop:" + setop.getClass().getSimpleName()
                    + (setop.getQualifier() == null ? "" : ":" + setop.getQualifier()));
            for (QueryRelation child : setop.getRelations()) {
                collect(child, out, c, depth + 1);
            }
        } else if (relation instanceof CTERelation) {
            collect(((CTERelation) relation).getCteQueryStatement().getQueryRelation(), out, c, depth + 1);
        } else if (relation instanceof TableFunctionRelation) {
            add(out, "tablefunction");
        } else if (relation instanceof ValuesRelation) {
            add(out, "values");
        } else if (relation instanceof ViewRelation) {
            add(out, "view");
            collect(((ViewRelation) relation).getQueryStatement().getQueryRelation(), out, c, depth + 1);
        }
    }

    private static void collectExpr(Expr expr, Set<String> out, Counters c, int depth) {
        if (expr == null || depth > MAX_DEPTH || out.size() > MAX_ELEMENTS) {
            return;
        }

        if (expr instanceof Subquery) {
            c.subqueries++;
            add(out, "subquery");
            QueryStatement inner = ((Subquery) expr).getQueryStatement();
            if (inner != null) {
                collect(inner.getQueryRelation(), out, c, depth + 1);
            }
            return;
        } else if (expr instanceof ExistsPredicate) {
            add(out, "exists" + (((ExistsPredicate) expr).isNotExists() ? ":not" : ""));
        } else if (expr instanceof InPredicate) {
            add(out, "in" + (((InPredicate) expr).isNotIn() ? ":not" : ""));
        } else if (expr instanceof AnalyticExpr) {
            add(out, "window");
            add(out, "windowfn:" + lower(((AnalyticExpr) expr).getFnCall().getFunctionName()));
        } else if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr call = (FunctionCallExpr) expr;
            // Matched by NAME against a fixed list, not by isAggregateFunction(): that method asserts
            // the function has been RESOLVED and throws outright on a parsed-but-unanalyzed tree,
            // which is every tree this class sees.
            //
            // Only aggregates are named at all. See the class comment: naming every scalar function
            // would flood the vocabulary and destroy the rarity signal for everything else.
            String name = lower(call.getFunctionName());
            if (AGGREGATES.contains(name)) {
                c.aggregates++;
                add(out, "agg:" + name + (call.getParams() != null && call.getParams().isDistinct()
                        ? ":distinct" : ""));
            }
        } else if (expr instanceof CaseExpr) {
            add(out, "case");
        } else if (expr instanceof CastExpr) {
            add(out, "cast");
        } else if (expr instanceof LikePredicate) {
            add(out, "like");
        } else if (expr instanceof BetweenPredicate) {
            add(out, "between");
        } else if (expr instanceof IsNullPredicate) {
            add(out, "isnull");
        } else if (expr instanceof CompoundPredicate) {
            add(out, "compound:" + ((CompoundPredicate) expr).getOp().name());
        } else if (expr instanceof ArrowExpr || expr instanceof SubfieldExpr
                || expr instanceof CollectionElementExpr) {
            // The complex-type accessors, which reach the subfield pruning and type-inference paths
            // that several confirmed defects have come out of.
            add(out, "complextype");
        } else if (expr instanceof LambdaFunctionExpr) {
            add(out, "lambda");
        }

        for (Expr child : expr.getChildren()) {
            collectExpr(child, out, c, depth + 1);
        }
    }

    /**
     * Powers of two, so "3 joins" and "4 joins" are not separate elements.
     *
     * <p>Exact counts would make the vocabulary unbounded in a dimension where the optimizer does
     * not actually behave differently -- the interesting distinction is none / one / a few / many,
     * not seven versus eight.
     */
    private static String bucket(int n) {
        if (n <= 0) {
            return "0";
        }
        if (n == 1) {
            return "1";
        }
        if (n <= 3) {
            return "2-3";
        }
        if (n <= 7) {
            return "4-7";
        }
        return n <= 15 ? "8-15" : "16+";
    }

    private static void add(Set<String> out, String feature) {
        if (out.size() <= MAX_ELEMENTS) {
            out.add("F:" + feature);
        }
    }

    private static String lower(String s) {
        return s == null ? "?" : s.toLowerCase();
    }
}
