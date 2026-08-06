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
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.ArraySliceExpr;
import com.starrocks.sql.ast.expression.ArrowExpr;
import com.starrocks.sql.ast.expression.CollectionElementExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.MapExpr;
import com.starrocks.sql.ast.expression.SubfieldExpr;
import com.starrocks.sql.parser.SqlParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * M7 — type stress, as enumerated in SQL_AST_FUZZER_PLAN.md §2.
 *
 * <p>Picks one expression position and wraps whatever sits there in a complex-type accessor: a CAST
 * chain, an array subscript, a map key lookup, a struct field access, a collection function, or a
 * JSON path step. The wrapped expression is built as <b>text</b> and reparsed, so no
 * {@code Expr} instance is ever shared between two trees and the injected fragment is always a shape
 * the grammar can actually produce.
 *
 * <p>The point is not to build queries that analyze. It is to reach the analyzer, the type system and
 * the subfield pruner with trees that are <em>well-formed but type-hostile</em>: {@code .field} on a
 * scalar, {@code [0]} on a one-based array, {@code CAST(<array> AS MAP<JSON, JSON>)},
 * {@code DECIMAL(38, 38)}, three levels of nested {@code STRUCT}. Those are the inputs that
 * historically break StarRocks' complex-type paths; a mutant the analyzer rejects with a clean
 * {@code SemanticException} costs nothing, while one that throws an internal error is the finding.
 *
 * <p>Contract notes (see {@link Mutation}):
 * <ul>
 *   <li>The tree is <b>unanalyzed</b>, so the wrap is chosen from the node class and the rendered text,
 *       never from {@code getType()}.</li>
 *   <li>If the inner expression cannot be rendered, the operator returns null instead of injecting
 *       something malformed — {@code AstToSQLBuilder} is not reliable before analysis.</li>
 *   <li>Positions the grammar fixes are skipped through {@link MutationRules}, exactly as the driver's
 *       own site collection does.</li>
 * </ul>
 */
public class TypeStressMutation implements Mutation {

    private static final long SQL_MODE = SqlModeHelper.MODE_DEFAULT;

    /** Rendering an expression longer than this and wrapping it again only grows the mutant. */
    private static final int MAX_INNER_TEXT = 300;

    /** The wrap families. Ordered, and every entry must be reachable — see TypeStressMutationTest. */
    enum Shape {
        /** {@code CAST(CAST(e AS VARCHAR) AS ARRAY<INT>)}, depth 1..3, across the whole type system. */
        CAST_CHAIN,
        /** {@code (e)[1]}, {@code (e)[0]}, {@code (e)[-1]} — arrays are one-based, so 0 is hostile. */
        ARRAY_SUBSCRIPT,
        /** {@code (e)['k']} — same grammar as the subscript, a different type demand. */
        MAP_KEY,
        /** {@code (e).a} — SubfieldExpr, the shape that historically explodes on a non-struct. */
        STRUCT_FIELD,
        /** {@code array_length(e)}, {@code map_keys(e)}, {@code unnest(e)}. */
        COLLECTION_FN,
        /** {@code CAST(e AS JSON) -> '$.a'} and friends, the flat-JSON / subfield-pruning path. */
        JSON_PATH,
        /**
         * {@code CASE WHEN p THEN e ELSE alt END}, {@code if(p, e, alt)}, {@code nullif}, and friends.
         *
         * <p>The mutator could not build a conditional at all before this. It could edit inside one the
         * seed happened to contain -- the branches are ordinary expression sites -- but a corpus without
         * a CASE meant no CASE, and {@code if} was reachable only by renaming a three-argument call into
         * it. Measured on the emitted corpus: every one of its 12522 CASE WHEN and 4553 {@code if(} came
         * from a seed.
         *
         * <p>Conditionals are where branch type unification meets null propagation, which is the point
         * of putting them in this operator rather than a new one. The alternative branch is drawn from
         * a different bucket than the expression it wraps, so the branches usually disagree on type and
         * the unification has to do something with that.
         */
        CONDITIONAL
    }

    private static final Shape[] SHAPES = Shape.values();

    /**
     * Cast targets, deliberately weighted towards the boundaries.
     *
     * <p>Scalars are here so a cast chain can cross a type boundary on the way, but the interesting
     * entries are the nested complex types and the DECIMAL precision/scale extremes.
     *
     * <p>Every entry must PARSE — the type system's own guards are enforced in the parser, not the
     * analyzer, so a target it refuses ({@code MAP<JSON, JSON>}: "Unsupported type specification: 'JSON'
     * for map's key") is not a hostile mutation but a dead one, and the operator would silently return
     * null forever. TypeStressMutationTest walks this table entry by entry for exactly that reason.
     */
    static final String[] CAST_TARGETS = {
            "BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "LARGEINT",
            "FLOAT", "DOUBLE", "VARCHAR", "VARCHAR(1)", "CHAR(1)", "STRING",
            "DATE", "DATETIME", "TIME", "JSON", "VARBINARY",
            "DECIMAL(1, 0)", "DECIMAL(38, 0)", "DECIMAL(38, 38)", "DECIMAL(38, 37)", "DECIMAL(27, 9)",
            "ARRAY<INT>", "ARRAY<VARCHAR(1)>", "ARRAY<ARRAY<INT>>", "ARRAY<MAP<VARCHAR(16), INT>>",
            "ARRAY<STRUCT<a INT, b VARCHAR(8)>>", "ARRAY<JSON>",
            "MAP<VARCHAR(16), INT>", "MAP<INT, ARRAY<INT>>", "MAP<VARCHAR(16), STRUCT<a INT>>",
            "MAP<VARCHAR(16), MAP<INT, ARRAY<JSON>>>",
            "STRUCT<a INT>", "STRUCT<a INT, b VARCHAR(8)>", "STRUCT<a ARRAY<INT>, b MAP<INT, INT>>",
            "STRUCT<a STRUCT<b STRUCT<c INT>>>",
    };

    /** One-based arrays make 0 an error and negatives count from the end; both are worth probing. */
    static final String[] SUBSCRIPTS = {
            "1", "0", "-1", "2", "2147483647", "-2147483648", "CAST(NULL AS INT)",
    };

    static final String[] MAP_KEYS = {
            "'k'", "''", "'a.b'", "'0'", "CAST(NULL AS VARCHAR)", "1",
    };

    /**
     * Conditional wraps. {@code %1$s} is the expression being wrapped, {@code %2$s} a predicate,
     * {@code %3$s} the alternative branch.
     *
     * <p>Chosen for the distinctions the analyzer actually has to make, not for variety:
     * the searched CASE and the simple CASE are different code paths; a CASE with no ELSE is nullable
     * by construction where one with an ELSE need not be; {@code nullif} produces a NULL the other
     * forms cannot; and the nested entry is here because 4167 of the corpus's CASEs are nested, so it
     * is a real shape rather than a fuzzer invention.
     */
    static final String[] CONDITIONAL_FORMS = {
            "CASE WHEN %2$s THEN %1$s ELSE %3$s END",
            "CASE WHEN %2$s THEN %1$s END",
            "CASE WHEN %2$s THEN %1$s WHEN NOT (%2$s) THEN %3$s ELSE NULL END",
            "CASE %1$s WHEN %3$s THEN 1 ELSE 0 END",
            "if(%2$s, %1$s, %3$s)",
            "ifnull(%1$s, %3$s)",
            "nullif(%1$s, %3$s)",
            "coalesce(%1$s, %3$s, NULL)",
            "CASE WHEN %2$s THEN CASE WHEN %2$s THEN %1$s ELSE NULL END ELSE %3$s END",
    };

    /** Used when the pool has no boolean fragment to serve as the condition. */
    private static final String[] FALLBACK_PREDICATES = {
            "TRUE", "FALSE", "NULL", "1 = 1", "NULL IS NULL",
    };

    /**
     * Used when the pool has no scalar fragment for the other branch.
     *
     * <p>Deliberately spread across types: a conditional whose branches agree is the case that already
     * works, and the unification is what this shape exists to reach.
     *
     * <p>{@code []} is deliberately absent. An untyped empty array carries no type until analysis, and
     * the deparser dereferences that type -- so the mutant dies at the gate's deparse with an NPE rather
     * than reaching the oracle. M3 still injects {@code []} through BOUNDARY_LITERALS, so the shape is
     * not lost; it just has no business being in a branch that has to render first.
     */
    private static final String[] FALLBACK_BRANCHES = {
            "NULL", "0", "''", "CAST(NULL AS JSON)", "CAST(NULL AS ARRAY<INT>)", "'1970-01-01'",
    };

    /** A pooled fragment long enough to dominate the mutant is not worth putting in a branch. */
    private static final int MAX_BRANCH_TEXT = 120;

    /** Field names that mostly do not exist, which is the point: subfield access on a non-struct. */
    static final String[] SUBFIELD_NAMES = {
            "a", "b", "c", "col", "`a`", "`no such field`",
    };

    static final String[] COLLECTION_FNS = {
            "array_length(%s)", "cardinality(%s)", "array_sum(%s)", "array_flatten(%s)",
            "array_slice(%s, 1, 3)", "array_distinct(%s)", "array_sort(%s)",
            "map_size(%s)", "map_keys(%s)", "map_values(%s)",
            // No element_at: the parser folds it straight into a CollectionElementExpr, so it builds the
            // identical node as ARRAY_SUBSCRIPT / MAP_KEY and would only double-count that family.
            // UNNEST is a table function; naming it in a scalar position is exactly the well-formed
            // but hostile input this operator exists to produce.
            "unnest(%s)",
    };

    /**
     * The left operand is always a CAST so the arrow cannot be re-read as a lambda: the grammar has
     * {@code primaryExpression ARROW string} and {@code identifierList '->' expression} side by side,
     * and {@code (c) -> 'k'} sits in the overlap.
     */
    static final String[] JSON_PATHS = {
            "CAST(%s AS JSON) -> 'k'", "CAST(%s AS JSON) -> '$.a'", "CAST(%s AS JSON) -> '$'",
            "json_query(CAST(%s AS JSON), '$.a')", "get_json_string(CAST(%s AS JSON), '$.a')",
            "json_length(CAST(%s AS JSON))", "json_each(CAST(%s AS JSON))",
    };

    @Override
    public String name() {
        return "M7-typestress";
    }

    /**
     * Complex-type accessors and the conditional/cast expressions around them -- the subfield
     * pruning and type-inference paths several confirmed defects have come out of.
     */
    @Override
    public Set<String> coverageTargets() {
        return TARGETS;
    }

    private static final Set<String> TARGETS = Set.of(
            "F:complextype", "F:case", "F:cast", "F:lambda", "F:tablefunction");

    @Override
    public String apply(QueryStatement stmt, AstMutationFuzzerTest.Pool pool, Random rnd) {
        return apply(stmt, pool, rnd, null);
    }

    /**
     * Visible for testing: run the operator with one wrap family forced.
     *
     * @param forced the family to inject, or null to let the operator choose
     */
    String apply(QueryStatement stmt, AstMutationFuzzerTest.Pool pool, Random rnd, Shape forced) {
        List<Site> sites = collectSites(stmt);
        if (sites.isEmpty()) {
            return null;
        }
        Site site = pickSite(sites, rnd);
        Expr current = site.current();
        if (current == null) {
            return null;
        }

        String inner = render(current);
        if (inner == null || inner.length() > MAX_INNER_TEXT) {
            return null;
        }

        Shape shape = forced != null ? forced : chooseShape(current, rnd);
        Wrap wrap = buildWrap(inner, shape, pool, rnd);
        if (wrap == null) {
            return null;
        }

        Expr replacement;
        try {
            replacement = SqlParser.parseSqlToExpr(wrap.text, SQL_MODE);
        } catch (Throwable t) {
            return null;
        }
        if (replacement == null) {
            return null;
        }
        // The marker is how the injected fragment READS BACK, not how it was written. The two differ
        // more often than one would guess: the parser folds element_at(m, 'k') into m['k'], and the
        // deparser renders a cast target through Type#toString, so STRING comes back as VARCHAR(65533)
        // and DECIMAL(38, 38) as DECIMAL128(38,38). Recording what was written would make the report
        // point at a substring that is not in the mutant.
        String marker = render(replacement);
        if (marker == null) {
            return null;
        }
        site.replace(replacement);
        return shape + " at " + site.label() + ": " + inner + " -> " + wrap.text + " || marker: " + marker;
    }

    // ------------------------------------------------------------------ wraps

    /** The family that was chosen and the exact text this operator injects. */
    static final class Wrap {
        final Shape shape;
        final String text;

        Wrap(Shape shape, String text) {
            this.shape = shape;
            this.text = text;
        }
    }

    /** Visible for testing: the exact expression text the operator would inject around {@code inner}. */
    static Wrap buildWrap(String inner, Shape shape, AstMutationFuzzerTest.Pool pool, Random rnd) {
        String p = "(" + inner + ")";
        switch (shape) {
            case CAST_CHAIN: {
                int depth = 1 + rnd.nextInt(3);
                String text = p;
                for (int i = 0; i < depth; i++) {
                    text = "CAST(" + text + " AS " + CAST_TARGETS[rnd.nextInt(CAST_TARGETS.length)] + ")";
                }
                return new Wrap(shape, text);
            }
            case ARRAY_SUBSCRIPT:
                return new Wrap(shape, p + "[" + SUBSCRIPTS[rnd.nextInt(SUBSCRIPTS.length)] + "]");
            case MAP_KEY:
                return new Wrap(shape, p + "[" + MAP_KEYS[rnd.nextInt(MAP_KEYS.length)] + "]");
            case STRUCT_FIELD:
                return new Wrap(shape, p + "." + pickSubfieldName(pool, rnd));
            case COLLECTION_FN:
                return new Wrap(shape,
                        String.format(COLLECTION_FNS[rnd.nextInt(COLLECTION_FNS.length)], p));
            case JSON_PATH:
                return new Wrap(shape, String.format(JSON_PATHS[rnd.nextInt(JSON_PATHS.length)], p));
            case CONDITIONAL:
                return new Wrap(shape, String.format(
                        CONDITIONAL_FORMS[rnd.nextInt(CONDITIONAL_FORMS.length)],
                        p, pickBranch(pool, true, rnd), pickBranch(pool, false, rnd)));
            default:
                return null;
        }
    }

    /**
     * A fragment for a conditional's condition or its other branch.
     *
     * <p>Prefers the pool because a fragment naming real columns keeps the mutant analyzable far more
     * often than a literal does, and an unanalyzable mutant reaches nothing. The literals are the floor,
     * not the intent.
     */
    private static String pickBranch(AstMutationFuzzerTest.Pool pool, boolean wantPredicate, Random rnd) {
        String[] fallback = wantPredicate ? FALLBACK_PREDICATES : FALLBACK_BRANCHES;
        if (pool != null && rnd.nextInt(100) < 70) {
            List<String> bucket = pool.bucketFor(wantPredicate);
            List<String> usable = new ArrayList<>();
            for (String s : bucket) {
                if (s.length() <= MAX_BRANCH_TEXT) {
                    usable.add(s);
                }
            }
            if (!usable.isEmpty()) {
                return usable.get(rnd.nextInt(usable.size()));
            }
        }
        return fallback[rnd.nextInt(fallback.length)];
    }

    /**
     * Prefers a column name harvested from the seed over an invented one, so that roughly half the
     * struct accesses name something that exists somewhere in the query. A field that never exists
     * only ever produces the same "unknown subfield" rejection.
     */
    private static String pickSubfieldName(AstMutationFuzzerTest.Pool pool, Random rnd) {
        if (pool != null && !pool.columnNames.isEmpty() && rnd.nextInt(100) < 50) {
            return "`" + pool.columnNames.get(rnd.nextInt(pool.columnNames.size())) + "`";
        }
        return SUBFIELD_NAMES[rnd.nextInt(SUBFIELD_NAMES.length)];
    }

    /**
     * Chooses the family from the node class alone — the tree is unanalyzed, so the type is unknown.
     *
     * <p>When the position already holds a complex-type accessor, stacking another one on top is the
     * cheapest way to reach deeply nested complex types, which is where the defects are.
     */
    private static Shape chooseShape(Expr current, Random rnd) {
        if (isComplexAccessor(current) && rnd.nextInt(100) < 70) {
            Shape[] nesting = {Shape.ARRAY_SUBSCRIPT, Shape.MAP_KEY,
                    Shape.STRUCT_FIELD, Shape.COLLECTION_FN};
            return nesting[rnd.nextInt(nesting.length)];
        }
        return SHAPES[rnd.nextInt(SHAPES.length)];
    }

    private static boolean isComplexAccessor(Expr e) {
        return e instanceof CollectionElementExpr || e instanceof SubfieldExpr
                || e instanceof ArrayExpr || e instanceof MapExpr
                || e instanceof ArrowExpr || e instanceof ArraySliceExpr;
    }

    // ------------------------------------------------------------------ sites

    /** A replaceable expression position. */
    private abstract static class Site {
        abstract Expr current();

        abstract void replace(Expr replacement);

        abstract String label();
    }

    private static final class ChildSite extends Site {
        private final Expr parent;
        private final int index;

        ChildSite(Expr parent, int index) {
            this.parent = parent;
            this.index = index;
        }

        @Override
        Expr current() {
            return parent.getChild(index);
        }

        @Override
        void replace(Expr replacement) {
            parent.setChild(index, replacement);
        }

        @Override
        String label() {
            return parent.getClass().getSimpleName() + "[" + index + "]";
        }
    }

    private static final class SelectItemSite extends Site {
        private final SelectListItem item;
        private final int ordinal;

        SelectItemSite(SelectListItem item, int ordinal) {
            this.item = item;
            this.ordinal = ordinal;
        }

        @Override
        Expr current() {
            return item.getExpr();
        }

        @Override
        void replace(Expr replacement) {
            item.setExpr(replacement);
        }

        @Override
        String label() {
            return "SelectListItem[" + ordinal + "]";
        }
    }

    private static final class WhereSite extends Site {
        private final SelectRelation relation;

        WhereSite(SelectRelation relation) {
            this.relation = relation;
        }

        @Override
        Expr current() {
            return relation.getWhereClause();
        }

        @Override
        void replace(Expr replacement) {
            relation.setWhereClause(replacement);
        }

        @Override
        String label() {
            return "WhereClause";
        }
    }

    private static final class HavingSite extends Site {
        private final SelectRelation relation;

        HavingSite(SelectRelation relation) {
            this.relation = relation;
        }

        @Override
        Expr current() {
            return relation.getHavingClause();
        }

        @Override
        void replace(Expr replacement) {
            relation.setHaving(replacement);
        }

        @Override
        String label() {
            return "HavingClause";
        }
    }

    /**
     * Every position this operator may wrap.
     *
     * <p>Two kinds. The first mirrors the driver's own walk: any child of any expression reachable from
     * {@code collectRootExprs}, minus whatever {@link MutationRules} declares off limits. The second is
     * the clause roots themselves — a select item, a WHERE, a HAVING. The driver has no need for those
     * because it substitutes one expression for another, but wrapping is different: without the roots
     * a plain {@code SELECT c_arr FROM t} would have no site at all, since {@code c_arr} is a root with
     * no children, and exactly the columns worth stressing would go untouched.
     */
    private static List<Site> collectSites(QueryStatement stmt) {
        List<Site> out = new ArrayList<>();
        QueryRelation root = stmt.getQueryRelation();
        for (Expr expr : AstMutationFuzzerTest.collectRootExprs(root)) {
            collectChildSites(expr, out);
        }
        collectClauseSites(root, out, new ArrayList<>());
        return out;
    }

    private static void collectChildSites(Expr e, List<Site> out) {
        if (e == null || out.size() > 2000) {
            return;
        }
        for (int i = 0; i < e.getChildren().size(); i++) {
            if (MutationRules.get().isBlocked(e, i)) {
                continue;
            }
            out.add(new ChildSite(e, i));
            collectChildSites(e.getChild(i), out);
        }
    }

    /** Clause-root positions, following the same relation descent as {@code collectRootExprs}. */
    private static void collectClauseSites(Relation relation, List<Site> out, List<Relation> seen) {
        if (relation == null || seen.size() > 64) {
            return;
        }
        for (Relation r : seen) {
            if (r == relation) {
                return;
            }
        }
        seen.add(relation);

        if (relation instanceof QueryRelation && ((QueryRelation) relation).getCteRelations() != null) {
            for (CTERelation cte : ((QueryRelation) relation).getCteRelations()) {
                collectClauseSites(cte, out, seen);
            }
        }
        if (relation instanceof SelectRelation) {
            SelectRelation sel = (SelectRelation) relation;
            if (sel.getSelectList() != null && sel.getSelectList().getItems() != null) {
                List<SelectListItem> items = sel.getSelectList().getItems();
                for (int i = 0; i < items.size(); i++) {
                    SelectListItem item = items.get(i);
                    if (!item.isStar() && item.getExpr() != null) {
                        out.add(new SelectItemSite(item, i));
                    }
                }
            }
            if (sel.getWhereClause() != null) {
                out.add(new WhereSite(sel));
            }
            if (sel.getHavingClause() != null) {
                out.add(new HavingSite(sel));
            }
            collectClauseSites(sel.getRelation(), out, seen);
        } else if (relation instanceof SubqueryRelation) {
            collectClauseSites(((SubqueryRelation) relation).getQueryStatement().getQueryRelation(), out, seen);
        } else if (relation instanceof SetOperationRelation) {
            for (Relation child : ((SetOperationRelation) relation).getRelations()) {
                collectClauseSites(child, out, seen);
            }
        } else if (relation instanceof JoinRelation) {
            collectClauseSites(((JoinRelation) relation).getLeft(), out, seen);
            collectClauseSites(((JoinRelation) relation).getRight(), out, seen);
        } else if (relation instanceof CTERelation) {
            collectClauseSites(((CTERelation) relation).getCteQueryStatement().getQueryRelation(), out, seen);
        }
    }

    /**
     * Biases towards positions that already hold a complex-type accessor, so nesting compounds instead
     * of every mutant starting from a bare column.
     */
    private static Site pickSite(List<Site> sites, Random rnd) {
        if (rnd.nextInt(100) < 60) {
            List<Site> complex = new ArrayList<>();
            for (Site s : sites) {
                Expr e = s.current();
                if (e != null && isComplexAccessor(e)) {
                    complex.add(s);
                }
            }
            if (!complex.isEmpty()) {
                return complex.get(rnd.nextInt(complex.size()));
            }
        }
        return sites.get(rnd.nextInt(sites.size()));
    }

    /**
     * The inner expression as text. Null when the deparser cannot render it: before analysis it is
     * unreliable, and injecting a half-rendered fragment would report the mutator's own damage as a
     * StarRocks defect.
     */
    private static String render(Expr e) {
        String s;
        try {
            s = AstToSQLBuilder.toSQL(e);
        } catch (Throwable t) {
            return null;
        }
        if (s == null || s.trim().isEmpty()) {
            return null;
        }
        return s.replace('\n', ' ').trim();
    }
}
