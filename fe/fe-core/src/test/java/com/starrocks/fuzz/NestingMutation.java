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

import com.starrocks.catalog.TableName;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.NormalizedTableFunctionRelation;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.parser.SqlParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * M6 -- nesting wrap. Re-shapes one relation of the tree into a deeper relational form: a derived
 * table, a WITH clause, a UNION of two copies, or a self join. Target: the CTE and subquery rewrite
 * paths, which a flat seed corpus barely touches.
 *
 * <p><b>Copying is the whole problem.</b> Three of the four shapes need the chosen relation twice, and
 * handing the same {@code Relation} instance to both sides aliases it into two positions of one tree:
 * the analyzer writes a {@code Scope} and resolved slots into the node when it visits the first
 * position and overwrites them at the second, so the first half silently ends up bound to the wrong
 * side. Nothing about that shows up as an exception -- it shows up as a "finding" that is really the
 * mutator's own bug.
 *
 * <p>So no node is ever shared. Every shape is built as <b>text</b>, parsed once, and the relation the
 * parser hands back is dropped into the slot. Both sides of a UNION or a self join come out of that one
 * parse as separate subtrees, and nothing in the replacement is reachable from the original relation.
 * The price is that the operator depends on deparsing an <b>unanalyzed</b> relation, where the deparser
 * is least reliable; when that fails, or when the generated text does not parse, the operator returns
 * null instead of falling back to sharing.
 *
 * <p>Aliases: the wrapper reuses the wrapped relation's own resolved name wherever a single name can
 * carry the whole wrapper ({@code (SELECT * FROM t) t}), so qualified references in the enclosing query
 * keep resolving and the mutant reaches the analyzer instead of bouncing off it. Names that have to be
 * invented -- the second side of a self join, the CTE -- use the {@value #PREFIX} prefix and are checked
 * against the deparsed statement so they cannot collide with anything already in it.
 */
public class NestingMutation implements Mutation {

    private static final long SQL_MODE = SqlModeHelper.MODE_DEFAULT;

    /** Distinctive enough that a corpus identifier will not collide with it by accident. */
    private static final String PREFIX = "srfuzz_n";

    private static final Pattern SIMPLE_IDENT = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /** How many randomly picked slots to try before giving up on this statement. */
    private static final int MAX_ATTEMPTS = 8;

    /** The relational shapes this operator can produce. */
    enum Shape {
        /** {@code (SELECT * FROM R) alias} */
        SUBQUERY,
        /** {@code WITH cte AS (SELECT * FROM R) ... FROM cte}, statement level where the slot allows it. */
        CTE,
        /** {@code (SELECT * FROM R UNION ALL SELECT * FROM R) alias} */
        UNION_ALL,
        /** {@code (SELECT * FROM R UNION SELECT * FROM R) alias} */
        UNION_DISTINCT,
        /** {@code (SELECT * FROM R EXCEPT SELECT * FROM R) alias} */
        EXCEPT,
        /** {@code (SELECT * FROM R INTERSECT SELECT * FROM R) alias} */
        INTERSECT,
        /** {@code (SELECT * FROM R) a JOIN (SELECT * FROM R) b ON a.c = b.c}, or a cross join. */
        SELF_JOIN,
        /**
         * {@code (SELECT * FROM R) a ASOF LEFT JOIN (SELECT * FROM R) b ON a.c = b.c AND a.t > b.t}.
         *
         * <p>The analyzer requires an ON clause with at least one equality and exactly one temporal
         * inequality, so this shape can only be built by guessing which pooled column is temporal --
         * the tree is unanalyzed, so there are no types to consult. The guess is by name. When it is
         * wrong the analyzer rejects the mutant, which is the same cheap noise as a wrong join key.
         */
        ASOF_JOIN,
        /** {@code (SELECT * FROM R) a, unnest(...) t(x)} -- adds a table function to the FROM clause. */
        TABLE_FUNCTION
    }

    private static final Shape[] SHAPES = Shape.values();

    @Override
    public String name() {
        return "M6-nesting";
    }

    @Override
    public String apply(QueryStatement stmt, AstMutationFuzzerTest.Pool pool, Random rnd) {
        List<Slot> slots = slotsOf(stmt);
        if (slots.isEmpty()) {
            return null;
        }
        Collections.shuffle(slots, rnd);
        Shape shape = SHAPES[rnd.nextInt(SHAPES.length)];
        for (int i = 0; i < Math.min(MAX_ATTEMPTS, slots.size()); i++) {
            String applied = applyAt(stmt, slots.get(i), shape, joinCondition(pool, rnd));
            if (applied != null) {
                return applied;
            }
        }
        return null;
    }

    // ------------------------------------------------------------------ slots

    /**
     * A relation position that can be rewritten in place through an existing setter. Positions with no
     * setter -- notably {@link QueryStatement}'s own root relation -- are simply not offered, because the
     * alternative would be adding a setter to a production AST class.
     */
    static final class Slot {
        private final String description;
        private final Supplier<Relation> getter;
        private final Consumer<Relation> setter;
        /** True when no query-statement boundary separates this slot from the statement root. */
        private final boolean rootQueryLevel;
        /** True when the slot is a whole FROM clause rather than one operand of a join. */
        private final boolean wholeFromClause;

        private Slot(String description, Supplier<Relation> getter, Consumer<Relation> setter,
                     boolean rootQueryLevel, boolean wholeFromClause) {
            this.description = description;
            this.getter = getter;
            this.setter = setter;
            this.rootQueryLevel = rootQueryLevel;
            this.wholeFromClause = wholeFromClause;
        }

        Relation get() {
            return getter.get();
        }

        void set(Relation relation) {
            setter.accept(relation);
        }

        String describe() {
            return description;
        }

        boolean isWholeFromClause() {
            return wholeFromClause;
        }
    }

    /** Every rewritable relation position in {@code stmt}, outermost first. */
    static List<Slot> slotsOf(QueryStatement stmt) {
        List<Slot> out = new ArrayList<>();
        collect(stmt.getQueryRelation(), true, out);
        return out;
    }

    private static void collect(Relation relation, boolean rootLevel, List<Slot> out) {
        if (relation == null || out.size() > 256) {
            return;
        }
        if (relation instanceof QueryRelation) {
            for (CTERelation cte : ((QueryRelation) relation).getCteRelations()) {
                // A CTE body is its own query statement, so a statement-level WITH added here would not
                // be visible from inside it.
                collect(cte.getCteQueryStatement().getQueryRelation(), false, out);
            }
        }

        if (relation instanceof SelectRelation) {
            SelectRelation select = (SelectRelation) relation;
            if (isWrappable(select.getRelation())) {
                out.add(new Slot("SelectRelation.from", select::getRelation, select::setRelation, rootLevel, true));
            }
            collect(select.getRelation(), rootLevel, out);
        } else if (relation instanceof NormalizedTableFunctionRelation) {
            // TABLE(f(...)) is a JoinRelation only by construction; rewriting either side changes what
            // the table function is applied to, which is a different statement rather than a mutant.
            return;
        } else if (relation instanceof JoinRelation) {
            JoinRelation join = (JoinRelation) relation;
            if (isWrappable(join.getLeft())) {
                out.add(new Slot("JoinRelation.left", join::getLeft, join::setLeft, rootLevel, false));
            }
            collect(join.getLeft(), rootLevel, out);
            // The right operand of a LATERAL join is correlated with the left one; hiding it behind a
            // derived table would parse and mean something else.
            if (!join.isLateral()) {
                if (isWrappable(join.getRight())) {
                    out.add(new Slot("JoinRelation.right", join::getRight, join::setRight, rootLevel, false));
                }
                collect(join.getRight(), rootLevel, out);
            }
        } else if (relation instanceof SubqueryRelation) {
            collect(((SubqueryRelation) relation).getQueryStatement().getQueryRelation(), false, out);
        } else if (relation instanceof SetOperationRelation) {
            for (QueryRelation child : ((SetOperationRelation) relation).getRelations()) {
                collect(child, rootLevel, out);
            }
        } else if (relation instanceof CTERelation) {
            collect(((CTERelation) relation).getCteQueryStatement().getQueryRelation(), false, out);
        }
    }

    private static boolean isWrappable(Relation relation) {
        if (relation == null || relation.isDualRelation()) {
            // The implicit relation behind `SELECT 1` has no text of its own -- the deparser prints
            // nothing for it -- so there is nothing to wrap.
            return false;
        }
        // unnest(...) is not a standalone FROM item, and its normalized join form is not one either.
        return !(relation instanceof TableFunctionRelation) && !(relation instanceof NormalizedTableFunctionRelation);
    }

    // ---------------------------------------------------------------- rewrite

    /**
     * Only INNER used to be produced, which leaves outer-join null padding and the semi/anti rewrites
     * -- a large share of the join code -- unreachable. CROSS is absent here on purpose: the cross
     * shape is already reachable through a null join condition.
     */
    private static final String[] JOIN_TYPES = {
            "INNER JOIN", "LEFT OUTER JOIN", "RIGHT OUTER JOIN", "FULL OUTER JOIN",
            "LEFT SEMI JOIN", "LEFT ANTI JOIN"};

    /**
     * Picks how the two sides of a self join are related.
     *
     * <p>An unconditional cross join was the original and only shape, which is the wrong default twice
     * over: it never reaches equi-join execution -- hash build and probe, runtime filters, colocate and
     * bucket-shuffle decisions -- and it squares the row count, so replaying one against a real cluster
     * costs a great deal and finds nothing. A cross join is still worth producing sometimes, because the
     * nested-loop path is real, just not as the only thing the operator can build.
     *
     * <p>The column comes from the pool, which is harvested from the same corpus file, so it usually
     * belongs to this relation; when it does not the mutant is rejected by the analyzer, which is
     * ordinary cheap noise rather than a false finding.
     */
    static String joinCondition(AstMutationFuzzerTest.Pool pool, Random rnd) {
        if (pool == null || pool.columnNames.isEmpty() || rnd.nextInt(100) < 10) {
            return null;
        }
        String col = pool.columnNames.get(rnd.nextInt(pool.columnNames.size()));
        // Second column for the ASOF temporal inequality, biased toward names that read like a
        // timestamp. The tree is unanalyzed so there are no types to consult; a wrong guess costs an
        // analyzer rejection, which is the same cheap noise as a wrong join key.
        String temporal = col;
        for (String c : pool.columnNames) {
            String lower = c.toLowerCase(Locale.ROOT);
            if (!c.equals(col) && (lower.contains("time") || lower.contains("date") || lower.contains("ts"))) {
                temporal = c;
                break;
            }
        }
        if (temporal.equals(col)) {
            for (String c : pool.columnNames) {
                if (!c.equals(col)) {
                    temporal = c;
                    break;
                }
            }
        }
        return (rnd.nextInt(100) < 80 ? "=" : ">") + col + ";" + temporal;
    }

    /** Applies one shape at one slot. Package-private so tests can pin both instead of rolling dice. */
    String applyAt(QueryStatement stmt, Slot slot, Shape shape) {
        return applyAt(stmt, slot, shape, null);
    }

    String applyAt(QueryStatement stmt, Slot slot, Shape shape, String joinCond) {
        String statementText = deparse(stmt);
        if (statementText == null) {
            // Without the statement text there is no way to tell which names are already taken, and a
            // seed the deparser cannot render would be dropped by the driver's grammar gate anyway.
            return null;
        }
        Relation target = slot.get();
        String targetText = deparse(target);
        if (targetText == null) {
            return null;
        }
        String inner = "SELECT * FROM " + targetText;
        if (parseQuery(inner) == null) {
            // The unanalyzed deparse is not always re-parsable; that is a dropped mutant, not a finding.
            return null;
        }

        NameGen names = new NameGen(statementText);
        String keptName = reusableName(target);

        if (shape == Shape.CTE && slot.rootQueryLevel) {
            return liftIntoStatementWith(stmt, slot, inner, keptName, names, targetText);
        }

        String aliasA = keptName != null ? keptName : names.next();
        String wrapper;
        switch (shape) {
            case SUBQUERY:
                wrapper = "(" + inner + ") `" + aliasA + "`";
                break;
            case CTE: {
                String cte = names.next();
                wrapper = "(WITH `" + cte + "` AS (" + inner + ") SELECT * FROM `" + cte + "`) `" + aliasA + "`";
                break;
            }
            case UNION_ALL:
                wrapper = "(" + inner + " UNION ALL " + inner + ") `" + aliasA + "`";
                break;
            case UNION_DISTINCT:
                wrapper = "(" + inner + " UNION " + inner + ") `" + aliasA + "`";
                break;
            case EXCEPT:
                wrapper = "(" + inner + " EXCEPT " + inner + ") `" + aliasA + "`";
                break;
            case INTERSECT:
                wrapper = "(" + inner + " INTERSECT " + inner + ") `" + aliasA + "`";
                break;
            case TABLE_FUNCTION: {
                String tf = names.next();
                // generate_series rather than unnest of an array literal: an array literal has no type
                // on an unanalyzed tree and the deparser dereferences that type, so wrapping with one
                // makes the mutant unrenderable before it ever reaches the grammar gate.
                wrapper = "(" + inner + ") `" + aliasA + "`, generate_series(1, 3) `" + tf + "`(`e`)";
                break;
            }
            case ASOF_JOIN: {
                if (joinCond == null) {
                    return null;
                }
                String aliasC = names.next();
                String eq = joinCond.substring(1, joinCond.indexOf(';'));
                String temporal = joinCond.substring(joinCond.indexOf(';') + 1);
                if (eq.equals(temporal)) {
                    return null;
                }
                wrapper = "(" + inner + ") `" + aliasA + "` ASOF LEFT JOIN (" + inner + ") `" + aliasC
                        + "` ON `" + aliasA + "`.`" + eq + "` = `" + aliasC + "`.`" + eq
                        + "` AND `" + aliasA + "`.`" + temporal + "` > `" + aliasC + "`.`" + temporal + "`";
                break;
            }
            case SELF_JOIN: {
                String aliasB = names.next();
                String on = "1 = 1";
                String joinType = JOIN_TYPES[Math.abs(aliasB.hashCode()) % JOIN_TYPES.length];
                if (joinCond != null) {
                    String op = joinCond.substring(0, 1);
                    String col = joinCond.substring(1, joinCond.indexOf(';'));
                    on = "`" + aliasA + "`.`" + col + "` " + op + " `" + aliasB + "`.`" + col + "`";
                }
                wrapper = "(" + inner + ") `" + aliasA + "` " + joinType + " (" + inner + ") `"
                        + aliasB + "` ON " + on;
                break;
            }
            default:
                return null;
        }

        QueryStatement parsed = parseQuery("SELECT * FROM " + wrapper);
        if (parsed == null || !(parsed.getQueryRelation() instanceof SelectRelation)) {
            return null;
        }
        Relation replacement = ((SelectRelation) parsed.getQueryRelation()).getRelation();
        if (replacement == null) {
            return null;
        }
        slot.set(replacement);
        return describe(shape, slot, targetText, wrapper);
    }

    /**
     * The genuine WITH shape: the relation moves into a CTE on the statement itself and the slot keeps
     * only a reference to it. Restricted to slots the statement-level CTE is actually visible from.
     *
     * <p>The CTE node is taken from a fresh parse rather than constructed, so its {@code cteMouldId} is
     * assigned by that parse and may repeat an id already used in {@code stmt}. That is harmless here
     * because the driver re-derives the whole statement from text before anything analyzes it -- but it
     * is the reason this tree must never be analyzed directly.
     */
    private String liftIntoStatementWith(QueryStatement stmt, Slot slot, String inner, String keptName,
                                         NameGen names, String targetText) {
        String cte = names.next();
        String reference = "SELECT * FROM `" + cte + "`" + (keptName != null ? " AS `" + keptName + "`" : "");
        String sql = "WITH `" + cte + "` AS (" + inner + ") " + reference;
        QueryStatement parsed = parseQuery(sql);
        if (parsed == null || !(parsed.getQueryRelation() instanceof SelectRelation)) {
            return null;
        }
        SelectRelation lifted = (SelectRelation) parsed.getQueryRelation();
        if (lifted.getCteRelations().size() != 1 || lifted.getRelation() == null) {
            return null;
        }
        // Appended, never prepended: the lifted body may itself reference a CTE the statement already
        // declares, and a CTE may only see the ones written before it.
        stmt.getQueryRelation().addCTERelation(lifted.getCteRelations().get(0));
        slot.set(lifted.getRelation());
        return describe(Shape.CTE, slot, targetText, sql);
    }

    private static String describe(Shape shape, Slot slot, String targetText, String wrapper) {
        return "M6-nesting " + shape + " at " + slot.describe()
                + ": " + abbrev(targetText) + " -> " + abbrev(wrapper);
    }

    // ------------------------------------------------------------------ utils

    /**
     * The name the wrapper should answer to, so that {@code t.c} in the enclosing query still resolves
     * after {@code t} became a derived table. Null when the relation has no single usable name.
     */
    private static String reusableName(Relation relation) {
        TableName resolved;
        try {
            resolved = relation.getResolveTableName();
        } catch (Throwable t) {
            return null;
        }
        if (resolved == null || resolved.getTbl() == null) {
            return null;
        }
        String name = resolved.getTbl();
        return SIMPLE_IDENT.matcher(name).matches() ? name : null;
    }

    private static String deparse(ParseNode node) {
        try {
            String sql = AstToSQLBuilder.toSQL(node);
            if (sql == null || sql.trim().isEmpty()) {
                return null;
            }
            return sql.replace('\n', ' ').trim();
        } catch (Throwable t) {
            return null;
        }
    }

    private static QueryStatement parseQuery(String sql) {
        try {
            List<StatementBase> parsed = SqlParser.parse(sql, SQL_MODE);
            if (parsed.isEmpty() || !(parsed.get(0) instanceof QueryStatement)) {
                return null;
            }
            return (QueryStatement) parsed.get(0);
        } catch (Throwable t) {
            return null;
        }
    }

    private static String abbrev(String s) {
        String flat = s.replaceAll("\\s+", " ").trim();
        return flat.length() > 160 ? flat.substring(0, 160) + " ..." : flat;
    }

    /** Hands out names the statement does not already contain. */
    private static final class NameGen {
        private final String haystack;
        private int counter = 1;

        NameGen(String statementText) {
            this.haystack = statementText.toLowerCase();
        }

        String next() {
            while (haystack.contains(PREFIX + counter)) {
                counter++;
            }
            return PREFIX + counter++;
        }
    }
}
