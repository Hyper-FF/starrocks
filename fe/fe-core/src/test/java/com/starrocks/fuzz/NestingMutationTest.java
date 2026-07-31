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
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;

/**
 * M6 is the operator where sharing a node between two trees would do the most damage, so these tests
 * are less about "did it return non-null" and more about two properties:
 *
 * <ul>
 *   <li>the mutant is a statement a user could have written -- it deparses, reparses, and for the
 *       name-preserving shapes it still analyzes;</li>
 *   <li>the two sides of a UNION or a self join are genuinely separate trees. Aliasing one relation into
 *       both sides would still deparse and still analyze; it would only show up later, as a finding that
 *       is really the mutator's own bug. So it is checked directly, by node identity and by mutating one
 *       side and watching the other stay put.</li>
 * </ul>
 */
public class NestingMutationTest {

    private static ConnectContext ctx;

    private final NestingMutation op = new NestingMutation();

    /** A join, a query that already has a WITH clause, a set operation and a flat select. */
    private static final String[] SEEDS = {
            "select k, v from a where v > 1",
            "select a.v, b.w from a join b on a.k = b.k where a.v > 1",
            "with t as (select k, v from a where v > 0) select t.v, b.w from t join b on t.k = b.k",
            "select k from a where v > 1 union all select k from b where w > 2",
            "select a.v from a left join (select k, w from b) s on a.k = s.k",
    };

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        StarRocksAssert a = new StarRocksAssert(ctx);
        a.withDatabase("fuzz_nest").useDatabase("fuzz_nest");
        a.withTable("CREATE TABLE a (k int, v int) DUPLICATE KEY(k)"
                + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        a.withTable("CREATE TABLE b (k int, w int) DUPLICATE KEY(k)"
                + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
    }

    // ------------------------------------------------------------------ tests

    /**
     * Every shape, at every rewritable position of every seed: the operator fires, says what it did, and
     * the mutant survives the same deparse/reparse gate the driver puts it through
     * ({@code AstMutationFuzzerTest#reparseThroughGrammar}).
     */
    @Test
    public void testEveryShapeFiresAtEverySlotAndSurvivesTheGrammarGate() {
        int applied = 0;
        for (String seed : SEEDS) {
            int slotCount = NestingMutation.slotsOf(parse(seed)).size();
            Assertions.assertTrue(slotCount > 0, () -> "no rewritable relation in " + seed);
            for (NestingMutation.Shape shape : NestingMutation.Shape.values()) {
                for (int i = 0; i < slotCount; i++) {
                    QueryStatement stmt = parse(seed);
                    NestingMutation.Slot slot = NestingMutation.slotsOf(stmt).get(i);
                    String before = AstToSQLBuilder.toSQL(stmt);

                    // ASOF needs real column names: the analyzer demands an ON clause with an
                    // equality and exactly one temporal inequality, which cannot be invented without a
                    // pool. Every other shape is built from the relation text alone.
                    String cond = shape == NestingMutation.Shape.ASOF_JOIN ? "=v1;v2" : null;
                    String description = op.applyAt(stmt, slot, shape, cond);
                    // A join nested in a join operand used to be declined here, because the
                    // deparser emitted it without parentheses and the mutant could never survive the
                    // reparse. That was a real deparser defect, now fixed, so the slot is offered like
                    // any other and the mutant is expected to round-trip.
                    Assertions.assertNotNull(description,
                            "operator did not fire: " + shape + " slot " + i + " of " + seed);
                    Assertions.assertTrue(description.contains(shape.name()),
                            () -> "description does not name the shape: " + description);
                    Assertions.assertTrue(description.contains(slot.describe()),
                            () -> "description does not name the site: " + description);

                    String after = reparseThroughGrammar(stmt);
                    Assertions.assertNotEquals(before, after,
                            "mutant is textually identical to the seed: " + shape + " slot " + i);
                    applied++;
                }
            }
        }
        Assertions.assertTrue(applied >= 50, "expected the seed set to exercise many sites, got " + applied);
    }

    /** The wrap is visible in the text, and reusing the wrapped relation's name keeps the mutant analyzable. */
    @Test
    public void testSubqueryWrapIsVisibleAndStillAnalyzes() {
        String sql = "select a.v, b.w from a join b on a.k = b.k where a.v > 1";
        QueryStatement stmt = parse(sql);
        // JoinRelation.left, i.e. table `a`.
        NestingMutation.Slot slot = slotNamed(stmt, "JoinRelation.left");
        Assertions.assertNotNull(op.applyAt(stmt, slot, NestingMutation.Shape.SUBQUERY));

        String text = reparseThroughGrammar(stmt);
        Assertions.assertTrue(flat(text).contains("(SELECT * FROM "), () -> "no derived table in: " + text);
        // The wrapper answers to the wrapped relation's own name, which is why the qualified references
        // a.v / a.k in the enclosing query still resolve and the mutant reaches the analyzer at all.
        Assertions.assertEquals("a", ((SubqueryRelation) slot.get()).getAlias().getTbl());
        Analyzer.analyze(parse(text), ctx);
    }

    /** M6's CTE shape on a plain query, and on a query that already carries a WITH clause. */
    @Test
    public void testCteLiftAddsAWithClauseWithoutCollidingWithAnExistingOne() {
        QueryStatement plain = parse("select k, v from a where v > 1");
        Assertions.assertNotNull(op.applyAt(plain, slotNamed(plain, "SelectRelation.from"),
                NestingMutation.Shape.CTE));
        Assertions.assertEquals(1, plain.getQueryRelation().getCteRelations().size());
        String plainText = reparseThroughGrammar(plain);
        Assertions.assertTrue(flat(plainText).toUpperCase(Locale.ROOT).startsWith("WITH "),
                () -> "no WITH clause in: " + plainText);
        Assertions.assertTrue(plainText.contains("srfuzz_n"), () -> "no generated CTE name in: " + plainText);
        Analyzer.analyze(parse(plainText), ctx);

        // Already has a CTE named `t`: the lifted one must be added, not replace it, and must not reuse
        // the name. It is appended, because the lifted body may reference `t`.
        String seed = "with t as (select k, v from a where v > 0) select t.v, b.w from t join b on t.k = b.k";
        QueryStatement withCte = parse(seed);
        NestingMutation.Slot slot = slotNamed(withCte, "JoinRelation.left");
        Assertions.assertNotNull(op.applyAt(withCte, slot, NestingMutation.Shape.CTE));

        List<CTERelation> ctes = withCte.getQueryRelation().getCteRelations();
        Assertions.assertEquals(2, ctes.size());
        Assertions.assertEquals("t", ctes.get(0).getName());
        Assertions.assertNotEquals(ctes.get(0).getName(), ctes.get(1).getName());
        Assertions.assertTrue(ctes.get(1).getName().startsWith("srfuzz_n"),
                () -> "invented name lacks the reserved prefix: " + ctes.get(1).getName());

        String text = reparseThroughGrammar(withCte);
        // Both CTEs are declared, and the lifted one comes second -- it references `t`, and a CTE only
        // sees the ones written before it.
        Assertions.assertTrue(flat(text).toUpperCase(Locale.ROOT).startsWith("WITH "), () -> "no WITH in: " + text);
        Assertions.assertTrue(text.contains(ctes.get(1).getName()), () -> "lifted CTE not in: " + text);
        Analyzer.analyze(parse(text), ctx);
    }

    /** {@code R UNION ALL R}: same text on both sides, but not one tree referenced twice. */
    @Test
    public void testUnionWrapProducesTwoIndependentCopies() {
        QueryStatement stmt = parse("select k, v from a where v > 1");
        NestingMutation.Slot slot = slotNamed(stmt, "SelectRelation.from");
        Set<Object> seedNodes = nodesOf(slot.get());

        Assertions.assertNotNull(op.applyAt(stmt, slot, NestingMutation.Shape.UNION_ALL));
        String text = reparseThroughGrammar(stmt);
        Assertions.assertTrue(flat(text).contains("(SELECT * FROM "), () -> "no derived table in: " + text);
        Assertions.assertTrue(flat(text).contains("UNION ALL "), () -> "no union in: " + text);

        SubqueryRelation wrapper = (SubqueryRelation) slot.get();
        QueryRelation inner = wrapper.getQueryStatement().getQueryRelation();
        Assertions.assertInstanceOf(SetOperationRelation.class, inner);
        List<QueryRelation> sides = ((SetOperationRelation) inner).getRelations();
        Assertions.assertEquals(2, sides.size());

        assertIndependentCopies(sides.get(0), sides.get(1));
        assertSharesNothingWith(seedNodes, sides.get(0));
        assertSharesNothingWith(seedNodes, sides.get(1));
        assertMutatingOneSideLeavesTheOtherAlone(sides.get(0), sides.get(1));
    }

    /** Self join: two derived tables over the same relation, distinct aliases, distinct trees. */
    @Test
    public void testSelfJoinWrapProducesTwoIndependentCopies() {
        QueryStatement stmt = parse("select k, v from a where v > 1");
        NestingMutation.Slot slot = slotNamed(stmt, "SelectRelation.from");
        Set<Object> seedNodes = nodesOf(slot.get());

        Assertions.assertNotNull(op.applyAt(stmt, slot, NestingMutation.Shape.SELF_JOIN));
        String text = reparseThroughGrammar(stmt);
        // The join type is varied on purpose, so pin the shape rather than one keyword.
        Assertions.assertTrue(flat(text).contains(" JOIN "), () -> "no self join in: " + text);

        JoinRelation join = (JoinRelation) slot.get();
        SubqueryRelation left = (SubqueryRelation) join.getLeft();
        SubqueryRelation right = (SubqueryRelation) join.getRight();
        Assertions.assertNotEquals(left.getAlias().getTbl(), right.getAlias().getTbl(),
                "the two sides of a self join must not share an alias");
        Assertions.assertTrue(right.getAlias().getTbl().startsWith("srfuzz_n"),
                () -> "invented alias lacks the reserved prefix: " + right.getAlias().getTbl());

        QueryRelation leftBody = left.getQueryStatement().getQueryRelation();
        QueryRelation rightBody = right.getQueryStatement().getQueryRelation();
        assertIndependentCopies(leftBody, rightBody);
        assertSharesNothingWith(seedNodes, leftBody);
        assertSharesNothingWith(seedNodes, rightBody);
        assertMutatingOneSideLeavesTheOtherAlone(leftBody, rightBody);
    }

    /** Nothing to wrap, so nothing happens -- returning null is the contract, not an error. */
    @Test
    public void testReturnsNullWhenThereIsNoRelationToWrap() {
        QueryStatement stmt = parse("select 1");
        Assertions.assertTrue(NestingMutation.slotsOf(stmt).isEmpty(),
                () -> "unexpected rewritable slot in a FROM-less query: "
                        + NestingMutation.slotsOf(stmt).size());
        Assertions.assertNull(op.apply(stmt, new AstMutationFuzzerTest.Pool(), new Random(1)));
    }

    /** The random entry point picks a site on its own and honours the same contract. */
    @Test
    public void testRandomEntryPointFiresAndRoundTrips() {
        int fired = 0;
        // A populated pool, because the driver never mutates without one: ASOF needs column names to
        // build the equality and temporal inequality its analyzer demands, and declines without them.
        AstMutationFuzzerTest.Pool pool = new AstMutationFuzzerTest.Pool();
        pool.columnNames.add("v1");
        pool.columnNames.add("event_time");
        for (int i = 0; i < 40; i++) {
            QueryStatement stmt = parse(SEEDS[i % SEEDS.length]);
            String description = op.apply(stmt, pool, new Random(i));
            if (description == null) {
                continue;
            }
            fired++;
            // Names the shape, not the operator: the driver prefixes name() itself, and describing it
            // here too is what produced "M6-nesting M6-nesting SELF_JOIN at ..." in the report.
            Assertions.assertFalse(description.startsWith(op.name()), description);
            Assertions.assertTrue(description.contains(" at "), description);
            reparseThroughGrammar(stmt);
        }
        Assertions.assertEquals(40, fired, "the operator should apply to every one of these seeds");
        Assertions.assertEquals("M6-nesting", op.name());
    }

    // ---------------------------------------------------------------- helpers

    private static QueryStatement parse(String sql) {
        return (QueryStatement) SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
    }

    private static NestingMutation.Slot slotNamed(QueryStatement stmt, String description) {
        for (NestingMutation.Slot slot : NestingMutation.slotsOf(stmt)) {
            if (slot.describe().equals(description)) {
                return slot;
            }
        }
        return Assertions.fail("no slot " + description + " in " + AstToSQLBuilder.toSQL(stmt));
    }

    /** Mirrors {@code AstMutationFuzzerTest#reparseThroughGrammar}, but asserts instead of dropping. */
    private static String reparseThroughGrammar(QueryStatement mutant) {
        String text = AstToSQLBuilder.toSQL(mutant);
        Assertions.assertNotNull(text);
        Assertions.assertFalse(text.trim().isEmpty());
        List<StatementBase> again = SqlParser.parse(text, ctx.getSessionVariable());
        Assertions.assertFalse(again.isEmpty(), () -> "mutant does not reparse: " + text);
        Assertions.assertNotNull(AstToSQLBuilder.toSQL(again.get(0)));
        return text;
    }

    /** Same SQL on both sides, but two objects and two disjoint sets of nodes. */
    private static void assertIndependentCopies(Relation left, Relation right) {
        Assertions.assertNotSame(left, right, "the two sides are the same Relation instance");
        Assertions.assertEquals(AstToSQLBuilder.toSQL(left), AstToSQLBuilder.toSQL(right),
                "the two sides should be copies of one relation");
        assertSharesNothingWith(nodesOf(left), right);
    }

    private static void assertSharesNothingWith(Set<Object> forbidden, Relation relation) {
        for (Object node : nodesOf(relation)) {
            Assertions.assertFalse(forbidden.contains(node),
                    () -> "node shared between two trees: " + node.getClass().getSimpleName());
        }
    }

    /**
     * The failure mode a plain identity check can still miss is a node shared deeper down, so also make
     * one side different and require the other not to move.
     */
    private static void assertMutatingOneSideLeavesTheOtherAlone(Relation left, Relation right) {
        String leftBefore = AstToSQLBuilder.toSQL(left);
        String rightBefore = AstToSQLBuilder.toSQL(right);
        ((SelectRelation) left).setWhereClause(SqlParser.parseSqlToExpr("1 = 1", SqlModeHelper.MODE_DEFAULT));
        Assertions.assertNotEquals(leftBefore, AstToSQLBuilder.toSQL(left), "the edit did not take");
        Assertions.assertEquals(rightBefore, AstToSQLBuilder.toSQL(right),
                "editing one side changed the other -- the sides share nodes");
    }

    private static String flat(String sql) {
        return sql.replaceAll("\\s+", " ").trim();
    }

    /** Identity set of every Relation and Expr reachable from {@code relation}. */
    private static Set<Object> nodesOf(Relation relation) {
        Set<Object> out = Collections.newSetFromMap(new IdentityHashMap<>());
        walk(relation, out);
        return out;
    }

    private static void walk(Relation relation, Set<Object> out) {
        if (relation == null || !out.add(relation)) {
            return;
        }
        for (Expr expr : AstMutationFuzzerTest.collectRootExprs(relation)) {
            walkExpr(expr, out);
        }
        if (relation instanceof QueryRelation) {
            for (CTERelation cte : ((QueryRelation) relation).getCteRelations()) {
                walk(cte, out);
            }
        }
        if (relation instanceof SelectRelation) {
            walk(((SelectRelation) relation).getRelation(), out);
        } else if (relation instanceof JoinRelation) {
            walk(((JoinRelation) relation).getLeft(), out);
            walk(((JoinRelation) relation).getRight(), out);
        } else if (relation instanceof SubqueryRelation) {
            walk(((SubqueryRelation) relation).getQueryStatement().getQueryRelation(), out);
        } else if (relation instanceof SetOperationRelation) {
            for (QueryRelation child : ((SetOperationRelation) relation).getRelations()) {
                walk(child, out);
            }
        } else if (relation instanceof CTERelation) {
            walk(((CTERelation) relation).getCteQueryStatement().getQueryRelation(), out);
        }
    }

    private static void walkExpr(Expr expr, Set<Object> out) {
        if (expr == null || !out.add(expr)) {
            return;
        }
        for (Expr child : expr.getChildren()) {
            walkExpr(child, out);
        }
    }

    @Test
    public void testSelfJoinPrefersAnEquiJoinOverACrossJoin() {
        // A cross join reaches none of the equi-join execution -- hash build and probe, runtime
        // filters, colocate and bucket-shuffle decisions -- and squares the row count, so replaying one
        // against a real cluster is expensive and finds nothing. It used to be the only shape this
        // operator could build. It is still produced sometimes, because the nested-loop path is real.
        NestingMutation op = new NestingMutation();
        AstMutationFuzzerTest.Pool pool = new AstMutationFuzzerTest.Pool();
        pool.columnNames.add("v1");
        pool.columnNames.add("v2");

        int equi = 0;
        int cross = 0;
        for (int i = 0; i < 200; i++) {
            QueryStatement stmt = parse("select v1 from t0");
            String applied = op.applyAt(stmt, NestingMutation.slotsOf(stmt).get(0),
                    NestingMutation.Shape.SELF_JOIN,
                    NestingMutation.joinCondition(pool, new Random(i)));
            if (applied == null) {
                continue;
            }
            String sql = AstToSQLBuilder.toSQL(stmt).replace('\n', ' ');
            if (sql.contains("ON 1 = 1")) {
                cross++;
            } else if (sql.contains("` = `") || sql.contains("` > `")) {
                equi++;
            }
        }
        Assertions.assertTrue(equi > cross * 3, "equi=" + equi + " cross=" + cross);
        Assertions.assertTrue(cross > 0, "the cross join shape disappeared entirely");
    }

}
