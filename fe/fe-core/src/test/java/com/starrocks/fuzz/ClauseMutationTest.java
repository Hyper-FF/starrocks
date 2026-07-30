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
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * What M5 actually does to a statement.
 *
 * <p>"apply() returned non-null" proves nothing here: an operator that reports {@code remove LIMIT} while
 * leaving the LIMIT in place still passes such a check, and the fuzz report would then carry a mutation
 * line nobody can replay. So every case below fires one named sub-operation on a realistic seed and then
 * checks three things about the result: that the deparsed text gained or lost exactly the clause the
 * description claims, that the description's own fragment really is the fragment that moved, and that the
 * mutant survives the same deparse/reparse gate the driver applies
 * ({@code AstMutationFuzzerTest#reparseThroughGrammar}).
 */
public class ClauseMutationTest {

    private static ConnectContext ctx;

    /** Seeds with the shapes M5 has to cope with: joins, aggregation, windows, CTEs, set ops, subqueries. */
    private static final List<String> SEEDS = Arrays.asList(
            "select a.v, b.w from a join b on a.k = b.k where a.v > 1 and b.w < 9",
            "select k, count(*) as c from a group by k having count(*) > 1 order by k limit 5",
            "select distinct v from a",
            "select k, row_number() over (partition by v order by k) as rn from a",
            "select k, sum(v) over (partition by k) as s from a",
            "select v from a union all select w from b",
            "with t as (select k, v from a where v > 0) select t.k from t join b on t.k = b.k",
            "select s.k from (select k, w from b where w < 9) s where s.k in (select k from a where v > 2)",
            "select * from a");

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("fuzz_clause").useDatabase("fuzz_clause");
        starRocksAssert.withTable("CREATE TABLE a (k int, v int) DUPLICATE KEY(k)"
                + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        starRocksAssert.withTable("CREATE TABLE b (k int, w int) DUPLICATE KEY(k)"
                + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
    }

    // ----------------------------------------------------------------- WHERE

    @Test
    public void testRemoveWhereFromJoinSeed() {
        String seed = "select a.v, b.w from a join b on a.k = b.k where a.v > 1 and b.w < 9";
        String before = deparse(seed);
        Assertions.assertTrue(before.contains("WHERE"), before);

        Fired fired = fire(seed, "remove WHERE");
        String after = deparse(fired.mutant);

        Assertions.assertFalse(after.contains("WHERE"), "WHERE survived the removal: " + after);
        // The ON predicate is a different clause and must not be collateral damage.
        Assertions.assertTrue(after.contains("ON "), after);
        // The description names the predicate that was actually dropped.
        String dropped = tail(fired.description, "remove WHERE ");
        Assertions.assertTrue(before.contains(dropped),
                "description quotes " + dropped + " which is not in the seed: " + before);
        Assertions.assertFalse(after.contains(dropped), after);
        assertRoundTrips(after);
    }

    @Test
    public void testAddWhereToJoinSeed() {
        String seed = "select a.v, b.w from a join b on a.k = b.k";
        String before = deparse(seed);
        Assertions.assertFalse(before.contains("WHERE"), before);

        Fired fired = fire(seed, "add WHERE");
        String after = deparse(fired.mutant);
        String injected = tail(fired.description, "add WHERE ");

        Assertions.assertTrue(after.contains("WHERE"), "no WHERE was added: " + after);
        Assertions.assertTrue(after.contains(injected),
                "description claims " + injected + " but the mutant is " + after);
        assertRoundTrips(after);
    }

    // ------------------------------------------------- aggregate seed, removal

    @Test
    public void testAggregateSeedLosesExactlyOneClause() {
        String seed = "select k, count(*) as c from a group by k having count(*) > 1 order by k limit 5";
        String before = deparse(seed);
        for (String clause : Arrays.asList("GROUP BY", "HAVING", "ORDER BY", "LIMIT")) {
            Assertions.assertTrue(before.contains(clause), clause + " missing from the seed: " + before);
        }

        for (String clause : Arrays.asList("GROUP BY", "HAVING", "ORDER BY", "LIMIT")) {
            Fired fired = fire(seed, "remove " + clause);
            String after = deparse(fired.mutant);
            Assertions.assertFalse(after.contains(clause),
                    "'remove " + clause + "' left it in place: " + after);
            // Surgical: the other three clauses are untouched.
            for (String other : Arrays.asList("GROUP BY", "HAVING", "ORDER BY", "LIMIT")) {
                if (!other.equals(clause)) {
                    Assertions.assertTrue(after.contains(other),
                            "'remove " + clause + "' also removed " + other + ": " + after);
                }
            }
            assertRoundTrips(after);
        }
    }

    // -------------------------------------------------- addition on new shapes

    @Test
    public void testPlainSeedGainsGroupByOrderByAndLimit() {
        String seed = "select k, v from a";
        String before = deparse(seed);

        Fired grouped = fire(seed, "add GROUP BY");
        String groupedSql = deparse(grouped.mutant);
        String keys = tail(grouped.description, "add GROUP BY ");
        Assertions.assertFalse(before.contains("GROUP BY"), before);
        Assertions.assertTrue(groupedSql.contains("GROUP BY " + keys),
                "expected GROUP BY " + keys + " in " + groupedSql);
        assertRoundTrips(groupedSql);

        Fired sorted = fire(seed, "add ORDER BY");
        String sortedSql = deparse(sorted.mutant);
        Assertions.assertTrue(sortedSql.contains("ORDER BY"), sortedSql);
        assertRoundTrips(sortedSql);

        Fired limited = fire(seed, "add LIMIT");
        String limitedSql = deparse(limited.mutant);
        Assertions.assertTrue(limitedSql.contains("LIMIT " + tail(limited.description, "add LIMIT ")),
                limited.description + " vs " + limitedSql);
        assertRoundTrips(limitedSql);
    }

    @Test
    public void testAggregateSeedGainsHaving() {
        String seed = "select k, count(*) as c from a group by k";
        Assertions.assertFalse(deparse(seed).contains("HAVING"));

        Fired fired = fire(seed, "add HAVING");
        String after = deparse(fired.mutant);
        String predicate = tail(fired.description, "add HAVING ");
        // Compared with parentheses and spaces squashed out: the deparser brackets the operands of a
        // binary predicate ("HAVING (count(*)) > 0"), which is the same predicate, differently printed.
        Assertions.assertTrue(squash(after).contains(squash("HAVING " + predicate)),
                fired.description + " vs " + after);
        Assertions.assertTrue(after.contains("GROUP BY"), "the GROUP BY must survive: " + after);
        assertRoundTrips(after);
    }

    /** HAVING needs an aggregation to attach to; offering it anywhere else only manufactures rejections. */
    @Test
    public void testHavingIsNotOfferedWithoutAnAggregation() {
        Set<String> seen = descriptions("select k, v from a where v > 0", 400);
        Assertions.assertFalse(seen.isEmpty());
        Assertions.assertTrue(seen.stream().noneMatch(d -> d.contains("add HAVING")),
                "HAVING offered on a non-aggregated block: " + seen);
    }

    // --------------------------------------------------------------- DISTINCT

    @Test
    public void testDistinctIsToggledOnTheSelectList() {
        Fired added = fire("select v from a", "add DISTINCT");
        String addedSql = deparse(added.mutant);
        Assertions.assertTrue(addedSql.startsWith("SELECT DISTINCT"), addedSql);
        assertRoundTrips(addedSql);

        Fired removed = fire("select distinct v from a", "remove DISTINCT");
        String removedSql = deparse(removed.mutant);
        Assertions.assertFalse(removedSql.contains("DISTINCT"), removedSql);
        assertRoundTrips(removedSql);
    }

    // ----------------------------------------------------------------- window

    @Test
    public void testWindowClauseIsAddedAndRemoved() {
        String plain = "select k, v from a";
        Assertions.assertFalse(deparse(plain).contains("OVER"));
        Fired added = fire(plain, "add window over item");
        String addedSql = deparse(added.mutant);
        Assertions.assertTrue(addedSql.contains("OVER ("), "no OVER clause was added: " + addedSql);
        assertRoundTrips(addedSql);

        String windowed = "select k, sum(v) over (partition by k) as s from a";
        Assertions.assertTrue(deparse(windowed).contains("OVER"));
        Fired removed = fire(windowed, "remove OVER clause");
        String removedSql = deparse(removed.mutant);
        Assertions.assertFalse(removedSql.contains("OVER"), "the OVER clause survived: " + removedSql);
        Assertions.assertTrue(removedSql.contains("sum(`v`)"), removedSql);
        assertRoundTrips(removedSql);
    }

    /**
     * The grammar's {@code windowFunction over} production makes OVER mandatory for the rank family, so
     * {@code row_number()} on its own is not an expression at all. Stripping the OVER there would build a
     * tree the parser cannot express, which the driver drops on the grammar-reachability gate — the
     * sub-operation has to decline instead of spending a mutant on it.
     */
    @Test
    public void testRankWindowKeepsItsOverClause() {
        String seed = "select k, row_number() over (partition by v order by k) as rn from a";
        Set<String> seen = descriptions(seed, 400);
        Assertions.assertFalse(seen.isEmpty());
        Assertions.assertTrue(seen.stream().noneMatch(d -> d.contains("remove OVER clause")),
                "row_number() cannot lose its OVER clause: " + seen);
    }

    /** A window function next to a GROUP BY is rejected before any optimizer rule runs, so it is not offered. */
    @Test
    public void testWindowIsNotOfferedNextToAnAggregation() {
        Set<String> seen = descriptions("select k, count(*) as c from a group by k", 400);
        Assertions.assertFalse(seen.isEmpty());
        Assertions.assertTrue(seen.stream().noneMatch(d -> d.contains("add window")),
                "window offered on an aggregated block: " + seen);
    }

    // ----------------------------------------------------------------- extent

    @Test
    public void testNestedQueryBlocksAreMutationSites() {
        // select#0 is the outer block, #1 the FROM-clause subquery, #2 the IN-subquery.
        String seed = "select s.k from (select k, w from b where w < 9) s where s.k in (select k from a where v > 2)";
        Set<String> blocks = targets(descriptions(seed, 600));
        Assertions.assertTrue(blocks.containsAll(Arrays.asList("select#0", "select#1", "select#2")),
                "M5 only reached " + blocks + "; nested blocks must be mutable too");
    }

    @Test
    public void testCteAndJoinOperandsAreMutationSites() {
        String seed = "with t as (select k, v from a where v > 0) select t.k from t join b on t.k = b.k";
        Set<String> blocks = targets(descriptions(seed, 600));
        Assertions.assertTrue(blocks.size() >= 2,
                "the CTE body must be a mutation site as well as the main block, got " + blocks);
    }

    /**
     * A UNION branch is visited by {@code processSetOp}, not by {@code visitQueryStatement}, so an ORDER BY
     * or LIMIT placed on it is dropped by the deparser and the "mutant" is textually the seed. The gate is
     * deliberate, and the control below shows the same two sub-operations do fire on an ordinary block.
     */
    @Test
    public void testSetOperationBranchesGetNoOrderByOrLimit() {
        Set<String> onUnion = descriptions("select v from a union all select w from b", 500);
        Assertions.assertTrue(targets(onUnion).size() >= 2, "both branches should be reached: " + onUnion);
        // Match the sub-operation, not the words: an injected window function legitimately carries its
        // own "ORDER BY" inside the OVER clause.
        Assertions.assertTrue(onUnion.stream().noneMatch(ClauseMutationTest::touchesOrderByOrLimit),
                "ORDER BY/LIMIT offered on a set-operation branch: " + onUnion);

        Set<String> control = descriptions("select v from a", 500);
        Assertions.assertTrue(control.stream().anyMatch(d -> d.contains(": add ORDER BY")), control.toString());
        Assertions.assertTrue(control.stream().anyMatch(d -> d.contains(": add LIMIT")), control.toString());
    }

    // ------------------------------------------------------------ whole corpus

    /**
     * The driver drops any mutant that cannot be deparsed and reparsed, so an operator that mostly produces
     * such trees looks healthy in the mutation count and contributes nothing. Every mutant M5 produces here
     * has to survive that gate, and has to differ from the seed -- a mutation whose text equals the seed's
     * is budget spent on re-testing the seed.
     */
    @Test
    public void testEveryMutantDeparsesReparsesAndDiffersFromItsSeed() {
        Random rnd = new Random(20260730L);
        int applied = 0;
        int attempted = 0;
        for (String seed : SEEDS) {
            String seedSql = deparse(seed);
            for (int i = 0; i < 200; i++) {
                attempted++;
                StatementBase stmt = parse(seed);
                String description = new ClauseMutation().apply((QueryStatement) stmt, pool(), rnd);
                if (description == null) {
                    continue;
                }
                applied++;
                String mutantSql = AstToSQLBuilder.toSQL(stmt);
                Assertions.assertNotNull(mutantSql, description);
                Assertions.assertNotEquals(seedSql, mutantSql,
                        "mutation '" + description + "' did not change the statement: " + seed);
                assertRoundTrips(mutantSql);
            }
        }
        // Every seed above offers at least DISTINCT, so the operator should essentially always apply.
        Assertions.assertTrue(applied > attempted * 9 / 10,
                "M5 only applied " + applied + " of " + attempted + " times");
    }

    @Test
    public void testNameIsStable() {
        Assertions.assertEquals("M5-clause", new ClauseMutation().name());
    }

    // ---------------------------------------------------------------- harness

    private static final class Fired {
        final String description;
        final StatementBase mutant;

        Fired(String description, StatementBase mutant) {
            this.description = description;
            this.mutant = mutant;
        }
    }

    /** Pool entries are text, exactly as the driver stores them. */
    private static AstMutationFuzzerTest.Pool pool() {
        AstMutationFuzzerTest.Pool pool = new AstMutationFuzzerTest.Pool();
        pool.addExpr("`a`.`v` > 1", true);
        pool.addExpr("`a`.`k` IS NOT NULL", true);
        pool.addExpr("`a`.`k` + 1", false);
        pool.columnNames.add("k");
        pool.columnNames.add("v");
        return pool;
    }

    private static StatementBase parse(String sql) {
        return SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
    }

    /** The seed as the driver would see it: deparsed from an unanalyzed tree, like the mutants. */
    private static String deparse(String sql) {
        return AstToSQLBuilder.toSQL(parse(sql));
    }

    private static String deparse(StatementBase stmt) {
        return AstToSQLBuilder.toSQL(stmt);
    }

    /** The gate from {@code AstMutationFuzzerTest#reparseThroughGrammar}: text must parse back. */
    private static void assertRoundTrips(String sql) {
        Assertions.assertNotNull(sql);
        Assertions.assertFalse(sql.trim().isEmpty());
        List<StatementBase> parsed = SqlParser.parse(sql, ctx.getSessionVariable());
        Assertions.assertFalse(parsed.isEmpty(), "reparse produced nothing for " + sql);
        Assertions.assertNotNull(AstToSQLBuilder.toSQL(parsed.get(0)), "reparsed tree is not renderable: " + sql);
    }

    /** Applies M5 to fresh parses of {@code sql} until a description mentions {@code needle}. */
    private static Fired fire(String sql, String needle) {
        Random rnd = new Random(7L);
        Set<String> seen = new LinkedHashSet<>();
        for (int i = 0; i < 600; i++) {
            StatementBase stmt = parse(sql);
            String description = new ClauseMutation().apply((QueryStatement) stmt, pool(), rnd);
            if (description != null && description.contains(needle)) {
                return new Fired(description, stmt);
            }
            if (description != null) {
                seen.add(description);
            }
        }
        // The sub-operations that did fire are the diagnosis: a missing one is either not offered at
        // all or declines at injection time, and the two look identical from outside.
        Assertions.fail("M5 never produced '" + needle + "' for: " + sql + "\n  it did produce: " + seen);
        return null;
    }

    private static Set<String> descriptions(String sql, int rounds) {
        Random rnd = new Random(11L);
        Set<String> out = new LinkedHashSet<>();
        for (int i = 0; i < rounds; i++) {
            StatementBase stmt = parse(sql);
            String description = new ClauseMutation().apply((QueryStatement) stmt, pool(), rnd);
            if (description != null) {
                out.add(description);
            }
        }
        return out;
    }

    private static final Pattern TARGET = Pattern.compile("^(select#\\d+):");

    private static Set<String> targets(Set<String> descriptions) {
        Set<String> out = new LinkedHashSet<>();
        for (String description : descriptions) {
            Matcher matcher = TARGET.matcher(description);
            if (matcher.find()) {
                out.add(matcher.group(1));
            }
        }
        return out;
    }

    private static boolean touchesOrderByOrLimit(String description) {
        return description.contains(": add ORDER BY") || description.contains(": remove ORDER BY")
                || description.contains(": add LIMIT") || description.contains(": remove LIMIT");
    }

    /** Drops parentheses and whitespace, so two renderings of one predicate compare equal. */
    private static String squash(String sql) {
        return sql.replace("(", "").replace(")", "").replaceAll("\\s+", "");
    }

    private static String tail(String description, String marker) {
        int at = description.indexOf(marker);
        Assertions.assertTrue(at >= 0, "'" + marker + "' not in description: " + description);
        return description.substring(at + marker.length()).trim();
    }
}
