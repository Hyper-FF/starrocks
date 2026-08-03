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
import com.starrocks.sql.analyzer.Analyzer;
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

/**
 * What M11 actually does to a statement.
 *
 * <p>"apply() returned non-null" proves nothing: an operator that reports {@code WHERE ... AND x} while
 * replacing the WHERE outright would still pass such a check, and the whole point of this operator is
 * that the condition it adds must reach the optimizer <i>on top of</i> what was already there. So each
 * case fires one named sub-operation and then checks that the seed's own predicate survived, that the
 * described fragment is really what landed, and that the mutant still parses.
 *
 * <p>The last test is the one that matters most. M11's failure mode is not a crash, it is producing
 * predicates the analyzer rejects: a rejected mutant costs a full parse/analyze cycle and reaches no
 * optimizer rule at all, so an operator that generates unresolvable conditions would look busy in the
 * report while adding nothing. That is measured here rather than assumed.
 */
public class PredicateMutationTest {

    private static ConnectContext ctx;

    private static final List<String> SEEDS = Arrays.asList(
            "select a.v, b.w from a join b on a.k = b.k where a.v > 1",
            "select a.v from a left join b on a.k = b.k",
            "select k, count(*) as c from a group by k",
            "select k, count(*) as c from a group by k having count(*) > 1",
            "select v from a where v > 0",
            "select v from a",
            "with t as (select k, v from a where v > 0) select t.k from t join b on t.k = b.k",
            "select s.k from (select k, w from b where w < 9) s where s.k in (select k from a where v > 2)",
            "select v from a union all select w from b");

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("fuzz_predicate").useDatabase("fuzz_predicate");
        starRocksAssert.withTable("CREATE TABLE a (k int, v int) DUPLICATE KEY(k)"
                + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        starRocksAssert.withTable("CREATE TABLE b (k int, w int) DUPLICATE KEY(k)"
                + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
    }

    // ----------------------------------------------------------------- WHERE

    @Test
    public void testConjunctionKeepsTheOriginalPredicate() {
        String seed = "select v from a where v > 0";
        Fired fired = fire(seed, ": WHERE ... AND ");
        String after = deparse(fired.mutant);

        // The seed's own condition has to still be there. An operator that overwrote it would raise the
        // predicate count in the report while testing nothing the seed did not already test.
        Assertions.assertTrue(squash(after).contains(squash("`v` > 0")),
                "the seed's predicate did not survive conjunction: " + after);
        Assertions.assertTrue(after.contains(" AND "), "no conjunction in: " + after);
        assertRoundTrips(after);
    }

    @Test
    public void testAddsWhereWhenThereIsNone() {
        String seed = "select v from a";
        String before = deparse(seed);
        Assertions.assertFalse(before.contains("WHERE"), before);

        Fired fired = fire(seed, ": add WHERE ");
        String after = deparse(fired.mutant);

        Assertions.assertTrue(after.contains("WHERE"), "no WHERE was added: " + after);
        assertRoundTrips(after);
    }

    @Test
    public void testDisjunctionAndNegationAreBothReachable() {
        String seed = "select v from a where v > 0";

        String disjoined = deparse(fire(seed, ": WHERE ... OR ").mutant);
        Assertions.assertTrue(disjoined.contains(" OR "), disjoined);
        Assertions.assertTrue(squash(disjoined).contains(squash("`v` > 0")), disjoined);
        assertRoundTrips(disjoined);

        String negated = deparse(fire(seed, ": negate WHERE").mutant);
        Assertions.assertTrue(negated.contains("NOT"), "negation did not render a NOT: " + negated);
        Assertions.assertTrue(squash(negated).contains(squash("`v` > 0")),
                "negation lost the predicate it was supposed to wrap: " + negated);
        assertRoundTrips(negated);
    }

    // ---------------------------------------------------------------- HAVING

    @Test
    public void testHavingIsAddedOnlyWhereAnAggregationExists() {
        String withAggregate = "select k, count(*) as c from a group by k";
        String after = deparse(fire(withAggregate, ": add HAVING ").mutant);
        Assertions.assertTrue(after.contains("HAVING"), after);
        assertRoundTrips(after);

        // HAVING without an aggregation analyzes for no select item that is not itself aggregated, so
        // offering it there would spend mutants on guaranteed rejections.
        Set<String> plain = descriptions("select v from a where v > 0", 400);
        Assertions.assertFalse(plain.stream().anyMatch(d -> d.contains("HAVING")),
                "HAVING was offered to a non-aggregate block: " + plain);
    }

    @Test
    public void testHavingConjunctionKeepsTheOriginalCondition() {
        String seed = "select k, count(*) as c from a group by k having count(*) > 1";
        String after = deparse(fire(seed, ": HAVING ... AND ").mutant);
        Assertions.assertTrue(squash(after).contains(squash("count(*) > 1")),
                "the seed's HAVING did not survive: " + after);
        assertRoundTrips(after);
    }

    // -------------------------------------------------------------------- ON

    @Test
    public void testJoinOnGainsAConjunctWithoutTouchingWhere() {
        String seed = "select a.v from a left join b on a.k = b.k";
        Fired fired = fire(seed, ": ON ... AND ");
        String after = deparse(fired.mutant);

        Assertions.assertTrue(squash(after).contains(squash("`a`.`k` = `b`.`k`")),
                "the join condition did not survive: " + after);
        Assertions.assertTrue(after.contains(" AND "), after);
        // The predicate belongs to the ON clause; putting it in a WHERE would change an outer join's
        // meaning entirely, which is the difference this operator exists to exercise, not to blur.
        Assertions.assertFalse(after.contains("WHERE"),
                "an ON-clause edit leaked into a WHERE: " + after);
        assertRoundTrips(after);
    }

    @Test
    public void testCrossJoinWithoutOnIsLeftAlone() {
        // Attaching a condition to a join that has none turns a cross join into an inner join. That is
        // a shape change and belongs to M6; doing it here would make M11's own report wrong about what
        // it changed.
        Set<String> seen = descriptions("select a.v from a, b", 400);
        Assertions.assertFalse(seen.stream().anyMatch(d -> d.contains(": ON ")),
                "a join with no ON clause was given one: " + seen);
    }

    // ------------------------------------------------------------- the point

    @Test
    public void testConjunctionIsTheDominantEdit() {
        // The measured reason chaining edits changed nothing was that three quarters of them replaced a
        // leaf. M11 exists to move the AND-per-query number, which starts at 0.36 across the corpus, so
        // conjunction has to be what it usually does -- not one option among many.
        Set<String> seen = descriptions("select v from a where v > 0", 400);
        long conjunctions = seen.stream().filter(d -> d.contains(": WHERE ... AND ")).count();
        Assertions.assertTrue(conjunctions * 2 >= seen.size(),
                "conjunction was only " + conjunctions + " of " + seen.size() + " distinct edits: " + seen);
    }

    @Test
    public void testTautologyFormIsGenerated() {
        // (c IS NULL OR c IS NOT NULL) holds for every row of every type, so a mutant carrying one must
        // return exactly what the seed returned while planning differently. It is the only predicate
        // here with a known-correct expected result, which is what makes it worth generating on purpose.
        Set<String> seen = descriptions("select v from a where v > 0", 400);
        Assertions.assertTrue(
                seen.stream().anyMatch(d -> d.contains("IS NULL OR") && d.contains("IS NOT NULL")),
                "no tautology was ever generated: " + seen);
    }

    @Test
    public void testMutantsMostlySurviveTheAnalyzer() {
        // A predicate the analyzer rejects reaches no optimizer rule, so an operator that mostly
        // produces unresolvable conditions is worthless however busy it looks. The bar is a rate rather
        // than "all of them" on purpose: BETWEEN draws its two bounds independently and a comparison
        // draws a pooled scalar of unknown type, so a minority of type rejections is by design -- those
        // are the mutants that reach analyzer paths a well-formed query never does.
        Random rnd = new Random(23L);
        int applied = 0;
        int analyzed = 0;
        StringBuilder rejections = new StringBuilder();
        for (String seed : SEEDS) {
            for (int i = 0; i < 60; i++) {
                StatementBase stmt = parse(seed);
                String description = new PredicateMutation().apply((QueryStatement) stmt, pool(), rnd);
                if (description == null) {
                    continue;
                }
                applied++;
                try {
                    // A fresh parse of the deparsed mutant, which is what the driver analyzes.
                    StatementBase reparsed = parse(AstToSQLBuilder.toSQL(stmt));
                    Analyzer.analyze(reparsed, ctx);
                    analyzed++;
                } catch (Throwable t) {
                    if (rejections.length() < 2000) {
                        rejections.append("\n  ").append(description).append(" -> ").append(t.getMessage());
                    }
                }
            }
        }
        Assertions.assertTrue(applied > 100, "M11 barely applied at all: " + applied);
        Assertions.assertTrue(analyzed * 10 >= applied * 6,
                "only " + analyzed + " of " + applied + " mutants analyzed:" + rejections);
    }

    @Test
    public void testEveryBlockOfANestedStatementIsReachable() {
        // A block the walk does not reach never gets a predicate, and the gap is silent: the operator
        // keeps reporting edits while a whole region of every seed stays untouched.
        String seed = "select s.k from (select k, w from b where w < 9) s where s.k in (select k from a where v > 2)";
        Set<String> targets = new LinkedHashSet<>();
        for (String description : descriptions(seed, 600)) {
            int colon = description.indexOf(':');
            if (colon > 0) {
                targets.add(description.substring(0, colon));
            }
        }
        Assertions.assertTrue(targets.size() >= 3,
                "only these blocks were ever edited in a three-block statement: " + targets);
    }

    // --------------------------------------------------------------- harness

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
        pool.addExpr("1", false);
        pool.addExpr("2", false);
        pool.addColumn("k");
        pool.addColumn("v");
        pool.addQualifiedColumn("`a`.`k`");
        pool.addQualifiedColumn("`a`.`v`");
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

    /** Applies M11 to fresh parses of {@code sql} until a description mentions {@code needle}. */
    private static Fired fire(String sql, String needle) {
        Random rnd = new Random(7L);
        Set<String> seen = new LinkedHashSet<>();
        for (int i = 0; i < 800; i++) {
            StatementBase stmt = parse(sql);
            String description = new PredicateMutation().apply((QueryStatement) stmt, pool(), rnd);
            if (description != null && description.contains(needle)) {
                return new Fired(description, stmt);
            }
            if (description != null) {
                seen.add(description);
            }
        }
        // What did fire is the diagnosis: a sub-operation that is never offered and one that always
        // declines at injection time look identical from outside.
        Assertions.fail("M11 never produced '" + needle + "' for: " + sql + "\n  it did produce: " + seen);
        return null;
    }

    private static Set<String> descriptions(String sql, int rounds) {
        Random rnd = new Random(11L);
        Set<String> out = new LinkedHashSet<>();
        for (int i = 0; i < rounds; i++) {
            StatementBase stmt = parse(sql);
            String description = new PredicateMutation().apply((QueryStatement) stmt, pool(), rnd);
            if (description != null) {
                out.add(description);
            }
        }
        return out;
    }

    /** Drops parentheses and whitespace, so two renderings of one predicate compare equal. */
    private static String squash(String sql) {
        return sql.replace("(", "").replace(")", "").replaceAll("\\s+", "");
    }
}
