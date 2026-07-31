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
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * What the mutator can see. An expression the walk does not reach is never mutated and never pooled,
 * so a gap here is silent: the fuzzer keeps reporting a healthy mutant count while a whole region of
 * every seed goes untouched.
 *
 * <p>Joins were exactly that gap. {@code collectRootExprs} had no {@code JoinRelation} branch, so a
 * join swallowed its entire subtree -- the ON predicate, and any subquery on either side. Around a
 * third of the SQL-Tester corpus joins.
 */
public class MutationReachabilityTest {
    private static ConnectContext ctx;

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        StarRocksAssert a = new StarRocksAssert(ctx);
        a.withDatabase("fuzz_reach").useDatabase("fuzz_reach");
        a.withTable("CREATE TABLE a (k int, v int) DUPLICATE KEY(k)"
                + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        a.withTable("CREATE TABLE b (k int, w int) DUPLICATE KEY(k)"
                + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        a.withTable("CREATE TABLE c (k int, z int) DUPLICATE KEY(k)"
                + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
    }

    /** The roots of the analyzed tree -- what {@code harvest} pools from. */
    private static List<String> roots(String sql) {
        StatementBase stmt = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        Analyzer.analyze(stmt, ctx);
        return render(stmt);
    }

    /**
     * The roots of the parsed-but-unanalyzed tree -- what the mutator actually edits.
     *
     * <p>Usually the same set, but not always: analysis drops a subquery's ORDER BY when no LIMIT makes
     * it observable, so a sort key that is a live mutation site disappears before {@link #roots} sees it.
     */
    private static List<String> parsedRoots(String sql) {
        return render(SqlParser.parse(sql, ctx.getSessionVariable()).get(0));
    }

    private static int blocks(StatementBase stmt) {
        return AstMutationFuzzerTest.RootWalk
                .of(((QueryStatement) stmt).getQueryRelation()).blocks;
    }

    /**
     * The scoped pool keys material by a block ordinal from a structural walk, and it harvests from the
     * analyzed tree while mutating a fresh parse. That is only sound while the two walks agree, so the
     * agreement is pinned here rather than assumed.
     *
     * <p>A mismatch is not a disaster at run time -- {@code Pool.materialFor} compares the counts and
     * falls back to the file-wide pool -- but it silently turns the whole change off for that seed, and
     * a silently disabled optimisation looks exactly like one that does not work.
     */
    private static void assertBlockCountSurvivesAnalysis(String sql) {
        StatementBase parsed = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        StatementBase analyzed = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        Analyzer.analyze(analyzed, ctx);
        Assertions.assertEquals(blocks(parsed), blocks(analyzed),
                () -> "block count changed across analysis for " + sql);
    }

    @Test
    public void testBlockCountIsStableAcrossAnalysis() {
        assertBlockCountSurvivesAnalysis("select k, v from a where v > 1");
        assertBlockCountSurvivesAnalysis("select a.v from a join b on a.k = b.k");
        assertBlockCountSurvivesAnalysis("select s.k from (select k from b where w > 1) s");
        assertBlockCountSurvivesAnalysis("with t as (select v + 7 as x from a) select x from t");
        assertBlockCountSurvivesAnalysis("select k from a union all select k from b");
        assertBlockCountSurvivesAnalysis("select k from a where v in (select w from b)");
        assertBlockCountSurvivesAnalysis("select k from a where exists (select 1 from b where b.k = a.k)");
        assertBlockCountSurvivesAnalysis("select u.e from a, unnest([1, 2]) as u(e)");
        assertBlockCountSurvivesAnalysis(
                "select x.k from (select k from a union select k from b) x order by 1 limit 3");
        assertBlockCountSurvivesAnalysis(
                "with t1 as (select k from a), t2 as (select k from b) select t1.k from t1 join t2 on t1.k = t2.k");
    }

    /** Distinct query blocks must get distinct ids, or scoping them apart does nothing. */
    @Test
    public void testNestedBlocksAreNumberedSeparately() {
        StatementBase stmt = SqlParser.parse(
                "select s.k from (select k from b where w > 1) s where s.k > 2",
                ctx.getSessionVariable()).get(0);
        Assertions.assertEquals(2, blocks(stmt));

        StatementBase flat = SqlParser.parse("select k from a where v > 1", ctx.getSessionVariable()).get(0);
        Assertions.assertEquals(1, blocks(flat));
    }

    private static List<String> render(StatementBase stmt) {
        List<Expr> found =
                AstMutationFuzzerTest.collectRootExprs(((QueryStatement) stmt).getQueryRelation());
        return found.stream().map(e -> {
            try {
                String s = AstToSQLBuilder.toSQL(e);
                return s == null ? "" : s.replace('\n', ' ');
            } catch (Throwable t) {
                return "<" + e.getClass().getSimpleName() + ">";
            }
        }).collect(Collectors.toList());
    }

    private static void assertReaches(String sql, String fragment) {
        List<String> got = roots(sql);
        Assertions.assertTrue(got.stream().anyMatch(r -> r.contains(fragment)),
                () -> "no root contains " + fragment + " for " + sql + "\n  roots: " + got);
    }

    /** Not reachable even before analysis, i.e. the parser removed it rather than the walk missing it. */
    private static void assertAbsentEvenUnanalyzed(String sql, String fragment) {
        List<String> got = parsedRoots(sql);
        Assertions.assertTrue(got.stream().noneMatch(r -> r.contains(fragment)),
                () -> "expected the parser to have dropped " + fragment + " for " + sql + "\n  roots: " + got);
    }

    @Test
    public void testJoinOnPredicateIsReachable() {
        assertReaches("select a.v from a join b on a.k = b.k where a.v > 1", "`a`.`k` = `fuzz_reach`.`b`.`k`");
        assertReaches("select a.v from a left join b on a.k = b.k", "`a`.`k` = `fuzz_reach`.`b`.`k`");
        // A chain keeps every ON predicate, not just the outermost.
        String chain = "select a.v from a join b on a.k = b.k join c on b.k + 1 = c.k";
        assertReaches(chain, "`a`.`k` = `fuzz_reach`.`b`.`k`");
        assertReaches(chain, "`b`.`k` + 1");
    }

    @Test
    public void testSubtreeUnderAJoinIsReachable() {
        // The subquery is visible on its own; being a join operand must not hide it.
        String sql = "select a.v from a join (select k, w + 1 as w2 from b where w < 9) s on a.k = s.k";
        assertReaches(sql, "`b`.`w` + 1");
        assertReaches(sql, "`b`.`w` < 9");
    }

    @Test
    public void testCteAndTableFunctionAreReachable() {
        assertReaches("with t as (select v + 7 as x from a) select x from t", "`a`.`v` + 7");
        assertReaches("select u.e from a, unnest([1, 2]) as u(e)", "1");
    }

    @Test
    public void testFlatQueryStillReachesEverything() {
        List<String> got = roots("select v from a where v > 1 group by v having count(*) > 2 order by v");
        Assertions.assertTrue(got.size() >= 4, () -> "expected select/where/having/group roots: " + got);
    }

    /**
     * Sort keys were the second silent gap after joins: M5 could add or remove a whole ORDER BY, but the
     * walk never entered one, so no mutation ever edited a sort expression and none was ever pooled.
     */
    @Test
    public void testOrderByExpressionsAreReachable() {
        assertReaches("select k, v from a order by abs(v + 1)", "abs(`fuzz_reach`.`a`.`v` + 1)");
        assertReaches("select k, v from a order by v desc, k + 2 asc", "`a`.`k` + 2");
        // ORDER BY hangs off QueryRelation, so the one on a set operation is reached by the same branch.
        assertReaches("select k from a union all select k from b order by k + 3", "+ 3");
        // A sort key inside a subquery is reached through the FROM descent -- but only when a LIMIT
        // keeps it alive. `SubqueryRelation`'s constructor calls clearOrder() on an unlimited subquery,
        // so that ORDER BY is gone before the mutator ever sees the tree. Pinned in both directions
        // because the absent case looks exactly like a hole in the walk.
        assertReaches("select s.k from (select k from b order by k * 5 limit 3) s", "`b`.`k` * 5");
        assertAbsentEvenUnanalyzed("select s.k from (select k from b order by k * 5) s", "* 5");
    }
}
