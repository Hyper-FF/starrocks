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

    /** The texts the mutator would see as roots for this statement. */
    private static List<String> roots(String sql) {
        StatementBase stmt = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        Analyzer.analyze(stmt, ctx);
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
}
