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

import com.starrocks.common.AnalysisException;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Triage for candidates reported by {@link AstMutationFuzzerTest}.
 *
 * <p>The fuzzer mutates the AST directly through {@code TreeNode.setChild}, which can build trees the
 * grammar could never produce (for example an {@code ExistsPredicate} whose child is a {@code SlotRef}).
 * A finding only counts as a real defect if it reproduces from <b>hand-written SQL that the parser
 * accepts</b>. This runs each candidate as plain SQL and prints how the FE fails:
 *
 * <ul>
 *   <li>{@code DECLARED} — SemanticException/AnalysisException. Correct behaviour, not a defect.</li>
 *   <li>{@code INTERNAL} — NPE/CCE/ISE/IAE. The FE should have rejected the statement cleanly.</li>
 *   <li>{@code UNPARSEABLE} — the grammar rejects it, so the fuzzer's tree was unreachable: artifact.</li>
 * </ul>
 */
public class FuzzFindingTriageTest {

    private static final String DB = "fuzz_triage_db";

    private static ConnectContext ctx;

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        StarRocksAssert srAssert = new StarRocksAssert(ctx);
        srAssert.withDatabase(DB).useDatabase(DB);
        srAssert.withTable("CREATE TABLE t1 (\n"
                + "  k1 int,\n"
                + "  c1 varchar(64),\n"
                + "  c6 int,\n"
                + "  d1 decimal(38,10),\n"
                + "  dt datetime\n"
                + ") DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES('replication_num'='1')");
        srAssert.withTable("CREATE TABLE arr (\n"
                + "  id int,\n"
                + "  a_bigint array<bigint>,\n"
                + "  a_datetime array<datetime>,\n"
                + "  a_str array<varchar(32)>\n"
                + ") DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES('replication_num'='1')");
    }

    /**
     * Full production-shape round trip. A statement that analyzes but whose deparsed form no longer
     * analyzes is a deparser defect: it is what gets persisted as a view/MV definition.
     */
    private static String roundTrip(String sql) {
        StatementBase stmt;
        try {
            stmt = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        } catch (Throwable t) {
            return "UNPARSEABLE (artifact) — " + t.getClass().getSimpleName();
        }
        try {
            Analyzer.analyze(stmt, ctx);
        } catch (Throwable t) {
            return "seed rejected — " + t.getClass().getSimpleName();
        }
        String out;
        try {
            out = com.starrocks.sql.analyzer.AstToSQLBuilder.toSQL(stmt);
        } catch (Throwable t) {
            return "DEPARSE THREW (BUG) — " + t.getClass().getSimpleName();
        }
        StatementBase again;
        try {
            again = SqlParser.parse(out, ctx.getSessionVariable()).get(0);
        } catch (Throwable t) {
            return "REPARSE FAIL (BUG) — " + t.getClass().getSimpleName() + " || out: " + abbrev2(out);
        }
        try {
            Analyzer.analyze(again, ctx);
        } catch (Throwable t) {
            String m = t.getMessage();
            return "REANALYZE FAIL (BUG) — " + (m == null ? t.getClass().getSimpleName() : m.replace('\n', ' '))
                    + " || out: " + abbrev2(out);
        }
        return "round-trips OK";
    }

    private static String abbrev2(String s) {
        String x = s.replace('\n', ' ').replaceAll("\\s+", " ").trim();
        return x.length() > 170 ? x.substring(0, 170) + " ..." : x;
    }

    @Test
    public void triageRemainingCandidates() {
        System.out.println();
        System.out.println("=== remaining candidates: analyze-only ===");
        String[][] analyzeOnly = {
                // x11 ClassCastException@PolymorphicFunctionAnalyzer — not a Preconditions guard
                {"array_contains(NULL, row(...))", "select array_contains(NULL, row(20, 'world'))"},
                {"array_contains baseline", "select array_contains(null, null)"},
                {"array_contains(NULL, array)", "select array_contains(NULL, [1,2])"},

                // x2 ClassCastException@visitMultiInPredicate
                {"multi-col IN non-subquery", "select * from t1 where (k1, c1) not in ('l')"},
        };
        for (String[] c : analyzeOnly) {
            System.out.printf("%-32s | %s%n", c[0], triage(c[1]));
        }

        System.out.println();
        System.out.println("=== remaining candidates: full round trip ===");
        String[][] roundTrips = {
                // x18 REANALYZE_FAIL@verifyNoSubQuery — ORDER BY ordinal expanded into a subquery
                {"ORDER BY ordinal over IN-subquery",
                        "select id in (select id from arr t) from arr s order by 1"},
                {"ORDER BY ordinal, plain item", "select id from arr s order by 1"},
                {"ORDER BY ordinal desc/nulls", "select id, a_str from arr order by 2 desc nulls first, 1"},
                {"ORDER BY explicit expression", "select id from arr order by id + 1"},
                {"ORDER BY alias", "select id + 1 as x from arr order by x"},
                {"ORDER BY on union", "select id from arr union all select id from arr order by 1"},

                // x6 REANALYZE_FAIL@getAnalyzedFunction — untyped array literal gains a concrete type
                {"array_contains_all with [NULL]",
                        "select id from arr where array_contains_all(a_datetime, [NULL])"},
                {"array literal in select", "select [NULL] from arr"},
                {"typed array literal kept", "select array_contains(a_datetime, cast('2020-01-01' as datetime)) from arr"},
                {"date array literal", "select cast(['2020-01-01'] as array<date>) from arr"},
                {"empty array literal", "select array_length([]) from arr"},
                {"nested null array", "select array_contains_all(a_bigint, [NULL, NULL]) from arr"},
        };
        for (String[] c : roundTrips) {
            System.out.printf("%-38s | %s%n", c[0], roundTrip(c[1]));
        }
    }

    private static String triage(String sql) {
        StatementBase stmt;
        try {
            stmt = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        } catch (Throwable t) {
            return "UNPARSEABLE (artifact) — " + t.getClass().getSimpleName();
        }
        try {
            Analyzer.analyze(stmt, ctx);
            return "ACCEPTED (no error at all)";
        } catch (Throwable t) {
            if (t instanceof SemanticException || t instanceof AnalysisException
                    || t instanceof StarRocksException) {
                return "DECLARED (correct) — " + t.getClass().getSimpleName();
            }
            String m = t.getMessage();
            return "INTERNAL (BUG) — " + t.getClass().getSimpleName() + ": "
                    + (m == null ? "<no message>" : m.replace('\n', ' '));
        }
    }

    @Test
    public void triageCandidates() {
        String[][] candidates = {
                // x1919 IllegalArgumentException@Preconditions#checkArgument
                {"array_generate wrong arg type", "select array_generate(0.0, '2025-10-05', 1, 'day')"},
                {"array_generate baseline", "select array_generate(1, 5, 1)"},

                // x492 ClassCastException@AST2StringVisitor#visitFunctionCall
                {"time_slice int 3rd arg", "select time_slice('9999-12-31 23:59:59', interval 5 year, 1)"},
                {"time_slice baseline", "select time_slice('9999-12-31 23:59:59', interval 5 year, ceil)"},

                // x50 NullPointerException@Preconditions#checkNotNull — plainly user-writable
                {"hex(to_bitmap(...))", "select bitmap_to_string(hex(to_bitmap(c6))) from t1"},
                {"to_bitmap baseline", "select bitmap_to_string(to_bitmap(c6)) from t1"},

                // x55 IllegalStateException@Preconditions#checkState (decimal arithmetic)
                {"decimal * datetime", "select d1 * dt from t1"},
                {"decimal * varchar", "select d1 * c1 from t1"},

                // x335 ClassCastException@ExpressionAnalyzer#visitLambdaFunctionExpr
                {"nested lambda arrow", "select array_map(x -> array_map(y -> y, x) -> x, [1,2])"},

                // x88 / x129 — suspected mutator artifacts (EXISTS with a non-subquery child)
                {"EXISTS on a string", "select * from t1 where exists 'HIGH'"},
                {"EXISTS on a column", "select * from t1 where not (exists c1)"},

                // x22 dict_mapping non-literal first arg
                {"dict_mapping non-literal", "select dict_mapping(cast(null as json), c1, c1) from t1"},
        };

        System.out.println();
        System.out.println("=== fuzz finding triage ===");
        for (String[] c : candidates) {
            System.out.printf("%-28s | %-46s | %s%n", c[0], abbrev(c[1]), triage(c[1]));
        }
    }

    private static String abbrev(String s) {
        return s.length() > 46 ? s.substring(0, 43) + "..." : s;
    }

    @Test
    public void probeArrayContainsNullHandling() {
        System.out.println();
        System.out.println("=== array_contains / array_position NULL handling ===");
        String[] sqls = {
                "select array_contains(null, null)",
                "select array_contains(NULL, 1)",
                "select array_contains(NULL, 'x')",
                "select array_contains(NULL, [1,2])",
                "select array_contains(NULL, row(20, 'world'))",
                "select array_contains([1,2], NULL)",
                "select array_contains(cast(null as array<int>), 1)",
                "select array_contains(cast(null as array<int>), NULL)",
                "select array_position(NULL, 1)",
                "select array_position(NULL, [1,2])",
                "select array_length(NULL)",
                "select array_append(NULL, 1)",
        };
        for (String sql : sqls) {
            System.out.printf("%-52s | %s%n", sql, triage(sql));
        }
    }
}
