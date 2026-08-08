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
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Narrows down a fuzzer finding: {@code NullPointerException} at {@code Objects#requireNonNull}
 * during planning.
 *
 * <p>The first reading of the mutation blamed a negative {@code array_slice} offset, and nine
 * variants of that all planned cleanly -- the edit was one link of a three-step chain and the
 * others mattered more. The full mutant is a correlated EXISTS whose subquery is a UNION ALL, with
 * the correlation to the outer table appearing in ONE branch, and that branch carrying a GROUP BY.
 * Subquery unnesting over a set operation is where that combination would bite.
 *
 * <p>One run over several variants, so a single build says which ingredient is required rather than
 * one build per guess. Result: a correlated EXISTS and a UNION in the subquery are both required;
 * aggregation, the DISTINCT derived table and the outer GROUP BY the original mutant carried are
 * all incidental. See srfuzz/docs/findings/correlated-exists-over-union-npe.md.
 *
 * <p>A probe, not an assertion: it reports and does not fail. What it is for is finding the minimal
 * statement; the regression test comes after, once there is something definite to pin.
 */
public class CorrelatedExistsUnionProbe {

    private static ConnectContext ctx;

    private static final Map<String, String> CANDIDATES = new LinkedHashMap<>();

    static {
        // The suspected combination, and then each ingredient removed in turn.
        CANDIDATES.put("corr-exists/union+agg",
                "select k1 from t where exists ("
                        + "select count(*) as n from t2 where t2.k1 = 1 group by t2.k1"
                        + " union all "
                        + "select count(*) as n from t3 where t3.k2 > t.k2 group by t3.k2)");
        CANDIDATES.put("corr-exists/union+agg/corr-first",
                "select k1 from t where exists ("
                        + "select count(*) as n from t2 where t2.k2 > t.k2 group by t2.k1"
                        + " union all "
                        + "select count(*) as n from t3 where t3.k1 = 1 group by t3.k2)");
        CANDIDATES.put("corr-exists/union-no-agg",
                "select k1 from t where exists ("
                        + "select k1 from t2 where t2.k1 = 1"
                        + " union all "
                        + "select k1 from t3 where t3.k2 > t.k2)");
        CANDIDATES.put("corr-exists/agg-no-union",
                "select k1 from t where exists ("
                        + "select count(*) as n from t3 where t3.k2 > t.k2 group by t3.k2)");
        CANDIDATES.put("uncorr-exists/union+agg",
                "select k1 from t where exists ("
                        + "select count(*) as n from t2 where t2.k1 = 1 group by t2.k1"
                        + " union all "
                        + "select count(*) as n from t3 where t3.k1 = 2 group by t3.k2)");
        CANDIDATES.put("corr-exists/union+agg/distinct-derived",
                "select k1 from t where exists ("
                        + "select count(*) as n from (select distinct k1, k2 from t2) d"
                        + " where d.k1 = 1 group by d.k1"
                        + " union all "
                        + "select count(*) as n from t3 where t3.k2 > t.k2 group by t3.k2)");
        CANDIDATES.put("corr-exists/union+agg/outer-agg",
                "select k1, min(k2) as m from t where exists ("
                        + "select count(*) as n from t2 where t2.k1 = 1 group by t2.k1"
                        + " union all "
                        + "select count(*) as n from t3 where t3.k2 > t.k2 group by t3.k2)"
                        + " group by k1 order by k1 limit 5");
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        StarRocksAssert srAssert = new StarRocksAssert(ctx);
        srAssert.withDatabase("corr_db").useDatabase("corr_db");
        for (String name : new String[] {"t", "t2", "t3"}) {
            srAssert.withTable("create table " + name + " (k1 int, k2 int, k3 varchar(64)) "
                    + "duplicate key(k1) distributed by hash(k1) buckets 1 "
                    + "properties('replication_num'='1')");
        }
    }

    /**
     * Run with {@code -Dsrfuzz.probe=1}. Gated because it reports rather than asserts: asserting the
     * current behaviour would pin a defect as correct, and asserting the fixed behaviour would fail
     * CI until someone fixes it. The finding is written up in
     * srfuzz/docs/findings/correlated-exists-over-union-npe.md; this is how to reproduce it.
     */
    @Test
    @EnabledIfSystemProperty(named = "srfuzz.probe", matches = ".+")
    public void probe() {
        for (Map.Entry<String, String> e : CANDIDATES.entrySet()) {
            String outcome;
            try {
                StatementBase stmt = SqlParser.parse(e.getValue(), ctx.getSessionVariable()).get(0);
                ctx.setThreadLocalInfo();
                StatementPlanner.plan(stmt, ctx);
                outcome = "OK";
            } catch (Throwable t) {
                // The frame that matters is the first one in StarRocks or the JDK call that threw,
                // not the exception type: every one of these is some flavour of NPE and the site is
                // what separates them.
                StackTraceElement[] st = t.getStackTrace();
                String site = st.length > 0 ? st[0].getClassName() + "#" + st[0].getMethodName() : "?";
                String starrocks = "?";
                for (StackTraceElement f : st) {
                    if (f.getClassName().startsWith("com.starrocks")) {
                        starrocks = f.getClassName().substring(f.getClassName().lastIndexOf('.') + 1)
                                + "#" + f.getMethodName() + ":" + f.getLineNumber();
                        break;
                    }
                }
                outcome = t.getClass().getSimpleName() + " at " + site + "  first-starrocks-frame: " + starrocks;
            }
            System.out.printf("%-26s %s%n", e.getKey(), outcome);
        }
    }
}
