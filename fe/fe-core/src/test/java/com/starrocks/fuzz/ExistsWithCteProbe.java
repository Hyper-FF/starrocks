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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Narrows down a fuzzer finding: {@code StarRocksPlannerException: Get columnRef with index 1202
 * out fieldMappings length}, thrown from {@code ExpressionMapping#getColumnRefWithIndex}.
 *
 * <p>1202 is not an off-by-one. A field index runs 0..fieldMappings.length-1 and is small; a number
 * that size is a ColumnRefOperator id, so an id from one space has been passed where an index of
 * another was expected. The mutation that produced it spliced an EXISTS whose subquery carries a
 * WITH clause, which is the shape these variants circle.
 *
 * <p>Reports rather than asserts, and prints the exception message so the index and the length are
 * both visible: the distance between them says whether this is a boundary case or a different id
 * space entirely.
 */
public class ExistsWithCteProbe {

    private static ConnectContext ctx;

    private static final Map<String, String> CANDIDATES = new LinkedHashMap<>();

    static {
        CANDIDATES.put("exists/cte", "select * from t where exists ("
                + "with c as (select k1 from t2) select k1 from c)");
        CANDIDATES.put("exists/cte-correlated", "select * from t where exists ("
                + "with c as (select k1, k2 from t2) select k1 from c where c.k2 > t.k2)");
        CANDIDATES.put("exists/cte-reused", "select * from t where exists ("
                + "with c as (select k1 from t2) select k1 from c union all select k1 from c)");
        CANDIDATES.put("exists/cte-case", "select * from t where exists ("
                + "with c as (select case when k2 is not null then 'a' else 'b' end as x, k1 from t2)"
                + " select k1 from c where c.x = 'a')");
        CANDIDATES.put("exists/cte-correlated-case", "select * from t where exists ("
                + "with c as (select case when k2 is not null then 'a' else 'b' end as x, k1, k2 from t2)"
                + " select k1 from c where c.x = 'a' and c.k2 > t.k2)");
        CANDIDATES.put("star/exists-cte", "select * from t where k1 = 2 and exists ("
                + "with c as (select k1 from t2) select k1 from c) limit 3");
        CANDIDATES.put("exists/nested-cte", "select * from t where exists ("
                + "with c as (with d as (select k1 from t2) select k1 from d) select k1 from c)");
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        StarRocksAssert srAssert = new StarRocksAssert(ctx);
        srAssert.withDatabase("cte_db").useDatabase("cte_db");
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
                String starrocks = "?";
                for (StackTraceElement f : st) {
                    if (f.getClassName().startsWith("com.starrocks")) {
                        starrocks = f.getClassName().substring(f.getClassName().lastIndexOf('.') + 1)
                                + "#" + f.getMethodName() + ":" + f.getLineNumber();
                        break;
                    }
                }
                outcome = t.getClass().getSimpleName() + " @ " + starrocks + "  :: "
                        + String.valueOf(t.getMessage()).replace('\n', ' ');
            }
            System.out.printf("%-26s %s%n", e.getKey(), outcome);
        }
    }
}
