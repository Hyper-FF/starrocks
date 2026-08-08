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
 * Minimises "Get columnRef with index N out fieldMappings length" down from an 18kB mutant.
 *
 * <p>The ingredient the hand-written shapes were missing turned out to be visible in the mutant's
 * text: the spliced CTE carried the SEED's predicate into a join's ON clause, unqualified --
 * {@code ... ON (b.k = a.k) AND (c250 = 2)} where {@code c250} exists only on the OUTER table. An
 * unqualified reference that resolves outward is what puts an outer-scope field index in front of
 * the inner mapping.
 *
 * <p>Width is the other half. {@code SqlToScalarOperatorTranslator$Visitor#visitSlot} hands
 * {@code resolvedField.getRelationFieldIndex()} -- an index into the scope the field resolved in --
 * to {@code ExpressionMapping#getColumnRefWithIndex}, which indexes THIS mapping. The guard only
 * fires when the outer index exceeds the inner mapping's length, so a wide outer relation and a
 * LATE column are both needed; with a narrow one the same mistake returns a wrong column quietly.
 *
 * <p>Reports rather than asserts. Prints the resolved position so "which column did it get" is
 * answerable for the cases that do not throw.
 */
public class OuterScopeIndexMinimiseProbe {

    /** Wide enough that a late column's index cannot be a valid index into a two-column mapping. */
    private static final int WIDE_COLUMNS = 300;
    private static final String LATE = "c" + (WIDE_COLUMNS - 10);

    private static ConnectContext ctx;

    private static final Map<String, String> CANDIDATES = new LinkedHashMap<>();

    static {
        // The suspected minimum: unqualified outer column, inside a join's ON clause, inside a CTE,
        // inside EXISTS -- each layer removed in turn below.
        CANDIDATES.put("cte+join-on/unqualified",
                "select * from wide where exists ("
                        + "with c as (select a.k1 from n1 a left join n2 b on a.k1 = b.k1 and " + LATE + " = 2)"
                        + " select k1 from c)");
        CANDIDATES.put("join-on/unqualified",
                "select * from wide where exists ("
                        + "select a.k1 from n1 a left join n2 b on a.k1 = b.k1 and " + LATE + " = 2)");
        CANDIDATES.put("where/unqualified",
                "select * from wide where exists (select k1 from n1 where " + LATE + " = 2)");
        CANDIDATES.put("where/qualified",
                "select * from wide where exists (select k1 from n1 where wide." + LATE + " = 2)");
        // Is the outer star required, or just the width of the relation?
        CANDIDATES.put("narrow-projection/unqualified",
                "select k1 from wide where exists (select k1 from n1 where " + LATE + " = 2)");
        // An EARLY outer column: same mistake, but the index lands inside the inner mapping, so
        // nothing fires. This is the silent half.
        CANDIDATES.put("early-column/unqualified",
                "select k1 from wide where exists (select k1 from n1 where c1 = 2)");
        // No subquery at all, as a control.
        CANDIDATES.put("control/no-subquery",
                "select k1 from wide where " + LATE + " = 2");
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        StarRocksAssert srAssert = new StarRocksAssert(ctx);
        srAssert.withDatabase("outer_db").useDatabase("outer_db");

        StringBuilder wide = new StringBuilder("create table wide (k1 int");
        for (int i = 1; i <= WIDE_COLUMNS; i++) {
            wide.append(", c").append(i).append(" int");
        }
        wide.append(") duplicate key(k1) distributed by hash(k1) buckets 1 "
                + "properties('replication_num'='1')");
        srAssert.withTable(wide.toString());

        // The inner tables are deliberately narrow: the defect needs the inner mapping to be
        // SHORTER than the outer field index, and two columns is as short as is useful.
        for (String name : new String[] {"n1", "n2"}) {
            srAssert.withTable("create table " + name + " (k1 int, k2 int) "
                    + "duplicate key(k1) distributed by hash(k1) buckets 1 "
                    + "properties('replication_num'='1')");
        }
    }

    @Test
    public void probe() {
        System.out.printf("wide table: %d columns, correlating on %s%n", WIDE_COLUMNS, LATE);
        for (Map.Entry<String, String> e : CANDIDATES.entrySet()) {
            String outcome;
            try {
                StatementBase stmt = SqlParser.parse(e.getValue(), ctx.getSessionVariable()).get(0);
                ctx.setThreadLocalInfo();
                StatementPlanner.plan(stmt, ctx);
                outcome = "OK";
            } catch (Throwable t) {
                String frame = "?";
                for (StackTraceElement f : t.getStackTrace()) {
                    if (f.getClassName().startsWith("com.starrocks")
                            && !f.getClassName().endsWith("ExpressionMapping")) {
                        frame = f.getClassName().substring(f.getClassName().lastIndexOf('.') + 1)
                                + "#" + f.getMethodName() + ":" + f.getLineNumber();
                        break;
                    }
                }
                outcome = t.getClass().getSimpleName() + " @ " + frame + " :: "
                        + String.valueOf(t.getMessage()).replace('\n', ' ');
            }
            System.out.printf("%-32s %s%n", e.getKey(), outcome.length() > 130
                    ? outcome.substring(0, 130) : outcome);
        }
    }
}
