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
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Random;

/** M9: the flag must actually change the analyzed tree, and must always be put back. */
public class SessionFlagPerturbationTest {
    private static ConnectContext ctx;

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        StarRocksAssert a = new StarRocksAssert(ctx);
        a.withDatabase("fuzz_flag").useDatabase("fuzz_flag");
        a.withTable("CREATE TABLE t (k int, v int) DUPLICATE KEY(k)"
                + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
    }

    /** Deparsed analyzed form, or the rejection, so both sides of a flag are comparable text. */
    private static String run(String sql) {
        try {
            StatementBase stmt = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
            Analyzer.analyze(stmt, ctx);
            return AstToSQLBuilder.toSQL(stmt).replace('\n', ' ');
        } catch (Throwable t) {
            return "<" + t.getClass().getSimpleName() + ">";
        }
    }

    @Test
    public void testEveryKnobActuallyChangesTheAnalyzedTree() {
        // The whole point of the operator, and the reason the set is only two entries: a flag that
        // leaves the deparsed text alone cannot be observed by a text oracle, so it would burn
        // iterations while looking like coverage. Each knob here has to earn its place.
        SessionVariable sv = ctx.getSessionVariable();

        boolean alias = sv.getEnableGroupbyUseOutputAlias();
        try {
            sv.setEnableGroupbyUseOutputAlias(false);
            String off = run("select v as k from t group by k");
            sv.setEnableGroupbyUseOutputAlias(true);
            String on = run("select v as k from t group by k");
            Assertions.assertNotEquals(off, on, "enable_groupby_use_output_alias is inert");
            // It flips the statement between rejected and accepted, which is the strongest form of
            // observable: whole regions of the analyzer are only reachable on one side of it.
            Assertions.assertTrue(off.contains("SemanticException"), off);
            Assertions.assertTrue(on.contains("GROUP BY"), on);
        } finally {
            sv.setEnableGroupbyUseOutputAlias(alias);
        }

        long mode = sv.getSqlMode();
        try {
            sv.setSqlMode(SqlModeHelper.MODE_DEFAULT);
            String off = run("select 'a' || 'b'");
            sv.setSqlMode(SqlModeHelper.MODE_DEFAULT | SqlModeHelper.MODE_PIPES_AS_CONCAT);
            String on = run("select 'a' || 'b'");
            Assertions.assertNotEquals(off, on, "sql_mode is inert");
            // Parser level, so the same text becomes a different tree outright.
            Assertions.assertTrue(off.contains("OR"), off);
            Assertions.assertTrue(on.contains("concat"), on);
        } finally {
            sv.setSqlMode(mode);
        }
    }

    @Test
    public void testPerturbationAlwaysRestores() {
        SessionVariable sv = ctx.getSessionVariable();
        Random rnd = new Random(42);
        // A leaked flag silently reinterprets every later seed, so restoring matters more than firing.
        for (int i = 0; i < 200; i++) {
            boolean alias = sv.getEnableGroupbyUseOutputAlias();
            long mode = sv.getSqlMode();

            SessionFlagPerturbation.Perturbation p = SessionFlagPerturbation.apply(sv, rnd);
            if (p != null) {
                Assertions.assertTrue(p.description().startsWith("M9-session: "), p.description());
                p.close();
            }

            Assertions.assertEquals(alias, sv.getEnableGroupbyUseOutputAlias());
            Assertions.assertEquals(mode, sv.getSqlMode());
        }
    }

    @Test
    public void testPerturbationActuallyFires() {
        SessionVariable sv = ctx.getSessionVariable();
        Random rnd = new Random(7);
        int fired = 0;
        for (int i = 0; i < 200; i++) {
            SessionFlagPerturbation.Perturbation p = SessionFlagPerturbation.apply(sv, rnd);
            if (p != null) {
                fired++;
                p.close();
            }
        }
        // apply() returns null when the drawn value equals the current one, which is common for the
        // boolean knobs; over 200 draws it must still fire most of the time.
        Assertions.assertTrue(fired > 100, "only fired " + fired + " times out of 200");
        Assertions.assertTrue(SessionFlagPerturbation.knobCount() >= 2,
                "knob set shrank to " + SessionFlagPerturbation.knobCount());
    }
}
