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

import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Reachability probe for the TEMPORARY PARTITION deparse loss found in P0.5
 * (see DEPARSER_ROUNDTRIP_P05_REPORT.md §4.1).
 *
 * <p>{@code AST2SQLVisitor.visitTable} used to emit {@code PARTITION (...)} without consulting
 * {@code PartitionNames.isTemp()}, so {@code TEMPORARY PARTITION (p)} and {@code PARTITION (p)}
 * serialized to byte-identical SQL and a view over a temporary partition never resolved again.
 * Now asserts the fixed behaviour.
 */
public class TempPartitionDeparseTest {

    private static final String DB = "temp_part_deparse_db";

    private static ConnectContext ctx;
    private static StarRocksAssert srAssert;

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        srAssert = new StarRocksAssert(ctx);
        srAssert.withDatabase(DB).useDatabase(DB);
        srAssert.withTable("CREATE TABLE t (k date, v int)\n"
                + "DUPLICATE KEY(k)\n"
                + "PARTITION BY RANGE(k) (\n"
                + "  PARTITION p1 VALUES LESS THAN ('2020-01-01'),\n"
                + "  PARTITION p2 VALUES LESS THAN ('2021-01-01')\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES('replication_num'='1')");
        // A real temporary partition, needed before anything can reference one.
        srAssert.ddl("ALTER TABLE t ADD TEMPORARY PARTITION tp1 VALUES LESS THAN ('2020-01-01')");
    }

    private static String deparse(String sql) {
        StatementBase stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        return AstToSQLBuilder.toSQL(stmt);
    }

    /** The core defect: the two forms are semantically different but serialize identically. */
    @Test
    public void testTemporaryQualifierIsLostOnDeparse() {
        String temp = deparse("select * from t temporary partition(tp1)");
        String formal = deparse("select * from t partition(tp1)");

        // Fixed: the two forms are semantically different and must serialize differently.
        Assertions.assertNotEquals(formal, temp, "TEMPORARY qualifier lost again");
        Assertions.assertTrue(temp.contains("TEMPORARY PARTITION (`tp1`)"), temp);
        Assertions.assertFalse(formal.toUpperCase().contains("TEMPORARY"), formal);
    }

    /** Same loss on the INSERT target, which would redirect writes to the formal partition. */
    @Test
    public void testTemporaryQualifierIsLostOnInsertTarget() {
        String temp = deparse("insert into t temporary partition(tp1) select * from t");
        Assertions.assertTrue(temp.contains("TEMPORARY PARTITION (tp1)"),
                "TEMPORARY qualifier lost again on INSERT target: " + temp);
    }

    /** Can a temporary partition share a name with a formal one? Decides the blast radius. */
    // A probe, not a test: it catches everything and asserts nothing, so it can only ever
    // pass. Useful to run by hand when deciding how far a defect reaches; useless in CI.
    @EnabledIfSystemProperty(named = "srfuzz.probe", matches = ".+")
    @Test
    public void testWhetherTempAndFormalPartitionsMayShareAName() {
        String outcome;
        try {
            srAssert.ddl("ALTER TABLE t ADD TEMPORARY PARTITION p1 VALUES LESS THAN ('2020-01-01')");
            outcome = "ALLOWED — a temp partition may reuse a formal partition's name";
        } catch (Throwable t) {
            outcome = "REJECTED — " + t.getClass().getSimpleName() + ": " + t.getMessage();
        }
        System.out.println("[same-name temp partition] " + outcome);
    }

    /** Does the loss reach persisted metadata via a view's stored (deparsed) definition? */
    // A probe, not a test: it catches everything and asserts nothing, so it can only ever
    // pass. Useful to run by hand when deciding how far a defect reaches; useless in CI.
    @EnabledIfSystemProperty(named = "srfuzz.probe", matches = ".+")
    @Test
    public void testWhetherLossReachesStoredViewDefinition() throws Exception {
        String outcome;
        String storedDef = null;
        try {
            srAssert.withView("CREATE VIEW v_temp AS SELECT * FROM t TEMPORARY PARTITION (tp1)");
            Table tbl = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(DB, "v_temp");
            storedDef = ((View) tbl).getInlineViewDef();
            outcome = "VIEW CREATED";
        } catch (Throwable t) {
            outcome = "VIEW REJECTED — " + t.getClass().getSimpleName() + ": " + t.getMessage();
        }
        System.out.println("[view over temp partition] " + outcome);
        if (storedDef != null) {
            System.out.println("[stored inlineViewDef   ] " + storedDef.replace('\n', ' '));
            System.out.println("[TEMPORARY preserved?   ] " + storedDef.toUpperCase().contains("TEMPORARY"));
            // If the stored text lost TEMPORARY, re-reading the view resolves tp1 as a formal
            // partition. Partition names are unique across both namespaces, so this errors rather
            // than silently reading the wrong data.
            System.out.println("[re-analyze stored def  ] " + reanalyzeOutcome(storedDef));
            // The ORDER BY ordinal defect showed that a failing round trip does not always mean a
            // broken view -- the view path may not re-run the same checks. Query it for real.
            System.out.println("[SELECT * FROM the view ] " + reanalyzeOutcome("select * from v_temp"));
        }
    }

    private static String reanalyzeOutcome(String sql) {
        try {
            StatementBase stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
            return "ANALYZES OK";
        } catch (Throwable t) {
            return "FAILS — " + t.getClass().getSimpleName() + ": " + t.getMessage();
        }
    }
}
