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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Replays one recorded mutant against the schema it was produced on, and prints where it broke.
 *
 * <p>Some findings only exist at the corpus's own scale. The one this was written for reports
 * {@code Get columnRef with index 1202 out fieldMappings length}: a hand-written three-column
 * schema cannot reach a column-ref id of 1202, and the seven shapes tried against one planned
 * cleanly. Rather than keep guessing at the shape, this rebuilds the actual database from the
 * corpus file and runs the actual 18kB statement.
 *
 * <p>Prints the first several StarRocks frames rather than one. The signature the soak records is
 * the top frame, which for this defect is where the check FIRED and not where the bad index was
 * produced -- and the second is the question worth answering.
 *
 * <pre>
 *   -Dsrfuzz.replay.setup=&lt;corpus .sql&gt; -Dsrfuzz.replay.sql=&lt;file with one statement&gt;
 * </pre>
 */
public class ExactMutantReplayProbe {

    private static ConnectContext ctx;
    private static StarRocksAssert srAssert;

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        srAssert = new StarRocksAssert(ctx);
    }

    @Test
    @EnabledIfSystemProperty(named = "srfuzz.replay.sql", matches = ".+")
    public void replay() throws Exception {
        String setupPath = System.getProperty("srfuzz.replay.setup");
        // The emitted statements are fully qualified with the database the corpus file ran in, so
        // the replay has to recreate it under that exact name or nothing binds.
        String db = System.getProperty("srfuzz.replay.db", "srfuzz_mut_0");
        srAssert.withDatabase(db).useDatabase(db);

        int applied = 0;
        int skipped = 0;
        if (setupPath != null) {
            String text = new String(Files.readAllBytes(Paths.get(setupPath)), StandardCharsets.UTF_8);
            for (String sql : CorpusReader.extractStatements(text)) {
                StatementBase ast;
                try {
                    ast = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
                } catch (Throwable t) {
                    skipped++;
                    continue;
                }
                if (!CorpusReader.isSchemaSetup(ast)) {
                    continue;
                }
                if (CorpusReader.applySchemaSetup(srAssert, sql, ast)) {
                    applied++;
                } else {
                    skipped++;
                }
            }
        }
        System.out.printf("setup: %d applied, %d skipped%n", applied, skipped);

        String mutant = new String(Files.readAllBytes(
                Paths.get(System.getProperty("srfuzz.replay.sql"))), StandardCharsets.UTF_8).trim();
        System.out.printf("statement: %d chars%n", mutant.length());

        try {
            StatementBase stmt = SqlParser.parse(mutant, ctx.getSessionVariable()).get(0);
            ctx.setThreadLocalInfo();
            StatementPlanner.plan(stmt, ctx);
            System.out.println("RESULT: planned cleanly -- the finding did not reproduce here");
        } catch (Throwable t) {
            System.out.println("RESULT: " + t.getClass().getName() + ": " + t.getMessage());
            int shown = 0;
            for (StackTraceElement f : t.getStackTrace()) {
                if (!f.getClassName().startsWith("com.starrocks")) {
                    continue;
                }
                System.out.printf("   at %s#%s:%d%n", f.getClassName(), f.getMethodName(), f.getLineNumber());
                if (++shown >= 12) {
                    break;
                }
            }
        }
    }
}
