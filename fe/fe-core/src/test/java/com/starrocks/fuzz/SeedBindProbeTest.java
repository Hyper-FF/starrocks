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
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Reports WHY a corpus fails to bind, which the fuzzer itself cannot tell you.
 *
 * {@code AstMutationFuzzerTest} drops any seed that does not analyze with a bare {@code continue}
 * (the analyze-probe at its seed loop). That is right for fuzzing -- an unbindable seed has nothing
 * to mutate -- but it means a corpus that binds at 2% and one that binds at 95% produce reports
 * that look alike, differing only in a seed count nobody reads as a percentage. When a corpus ships
 * without DDL and the schema underneath it is synthesised, that silent drop hides the one number
 * that decides whether the run was worth anything.
 *
 * Run with {@code -Dsrfuzz.probe=<file-or-dir>}.
 */
public class SeedBindProbeTest {

    private static ConnectContext ctx;
    private static StarRocksAssert srAssert;

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        srAssert = new StarRocksAssert(ctx);
    }

    @Test
    @EnabledIfSystemProperty(named = "srfuzz.probe", matches = ".+")
    public void probe() throws Exception {
        List<Path> files = new ArrayList<>();
        Path root = Paths.get(System.getProperty("srfuzz.probe"));
        if (Files.isDirectory(root)) {
            try (java.util.stream.Stream<Path> s = Files.walk(root)) {
                s.filter(Files::isRegularFile).filter(p -> p.toString().endsWith(".sql")).sorted()
                        .forEach(files::add);
            }
        } else {
            files.add(root);
        }

        int fileIdx = 0;
        for (Path f : files) {
            String db = "probe_db_" + (fileIdx++);
            srAssert.withDatabase(db).useDatabase(db);

            String text = new String(Files.readAllBytes(f), StandardCharsets.UTF_8);
            List<String> statements = CorpusReader.extractStatements(text);

            int ddlOk = 0;
            int ddlFail = 0;
            int queries = 0;
            int bound = 0;
            Map<String, Integer> reasons = new LinkedHashMap<>();
            Map<String, String> firstExample = new LinkedHashMap<>();

            for (String sql : statements) {
                StatementBase ast;
                try {
                    ast = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
                } catch (Throwable t) {
                    bump(reasons, "PARSE: " + oneLine(t));
                    firstExample.putIfAbsent("PARSE: " + oneLine(t), snippet(sql));
                    continue;
                }
                if (CorpusReader.isSchemaSetup(ast)) {
                    try {
                        srAssert.withTable(sql);
                        ddlOk++;
                    } catch (Throwable t) {
                        ddlFail++;
                        bump(reasons, "DDL: " + oneLine(t));
                        firstExample.putIfAbsent("DDL: " + oneLine(t), snippet(sql));
                    }
                    continue;
                }
                if (!(ast instanceof QueryStatement)) {
                    continue;
                }
                queries++;
                try {
                    Analyzer.analyze(ast, ctx);
                    bound++;
                } catch (Throwable t) {
                    String key = oneLine(t);
                    bump(reasons, key);
                    firstExample.putIfAbsent(key, snippet(sql));
                }
            }

            System.out.println("=== " + f.getFileName() + " ===");
            System.out.println(String.format("DDL applied: %d ok / %d failed", ddlOk, ddlFail));
            System.out.println(String.format("queries: %d, bound: %d (%.1f%%)",
                    queries, bound, queries == 0 ? 0.0 : 100.0 * bound / queries));
            System.out.println("-- failure reasons, most frequent first --");
            new TreeMap<>(reasons).entrySet().stream()
                    .sorted(Comparator.comparingInt(e -> -e.getValue()))
                    .limit(25)
                    .forEach(e -> {
                        System.out.println(String.format("%5d  %s", e.getValue(), e.getKey()));
                        System.out.println("       e.g. " + firstExample.get(e.getKey()));
                    });
            System.out.println();
        }
    }

    private static void bump(Map<String, Integer> m, String k) {
        m.merge(k, 1, Integer::sum);
    }

    /** Collapses a throwable to a stable one-line key: identifiers vary, the shape does not. */
    private static String oneLine(Throwable t) {
        String msg = t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage();
        msg = msg.replaceAll("\\s+", " ").trim();
        msg = msg.replaceAll("(col|tbl|sch)_\\d+", "$1_N");
        if (msg.length() > 160) {
            msg = msg.substring(0, 160) + "...";
        }
        return msg;
    }

    private static String snippet(String sql) {
        String s = sql.replaceAll("\\s+", " ").trim();
        return s.length() > 200 ? s.substring(0, 200) + "..." : s;
    }
}
