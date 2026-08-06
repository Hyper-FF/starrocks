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
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.StorageAccessException;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.PrintWriter;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * P0.5 of srfuzz/docs/SQL_AST_FUZZER_PLAN.md.
 *
 * <p>{@code RoundTripFidelityChecker} in fe-fuzz measures the deparser on <b>unanalyzed</b> ASTs,
 * which is not how {@link AstToSQLBuilder} is used in production. This re-measures the same corpus in
 * the production shape — {@code parse -> analyze -> deparse -> parse -> analyze -> deparse} — against a
 * real in-process catalog built from each corpus file's own DDL.
 *
 * <p>It lives in fe-core's test sources rather than the fe-fuzz module because it needs
 * {@link UtFrameUtils}, and fe-core does not publish a test-jar.
 *
 * <p>Two categories exist here that the parse-only checker cannot produce:
 * <ul>
 *   <li>{@code ANALYZE_FAIL_INTERNAL} — the analyzer threw NPE/ISE/CCE rather than a declared
 *       semantic error. Each one is a bug candidate.</li>
 *   <li>{@code REANALYZE_FAIL} — the deparsed SQL parses but no longer analyzes, i.e. the deparser
 *       silently changed the statement's meaning.</li>
 * </ul>
 *
 * <p>Disabled unless {@code -Dsrfuzz.corpus=<root>[,<root>...]} is set, so it never runs in CI.
 * <pre>
 *   mvn -pl fe-core -am test -Dtest=AnalyzedRoundTripCheckerTest \
 *       -Dsrfuzz.corpus=../test/sql -Dsrfuzz.report=/tmp/analyzed_roundtrip.md
 * </pre>
 */
public class AnalyzedRoundTripCheckerTest {

    private static ConnectContext ctx;
    private static StarRocksAssert srAssert;

    enum Category {
        OK,
        /** Corpus statement does not parse. Not a deparser result; excluded from the denominator. */
        PARSE_FAIL,
        /** Analyzer rejected it with a declared error — expected for corpus statements whose DDL failed. */
        ANALYZE_FAIL_DECLARED,
        /** Analyzer threw an internal error. Bug candidate. */
        ANALYZE_FAIL_INTERNAL,
        DEPARSE_THROW,
        DEPARSE_EMPTY,
        /** Deparsed SQL does not parse back. */
        REPARSE_FAIL,
        /** Deparsed SQL parses but no longer analyzes: the deparser changed the meaning. */
        REANALYZE_FAIL,
        FIXPOINT_MISMATCH
    }

    private static final int MAX_SAMPLES = 6;

    static final class Bucket {
        final Map<Category, Integer> counts = new LinkedHashMap<>();
        final Map<Category, List<String>> samples = new LinkedHashMap<>();

        void add(Category c, String sample) {
            counts.merge(c, 1, Integer::sum);
            if (sample != null) {
                List<String> s = samples.computeIfAbsent(c, k -> new ArrayList<>());
                if (s.size() < MAX_SAMPLES) {
                    s.add(sample);
                }
            }
        }

        int get(Category c) {
            return counts.getOrDefault(c, 0);
        }

        int total() {
            return counts.values().stream().mapToInt(Integer::intValue).sum();
        }
    }

    static final class Result {
        final Category category;
        final String detail;

        Result(Category category, String detail) {
            this.category = category;
            this.detail = detail;
        }
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        srAssert = new StarRocksAssert(ctx);
    }

    @Test
    @EnabledIfSystemProperty(named = "srfuzz.corpus", matches = ".+")
    // junit-platform.properties imposes a global 300s method timeout; a full corpus sweep takes ~12min.
    @Timeout(value = 6, unit = TimeUnit.HOURS)
    public void checkAnalyzedRoundTrip() throws Exception {
        List<Path> roots = new ArrayList<>();
        for (String r : System.getProperty("srfuzz.corpus").split(",")) {
            roots.add(Paths.get(r.trim()));
        }
        int maxFiles = Integer.getInteger("srfuzz.maxFiles", Integer.MAX_VALUE);
        Path report = Paths.get(System.getProperty("srfuzz.report", "analyzed_roundtrip_report.md"));

        List<Path> files = new ArrayList<>();
        for (Path root : roots) {
            if (!Files.exists(root)) {
                System.err.println("skip missing corpus root: " + root);
                continue;
            }
            try (Stream<Path> s = Files.walk(root)) {
                files.addAll(s.filter(Files::isRegularFile)
                        .filter(p -> p.toString().contains("/T/") || p.toString().endsWith(".sql"))
                        .sorted()
                        .collect(Collectors.toList()));
            }
        }
        if (files.size() > maxFiles) {
            files = files.subList(0, maxFiles);
        }
        System.err.println("corpus: " + files.size() + " files");

        Map<String, Bucket> byClass = new TreeMap<>();
        Bucket overall = new Bucket();
        int ddlOk = 0;
        int ddlFail = 0;

        for (int i = 0; i < files.size(); i++) {
            String db = "srfuzz_db_" + i;
            try {
                srAssert.withDatabase(db).useDatabase(db);
            } catch (Throwable t) {
                continue;
            }
            try {
                String text = new String(Files.readAllBytes(files.get(i)), StandardCharsets.UTF_8);
                for (String sql : extractStatements(text)) {
                    StatementBase ast;
                    try {
                        List<StatementBase> parsed = SqlParser.parse(sql, ctx.getSessionVariable());
                        if (parsed.isEmpty()) {
                            continue;
                        }
                        ast = parsed.get(0);
                    } catch (Throwable t) {
                        overall.add(Category.PARSE_FAIL, null);
                        byClass.computeIfAbsent("?", k -> new Bucket()).add(Category.PARSE_FAIL, null);
                        continue;
                    }

                    if (isSchemaSetup(ast)) {
                        if (applySchemaSetup(sql, ast)) {
                            ddlOk++;
                        } else {
                            ddlFail++;
                        }
                        continue;
                    }
                    if (!isMeasured(ast)) {
                        continue;
                    }

                    Result r = measure(sql);
                    String cls = ast.getClass().getSimpleName();
                    overall.add(r.category, null);
                    byClass.computeIfAbsent(cls, k -> new Bucket()).add(r.category, r.detail);
                }
            } catch (Throwable t) {
                System.err.println("file failed: " + files.get(i) + " -> " + t);
            } finally {
                try {
                    srAssert.dropDatabase(db);
                } catch (Throwable ignored) {
                    // best effort; a leaked db only costs memory
                }
            }
            if ((i + 1) % 100 == 0) {
                System.err.println("  ... " + (i + 1) + "/" + files.size());
            }
        }

        writeReport(report, overall, byClass, ddlOk, ddlFail);
        printSummary(overall, byClass, ddlOk, ddlFail);
    }

    // ------------------------------------------------------------------ measure

    private Result measure(String sql) {
        StatementBase ast1;
        try {
            ast1 = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        } catch (Throwable t) {
            return new Result(Category.PARSE_FAIL, null);
        }

        try {
            Analyzer.analyze(ast1, ctx);
        } catch (Throwable t) {
            return new Result(classifyAnalyzeFailure(t),
                    oneLine(t) + " || " + abbrev(sql));
        }

        String s1;
        try {
            s1 = AstToSQLBuilder.toSQL(ast1);
        } catch (Throwable t) {
            return new Result(Category.DEPARSE_THROW, oneLine(t) + " || " + abbrev(sql));
        }
        if (s1 == null || s1.trim().isEmpty()) {
            return new Result(Category.DEPARSE_EMPTY, abbrev(sql));
        }

        StatementBase ast2;
        try {
            ast2 = SqlParser.parse(s1, ctx.getSessionVariable()).get(0);
        } catch (Throwable t) {
            return new Result(Category.REPARSE_FAIL,
                    oneLine(t) + " || in: " + abbrev(sql) + " || out: " + abbrev(s1));
        }

        try {
            Analyzer.analyze(ast2, ctx);
        } catch (Throwable t) {
            return new Result(Category.REANALYZE_FAIL,
                    oneLine(t) + " || in: " + abbrev(sql) + " || out: " + abbrev(s1));
        }

        String s2;
        try {
            s2 = AstToSQLBuilder.toSQL(ast2);
        } catch (Throwable t) {
            return new Result(Category.DEPARSE_THROW, "2nd pass: " + oneLine(t) + " || out1: " + abbrev(s1));
        }
        if (!s1.equals(s2)) {
            return new Result(Category.FIXPOINT_MISMATCH,
                    "in: " + abbrev(sql) + "\n      s1: " + abbrev(s1) + "\n      s2: " + abbrev(s2));
        }
        return new Result(Category.OK, null);
    }

    /** A declared semantic rejection is expected corpus noise; anything else is a bug candidate. */
    private static Category classifyAnalyzeFailure(Throwable t) {
        if (t instanceof SemanticException || t instanceof AnalysisException
                || t instanceof StarRocksException
                // Corpus statements referencing files()/external storage cannot resolve in-process.
                // StorageAccessException extends RuntimeException, so it needs naming explicitly.
                || t instanceof StorageAccessException) {
            return Category.ANALYZE_FAIL_DECLARED;
        }
        // Guava Preconditions guards are a normal way the FE rejects invalid input: they carry a
        // developer-written explanation and fail the statement cleanly, even though the exception type
        // is IllegalState/IllegalArgument/NullPointer. Key on the throw site, not the exception type.
        StackTraceElement[] st = t.getStackTrace();
        if (st.length > 0 && st[0].getClassName().startsWith("com.google.common.base.Preconditions")) {
            return Category.ANALYZE_FAIL_DECLARED;
        }
        return Category.ANALYZE_FAIL_INTERNAL;
    }

    // ------------------------------------------------------------------ catalog

    private static boolean isSchemaSetup(StatementBase ast) {
        return ast instanceof CreateTableStmt
                || ast instanceof CreateViewStmt
                || ast instanceof CreateMaterializedViewStatement
                || ast instanceof CreateTableAsSelectStmt
                || ast instanceof AlterTableStmt
                || ast instanceof DropTableStmt;
    }

    private static boolean isMeasured(StatementBase ast) {
        return ast instanceof QueryStatement || ast instanceof InsertStmt;
    }

    private boolean applySchemaSetup(String sql, StatementBase ast) {
        try {
            if (ast instanceof CreateTableStmt) {
                srAssert.withTable(sql);
            } else if (ast instanceof CreateViewStmt) {
                srAssert.withView(sql);
            } else if (ast instanceof CreateMaterializedViewStatement) {
                srAssert.withMaterializedView(sql);
            } else {
                srAssert.ddl(sql);
            }
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    // -------------------------------------------------------------- corpus IO

    /** Strips SQL-Tester directives, then splits on literal-aware semicolons. */
    static List<String> extractStatements(String text) {
        StringBuilder cleaned = new StringBuilder();
        for (String line : text.split("\n", -1)) {
            String t = line.trim();
            if (t.isEmpty() || t.startsWith("--") || t.startsWith("#")
                    || t.startsWith("function:") || t.startsWith("shell:") || t.startsWith("spark:")
                    || t.startsWith("hive:") || t.startsWith("trino:") || t.startsWith("[UC]")) {
                continue;
            }
            cleaned.append(line).append('\n');
        }
        List<String> out = new ArrayList<>();
        for (String piece : splitStatements(cleaned.toString())) {
            String t = piece.trim();
            if (t.isEmpty() || t.contains("${") || t.length() > 200_000) {
                continue;
            }
            out.add(t);
        }
        return out;
    }

    static List<String> splitStatements(String sql) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        char quote = 0;
        boolean lineComment = false;
        boolean blockComment = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char next = i + 1 < sql.length() ? sql.charAt(i + 1) : 0;
            if (lineComment) {
                if (c == '\n') {
                    lineComment = false;
                    cur.append(c);
                }
                continue;
            }
            if (blockComment) {
                if (c == '*' && next == '/') {
                    blockComment = false;
                    i++;
                }
                continue;
            }
            if (quote != 0) {
                cur.append(c);
                if (c == '\\' && quote != '`') {
                    if (next != 0) {
                        cur.append(next);
                        i++;
                    }
                } else if (c == quote) {
                    quote = 0;
                }
                continue;
            }
            if (c == '-' && next == '-') {
                lineComment = true;
                continue;
            }
            if (c == '/' && next == '*') {
                blockComment = true;
                i++;
                continue;
            }
            if (c == '\'' || c == '"' || c == '`') {
                quote = c;
                cur.append(c);
                continue;
            }
            if (c == ';') {
                out.add(cur.toString());
                cur.setLength(0);
                continue;
            }
            cur.append(c);
        }
        out.add(cur.toString());
        return out;
    }

    // --------------------------------------------------------------- reporting

    private static void writeReport(Path out, Bucket overall, Map<String, Bucket> byClass,
                                    int ddlOk, int ddlFail) throws Exception {
        if (out.getParent() != null) {
            Files.createDirectories(out.getParent());
        }
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out, StandardCharsets.UTF_8))) {
            w.println("# Analyzed Round-Trip Fidelity Report (P0.5)");
            w.println();
            w.printf("Catalog setup: %d DDL applied, %d DDL failed%n", ddlOk, ddlFail);
            w.println();
            w.println("| Category | Count |");
            w.println("|---|---:|");
            for (Category c : Category.values()) {
                w.printf("| %s | %d |%n", c, overall.get(c));
            }
            int analyzed = overall.total() - overall.get(Category.PARSE_FAIL)
                    - overall.get(Category.ANALYZE_FAIL_DECLARED);
            int bad = analyzed - overall.get(Category.OK);
            w.println();
            w.printf("Analyzable statements: %d; round-trip clean: %d (%.2f%%); violations: %d%n",
                    analyzed, overall.get(Category.OK), pct(overall.get(Category.OK), analyzed), bad);

            w.println();
            w.println("## Per statement type");
            w.println();
            w.println("| Statement | analyzed | OK | ANALYZE_INTERNAL | DEPARSE_THROW | REPARSE_FAIL "
                    + "| REANALYZE_FAIL | FIXPOINT | clean% |");
            w.println("|---|---:|---:|---:|---:|---:|---:|---:|---:|");
            List<Map.Entry<String, Bucket>> entries = new ArrayList<>(byClass.entrySet());
            entries.sort(Comparator.comparingInt((Map.Entry<String, Bucket> e) -> -e.getValue().total()));
            for (Map.Entry<String, Bucket> e : entries) {
                Bucket b = e.getValue();
                int a = b.total() - b.get(Category.PARSE_FAIL) - b.get(Category.ANALYZE_FAIL_DECLARED);
                if (a == 0) {
                    continue;
                }
                w.printf("| %s | %d | %d | %d | %d | %d | %d | %d | %.1f%% |%n",
                        e.getKey(), a, b.get(Category.OK), b.get(Category.ANALYZE_FAIL_INTERNAL),
                        b.get(Category.DEPARSE_THROW), b.get(Category.REPARSE_FAIL),
                        b.get(Category.REANALYZE_FAIL), b.get(Category.FIXPOINT_MISMATCH),
                        pct(b.get(Category.OK), a));
            }

            w.println();
            w.println("## Violation samples");
            for (Map.Entry<String, Bucket> e : entries) {
                Bucket b = e.getValue();
                for (Category c : new Category[] {Category.ANALYZE_FAIL_INTERNAL, Category.REANALYZE_FAIL,
                        Category.FIXPOINT_MISMATCH, Category.REPARSE_FAIL, Category.DEPARSE_THROW,
                        Category.DEPARSE_EMPTY}) {
                    List<String> s = b.samples.get(c);
                    if (s == null || s.isEmpty()) {
                        continue;
                    }
                    w.println();
                    w.println("### " + e.getKey() + " / " + c + " (" + b.get(c) + ")");
                    w.println();
                    for (String x : s) {
                        w.println("  - " + x);
                    }
                }
            }
        }
        System.err.println("report written to " + out.toAbsolutePath());
    }

    private static void printSummary(Bucket overall, Map<String, Bucket> byClass, int ddlOk, int ddlFail) {
        System.out.println();
        System.out.printf("=== catalog: %d DDL applied, %d failed ===%n", ddlOk, ddlFail);
        System.out.println("=== overall ===");
        for (Category c : Category.values()) {
            System.out.printf("%-24s %d%n", c, overall.get(c));
        }
        int analyzed = overall.total() - overall.get(Category.PARSE_FAIL)
                - overall.get(Category.ANALYZE_FAIL_DECLARED);
        System.out.printf("%nanalyzed=%d clean=%d (%.2f%%)%n",
                analyzed, overall.get(Category.OK), pct(overall.get(Category.OK), analyzed));
        System.out.println();
        System.out.println("=== worst statement types ===");
        byClass.entrySet().stream()
                .filter(e -> !e.getKey().equals("?"))
                .sorted(Comparator.comparingInt(e -> -violations(e.getValue())))
                .limit(20)
                .forEach(e -> {
                    Bucket b = e.getValue();
                    int a = b.total() - b.get(Category.PARSE_FAIL) - b.get(Category.ANALYZE_FAIL_DECLARED);
                    System.out.printf("%-40s analyzed=%-6d viol=%-5d (internal=%d thr=%d reparse=%d "
                                    + "reanalyze=%d fix=%d)%n",
                            e.getKey(), a, violations(b), b.get(Category.ANALYZE_FAIL_INTERNAL),
                            b.get(Category.DEPARSE_THROW), b.get(Category.REPARSE_FAIL),
                            b.get(Category.REANALYZE_FAIL), b.get(Category.FIXPOINT_MISMATCH));
                });
    }

    private static int violations(Bucket b) {
        return b.get(Category.ANALYZE_FAIL_INTERNAL) + b.get(Category.DEPARSE_THROW)
                + b.get(Category.DEPARSE_EMPTY) + b.get(Category.REPARSE_FAIL)
                + b.get(Category.REANALYZE_FAIL) + b.get(Category.FIXPOINT_MISMATCH);
    }

    private static double pct(int a, int b) {
        return b == 0 ? 0.0 : 100.0 * a / b;
    }

    private static String oneLine(Throwable t) {
        String m = t.getMessage();
        if (m == null) {
            m = t.getClass().getSimpleName();
        }
        m = m.replace('\n', ' ').replace('\r', ' ');
        return t.getClass().getSimpleName() + ": " + (m.length() > 200 ? m.substring(0, 200) + "..." : m);
    }

    private static String abbrev(String s) {
        String x = s.replace('\n', ' ').replace('\r', ' ').replaceAll("\\s+", " ").trim();
        return x.length() > 300 ? x.substring(0, 300) + " ..." : x;
    }
}
