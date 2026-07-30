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

import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * P0 of SQL_AST_FUZZER_PLAN.md: quantify the round-trip fidelity of the FE deparser.
 *
 * For every statement in the existing corpus we check the deparser fixpoint invariant:
 * <pre>
 *   ast1 = parse(s)
 *   s1   = toSQL(ast1)
 *   ast2 = parse(s1)      <- must not fail
 *   s2   = toSQL(ast2)
 *   s1  == s2             <- must hold
 * </pre>
 * The original text {@code s} is deliberately NOT compared against {@code s1}: formatting
 * differences there are noise. {@code s1 != s2} is the real invariant, and it is exactly the
 * property an AST-mutation fuzzer depends on.
 */
public class RoundTripFidelityChecker {

    private static final long SQL_MODE = SqlModeHelper.MODE_DEFAULT;

    enum Category {
        OK,
        /** Corpus statement does not parse at all. Not a deparser bug; excluded from the denominator. */
        PARSE_FAIL,
        /** toSQL() threw. */
        DEPARSE_THROW,
        /** toSQL() produced null/blank. */
        DEPARSE_EMPTY,
        /** toSQL() produced SQL the parser cannot read back. */
        REPARSE_FAIL,
        /** toSQL(parse(toSQL(ast))) != toSQL(ast). */
        FIXPOINT_MISMATCH
    }

    static final class Result {
        final Category category;
        final String stmtClass;
        final String detail;

        Result(Category category, String stmtClass, String detail) {
            this.category = category;
            this.stmtClass = stmtClass;
            this.detail = detail;
        }
    }

    static final class Bucket {
        final Map<Category, Integer> counts = new LinkedHashMap<>();
        final Map<Category, List<String>> samples = new LinkedHashMap<>();

        void add(Category c, String sample) {
            counts.merge(c, 1, Integer::sum);
            List<String> s = samples.computeIfAbsent(c, k -> new ArrayList<>());
            if (s.size() < MAX_SAMPLES) {
                s.add(sample);
            }
        }

        int total() {
            return counts.values().stream().mapToInt(Integer::intValue).sum();
        }

        int get(Category c) {
            return counts.getOrDefault(c, 0);
        }
    }

    private static final int MAX_SAMPLES = 5;

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("usage: RoundTripFidelityChecker <report-out> <corpus-root> [<corpus-root> ...]");
            System.exit(2);
        }
        Path reportOut = Paths.get(args[0]);
        List<Path> roots = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            roots.add(Paths.get(args[i]));
        }

        Set<String> statements = collectStatements(roots);
        System.err.println("collected " + statements.size() + " unique statements");

        Map<String, Bucket> byClass = new TreeMap<>();
        Bucket overall = new Bucket();
        int n = 0;
        for (String stmt : statements) {
            Result r = check(stmt);
            byClass.computeIfAbsent(r.stmtClass, k -> new Bucket()).add(r.category, r.detail);
            overall.add(r.category, null);
            if (++n % 2000 == 0) {
                System.err.println("  ... " + n + "/" + statements.size());
            }
        }

        writeReport(reportOut, statements.size(), overall, byClass);
        printSummary(overall, byClass);
    }

    // ------------------------------------------------------------------ check

    static Result check(String sql) {
        StatementBase ast1;
        try {
            List<StatementBase> parsed = SqlParser.parse(sql, SQL_MODE);
            if (parsed.isEmpty()) {
                return new Result(Category.PARSE_FAIL, "?", "empty parse result: " + abbrev(sql));
            }
            ast1 = parsed.get(0);
        } catch (Throwable t) {
            return new Result(Category.PARSE_FAIL, "?", oneLine(t) + " || " + abbrev(sql));
        }

        String cls = ast1.getClass().getSimpleName();

        String s1;
        try {
            s1 = AstToSQLBuilder.toSQL(ast1);
        } catch (Throwable t) {
            return new Result(Category.DEPARSE_THROW, cls, oneLine(t) + " || " + abbrev(sql));
        }
        if (s1 == null || s1.trim().isEmpty()) {
            return new Result(Category.DEPARSE_EMPTY, cls, abbrev(sql));
        }

        StatementBase ast2;
        try {
            List<StatementBase> parsed = SqlParser.parse(s1, SQL_MODE);
            if (parsed.isEmpty()) {
                return new Result(Category.REPARSE_FAIL, cls, "empty reparse || in: " + abbrev(sql) + " || out: " + abbrev(s1));
            }
            ast2 = parsed.get(0);
        } catch (Throwable t) {
            return new Result(Category.REPARSE_FAIL, cls,
                    oneLine(t) + " || in: " + abbrev(sql) + " || out: " + abbrev(s1));
        }

        String s2;
        try {
            s2 = AstToSQLBuilder.toSQL(ast2);
        } catch (Throwable t) {
            return new Result(Category.DEPARSE_THROW, cls, "2nd pass: " + oneLine(t) + " || out1: " + abbrev(s1));
        }

        if (!s1.equals(s2)) {
            return new Result(Category.FIXPOINT_MISMATCH, cls,
                    "in: " + abbrev(sql) + "\n      s1: " + abbrev(s1) + "\n      s2: " + abbrev(s2));
        }
        return new Result(Category.OK, cls, null);
    }

    // -------------------------------------------------------------- corpus IO

    static Set<String> collectStatements(List<Path> roots) throws IOException {
        Set<String> out = new LinkedHashSet<>();
        for (Path root : roots) {
            if (!Files.exists(root)) {
                System.err.println("skip missing root: " + root);
                continue;
            }
            try (Stream<Path> s = Files.walk(root)) {
                List<Path> files = s.filter(Files::isRegularFile)
                        .filter(RoundTripFidelityChecker::isCorpusFile)
                        .collect(Collectors.toList());
                for (Path f : files) {
                    String text;
                    try {
                        text = new String(Files.readAllBytes(f), StandardCharsets.UTF_8);
                    } catch (Exception e) {
                        continue;
                    }
                    out.addAll(extractStatements(text));
                }
            }
        }
        return out;
    }

    static boolean isCorpusFile(Path p) {
        String s = p.toString();
        // SQL-Tester inputs live in .../T/<case>; FE UT fixtures are plain .sql
        return s.contains("/T/") || s.endsWith(".sql");
    }

    /**
     * Strips SQL-Tester directives and comments, then splits on literal-aware semicolons.
     * Statements containing ${...} interpolation are dropped (they are not valid SQL).
     */
    static List<String> extractStatements(String text) {
        StringBuilder cleaned = new StringBuilder();
        for (String line : text.split("\n", -1)) {
            String t = line.trim();
            if (t.isEmpty() || t.startsWith("--") || t.startsWith("#")) {
                continue;
            }
            // SQL-Tester non-SQL directives
            if (t.startsWith("function:") || t.startsWith("shell:") || t.startsWith("spark:")
                    || t.startsWith("hive:") || t.startsWith("trino:") || t.startsWith("[UC]")) {
                continue;
            }
            cleaned.append(line).append('\n');
        }

        List<String> out = new ArrayList<>();
        for (String piece : splitStatements(cleaned.toString())) {
            String t = piece.trim();
            if (t.isEmpty() || t.contains("${")) {
                continue;
            }
            if (t.length() > 200_000) {
                continue; // pathological generated INSERT blobs
            }
            out.add(t);
        }
        return out;
    }

    /** Split on ';' while respecting '..', ".." , `..`, -- comments and block comments. */
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

    static void writeReport(Path out, int total, Bucket overall, Map<String, Bucket> byClass) throws IOException {
        if (out.getParent() != null) {
            Files.createDirectories(out.getParent());
        }
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out, StandardCharsets.UTF_8))) {
            w.println("# Deparser Round-Trip Fidelity Report (P0)");
            w.println();
            w.println("Corpus: " + total + " unique statements");
            w.println();
            w.println("## Overall");
            w.println();
            w.println("| Category | Count |");
            w.println("|---|---:|");
            for (Category c : Category.values()) {
                w.printf("| %s | %d |%n", c, overall.get(c));
            }
            int parsed = total - overall.get(Category.PARSE_FAIL);
            int bad = overall.get(Category.DEPARSE_THROW) + overall.get(Category.DEPARSE_EMPTY)
                    + overall.get(Category.REPARSE_FAIL) + overall.get(Category.FIXPOINT_MISMATCH);
            w.println();
            w.printf("Parsed statements: %d; round-trip clean: %d (%.2f%%); violations: %d (%.2f%%)%n",
                    parsed, parsed - bad, pct(parsed - bad, parsed), bad, pct(bad, parsed));

            w.println();
            w.println("## Per statement type");
            w.println();
            w.println("| Statement | parsed | OK | DEPARSE_THROW | DEPARSE_EMPTY | REPARSE_FAIL | FIXPOINT_MISMATCH | clean% |");
            w.println("|---|---:|---:|---:|---:|---:|---:|---:|");
            List<Map.Entry<String, Bucket>> entries = new ArrayList<>(byClass.entrySet());
            entries.sort(Comparator.comparingInt((Map.Entry<String, Bucket> e) -> -e.getValue().total()));
            for (Map.Entry<String, Bucket> e : entries) {
                Bucket b = e.getValue();
                int p = b.total() - b.get(Category.PARSE_FAIL);
                if (p == 0) {
                    continue;
                }
                int okc = b.get(Category.OK);
                w.printf("| %s | %d | %d | %d | %d | %d | %d | %.1f%% |%n",
                        e.getKey(), p, okc,
                        b.get(Category.DEPARSE_THROW), b.get(Category.DEPARSE_EMPTY),
                        b.get(Category.REPARSE_FAIL), b.get(Category.FIXPOINT_MISMATCH),
                        pct(okc, p));
            }

            w.println();
            w.println("## Violation samples");
            for (Map.Entry<String, Bucket> e : entries) {
                Bucket b = e.getValue();
                for (Category c : new Category[] {Category.FIXPOINT_MISMATCH, Category.REPARSE_FAIL,
                        Category.DEPARSE_THROW, Category.DEPARSE_EMPTY}) {
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

            w.println();
            w.println("## PARSE_FAIL samples (corpus noise, not deparser bugs)");
            w.println();
            List<String> pf = byClass.getOrDefault("?", new Bucket()).samples.getOrDefault(Category.PARSE_FAIL,
                    new ArrayList<>());
            for (String x : pf) {
                w.println("  - " + x);
            }
        }
        System.err.println("report written to " + out.toAbsolutePath());
    }

    static void printSummary(Bucket overall, Map<String, Bucket> byClass) {
        System.out.println();
        System.out.println("=== overall ===");
        for (Category c : Category.values()) {
            System.out.printf("%-20s %d%n", c, overall.get(c));
        }
        int total = overall.total();
        int parsed = total - overall.get(Category.PARSE_FAIL);
        int bad = overall.get(Category.DEPARSE_THROW) + overall.get(Category.DEPARSE_EMPTY)
                + overall.get(Category.REPARSE_FAIL) + overall.get(Category.FIXPOINT_MISMATCH);
        System.out.printf("%nparsed=%d clean=%d (%.2f%%) violations=%d (%.2f%%)%n",
                parsed, parsed - bad, pct(parsed - bad, parsed), bad, pct(bad, parsed));

        System.out.println();
        System.out.println("=== worst statement types (by violations) ===");
        byClass.entrySet().stream()
                .filter(e -> !e.getKey().equals("?"))
                .sorted(Comparator.comparingInt(e -> -violations(e.getValue())))
                .limit(25)
                .forEach(e -> {
                    Bucket b = e.getValue();
                    int p = b.total() - b.get(Category.PARSE_FAIL);
                    System.out.printf("%-42s parsed=%-6d viol=%-6d (thr=%d empty=%d reparse=%d fix=%d)%n",
                            e.getKey(), p, violations(b),
                            b.get(Category.DEPARSE_THROW), b.get(Category.DEPARSE_EMPTY),
                            b.get(Category.REPARSE_FAIL), b.get(Category.FIXPOINT_MISMATCH));
                });
    }

    static int violations(Bucket b) {
        return b.get(Category.DEPARSE_THROW) + b.get(Category.DEPARSE_EMPTY)
                + b.get(Category.REPARSE_FAIL) + b.get(Category.FIXPOINT_MISMATCH);
    }

    static double pct(int a, int b) {
        return b == 0 ? 0.0 : 100.0 * a / b;
    }

    static String oneLine(Throwable t) {
        String m = t.getMessage();
        if (m == null) {
            m = t.getClass().getSimpleName();
        }
        m = m.replace('\n', ' ').replace('\r', ' ');
        return t.getClass().getSimpleName() + ": " + (m.length() > 220 ? m.substring(0, 220) + "..." : m);
    }

    static String abbrev(String s) {
        String x = s.replace('\n', ' ').replace('\r', ' ').replaceAll("\\s+", " ").trim();
        return x.length() > 320 ? x.substring(0, 320) + " ..." : x;
    }
}
