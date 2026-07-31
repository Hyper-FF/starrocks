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
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.StorageAccessException;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.Predicate;
import com.starrocks.sql.ast.expression.SlotRef;
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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * P1 of SQL_AST_FUZZER_PLAN.md — AST-mutation fuzzer, FE-only mode, no coverage feedback yet.
 *
 * <p>Loop, per corpus file:
 * <pre>
 *   build a database from the file's own DDL
 *   seeds  = the file's statements that parse and analyze cleanly
 *   pool   = expression texts and column names harvested from those seeds
 *   repeat:
 *     ast = parse(seed)              // fresh, unanalyzed — never mutate a shared tree
 *     mutate(ast, pool)              // Expr-level, via TreeNode.setChild
 *     analyze(ast)                   // primary oracle
 *     deparse / reparse / reanalyze  // deparser oracles, same chain as P0.5
 * </pre>
 *
 * <p>Design notes carried over from P0.5 (see DEPARSER_ROUNDTRIP_P05_REPORT.md §6):
 * <ul>
 *   <li>Mutation happens on the <b>unanalyzed</b> tree and deparsing only on the <b>analyzed</b> one,
 *       because the deparser is only reliable after analysis.</li>
 *   <li>Injected fragments are stored as <b>text</b> and re-parsed at injection time, so no Expr
 *       instance is ever shared between two trees.</li>
 *   <li>The fixpoint check is <b>not</b> a strict string comparison gate — analysis legitimately
 *       normalizes (e.g. {@code ORDER BY 3} to a bound column), so a mismatch is reported separately
 *       rather than treated as a defect.</li>
 * </ul>
 *
 * <p>Disabled unless {@code -Dsrfuzz.corpus=<root>} is set, so it never runs in CI.
 * <pre>
 *   mvn -pl fe-core -am test -Dtest=AstMutationFuzzerTest \
 *       -Dsrfuzz.corpus=$PWD/../test/sql -Dsrfuzz.maxFiles=50 -Dsrfuzz.mutations=20 \
 *       -Dsrfuzz.report=/tmp/fuzz.md
 * </pre>
 *
 * <p>Give the corpus as an absolute path. Surefire runs with the module directory as its working
 * directory, not the one mvn was invoked from, so a path relative to {@code fe/} silently resolves to
 * nothing and the run reports zero seeds and zero findings -- which reads exactly like a clean run.
 */
public class AstMutationFuzzerTest {

    private static final long SQL_MODE = SqlModeHelper.MODE_DEFAULT;

    private static ConnectContext ctx;
    private static StarRocksAssert srAssert;

    /**
     * When {@code -Dsrfuzz.emit=<dir>} is set, every mutant that analyzed and round-tripped cleanly is
     * written out next to the DDL it needs, so the same statements can be replayed against a real
     * cluster. This harness is FE-only and in-process, so it can never see a BE crash; the emitted
     * corpus is how these mutants reach an ASAN build.
     */
    private List<String> emitSetup;
    private List<String> emitQueries;

    enum Outcome {
        /** Mutant analyzed and round-tripped cleanly. */
        OK,
        /** Analyzer rejected the mutant with a declared error. Expected and uninteresting. */
        ANALYZE_REJECTED,
        /** Analyzer threw an internal error. BUG. */
        ANALYZE_INTERNAL_ERROR,
        /** Deparsing the analyzed mutant threw. BUG. */
        DEPARSE_THROW,
        /** Deparsed mutant does not parse back. BUG. */
        REPARSE_FAIL,
        /** Deparsed mutant parses but no longer analyzes. BUG (semantic drift). */
        REANALYZE_FAIL,
        /** Deparse is not a fixpoint. Reported, not necessarily a defect — see class javadoc. */
        FIXPOINT_MISMATCH
    }

    private static boolean isBug(Outcome o) {
        return o == Outcome.ANALYZE_INTERNAL_ERROR || o == Outcome.DEPARSE_THROW
                || o == Outcome.REPARSE_FAIL || o == Outcome.REANALYZE_FAIL;
    }

    /**
     * Per-file harvest used to build mutants that have a chance of analyzing.
     *
     * <p>Fragments are bucketed by the analyzed type of the expression they came from. Injecting a
     * boolean-valued fragment where a boolean is expected (and a scalar where a scalar is expected)
     * is the cheapest way to raise the share of mutants that survive analysis — an untyped pool
     * makes most mutants fail in the analyzer and never reach the deeper code we want to exercise.
     */
    static final class Pool {
        final List<String> booleanTexts = new ArrayList<>();
        final List<String> scalarTexts = new ArrayList<>();
        final List<String> columnNames = new ArrayList<>();

        void addExpr(String text, boolean isBoolean) {
            List<String> bucket = isBoolean ? booleanTexts : scalarTexts;
            if (bucket.size() < 512 && !bucket.contains(text)) {
                bucket.add(text);
            }
        }

        List<String> bucketFor(boolean wantBoolean) {
            List<String> preferred = wantBoolean ? booleanTexts : scalarTexts;
            if (!preferred.isEmpty()) {
                return preferred;
            }
            return wantBoolean ? scalarTexts : booleanTexts;
        }

        boolean usable() {
            return !booleanTexts.isEmpty() || !scalarTexts.isEmpty();
        }
    }

    static final class Finding {
        final Outcome outcome;
        final String signature;
        final String seedSql;
        final String mutation;
        final String mutantSql;
        final String detail;
        int count = 1;

        Finding(Outcome outcome, String signature, String seedSql, String mutation, String mutantSql,
                String detail) {
            this.outcome = outcome;
            this.signature = signature;
            this.seedSql = seedSql;
            this.mutation = mutation;
            this.mutantSql = mutantSql;
            this.detail = detail;
        }
    }

    private static final String[] BOUNDARY_LITERALS = {
            "0", "-1", "1", "9223372036854775807", "-9223372036854775808",
            "170141183460469231731687303715884105727", "2147483647", "-2147483648",
            "''", "'\\\\'", "'%'", "'0'", "repeat('x', 65536)",
            "NULL", "TRUE", "FALSE", "0.0", "-0.0", "1e308", "-1e308",
            "'1970-01-01'", "'9999-12-31 23:59:59'", "'0000-00-00'",
            "CAST(NULL AS JSON)", "[]", "map{}",
    };

    /** Same arity swaps, chosen to cross type boundaries where the analyzer should object cleanly. */
    private static final String[][] FUNCTION_SWAPS = {
            {"abs", "sqrt", "ln", "exp", "floor", "ceil", "sign", "bitmap_count", "hll_cardinality",
                    "length", "char_length", "lower", "upper", "to_json", "hex", "unhex", "md5", "year", "month"},
            {"if", "concat_ws", "substr", "date_add", "date_sub", "round", "truncate", "split", "json_query"},
    };

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        srAssert = new StarRocksAssert(ctx);
    }

    @Test
    @EnabledIfSystemProperty(named = "srfuzz.corpus", matches = ".+")
    @Timeout(value = 6, unit = TimeUnit.HOURS)
    public void fuzz() throws Exception {
        List<Path> roots = new ArrayList<>();
        for (String r : System.getProperty("srfuzz.corpus").split(",")) {
            roots.add(Paths.get(r.trim()));
        }
        int maxFiles = Integer.getInteger("srfuzz.maxFiles", Integer.MAX_VALUE);
        int mutationsPerSeed = Integer.getInteger("srfuzz.mutations", 10);
        long seedValue = Long.getLong("srfuzz.seed", 20260730L);
        Path report = Paths.get(System.getProperty("srfuzz.report", "ast_mutation_fuzz_report.md"));
        String emitProp = System.getProperty("srfuzz.emit");
        Path emitDir = emitProp == null ? null : Paths.get(emitProp);
        if (emitDir != null) {
            Files.createDirectories(emitDir);
        }

        Random rnd = new Random(seedValue);

        List<Path> files = new ArrayList<>();
        for (Path root : roots) {
            if (!Files.exists(root)) {
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
        System.err.println("corpus: " + files.size() + " files, " + mutationsPerSeed
                + " mutations/seed, rng seed " + seedValue);

        Map<Outcome, Integer> tally = new LinkedHashMap<>();
        Map<String, Finding> findings = new LinkedHashMap<>();
        int seedCount = 0;
        int mutantCount = 0;
        int unreachableCount = 0;
        Map<String, Drop> drops = new LinkedHashMap<>();

        for (int i = 0; i < files.size(); i++) {
            String db = "srfuzz_mut_" + i;
            try {
                srAssert.withDatabase(db).useDatabase(db);
            } catch (Throwable t) {
                continue;
            }
            try {
                String text = new String(Files.readAllBytes(files.get(i)), StandardCharsets.UTF_8);
                List<String> statements = CorpusReader.extractStatements(text);
                emitSetup = emitDir == null ? null : new ArrayList<>();
                emitQueries = emitDir == null ? null : new ArrayList<>();

                List<String> seeds = new ArrayList<>();
                Pool pool = new Pool();
                for (String sql : statements) {
                    StatementBase ast = tryParse(sql);
                    if (ast == null) {
                        continue;
                    }
                    if (CorpusReader.isSchemaSetup(ast)) {
                        applySchemaSetup(sql, ast);
                        if (emitSetup != null) {
                            emitSetup.add(terminated(sql));
                        }
                        continue;
                    }
                    if (!(ast instanceof QueryStatement)) {
                        // The in-process harness has no BE, so it cannot load anything and skips these.
                        // The emitted corpus still needs them: without the INSERTs every replayed query
                        // runs against an empty table, scans nothing, and never reaches the BE code that
                        // an ASAN build exists to check.
                        if (emitSetup != null && ast instanceof InsertStmt) {
                            emitSetup.add(terminated(sql));
                        }
                        continue;
                    }
                    // Validate and harvest on a throwaway analyzed copy.
                    StatementBase probe = tryParse(sql);
                    try {
                        Analyzer.analyze(probe, ctx);
                    } catch (Throwable t) {
                        continue;
                    }
                    seeds.add(sql);
                    harvest((QueryStatement) probe, pool);

                    // Evaluate the seed itself, unmutated. The grammar-reachability gate below drops
                    // any mutant whose deparse will not parse back, on the grounds that the mutator
                    // can build trees the grammar cannot express -- but a deparser defect produces
                    // exactly the same symptom, so the gate swallows the whole REPARSE_FAIL class.
                    // Measured: with the nested-join parenthesis defect present, 100% of that seed's
                    // mutants were dropped as unreachable and nothing was reported.
                    // The seed came from the corpus, so it is SQL a user wrote and the parser
                    // accepted. Anything the oracle says about it is unambiguously a defect, and it
                    // costs one evaluation per seed rather than one per mutant.
                    StatementBase baseline = tryParse(sql);
                    if (baseline != null) {
                        seedCount++;
                        evaluate(sql, "<unmutated seed>", baseline, tally, findings);
                    }
                }

                if (!pool.usable()) {
                    continue;
                }
                for (String seed : seeds) {
                    for (int m = 0; m < mutationsPerSeed; m++) {
                        StatementBase ast = tryParse(seed);
                        if (ast == null) {
                            continue;
                        }
                        String mutation = Mutator.mutate((QueryStatement) ast, pool, rnd);
                        if (mutation == null) {
                            continue;
                        }
                        StatementBase reachable = reparseThroughGrammar(ast, drops);
                        if (reachable == null) {
                            unreachableCount++;
                            continue;
                        }
                        mutantCount++;
                        // M9: on a minority of mutants, evaluate under a perturbed session flag. The
                        // flag has to be restored even when evaluate throws -- it is shared with every
                        // later seed, so a leak silently reinterprets the rest of the run.
                        SessionFlagPerturbation.Perturbation flag = rnd.nextInt(100) < 15
                                ? SessionFlagPerturbation.apply(ctx.getSessionVariable(), rnd)
                                : null;
                        try {
                            evaluate(seed, flag == null ? mutation : mutation + " | " + flag.description(),
                                    reachable, tally, findings);
                        } finally {
                            if (flag != null) {
                                flag.close();
                            }
                        }
                    }
                }
            } catch (Throwable t) {
                System.err.println("file failed: " + files.get(i) + " -> " + t);
            } finally {
                if (emitQueries != null && !emitQueries.isEmpty()) {
                    // Split, so a replay can create the schema, load data into it, and only then run
                    // the queries. Interleaved, every query before the last INSERT scans an empty table.
                    Files.write(emitDir.resolve(String.format("mut_%03d.setup.sql", i)),
                            String.join("\n", emitSetup).getBytes(StandardCharsets.UTF_8));
                    Files.write(emitDir.resolve(String.format("mut_%03d.query.sql", i)),
                            String.join("\n", emitQueries).getBytes(StandardCharsets.UTF_8));
                }
                emitSetup = null;
                emitQueries = null;
                try {
                    srAssert.dropDatabase(db);
                } catch (Throwable ignored) {
                    // best effort
                }
            }
            if ((i + 1) % 25 == 0) {
                System.err.println("  ... " + (i + 1) + "/" + files.size()
                        + " seeds=" + seedCount + " mutants=" + mutantCount
                        + " bugs=" + findings.values().stream().filter(f -> isBug(f.outcome)).count());
            }
        }

        writeReport(report, tally, findings, seedCount, mutantCount, unreachableCount, drops);
        printSummary(tally, findings, seedCount, mutantCount, unreachableCount);
    }

    /**
     * Grammar-reachability gate: re-derive the mutant from its own text and evaluate that tree instead.
     *
     * <p>{@code setChild} edits one AST edge, but several node types keep a parsed field alongside a
     * mirrored child list -- AnalyticExpr's flattened arguments, DictQueryExpr's table name,
     * FunctionParams' ORDER BY, percentile_disc's percentile. Editing the child desynchronises the two
     * halves and produces a tree the parser could never build, and every "finding" from such a tree was
     * an artifact: three separate rounds of triage each ended there.
     *
     * <p>Serializing and reparsing removes the whole class at once. Whatever the deparser reads becomes
     * the text, and the text parses into a consistent tree or into nothing at all. A mutant that cannot
     * survive the trip is dropped rather than reported.
     *
     * <p>The serialization happens on an unanalyzed tree, where the deparser is least reliable, so this
     * costs coverage. That is the intended trade: the survivors are trees a user could actually write.
     */
    private static String terminated(String sql) {
        String t = sql.trim();
        return t.endsWith(";") ? t : t + ";";
    }

    private static StatementBase reparseThroughGrammar(StatementBase mutated, Map<String, Drop> drops) {
        String text;
        try {
            text = AstToSQLBuilder.toSQL(mutated);
        } catch (Throwable t) {
            noteDrop(drops, "deparse-threw:" + signatureOf(t), "<unrenderable>");
            return null;
        }
        if (text == null || text.trim().isEmpty()) {
            noteDrop(drops, "deparse-empty", "<empty>");
            return null;
        }
        try {
            List<StatementBase> parsed = SqlParser.parse(text, ctx.getSessionVariable());
            if (parsed.isEmpty()) {
                noteDrop(drops, "parse-empty", oneLineSql(text));
                return null;
            }
            return parsed.get(0);
        } catch (Throwable t) {
            noteDrop(drops, "reparse-failed:" + oneLine(t), oneLineSql(text));
            return null;
        }
    }

    /**
     * A dropped mutant is ambiguous: either the mutator built a tree the grammar cannot express, or
     * the deparser cannot render a tree the parser itself produced. Only the second is a defect, and
     * nothing here can tell them apart -- which is why the drop is not counted as a finding. But
     * discarding it silently is how the nested-join defect stayed invisible while 100% of that seed's
     * mutants were being thrown away, so the shapes are kept and printed for triage.
     */
    private static void noteDrop(Map<String, Drop> drops, String reason, String sampleSql) {
        Drop d = drops.get(reason);
        if (d == null) {
            drops.put(reason, new Drop(reason, sampleSql));
        } else {
            d.count++;
        }
    }

    private static String oneLineSql(String sql) {
        String flat = sql.replace('\n', ' ').trim();
        return flat.length() > 400 ? flat.substring(0, 400) + " ..." : flat;
    }

    static final class Drop {
        final String reason;
        final String sampleSql;
        int count = 1;

        Drop(String reason, String sampleSql) {
            this.reason = reason;
            this.sampleSql = sampleSql;
        }
    }

    // ----------------------------------------------------------------- oracle

    private void evaluate(String seed, String mutation, StatementBase mutant,
                          Map<Outcome, Integer> tally, Map<String, Finding> findings) {
        String mutantSql;
        // The mutated tree is unanalyzed, so serialize only after analysis succeeds. To report the
        // mutant when analysis fails we fall back to a best-effort rendering.
        try {
            Analyzer.analyze(mutant, ctx);
        } catch (Throwable t) {
            Outcome o = classifyAnalyzeFailure(t);
            record(tally, findings, o, signatureOf(t), seed, mutation, bestEffortSql(mutant), oneLine(t));
            return;
        }

        try {
            mutantSql = AstToSQLBuilder.toSQL(mutant);
        } catch (Throwable t) {
            record(tally, findings, Outcome.DEPARSE_THROW, signatureOf(t), seed, mutation, "<unrenderable>", oneLine(t));
            return;
        }
        if (mutantSql == null || mutantSql.trim().isEmpty()) {
            record(tally, findings, Outcome.DEPARSE_THROW, "empty-deparse", seed, mutation, "<empty>",
                    "deparse produced nothing");
            return;
        }

        StatementBase again;
        try {
            again = SqlParser.parse(mutantSql, ctx.getSessionVariable()).get(0);
        } catch (Throwable t) {
            record(tally, findings, Outcome.REPARSE_FAIL, signatureOf(t), seed, mutation, mutantSql, oneLine(t));
            return;
        }
        try {
            Analyzer.analyze(again, ctx);
        } catch (Throwable t) {
            record(tally, findings, Outcome.REANALYZE_FAIL, signatureOf(t), seed, mutation, mutantSql, oneLine(t));
            return;
        }

        String second;
        try {
            second = AstToSQLBuilder.toSQL(again);
        } catch (Throwable t) {
            record(tally, findings, Outcome.DEPARSE_THROW, signatureOf(t), seed, mutation, mutantSql,
                    "2nd pass: " + oneLine(t));
            return;
        }
        if (!mutantSql.equals(second)) {
            // P0.5 left 683 fixpoint mismatches unclassified. Key them by the shape of the first
            // difference so distinct causes (benign ORDER BY ordinal binding vs. real deparser
            // defects such as parenthesis growth) cluster into separate signatures.
            String[] diff = firstDiffWindow(mutantSql, second);
            record(tally, findings, Outcome.FIXPOINT_MISMATCH, "fixpoint:" + diffShape(diff[0], diff[1]),
                    seed, mutation, mutantSql, "s1 ..." + diff[0] + "...  |  s2 ..." + diff[1] + "...");
            return;
        }
        tally.merge(Outcome.OK, 1, Integer::sum);
        if (emitQueries != null) {
            emitQueries.add(mutantSql.replace('\n', ' ').trim() + ";");
        }
    }

    private static Outcome classifyAnalyzeFailure(Throwable t) {
        if (t instanceof SemanticException || t instanceof AnalysisException
                || t instanceof StarRocksException || t instanceof StorageAccessException) {
            return Outcome.ANALYZE_REJECTED;
        }
        if (isDeliberateGuard(t)) {
            return Outcome.ANALYZE_REJECTED;
        }
        return Outcome.ANALYZE_INTERNAL_ERROR;
    }

    /**
     * Guava {@code Preconditions} guards are how large parts of the FE reject invalid input: they carry
     * a developer-written explanation and the statement fails cleanly, even though the exception type is
     * IllegalState/IllegalArgument/NullPointer rather than SemanticException. Classifying those as
     * internal errors floods the report with non-defects, so key on the throw site instead of the type.
     */
    private static boolean isDeliberateGuard(Throwable t) {
        StackTraceElement[] st = t.getStackTrace();
        return st.length > 0 && st[0].getClassName().startsWith("com.google.common.base.Preconditions");
    }

    private void record(Map<Outcome, Integer> tally, Map<String, Finding> findings, Outcome o,
                        String signature, String seed, String mutation, String mutantSql, String detail) {
        tally.merge(o, 1, Integer::sum);
        if (o == Outcome.ANALYZE_REJECTED) {
            return; // expected noise, do not keep samples
        }
        String key = o + "|" + signature;
        Finding existing = findings.get(key);
        if (existing != null) {
            existing.count++;
        } else {
            findings.put(key, new Finding(o, signature, seed, mutation, mutantSql, detail));
        }
    }

    // ---------------------------------------------------------------- harvest

    private static void harvest(QueryStatement stmt, Pool pool) {
        for (Expr root : collectRootExprs(stmt.getQueryRelation())) {
            collectExprs(root, e -> {
                if (e instanceof SlotRef) {
                    String name = ((SlotRef) e).getColumnName();
                    if (name != null && pool.columnNames.size() < 256 && !pool.columnNames.contains(name)) {
                        pool.columnNames.add(name);
                    }
                }
                try {
                    String s = AstToSQLBuilder.toSQL(e);
                    if (s != null && !s.trim().isEmpty() && s.length() < 400) {
                        // Harvested from an analyzed tree, so the type is populated.
                        pool.addExpr(s, e.getType() != null && e.getType().isBoolean());
                    }
                } catch (Throwable ignored) {
                    // an un-renderable fragment is simply not pooled
                }
            });
        }
    }

    /** Expression roots reachable without analysis, so the same walk works pre- and post-analyze. */
    static List<Expr> collectRootExprs(Relation relation) {
        List<Expr> out = new ArrayList<>();
        if (relation instanceof QueryRelation && ((QueryRelation) relation).getCteRelations() != null) {
            // A WITH clause hangs off the query, not the FROM clause, so it needs its own descent.
            for (CTERelation cte : ((QueryRelation) relation).getCteRelations()) {
                out.addAll(collectRootExprs(cte));
            }
        }
        if (relation instanceof QueryRelation && ((QueryRelation) relation).hasOrderByClause()) {
            // ORDER BY lives on QueryRelation, not on SelectRelation, so a set operation's ORDER BY is
            // picked up by the same branch. Without it the sort keys were the one clause the walk never
            // reached: M5 could add or drop a whole ORDER BY, but nothing ever edited inside one, so
            // `ORDER BY abs(c2 + c3)` was as opaque to the mutator as a table name.
            //
            // The ordinal form (`ORDER BY 1`) contributes a childless root and therefore no site, which
            // is correct -- rewriting an ordinal is M5's job, and a mutated ordinal is almost always an
            // out-of-range analyzer rejection.
            for (OrderByElement e : ((QueryRelation) relation).getOrderBy()) {
                if (e != null && e.getExpr() != null) {
                    out.add(e.getExpr());
                }
            }
        }
        if (relation instanceof SelectRelation) {
            SelectRelation sel = (SelectRelation) relation;
            if (sel.getSelectList() != null && sel.getSelectList().getItems() != null) {
                sel.getSelectList().getItems().stream()
                        .filter(it -> !it.isStar() && it.getExpr() != null)
                        .forEach(it -> out.add(it.getExpr()));
            }
            if (sel.getPredicate() != null) {
                out.add(sel.getPredicate());
            }
            if (sel.getHavingClause() != null) {
                out.add(sel.getHavingClause());
            }
            if (sel.getGroupByClause() != null && sel.getGroupByClause().getGroupingExprs() != null) {
                out.addAll(sel.getGroupByClause().getGroupingExprs());
            }
            if (sel.getRelation() != null) {
                out.addAll(collectRootExprs(sel.getRelation()));
            }
        } else if (relation instanceof SubqueryRelation) {
            out.addAll(collectRootExprs(((SubqueryRelation) relation).getQueryStatement().getQueryRelation()));
        } else if (relation instanceof SetOperationRelation) {
            for (Relation child : ((SetOperationRelation) relation).getRelations()) {
                out.addAll(collectRootExprs(child));
            }
        } else if (relation instanceof JoinRelation) {
            // Without this branch a join swallowed its whole subtree: the ON predicate was never a
            // mutation site and never reached the pool, and so was everything below the join -- a
            // subquery on either side went dark purely because of its parent. Around a third of the
            // SQL-Tester corpus joins, so that was a third of the seeds with no reachable FROM clause.
            JoinRelation join = (JoinRelation) relation;
            if (join.getOnPredicate() != null) {
                out.add(join.getOnPredicate());
            }
            out.addAll(collectRootExprs(join.getLeft()));
            out.addAll(collectRootExprs(join.getRight()));
        } else if (relation instanceof CTERelation) {
            out.addAll(collectRootExprs(((CTERelation) relation).getCteQueryStatement().getQueryRelation()));
        } else if (relation instanceof TableFunctionRelation) {
            List<Expr> args = ((TableFunctionRelation) relation).getChildExpressions();
            if (args != null) {
                out.addAll(args);
            }
        }
        return out;
    }

    private static void collectExprs(Expr e, java.util.function.Consumer<Expr> sink) {
        if (e == null) {
            return;
        }
        sink.accept(e);
        for (Expr c : e.getChildren()) {
            collectExprs(c, sink);
        }
    }

    // ---------------------------------------------------------------- mutator

    static final class Mutator {

        /** A replaceable position: child {@code index} of {@code parent}. */
        static final class Site {
            final Expr parent;
            final int index;

            Site(Expr parent, int index) {
                this.parent = parent;
                this.index = index;
            }

            Expr child() {
                return parent.getChild(index);
            }
        }

        /**
         * The operators that edit more than a single expression. Weights are attempt counts, not
         * probabilities: the list is drawn from without replacement until one operator applies, so a
         * draw that lands on an operator with nothing to do falls through to another instead of
         * wasting the iteration. M1-M4 stay as the implicit fallback because they apply to almost
         * every tree, which is exactly what makes them a poor first choice -- picking them first would
         * starve the structural operators.
         */
        private static final List<Mutation> OPERATORS = java.util.Arrays.asList(
                new TypeStressMutation(),   // most likely to reach a real complex-type defect
                new ClauseMutation(),
                new NestingMutation());

        /**
         * Applies one mutation and returns a description of it, or null when nothing was applied.
         *
         * <p>The description matters: when the mutant cannot be deparsed the report has no SQL to show,
         * and a finding nobody can reproduce is worth nothing. Recording the site and the injected text
         * makes such a finding reconstructible from the seed alone.
         */
        static String mutate(QueryStatement stmt, Pool pool, Random rnd) {
            List<Mutation> order = new ArrayList<>(OPERATORS);
            java.util.Collections.shuffle(order, rnd);
            // Expression-level mutation stays the majority of iterations; the structural operators
            // change the shape of the statement, which is much more likely to leave the mutant
            // unanalyzable, so a run made mostly of them buys less depth per unit of time.
            int structuralShare = rnd.nextInt(100) < 45 ? order.size() : 0;
            for (int i = 0; i < structuralShare; i++) {
                String applied = tryOperator(order.get(i), stmt, pool, rnd);
                if (applied != null) {
                    return applied;
                }
            }
            return mutateExpression(stmt, pool, rnd);
        }

        /** An operator that throws is a harness defect, not a finding: report it as such, do not hide it. */
        private static String tryOperator(Mutation op, QueryStatement stmt, Pool pool, Random rnd) {
            try {
                String applied = op.apply(stmt, pool, rnd);
                return applied == null ? null : op.name() + " " + applied;
            } catch (Throwable t) {
                System.err.println("mutation operator " + op.name() + " threw: " + t);
                return null;
            }
        }

        /** M1 subtree swap / M2 function swap / M3 literal boundary / M4 identifier rebind. */
        private static String mutateExpression(QueryStatement stmt, Pool pool, Random rnd) {
            List<Site> sites = new ArrayList<>();
            for (Expr root : collectRootExprs(stmt.getQueryRelation())) {
                collectSites(root, sites);
            }
            if (sites.isEmpty()) {
                return null;
            }
            Site site = sites.get(rnd.nextInt(sites.size()));
            Expr current = site.child();
            Expr replacement = buildReplacement(current, pool, rnd);
            if (replacement == null) {
                return null;
            }
            String before = safeRender(current);
            site.parent.setChild(site.index, replacement);
            return site.parent.getClass().getSimpleName() + "[" + site.index + "]: "
                    + before + " -> " + safeRender(replacement);
        }

        private static String safeRender(Expr e) {
            try {
                String s = AstToSQLBuilder.toSQL(e);
                return s == null ? "<null>" : s.replace('\n', ' ');
            } catch (Throwable t) {
                return "<" + e.getClass().getSimpleName() + ">";
            }
        }

        private static void collectSites(Expr e, List<Site> sites) {
            if (e == null || sites.size() > 2000) {
                return;
            }
            for (int i = 0; i < e.getChildren().size(); i++) {
                if (isContractedPosition(e, i)) {
                    continue;
                }
                sites.add(new Site(e, i));
                collectSites(e.getChild(i), sites);
            }
        }

        /**
         * Grammar-reachability gate.
         *
         * <p>Mutating through {@code TreeNode.setChild} can build trees the parser could never produce,
         * for example an {@code ExistsPredicate} whose child is a {@code SlotRef}. Findings from such a
         * tree are artifacts of the mutator, not defects -- in the first full run they accounted for 4 of
         * 20 signatures and a larger share of the instances.
         *
         * <p>Which positions are off limits is declared in {@code fuzz/mutation-rules.xml}, so adding an
         * exclusion costs a rule rather than a recompile. See {@link MutationRules}.
         */
        private static boolean isContractedPosition(Expr parent, int index) {
            return MutationRules.get().isBlocked(parent, index);
        }

        /** M1 subtree swap / M2 function swap / M3 literal boundary / M4 identifier rebind. */
        private static Expr buildReplacement(Expr current, Pool pool, Random rnd) {
            int roll = rnd.nextInt(100);
            String text = null;
            if (current instanceof LiteralExpr && roll < 45) {
                text = BOUNDARY_LITERALS[rnd.nextInt(BOUNDARY_LITERALS.length)];                 // M3
            } else if (current instanceof SlotRef && roll < 45 && !pool.columnNames.isEmpty()) {
                text = "`" + pool.columnNames.get(rnd.nextInt(pool.columnNames.size())) + "`";    // M4
            } else if (current instanceof FunctionCallExpr && roll < 70) {
                text = swapFunction((FunctionCallExpr) current, rnd);                             // M2
            }
            if (text == null) {                                                                   // M1
                // The replaced node is unanalyzed, so its type is unknown; approximate the expected
                // shape from the node class instead.
                List<String> bucket = pool.bucketFor(current instanceof Predicate);
                if (bucket.isEmpty()) {
                    return null;
                }
                text = bucket.get(rnd.nextInt(bucket.size()));
            }
            try {
                return SqlParser.parseSqlToExpr(text, SQL_MODE);
            } catch (Throwable t) {
                return null;
            }
        }

        /** Rebuilds the call with a different name of the same arity, keeping the argument text. */
        private static String swapFunction(FunctionCallExpr call, Random rnd) {
            int arity = call.getChildren().size();
            String[] candidates = null;
            for (String[] group : FUNCTION_SWAPS) {
                if (arity == 1 && group == FUNCTION_SWAPS[0]) {
                    candidates = group;
                } else if (arity >= 2 && group == FUNCTION_SWAPS[1]) {
                    candidates = group;
                }
            }
            if (candidates == null) {
                return null;
            }
            List<String> args = new ArrayList<>();
            for (Expr c : call.getChildren()) {
                try {
                    String s = AstToSQLBuilder.toSQL(c);
                    if (s == null || s.trim().isEmpty()) {
                        return null;
                    }
                    args.add(s);
                } catch (Throwable t) {
                    return null;
                }
            }
            return candidates[rnd.nextInt(candidates.length)] + "(" + String.join(", ", args) + ")";
        }
    }


    // ------------------------------------------------------------------ misc

    private static StatementBase tryParse(String sql) {
        try {
            List<StatementBase> parsed = SqlParser.parse(sql, ctx.getSessionVariable());
            return parsed.isEmpty() ? null : parsed.get(0);
        } catch (Throwable t) {
            return null;
        }
    }

    private boolean applySchemaSetup(String sql, StatementBase ast) {
        try {
            CorpusReader.applySchemaSetup(srAssert, sql, ast);
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    private static String bestEffortSql(StatementBase stmt) {
        try {
            String s = AstToSQLBuilder.toSQL(stmt);
            return s == null ? "<null>" : s;
        } catch (Throwable t) {
            return "<unrenderable: " + t.getClass().getSimpleName() + ">";
        }
    }

    /**
     * Window around the first difference in each string, expanded outwards to whitespace.
     *
     * <p>Cutting at the exact differing character splits a token in half, which leaves an unmatched
     * backtick or quote and defeats {@link #normalizeShape}. One cause then scatters across dozens of
     * signatures keyed by whichever database name happened to appear -- that is what left 2345 fixpoint
     * mismatches unclassified in the previous run.
     */
    static String[] firstDiffWindow(String a, String b) {
        String x = a.replace('\n', ' ');
        String y = b.replace('\n', ' ');
        int i = 0;
        int min = Math.min(x.length(), y.length());
        while (i < min && x.charAt(i) == y.charAt(i)) {
            i++;
        }
        return new String[] {window(x, i), window(y, i)};
    }

    private static String window(String s, int at) {
        int from = Math.min(Math.max(0, at - 16), s.length());
        int to = Math.min(s.length(), at + 64);
        // Expand to a token boundary, but not indefinitely: a very long literal would swallow the window.
        int guard = 0;
        while (from > 0 && !Character.isWhitespace(s.charAt(from - 1)) && guard++ < 40) {
            from--;
        }
        guard = 0;
        while (to < s.length() && !Character.isWhitespace(s.charAt(to)) && guard++ < 40) {
            to++;
        }
        return s.substring(from, to);
    }

    /**
     * Collapses a diff window to a coarse shape: identifiers, strings and numbers become placeholders
     * so that many instances of one cause share a single signature.
     */
    static String diffShape(String a, String b) {
        return normalizeShape(a) + " => " + normalizeShape(b);
    }

    private static String normalizeShape(String s) {
        String t = s
                // The per-file database the harness creates would otherwise key every signature.
                .replaceAll("srfuzz_[a-z_]*\\d+", "DB")
                .replaceAll("`[^`]*`", "`ID`")
                .replaceAll("'[^']*'", "'S'")
                .replaceAll("\\b\\d+(\\.\\d+)?([eE][+-]?\\d+)?\\b", "N")
                .replaceAll("\\s+", " ")
                .trim();
        return t.length() > 70 ? t.substring(0, 70) : t;
    }

    /** Groups findings that share a root cause so one bug does not flood the report. */
    private static String signatureOf(Throwable t) {
        StackTraceElement[] st = t.getStackTrace();
        String frame = st.length > 0 ? st[0].getClassName() + "#" + st[0].getMethodName() : "?";
        return t.getClass().getSimpleName() + "@" + frame;
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

    // -------------------------------------------------------------- reporting

    /**
     * The dropped mutants, by why they were dropped. Not findings -- see {@link #noteDrop} -- but a
     * reason that recurs across many mutants is worth a look, because a deparser defect and a mutator
     * artifact are indistinguishable here and only the first repeats with the same shape.
     */
    private static void writeDropSection(PrintWriter w, Map<String, Drop> drops) {
        w.printf("## Dropped mutants (%d distinct reasons)%n%n", drops.size());
        if (drops.isEmpty()) {
            w.println("None.");
            w.println();
            return;
        }
        w.println("| count | reason | sample |");
        w.println("|---:|---|---|");
        drops.values().stream()
                .sorted(Comparator.comparingInt((Drop d) -> d.count).reversed())
                .limit(25)
                .forEach(d -> w.printf("| %d | %s | `%s` |%n", d.count,
                        d.reason.replace('|', '/'), d.sampleSql.replace('|', '/')));
        w.println();
    }

    private static void writeReport(Path out, Map<Outcome, Integer> tally, Map<String, Finding> findings,
                                    int seeds, int mutants, int unreachable,
                                    Map<String, Drop> drops) throws Exception {
        if (out.getParent() != null) {
            Files.createDirectories(out.getParent());
        }
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out, StandardCharsets.UTF_8))) {
            w.println("# AST Mutation Fuzz Report (P1)");
            w.println();
            w.printf("seeds: %d, mutants: %d, dropped as grammar-unreachable: %d (%.1f%%)%n",
                    seeds, mutants, unreachable,
                    mutants + unreachable == 0 ? 0.0 : 100.0 * unreachable / (mutants + unreachable));
            w.println();
            writeDropSection(w, drops);
            w.println("| Outcome | Count |");
            w.println("|---|---:|");
            for (Outcome o : Outcome.values()) {
                w.printf("| %s | %d |%n", o, tally.getOrDefault(o, 0));
            }

            List<Finding> bugs = findings.values().stream()
                    .filter(f -> isBug(f.outcome))
                    .sorted(Comparator.comparingInt((Finding f) -> -f.count))
                    .collect(Collectors.toList());
            w.println();
            w.println("## Bug candidates (" + bugs.size() + " distinct signatures)");
            for (Finding f : bugs) {
                w.println();
                w.println("### " + f.outcome + " — " + f.signature + " (x" + f.count + ")");
                w.println();
                w.println("- detail: " + f.detail);
                w.println("- seed:     " + abbrev(f.seedSql));
                w.println("- mutation: " + abbrev(f.mutation));
                w.println("- mutant:   " + abbrev(f.mutantSql));
            }

            List<Finding> nonDefect = findings.values().stream()
                    .filter(f -> !isBug(f.outcome))
                    .sorted(Comparator.comparingInt((Finding f) -> -f.count))
                    .collect(Collectors.toList());

            // Full histogram first: truncating it hid 521 of 1181 fixpoint instances in an earlier run,
            // which reads as "these are all the causes" when it is not.
            w.println();
            w.printf("## Non-defect signature histogram (%d signatures, %d instances)%n",
                    nonDefect.size(), nonDefect.stream().mapToInt(f -> f.count).sum());
            w.println();
            w.println("| count | outcome | signature |");
            w.println("|---:|---|---|");
            for (Finding f : nonDefect) {
                w.printf("| %d | %s | %s |%n", f.count, f.outcome, f.signature.replace("|", "\\|"));
            }

            List<Finding> other = nonDefect.stream().limit(20).collect(Collectors.toList());
            w.println();
            w.println("## Non-defect outcomes (details for the 20 largest)");
            for (Finding f : other) {
                w.println();
                w.println("### " + f.outcome + " — " + f.signature + " (x" + f.count + ")");
                w.println("- mutant: " + abbrev(f.mutantSql));
                w.println("- detail: " + f.detail);
            }
        }
        System.err.println("report written to " + out.toAbsolutePath());
    }

    private static void printSummary(Map<Outcome, Integer> tally, Map<String, Finding> findings,
                                     int seeds, int mutants, int unreachable) {
        System.out.println();
        System.out.printf("=== seeds=%d mutants=%d unreachable=%d (%.1f%% dropped) ===%n",
                seeds, mutants, unreachable,
                mutants + unreachable == 0 ? 0.0 : 100.0 * unreachable / (mutants + unreachable));
        for (Outcome o : Outcome.values()) {
            System.out.printf("%-24s %d%n", o, tally.getOrDefault(o, 0));
        }
        long bugSigs = findings.values().stream().filter(f -> isBug(f.outcome)).count();
        long fixpointSigs = findings.values().stream()
                .filter(f -> f.outcome == Outcome.FIXPOINT_MISMATCH).count();
        System.out.println();
        System.out.println("distinct bug signatures: " + bugSigs);
        System.out.println("distinct fixpoint signatures: " + fixpointSigs);
        findings.values().stream()
                .filter(f -> isBug(f.outcome))
                .sorted(Comparator.comparingInt((Finding f) -> -f.count))
                .limit(20)
                .forEach(f -> System.out.printf("  x%-5d %-24s %s%n", f.count, f.outcome, f.signature));
    }
}
