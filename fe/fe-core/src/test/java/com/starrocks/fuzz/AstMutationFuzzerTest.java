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
import com.starrocks.sql.common.UnsupportedException;
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
     * Expression texts and column names available for injection at one place.
     *
     * <p>Fragments are bucketed by the analyzed type of the expression they came from. Injecting a
     * boolean-valued fragment where a boolean is expected (and a scalar where a scalar is expected)
     * is the cheapest way to raise the share of mutants that survive analysis — an untyped pool
     * makes most mutants fail in the analyzer and never reach the deeper code we want to exercise.
     */
    static class Material {
        final List<String> booleanTexts = new ArrayList<>();
        final List<String> scalarTexts = new ArrayList<>();
        final List<String> columnNames = new ArrayList<>();

        /**
         * The same columns as {@link #columnNames}, rendered the way the analyzed tree referred to them.
         *
         * <p>M4 used to rebind an identifier by writing the bare name, which turned {@code tt5.k1} into
         * {@code k1}. In anything with more than one relation in scope that is ambiguous, and it was the
         * single largest source of ambiguity rejections in a full run -- 5801 of them. The qualified form
         * is what the tree already said, so it resolves wherever the column it came from resolves.
         *
         * <p>Kept separate from {@code columnNames} because the other operators want a bare name: M6
         * builds {@code `a`.`col` = `b`.`col`} from one, and M7 uses one as a struct field name.
         */
        final List<String> qualifiedColumns = new ArrayList<>();

        void addExpr(String text, boolean isBoolean) {
            List<String> bucket = isBoolean ? booleanTexts : scalarTexts;
            if (bucket.size() < 512 && !bucket.contains(text)) {
                bucket.add(text);
            }
        }

        void addColumn(String name) {
            if (name != null && columnNames.size() < 256 && !columnNames.contains(name)) {
                columnNames.add(name);
            }
        }

        void addQualifiedColumn(String text) {
            if (text != null && qualifiedColumns.size() < 256 && !qualifiedColumns.contains(text)) {
                qualifiedColumns.add(text);
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

    /** One seed's material, split by the query block it was harvested from. */
    static final class ScopedPool {
        final Map<Integer, Material> blocks = new LinkedHashMap<>();
        int blockCount;
    }

    /**
     * The file-wide harvest, plus a per-seed, per-block breakdown of the same material.
     *
     * <p>The file-wide lists were the only thing here for a long time, and injecting from them is what
     * made name resolution roughly 43% of all analyzer rejections: a fragment harvested from one seed
     * names tables the target seed never mentions, and a fragment harvested from an inner query block
     * names columns that are out of scope at the point it lands. Both come back as "Unknown table" or
     * "Column cannot be resolved", and the mutant dies in the analyzer without exercising anything.
     *
     * <p>The narrow material is a preference, not a rule -- see {@link #materialFor}. Cross-pollination
     * between blocks and seeds is a large part of what makes a mutant strange, and a fuzzer that only
     * ever injects locally valid fragments trades away reach for a better-looking rejection count.
     */
    static final class Pool extends Material {
        final Map<String, ScopedPool> scoped = new LinkedHashMap<>();

        /**
         * Every query seed in the same corpus file, for M10 to splice against.
         *
         * <p>Same file on purpose: those statements were written against one schema, so a spliced pair
         * resolves. Splicing across files names tables that do not exist in the target's schema, and
         * name resolution is already the largest single category of analyzer rejection.
         */
        final List<String> siblingSeeds = new ArrayList<>();

        /**
         * How often to inject file-wide material at a site that has narrower material available.
         *
         * <p>Not zero on purpose. The out-of-scope fragments this change exists to reduce are also the
         * ones that reach analyzer paths a well-formed query never does, so the aim is to stop spending
         * most of the budget there, not to stop going there at all.
         */
        static final int CROSS_SCOPE_PERCENT = 15;

        Material materialFor(String seed, int block, int blocksNow, Random rnd) {
            if (rnd.nextInt(100) < CROSS_SCOPE_PERCENT) {
                return this;
            }
            ScopedPool sp = scoped.get(seed);
            // The block index is an ordinal in a structural walk, so it only means the same thing on
            // both sides if both walks saw the same tree. Harvest runs on the analyzed copy and mutation
            // on a fresh parse; if those ever disagree the indices silently point at the wrong block, so
            // compare the counts and fall back to the file-wide pool rather than inject nonsense.
            if (sp == null || sp.blockCount != blocksNow) {
                return this;
            }
            Material m = sp.blocks.get(block);
            return m == null || !m.usable() ? this : m;
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

    /**
     * Same-arity swaps, chosen to cross type boundaries where the analyzer should object cleanly.
     *
     * <p>Indexed by argument count. The buckets used to be "one argument" and "two or more", which is
     * not an arity: {@code if} takes exactly three, {@code nullif} and {@code date_add} exactly two,
     * {@code concat_ws} any number. Renaming a two-argument call to {@code if} produced {@code if(a, b)},
     * which the analyzer rejects for its arity before it can object to anything interesting -- the swap
     * was spent without testing what it meant to test.
     */
    private static final String[] SWAPS_ARITY_1 = {
            "abs", "sqrt", "ln", "exp", "floor", "ceil", "sign", "bitmap_count", "hll_cardinality",
            "length", "char_length", "lower", "upper", "to_json", "hex", "unhex", "md5", "year", "month"};
    private static final String[] SWAPS_ARITY_2 = {
            "nullif", "date_add", "date_sub", "datediff", "ifnull", "round", "truncate", "split",
            "concat_ws", "least", "greatest"};
    private static final String[] SWAPS_ARITY_3 = {
            "if", "substr", "json_query", "concat_ws", "coalesce", "lpad", "rpad"};

    /** Variadic, so they are legal at any arity the source call happened to have. */
    private static final String[] SWAPS_ANY_ARITY = {
            "concat_ws", "coalesce", "least", "greatest"};

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
        // Maximum edits stacked on one mutant. 1 restores the previous behaviour of a single edit on a
        // pristine seed, which is the right setting when reducing a finding rather than hunting for one.
        int chainMax = Math.max(1, Integer.getInteger("srfuzz.chain", 4));
        long seedValue = Long.getLong("srfuzz.seed", 20260730L);
        Path report = Paths.get(System.getProperty("srfuzz.report", "ast_mutation_fuzz_report.md"));
        // On by default. The switch exists so the schema-drift it fixes can be measured against the same
        // build rather than against a different commit, which is the only honest way to size it.
        boolean replay = !"false".equalsIgnoreCase(System.getProperty("srfuzz.replay", "true"));
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

        // Corpus sharding, so several JVMs can fuzz disjoint slices at once.
        //
        // Threads are not an option here: schema setup goes through StarRocksAssert into the process's
        // one in-process catalog, and GlobalStateMgr is a singleton, so two files creating the same
        // table name would race and see each other's schemas. Separate processes each get their own
        // catalog and share nothing. The box this runs on has 104 cores and the single-process soak
        // uses under three of them, so the whole gain is here rather than in any per-mutant tuning.
        //
        // Sharding by index rather than by contiguous block: file order follows the corpus directory
        // layout, so neighbouring files are related and a contiguous split would give one shard all the
        // JSON tests and another all the joins. Striding spreads that evenly, which matters because a
        // shard that finishes early is idle for the rest of the round.
        int shards = Math.max(1, Integer.getInteger("srfuzz.shards", 1));
        int shard = Math.max(0, Integer.getInteger("srfuzz.shard", 0));
        if (shards > 1) {
            List<Path> mine = new ArrayList<>();
            for (int fi = shard; fi < files.size(); fi += shards) {
                mine.add(files.get(fi));
            }
            files = mine;
            System.err.println("shard " + shard + "/" + shards + ": " + files.size() + " files");
        }
        System.err.println("corpus: " + files.size() + " files, " + mutationsPerSeed
                + " mutations/seed, rng seed " + seedValue);

        Map<Outcome, Integer> tally = new LinkedHashMap<>();
        Map<String, Finding> findings = new LinkedHashMap<>();
        int seedCount = 0;
        int mutantCount = 0;
        int unreachableCount = 0;
        int staleSeeds = 0;
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
                    // Only seeds that analyzed get here, so M10 always splices against something that
                    // resolves in this file's schema.
                    pool.siblingSeeds.add(sql);
                    harvest((QueryStatement) probe, sql, pool);

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

                // Second pass, from an empty database. The first pass ran every statement in the file,
                // so by the time it ended the catalog held the file's FINAL schema -- and 36% of the
                // corpus (479 of 1320 files) drops, renames or re-creates a table after its first query.
                // Mutating there meant a seed written before a `DROP TABLE t` was analyzed against a
                // catalog with no t: every one of its mutants died with "Unknown table" or "Column cannot
                // be resolved", the seed contributed nothing, and the failure was indistinguishable in
                // the report from a mutant that really was invalid.
                //
                // Replaying gives each seed the catalog state that existed where it was written. The pool
                // still comes from the first pass, so the material is the whole file's rather than only
                // the part of it that precedes the seed.
                if (replay && !replayFrom(db)) {
                    continue;
                }
                java.util.Set<String> seedSet = new java.util.HashSet<>(seeds);
                for (String sql : statements) {
                    StatementBase replayed = tryParse(sql);
                    if (replayed == null) {
                        continue;
                    }
                    if (CorpusReader.isSchemaSetup(replayed)) {
                        if (replay) {
                            applySchemaSetup(sql, replayed);
                        }
                        continue;
                    }
                    if (!seedSet.contains(sql)) {
                        continue;
                    }
                    String seed = sql;
                    // A seed that no longer analyzes cannot produce a usable mutant, so mutating it
                    // mutationsPerSeed times only manufactures rejections that look like ordinary noise.
                    // Counting them is what makes schema drift visible: with the replay above this should
                    // be nearly zero, and if it ever is not, something other than drift is wrong.
                    StatementBase check = tryParse(seed);
                    if (check == null) {
                        continue;
                    }
                    try {
                        Analyzer.analyze(check, ctx);
                    } catch (Throwable t) {
                        staleSeeds++;
                        continue;
                    }
                    for (int m = 0; m < mutationsPerSeed; m++) {
                        // Compound edits: each link re-parses the text the previous link round-tripped
                        // through, so a mutation is always applied to a well-formed tree and the earlier
                        // result is never mutated in place underneath us.
                        String currentSql = seed;
                        StringBuilder applied = new StringBuilder();
                        Reparsed reachable = null;
                        String reachedBy = null;
                        int depth = chainDepth(rnd, chainMax);
                        for (int step = 0; step < depth; step++) {
                            StatementBase ast = tryParse(currentSql);
                            if (!(ast instanceof QueryStatement)) {
                                break;
                            }
                            // The first edit keeps the default mix so a depth-1 mutant stays what it
                            // always was; every edit after it goes structural, which is the only way the
                            // statement gets deeper rather than merely different.
                            String mutation = Mutator.mutate((QueryStatement) ast, pool, seed, rnd,
                                    step == 0 ? 45 : 90);
                            if (mutation == null) {
                                break;
                            }
                            if (applied.length() > 0) {
                                applied.append(" + ");
                            }
                            applied.append(mutation);
                            Reparsed next = reparseThroughGrammar(ast, drops, seed, applied.toString());
                            if (next == null) {
                                // Keep the deepest prefix that did survive. Throwing away a good
                                // depth-1 mutant because a second edit broke it is pure loss.
                                if (reachable == null) {
                                    unreachableCount++;
                                }
                                break;
                            }
                            reachable = next;
                            reachedBy = applied.toString();
                            currentSql = next.sql;
                        }
                        if (reachable == null) {
                            continue;
                        }
                        String mutation = reachedBy;
                        mutantCount++;
                        // M9: on a minority of mutants, evaluate under a perturbed session flag. The
                        // flag has to be restored even when evaluate throws -- it is shared with every
                        // later seed, so a leak silently reinterprets the rest of the run.
                        SessionFlagPerturbation.Perturbation flag = rnd.nextInt(100) < 15
                                ? SessionFlagPerturbation.apply(ctx.getSessionVariable(), rnd)
                                : null;
                        try {
                            evaluate(seed, flag == null ? mutation : mutation + " | " + flag.description(),
                                    reachable.stmt, tally, findings);
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

        writeReport(report, tally, findings, seedCount, mutantCount, unreachableCount, staleSeeds, drops);
        printSummary(tally, findings, seedCount, mutantCount, unreachableCount, staleSeeds);
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

    /** A mutant that survived deparse and reparse, with the text it round-tripped through. */
    static final class Reparsed {
        final StatementBase stmt;
        final String sql;

        Reparsed(StatementBase stmt, String sql) {
            this.stmt = stmt;
            this.sql = sql;
        }
    }

    private static Reparsed reparseThroughGrammar(StatementBase mutated, Map<String, Drop> drops,
                                                  String seed, String mutation) {
        String text;
        try {
            text = AstToSQLBuilder.toSQL(mutated);
        } catch (Throwable t) {
            noteDrop(drops, "deparse-threw:" + signatureOf(t), unrenderableSample(seed, mutation));
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
            return new Reparsed(parsed.get(0), text);
        } catch (Throwable t) {
            noteDrop(drops, "reparse-failed:" + oneLine(t), oneLineSql(text));
            return null;
        }
    }

    /**
     * How many mutations to apply on top of each other for one mutant.
     *
     * <p>Every mutant used to be exactly one edit on a freshly parsed seed, which caps a mutant's
     * complexity at its seed's: the corpus is SQL-Tester cases whose median query is 91 characters,
     * 89% have no join and 96.5% have no CTE, and a single local edit does not change that. Chaining
     * edits is the only way the mutator can produce a statement deeper than anything it was given.
     *
     * <p>Most mutants stay at depth 1 on purpose. A one-edit delta from a known-good seed is what
     * makes a finding cheap to reduce, and losing that would trade triage cost for depth.
     */
    private static int chainDepth(Random rnd, int max) {
        int depth = 1;
        // 35% chance of another edit at each step, so depth 1 stays the common case and the tail thins
        // out quickly: roughly 65 / 23 / 8 / 3 percent for depths 1 through 4.
        while (depth < max && rnd.nextInt(100) < 35) {
            depth++;
        }
        return depth;
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
            String signature = o == Outcome.ANALYZE_REJECTED
                    ? rejectionSignature(t, mutation) : signatureOf(t);
            record(tally, findings, o, signature, seed, mutation, bestEffortSql(mutant), oneLine(t));
            return;
        }

        try {
            mutantSql = AstToSQLBuilder.toSQL(mutant);
        } catch (Throwable t) {
            record(tally, findings, Outcome.DEPARSE_THROW, signatureOf(t), seed, mutation,
                    unrenderableSample(seed, mutation), oneLine(t));
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
        // UnsupportedException is a declared refusal -- "table function cannot appear on the left side
        // of a join", "unsupported type", and so on -- carrying a message written for the user. It just
        // happens to extend StarRocksPlannerException extends RuntimeException rather than
        // SemanticException, so keying on the type alone filed every one of them as an internal error
        // and reported a non-defect as a bug.
        if (t instanceof UnsupportedException) {
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
        String key = o + "|" + signature;
        Finding existing = findings.get(key);
        if (existing != null) {
            existing.count++;
        } else {
            findings.put(key, new Finding(o, signature, sample(seed), sample(mutation), sample(mutantSql), detail));
        }
    }

    /**
     * Caps a retained sample. Keeping the rejections turned this map from a few hundred entries into
     * potentially thousands, each holding a full seed and a full mutant, and a mutant can run to tens of
     * kilobytes. The cap is well above the 300 characters {@link #abbrev} prints, so no report output
     * changes -- it only stops the run from carrying whole SQL texts it will never show.
     */
    private static String sample(String s) {
        return s == null || s.length() <= 1000 ? s : s.substring(0, 1000);
    }

    // ---------------------------------------------------------------- harvest

    private static void harvest(QueryStatement stmt, String seed, Pool pool) {
        RootWalk walk = RootWalk.of(stmt.getQueryRelation());
        ScopedPool scoped = pool.scoped.computeIfAbsent(seed, k -> new ScopedPool());
        scoped.blockCount = walk.blocks;
        for (Root root : walk.roots) {
            Material block = scoped.blocks.computeIfAbsent(root.block, k -> new Material());
            collectExprs(root.expr, e -> {
                if (e instanceof SlotRef) {
                    String name = ((SlotRef) e).getColumnName();
                    pool.addColumn(name);
                    block.addColumn(name);
                }
                try {
                    String s = AstToSQLBuilder.toSQL(e);
                    if (s != null && !s.trim().isEmpty() && s.length() < 400) {
                        // Harvested from an analyzed tree, so the type is populated.
                        boolean isBoolean = e.getType() != null && e.getType().isBoolean();
                        pool.addExpr(s, isBoolean);
                        block.addExpr(s, isBoolean);
                        if (e instanceof SlotRef) {
                            // The analyzed rendering carries whatever qualification the reference needs.
                            pool.addQualifiedColumn(s);
                            block.addQualifiedColumn(s);
                        }
                    }
                } catch (Throwable ignored) {
                    // an un-renderable fragment is simply not pooled
                }
            });
        }
    }

    /** An expression root together with the query block whose names are in scope where it sits. */
    static final class Root {
        final int block;
        final Expr expr;

        Root(int block, Expr expr) {
            this.block = block;
            this.expr = expr;
        }
    }

    /**
     * The roots of a tree, each tagged with the query block that owns it.
     *
     * <p>A block is an ordinal: blocks are numbered in the order this walk first enters them. That is
     * only a valid key across two separate walks if both see the same tree shape, which is why
     * {@link Pool#materialFor} compares block counts before trusting an index. It is enough here
     * because the harvested tree and the mutated tree are two parses of the same seed text.
     *
     * <p>Blocks are allocated per SelectRelation and per set operation, i.e. per thing that has its own
     * output columns. Everything reached from a block without passing through another one -- join
     * operands, ON predicates, table function arguments -- belongs to it, which is what "in scope here"
     * means for the purpose of choosing a fragment to inject.
     */
    static final class RootWalk {
        final List<Root> roots = new ArrayList<>();
        int blocks;

        /**
         * Relations already walked, by identity.
         *
         * <p>Analysis resolves a {@code FROM t} that names a CTE into a reference to the very same
         * CTERelation object that hangs off the WITH clause, so the analyzed tree reaches it twice: once
         * through the WITH descent and once through the FROM. Counting it twice made the analyzed tree
         * report three blocks where the parsed tree reported two, which would have disabled the scoped
         * pool for every seed with a CTE -- silently, since the fallback is the old behaviour.
         *
         * <p>Skipping a repeat visit is right regardless of the counting: it is the same object, so its
         * expressions are the same objects, and a mutation applied "twice" would be one mutation.
         */
        private final java.util.Set<Relation> seen =
                java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());

        static RootWalk of(Relation relation) {
            RootWalk walk = new RootWalk();
            walk.descend(relation, 0);
            return walk;
        }

        private void descend(Relation relation, int block) {
            if (relation == null || !seen.add(relation)) {
                return;
            }
            if (relation instanceof SelectRelation || relation instanceof SetOperationRelation) {
                block = blocks++;
            }
            collectInto(relation, block, this);
        }
    }

    /** Expression roots reachable without analysis, so the same walk works pre- and post-analyze. */
    static List<Expr> collectRootExprs(Relation relation) {
        return RootWalk.of(relation).roots.stream().map(r -> r.expr).collect(Collectors.toList());
    }

    private static void collectInto(Relation relation, int block, RootWalk walk) {
        if (relation instanceof QueryRelation && ((QueryRelation) relation).getCteRelations() != null) {
            // A WITH clause hangs off the query, not the FROM clause, so it needs its own descent.
            for (CTERelation cte : ((QueryRelation) relation).getCteRelations()) {
                walk.descend(cte, block);
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
                    walk.roots.add(new Root(block, e.getExpr()));
                }
            }
        }
        if (relation instanceof SelectRelation) {
            SelectRelation sel = (SelectRelation) relation;
            if (sel.getSelectList() != null && sel.getSelectList().getItems() != null) {
                sel.getSelectList().getItems().stream()
                        .filter(it -> !it.isStar() && it.getExpr() != null)
                        .forEach(it -> walk.roots.add(new Root(block, it.getExpr())));
            }
            if (sel.getPredicate() != null) {
                walk.roots.add(new Root(block, sel.getPredicate()));
            }
            if (sel.getHavingClause() != null) {
                walk.roots.add(new Root(block, sel.getHavingClause()));
            }
            if (sel.getGroupByClause() != null && sel.getGroupByClause().getGroupingExprs() != null) {
                for (Expr e : sel.getGroupByClause().getGroupingExprs()) {
                    walk.roots.add(new Root(block, e));
                }
            }
            if (sel.getRelation() != null) {
                walk.descend(sel.getRelation(), block);
            }
        } else if (relation instanceof SubqueryRelation) {
            walk.descend(((SubqueryRelation) relation).getQueryStatement().getQueryRelation(), block);
        } else if (relation instanceof SetOperationRelation) {
            for (Relation child : ((SetOperationRelation) relation).getRelations()) {
                walk.descend(child, block);
            }
        } else if (relation instanceof JoinRelation) {
            // Without this branch a join swallowed its whole subtree: the ON predicate was never a
            // mutation site and never reached the pool, and so was everything below the join -- a
            // subquery on either side went dark purely because of its parent. Around a third of the
            // SQL-Tester corpus joins, so that was a third of the seeds with no reachable FROM clause.
            JoinRelation join = (JoinRelation) relation;
            if (join.getOnPredicate() != null) {
                walk.roots.add(new Root(block, join.getOnPredicate()));
            }
            walk.descend(join.getLeft(), block);
            walk.descend(join.getRight(), block);
        } else if (relation instanceof CTERelation) {
            walk.descend(((CTERelation) relation).getCteQueryStatement().getQueryRelation(), block);
        } else if (relation instanceof TableFunctionRelation) {
            List<Expr> args = ((TableFunctionRelation) relation).getChildExpressions();
            if (args != null) {
                for (Expr e : args) {
                    walk.roots.add(new Root(block, e));
                }
            }
        }
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
                new NestingMutation(),
                new PredicateMutation(),    // the only operator that produces a condition at all
                new SpliceMutation());      // the only operator that can exceed the corpus's own depth

        /**
         * Applies one mutation and returns a description of it, or null when nothing was applied.
         *
         * <p>The description matters: when the mutant cannot be deparsed the report has no SQL to show,
         * and a finding nobody can reproduce is worth nothing. Recording the site and the injected text
         * makes such a finding reconstructible from the seed alone.
         */
        static String mutate(QueryStatement stmt, Pool pool, String seed, Random rnd) {
            return mutate(stmt, pool, seed, rnd, 45);
        }

        /**
         * @param structuralPercent how often to reach for a structural operator before falling back to an
         *     expression edit. Measured over 6154 rejections, the default mix is 48% M1-M4-expr, 27%
         *     M7-typestress, 14% M5-clause and only 11% M6-nesting -- three quarters of all edits replace a
         *     leaf. That is why stacking edits alone did not deepen anything: chaining three leaf swaps
         *     still yields a leaf swap. Later links in a chain raise this, because they start from a tree
         *     that already round-tripped and adding nesting on top of it is the point.
         */
        static String mutate(QueryStatement stmt, Pool pool, String seed, Random rnd, int structuralPercent) {
            List<Mutation> order = new ArrayList<>(OPERATORS);
            java.util.Collections.shuffle(order, rnd);
            int structuralShare = rnd.nextInt(100) < structuralPercent ? order.size() : 0;
            for (int i = 0; i < structuralShare; i++) {
                String applied = tryOperator(order.get(i), stmt, pool, rnd);
                if (applied != null) {
                    return applied;
                }
            }
            return mutateExpression(stmt, pool, seed, rnd);
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
        private static String mutateExpression(QueryStatement stmt, Pool pool, String seed, Random rnd) {
            RootWalk walk = RootWalk.of(stmt.getQueryRelation());
            List<Site> sites = new ArrayList<>();
            // Which block each site sits in, kept alongside rather than on Site, because a Site is a
            // position in an expression and knows nothing about the relation that contains it.
            List<Integer> blocks = new ArrayList<>();
            for (Root root : walk.roots) {
                int before = sites.size();
                collectSites(root.expr, sites);
                for (int i = before; i < sites.size(); i++) {
                    blocks.add(root.block);
                }
            }
            if (sites.isEmpty()) {
                return null;
            }
            int chosen = rnd.nextInt(sites.size());
            Site site = sites.get(chosen);
            Expr current = site.child();
            Material material = pool.materialFor(seed, blocks.get(chosen), walk.blocks, rnd);
            Expr replacement = buildReplacement(current, material, rnd);
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

        /**
         * M1 subtree swap / M2 function swap / M3 literal boundary / M4 identifier rebind.
         *
         * <p>{@code material} is what is in scope where the replacement will land, which is usually a
         * single query block of a single seed rather than the whole file -- see {@link Pool#materialFor}.
         */
        private static Expr buildReplacement(Expr current, Material material, Random rnd) {
            int roll = rnd.nextInt(100);
            String text = null;
            if (current instanceof LiteralExpr && roll < 45) {
                text = BOUNDARY_LITERALS[rnd.nextInt(BOUNDARY_LITERALS.length)];                 // M3
            } else if (current instanceof SlotRef && roll < 45) {                                 // M4
                // Prefer the qualified rendering. Writing the bare name turned `tt5`.`k1` into `k1`,
                // which is ambiguous as soon as two relations are in scope -- 5801 rejections in a full
                // run, the largest single ambiguity bucket. A bare name is still worth writing
                // sometimes: it is what a user writes, and unqualified resolution is its own code path.
                List<String> names = rnd.nextInt(100) < 15 || material.qualifiedColumns.isEmpty()
                        ? material.columnNames : material.qualifiedColumns;
                if (!names.isEmpty()) {
                    String picked = names.get(rnd.nextInt(names.size()));
                    text = names == material.columnNames ? "`" + picked + "`" : picked;
                }
            } else if (current instanceof FunctionCallExpr && roll < 70) {
                text = swapFunction((FunctionCallExpr) current, rnd);                             // M2
            }
            if (text == null) {                                                                   // M1
                // The replaced node is unanalyzed, so its type is unknown; approximate the expected
                // shape from the node class instead.
                List<String> bucket = material.bucketFor(current instanceof Predicate);
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
            String[] candidates;
            switch (arity) {
                case 1:
                    candidates = SWAPS_ARITY_1;
                    break;
                case 2:
                    candidates = SWAPS_ARITY_2;
                    break;
                case 3:
                    candidates = SWAPS_ARITY_3;
                    break;
                default:
                    // Four or more: only the variadic names are legal at an arity we did not enumerate.
                    candidates = arity >= 4 ? SWAPS_ANY_ARITY : null;
                    break;
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

    /**
     * Drops and recreates the file's database so its statements can be replayed onto an empty catalog.
     *
     * <p>Returns false when that fails, which costs the file its mutants rather than running them
     * against whatever the catalog happens to hold -- the state this whole replay exists to avoid.
     */
    private boolean replayFrom(String db) {
        try {
            srAssert.dropDatabase(db);
            srAssert.withDatabase(db).useDatabase(db);
            return true;
        } catch (Throwable t) {
            System.err.println("replay setup failed for " + db + " -> " + t);
            return false;
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

    /**
     * A sample for a mutant whose deparse threw. Re-running the deparser is pointless -- it is the thing
     * that just failed -- and "<unrenderable>" throws away the only two facts that are always available
     * and always sufficient to reproduce: the seed, which parses by construction, and the mutation that
     * was applied to it. A single NullPointerException in the deparser discarded 24046 mutants in one
     * round while the report showed nothing but "<unrenderable>" for every one of them.
     */
    private static String unrenderableSample(String seed, String mutation) {
        return "<deparse threw> mutation: " + (mutation == null ? "?" : mutation)
                + " | seed: " + oneLineSql(seed);
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
        return normalizeShape(s, 70);
    }

    private static String normalizeShape(String s, int cap) {
        String t = s
                // The per-file database the harness creates would otherwise key every signature.
                .replaceAll("srfuzz_[a-z_]*\\d+", "DB")
                .replaceAll("`[^`]*`", "`ID`")
                .replaceAll("'[^']*'", "'S'")
                .replaceAll("\\b\\d+(\\.\\d+)?([eE][+-]?\\d+)?\\b", "N")
                .replaceAll("\\s+", " ")
                .trim();
        return t.length() > cap ? t.substring(0, cap) : t;
    }

    /** Field separator inside a rejection signature: operator, then throw site, then message shape. */
    private static final String SIG_SEP = " :: ";

    /**
     * Signature for an analyzer rejection: which operator built the mutant, where the analyzer threw,
     * and the shape of what it said.
     *
     * <p>{@link #signatureOf} alone is too coarse. Half of every run ends in ANALYZE_REJECTED, and those
     * rejections funnel through a handful of throw sites -- keying on the site alone collapses "column
     * does not exist" and "function signature not found" into one bucket, which is exactly the
     * distinction that makes the histogram readable. The message shape is what separates them.
     *
     * <p>The operator is part of the key rather than a derived column because one message means
     * different things depending on who produced it. "Column cannot be resolved" from M4 is an identifier
     * rebind that ignored scope; the same message from M6 is a nesting shape that moved a column out of
     * scope. Counting them together would hide both. Deriving the split afterwards from one retained
     * sample per signature would also be wrong -- a sample is not a distribution.
     *
     * <p>The point of keeping these at all: a rejection is only noise if the mutant really is invalid.
     * A rejection the analyzer should not have issued looks identical in the tally and was previously
     * discarded on the spot, so that whole class -- roughly half the run's work -- was unobservable.
     */
    private static String rejectionSignature(Throwable t, String mutation) {
        return operatorOf(mutation) + SIG_SEP + shortSite(t) + SIG_SEP + messageShape(t);
    }

    /**
     * Which mutation operator built this mutant, from the description the operator itself wrote.
     *
     * <p>M9 is deliberately not an answer: it perturbs a session flag on top of whatever edit was made,
     * so it rides along in the description without being the edit. Attributing a rejection to it would
     * take the rejection away from the operator that actually caused it.
     */
    static String operatorOf(String mutation) {
        if (mutation == null || mutation.startsWith("<")) {
            return "seed-baseline";
        }
        int rider = mutation.indexOf(" | M9-session");
        String head = rider < 0 ? mutation : mutation.substring(0, rider);
        int space = head.indexOf(' ');
        String first = space < 0 ? head : head.substring(0, space);
        // M5/M6/M7 name themselves; M1-M4 are the implicit expression-level fallback and start with the
        // class of the node they edited, so anything not spelled M<digit> is one of those.
        return first.length() > 1 && first.charAt(0) == 'M' && Character.isDigit(first.charAt(1))
                ? first : "M1-M4-expr";
    }

    /** The throw site without the package prefix every analyzer frame shares. */
    private static String shortSite(Throwable t) {
        String site = signatureOf(t);
        return site.replace("com.starrocks.sql.analyzer.", "").replace("com.starrocks.sql.", "");
    }

    private static String messageShape(Throwable t) {
        String m = t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage();
        // "Getting analyzing error at line 1, column 42. Detail message: <the actual reason>" -- the
        // prefix carries a position that differs for every mutant, so it would defeat the grouping.
        int at = m.indexOf("Detail message: ");
        if (at >= 0) {
            m = m.substring(at + "Detail message: ".length());
        }
        // Wider than the diff windows normalizeShape was written for: at 70 characters the analyzer's
        // longer messages were cut mid-clause, so distinct causes shared a bucket and read as one.
        return normalizeShape(m.replace('\n', ' '), 160);
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

    /** Highest-volume rejection signatures printed. Enough to see where the mass sits. */
    private static final int REJECTION_VOLUME_ROWS = 40;

    /** A signature at or below this count is rare enough to be worth reading individually. */
    private static final int RARE_REJECTION_MAX = 2;

    /** Rare signatures printed. The tail is long, so it is sampled rather than dumped. */
    private static final int RARE_REJECTION_ROWS = 60;

    /**
     * Why the rejections got rejected. They are not findings and most of them are not defects, but they
     * are around half of every run, and until this section existed that half produced one number and
     * nothing else -- a false rejection (a mutant the analyzer should have accepted) was indistinguishable
     * from the expected noise, because both are a count in the same cell.
     *
     * <p>Three tables, because two different questions are being asked of the same data:
     * <ul>
     *   <li><b>By operator</b> answers "where is the fuzzer wasting its budget". An operator whose mutants
     *       are mostly rejected is reaching the analyzer and stopping there, never exercising anything
     *       deeper, and its share here is the size of the prize for fixing it.</li>
     *   <li><b>Highest volume</b> answers the same question at signature granularity.</li>
     *   <li><b>Rare signatures</b> is where a false rejection would actually be. A rejection the analyzer
     *       should not have issued is by nature uncommon -- if it were common the corpus would have
     *       tripped over it already. Ranking by count and truncating, which is what the first version of
     *       this section did, cut off precisely the rows worth reading.</li>
     * </ul>
     */
    private static void writeRejectionSection(PrintWriter w, Map<String, Finding> findings) {
        List<Finding> rejections = findings.values().stream()
                .filter(f -> f.outcome == Outcome.ANALYZE_REJECTED)
                .collect(Collectors.toList());
        int total = rejections.stream().mapToInt(f -> f.count).sum();
        w.println();
        w.printf("## Analyzer rejections (%d signatures, %d instances)%n", rejections.size(), total);
        w.println();
        if (rejections.isEmpty()) {
            w.println("None.");
            w.println();
            return;
        }

        w.println("### By operator");
        w.println();
        w.println("| operator | instances | share | signatures | most common reason |");
        w.println("|---|---:|---:|---:|---|");
        Map<String, List<Finding>> byOperator = rejections.stream()
                .collect(Collectors.groupingBy(f -> field(f.signature, 0), LinkedHashMap::new,
                        Collectors.toList()));
        byOperator.entrySet().stream()
                .sorted(Comparator.comparingInt((Map.Entry<String, List<Finding>> e) ->
                        -e.getValue().stream().mapToInt(f -> f.count).sum()))
                .forEach(e -> {
                    int instances = e.getValue().stream().mapToInt(f -> f.count).sum();
                    Finding top = e.getValue().stream()
                            .max(Comparator.comparingInt(f -> f.count)).orElse(null);
                    w.printf("| %s | %d | %.1f%% | %d | %s |%n", e.getKey(), instances,
                            100.0 * instances / total, e.getValue().size(),
                            top == null ? "" : escape(field(top.signature, 2)));
                });
        w.println();

        List<Finding> byVolume = rejections.stream()
                .sorted(Comparator.comparingInt((Finding f) -> -f.count))
                .collect(Collectors.toList());
        w.printf("### Highest volume (%d of %d signatures)%n",
                Math.min(REJECTION_VOLUME_ROWS, byVolume.size()), byVolume.size());
        w.println();
        writeRejectionRows(w, byVolume.subList(0, Math.min(REJECTION_VOLUME_ROWS, byVolume.size())));
        if (byVolume.size() > REJECTION_VOLUME_ROWS) {
            List<Finding> tail = byVolume.subList(REJECTION_VOLUME_ROWS, byVolume.size());
            w.println();
            w.printf("%d further signatures (%d instances) below this cut.%n",
                    tail.size(), tail.stream().mapToInt(f -> f.count).sum());
        }
        w.println();

        List<Finding> rare = rejections.stream()
                .filter(f -> f.count <= RARE_REJECTION_MAX)
                .sorted(Comparator.comparingInt(f -> f.count))
                .collect(Collectors.toList());
        w.printf("### Rare signatures, count <= %d (%d of them, %d instances)%n",
                RARE_REJECTION_MAX, rare.size(), rare.stream().mapToInt(f -> f.count).sum());
        w.println();
        w.println("A false rejection would be here rather than above: one the analyzer should not have");
        w.println("issued is by nature uncommon. Read the message against the mutation next to it and ask");
        w.println("whether the mutant really was invalid.");
        w.println();
        writeRejectionRows(w, rare.subList(0, Math.min(RARE_REJECTION_ROWS, rare.size())));
        if (rare.size() > RARE_REJECTION_ROWS) {
            // Say what was cut, every time. A truncated table that does not admit it reads as the whole
            // picture -- and here the omitted rows are the same kind as the shown ones, not lesser ones.
            w.println();
            w.printf("%d further rare signatures not shown.%n", rare.size() - RARE_REJECTION_ROWS);
        }
        w.println();
    }

    private static void writeRejectionRows(PrintWriter w, List<Finding> rows) {
        w.println("| count | operator | throw site | message shape | sample mutation |");
        w.println("|---:|---|---|---|---|");
        for (Finding f : rows) {
            w.printf("| %d | %s | %s | %s | %s |%n", f.count,
                    field(f.signature, 0), escape(field(f.signature, 1)), escape(field(f.signature, 2)),
                    escape(abbrev(f.mutation)));
        }
    }

    /** One field of a {@link #SIG_SEP}-joined signature, or "" when the signature has fewer. */
    private static String field(String signature, int index) {
        String[] parts = signature.split(SIG_SEP, 3);
        return index < parts.length ? parts[index] : "";
    }

    /** A bare '|' would end the markdown cell it sits in, and deparsed SQL is full of them. */
    private static String escape(String s) {
        return s.replace("|", "\\|");
    }

    private static void writeReport(Path out, Map<Outcome, Integer> tally, Map<String, Finding> findings,
                                    int seeds, int mutants, int unreachable, int staleSeeds,
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
            w.printf("seeds skipped as stale (no longer analyze where they are mutated): %d%n",
                    staleSeeds);
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

            writeRejectionSection(w, findings);

            List<Finding> nonDefect = findings.values().stream()
                    .filter(f -> !isBug(f.outcome) && f.outcome != Outcome.ANALYZE_REJECTED)
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
                                     int seeds, int mutants, int unreachable, int staleSeeds) {
        System.out.println();
        System.out.printf("=== seeds=%d mutants=%d unreachable=%d (%.1f%% dropped) stale=%d ===%n",
                seeds, mutants, unreachable,
                mutants + unreachable == 0 ? 0.0 : 100.0 * unreachable / (mutants + unreachable),
                staleSeeds);
        for (Outcome o : Outcome.values()) {
            System.out.printf("%-24s %d%n", o, tally.getOrDefault(o, 0));
        }
        long bugSigs = findings.values().stream().filter(f -> isBug(f.outcome)).count();
        long fixpointSigs = findings.values().stream()
                .filter(f -> f.outcome == Outcome.FIXPOINT_MISMATCH).count();
        long rejectionSigs = findings.values().stream()
                .filter(f -> f.outcome == Outcome.ANALYZE_REJECTED).count();
        System.out.println();
        System.out.println("distinct bug signatures: " + bugSigs);
        System.out.println("distinct fixpoint signatures: " + fixpointSigs);
        System.out.println("distinct rejection signatures: " + rejectionSigs);
        findings.values().stream()
                .filter(f -> isBug(f.outcome))
                .sorted(Comparator.comparingInt((Finding f) -> -f.count))
                .limit(20)
                .forEach(f -> System.out.printf("  x%-5d %-24s %s%n", f.count, f.outcome, f.signature));
    }
}
