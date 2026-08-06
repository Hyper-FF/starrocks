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

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.StorageAccessException;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

/**
 * M8 of srfuzz/docs/SQL_AST_FUZZER_PLAN.md §2 — statement-sequence splicing. The plan calls it the highest-value
 * operator; this class is the FE-only half of it, and the class javadoc is the design note the rest of
 * the harness has to be read against.
 *
 * <h2>1. Why this is not a {@link Mutation}</h2>
 *
 * <p>It does not fit, and forcing it in would break both sides. Three independent mismatches:
 *
 * <ul>
 *   <li><b>Wrong unit of work.</b> {@code Mutation.apply(QueryStatement, Pool, Random)} edits one
 *       unanalyzed statement in place. M8 does not edit a statement at all — its unit is a triple
 *       (schema, probe set, DDL step), and the statements it interleaves are, by the Decision Log entry
 *       of 2026-07-29 quoted in §4 below, <i>text templates</i> rather than ASTs. There is nothing to
 *       hand to the existing signature and nothing for it to return.</li>
 *   <li><b>No handle on the world.</b> The signature carries no {@link ConnectContext} and no
 *       {@link StarRocksAssert}. A DDL step is meaningless without a live catalog to apply it to, and
 *       the {@code Mutation} contract goes out of its way to say the tree is unanalyzed and that
 *       operators must not depend on catalog state. M8 is the exact opposite: catalog state is its
 *       entire subject.</li>
 *   <li><b>Different oracle, and a stateful one.</b> {@code AstMutationFuzzerTest} judges one statement
 *       by {@code analyze → deparse → reparse → reanalyze → fixpoint}. M8's signal is a <i>difference
 *       between two evaluations of the same unchanged statement</i>, taken across a catalog mutation.
 *       A per-statement oracle has no way to express "compare against the previous run". Worse, the
 *       existing operators are pure — the driver may retry, reorder and interleave them freely — while
 *       M8 mutates global state, so it must own setup and teardown and cannot be scheduled like a peer.
 *       Plan §7 risk 7 already anticipates this ("the heavy operator is scheduled separately, not at
 *       equal weight with the light ones").</li>
 * </ul>
 *
 * <p>So M8 gets its own class, which is operator, driver and oracle in one, and keeps only the
 * <i>conventions</i> of {@code Mutation}: a stable {@link #NAME}, and "not applicable" expressed as a
 * null/no-op rather than an exception. The name {@code StatementSequenceMutation} is kept even though
 * it is not a {@code Mutation}, because it is the plan's name for the operator.
 *
 * <h2>2. The oracle</h2>
 *
 * <p>Differential, over a probe that never changes:
 *
 * <pre>
 *   before[p] = evaluate(p)      for every probe p        // must be clean, or p is dropped
 *   apply(step)                                            // one DDL text template
 *   after[p]  = evaluate(p)      for every probe p
 * </pre>
 *
 * <p>{@code evaluate} is {@code parse → analyze → deparse the analyzed tree}. The deparse of the
 * analyzed tree is the strongest observable available with no BE: it carries the resolved column set,
 * the resolved types and the resolved table names, so it moves whenever the meaning of the query moves.
 *
 * <p>Each probe is classified as <b>dependent</b> or <b>independent</b> of the step, and the verdict
 * follows from that (see {@link Verdict}). Dependency is computed from the <b>analyzed</b> baseline
 * tree, not from the probe text, so it resolves through views: {@code SELECT * FROM v} where
 * {@code v = SELECT ... FROM a} is correctly dependent on an {@code ALTER TABLE a}. A text-level check
 * would call it independent and report every such break as a defect.
 *
 * <p>What counts as a bug, concretely:
 *
 * <ul>
 *   <li><b>Any internal error, dependent or not</b> ({@link Verdict#BUG_INTERNAL_ERROR}). This is O2
 *       tier C from plan §3 applied after a state change, and it is the case worth the whole operator:
 *       after {@code DROP COLUMN}, "that column is gone" must come back as a declared
 *       {@code SemanticException}, never as an NPE / ClassCastException / IllegalStateException from
 *       something that cached the old schema.</li>
 *   <li><b>An independent probe that stops analyzing</b> ({@link Verdict#BUG_UNRELATED_BREAK}). A DDL
 *       naming objects the probe does not touch must not break the probe.</li>
 *   <li><b>An independent probe whose analyzed form moves</b> ({@link Verdict#BUG_UNRELATED_DRIFT}).
 *       Adding a column to {@code a} must not change the resolved form of a query that selects named
 *       columns from {@code b} — or from {@code a}, for that matter.</li>
 *   <li><b>The DDL itself failing internally</b> ({@link SequenceResult#ddlInternalError}). A DDL the
 *       analyzer should have rejected but that instead crashed a handler is the "FE lets it through,
 *       something downstream explodes" shape from plan §3.</li>
 *   <li><b>Teardown residue</b> ({@link SequenceResult#residue}). See §3.</li>
 * </ul>
 *
 * <p>What is <b>expected behaviour</b>, and is recorded but never reported as a defect:
 *
 * <ul>
 *   <li>A dependent probe rejected with a <i>declared</i> error ({@link Verdict#EXPECTED_BREAK}) —
 *       {@code SELECT dropped_col FROM a} after {@code ALTER TABLE a DROP COLUMN dropped_col} must
 *       fail, and a fuzzer that reported it would drown in its own noise.</li>
 *   <li>A dependent probe that still analyzes but resolves differently
 *       ({@link Verdict#EXPECTED_DRIFT}) — {@code SELECT * FROM a} expands wider after
 *       {@code ADD COLUMN}. That <i>is</i> the schema change, observed.</li>
 * </ul>
 *
 * <p>A star select is treated as depending on every column of every table it reads. That flag is taken
 * from the <b>unanalyzed</b> parse, because analysis has already expanded the star into the baseline
 * column set — and the whole point is that the post-DDL column set is different.
 *
 * <p>Deliberately <b>not</b> in the oracle: plan-shape comparison. A step may legitimately change the
 * plan (an index or an MV exists to change it), so plan drift is not a sound defect signal without a
 * far finer model of what each step licenses. Analyzed-form drift is sound; plan drift is not.
 *
 * <h2>3. State: what is set up, and how one sequence is stopped from poisoning the next</h2>
 *
 * <ul>
 *   <li><b>One database per sequence</b>, named by the caller and never reused. Everything the sequence
 *       creates — tables, views, MVs, indexes — lands inside it and dies with it.</li>
 *   <li><b>Teardown in a {@code finally}</b>, and teardown failure is itself a finding
 *       ({@link SequenceResult#residue}) rather than an exception, so one leaked database cannot abort
 *       a fuzzing run.</li>
 *   <li><b>The session's current database is cleared</b> after teardown. A {@code ConnectContext} left
 *       pointing at a dropped database resolves unqualified names in the next sequence against nothing,
 *       which shows up much later as a wave of phantom findings.</li>
 *   <li><b>The step is verified, not assumed</b> ({@link Step#verify}). In FE-only mode some ALTERs
 *       complete synchronously in the FE and some enqueue an {@code AlterJobV2} that no BE will ever
 *       run. Observing a half-applied schema would manufacture findings, so an unverified step ends the
 *       sequence with {@code applied=false} and produces no observations at all.</li>
 *   <li><b>Session variables are not touched.</b> That is M9's job; mixing the two would make a finding
 *       impossible to attribute.</li>
 * </ul>
 *
 * <h2>4. FE-only scope — what M8 cannot reach here, and is not faked</h2>
 *
 * <p>The plan's M8 row lists "ALTER / CREATE INDEX / CREATE MV / load / compaction". With
 * {@code UtFrameUtils.createMinStarRocksCluster()} there is no BE, so:
 *
 * <ul>
 *   <li><b>Compaction is unreachable.</b> There are no tablets holding rowsets, so there is nothing to
 *       compact and no way to trigger it. No stub is provided; a fake trigger would only prove that the
 *       fake works.</li>
 *   <li><b>Loading is unreachable.</b> {@code INSERT}, stream load and broker load all need a BE sink,
 *       so the "interleave DML between DDL" half of M8 degenerates to "analyze the DML", which the
 *       single-statement driver already does.</li>
 *   <li><b>Global-dictionary and low-cardinality state are unreachable</b> — they are built from BE
 *       column statistics over real data.</li>
 *   <li><b>Heavyweight schema changes are only partly reachable.</b> Anything that rewrites data
 *       (type changes, bitmap indexes, sort-key changes) parks an {@code AlterJobV2} in PENDING, which
 *       {@link Step#verify} detects and skips.</li>
 * </ul>
 *
 * <p>What <i>is</i> reachable, and is what this class covers: the FE's own bookkeeping — fast schema
 * evolution (add / drop / rename column), table rename, partition DDL, view and materialized-view
 * creation, and table property changes. That is metadata state, which is exactly where the schema
 * change half of StarRocks' incident history lives.
 *
 * <p>Decision Log, 2026-07-29 (after P0), quoted because it constrains the shape of this class:
 * "deparser 对 <b>40 个 DDL 语句类型完全未实现</b>（deparse 率 0%），故 M8 语句序列拼接必须把 DDL 当文本模板处理，
 * 只有 QueryStatement / InsertStmt 走 AST 变异." Every step below is therefore a string template; no DDL
 * AST is ever built or deparsed here.
 */
public final class StatementSequenceMutation {

    /** Stable id, following the {@code Mutation.name()} convention even though this is not one. */
    public static final String NAME = "M8-sequence";

    private static final String ADDED_COLUMN = "fz_added";
    private static final String RENAME_SUFFIX = "_fz";

    private StatementSequenceMutation() {
    }

    // ------------------------------------------------------------------ verdicts

    public enum Verdict {
        /** Probe analyzed the same way before and after. */
        OK,
        /** Dependent probe rejected with a declared error after the step. Expected. */
        EXPECTED_BREAK,
        /** Dependent probe still analyzes but resolves differently. Expected. */
        EXPECTED_DRIFT,
        /** Probe did not analyze cleanly on the baseline, so it carries no signal. */
        BASELINE_UNUSABLE,
        /** Analyzer threw an internal error after the step. BUG. */
        BUG_INTERNAL_ERROR,
        /** Probe that names nothing the step touched stopped analyzing. BUG. */
        BUG_UNRELATED_BREAK,
        /** Probe that names nothing the step touched resolves differently. BUG. */
        BUG_UNRELATED_DRIFT
    }

    public static boolean isBug(Verdict v) {
        return v == Verdict.BUG_INTERNAL_ERROR || v == Verdict.BUG_UNRELATED_BREAK
                || v == Verdict.BUG_UNRELATED_DRIFT;
    }

    // ---------------------------------------------------------------------- step

    /**
     * Checks that a step actually landed in the catalog. Returning false is not a defect — in FE-only
     * mode it usually means the step needs a BE — it only means the sequence must not be observed.
     */
    public interface StepVerifier {
        boolean applied(StarRocksAssert sr, String db);
    }

    /**
     * One spliced DDL statement, as text, plus the catalog objects it is allowed to affect.
     *
     * <p>{@code tables} and {@code columns} are the declaration the oracle is defined against: a probe
     * touching none of them must come out of the step unchanged. An empty {@code columns} set means
     * "the whole table", which is the conservative reading and the right one for renames and drops.
     */
    public static final class Step {
        public final String name;
        public final String sql;
        public final Set<String> tables;
        public final Set<String> columns;
        private final StepVerifier verifier;

        public Step(String name, String sql, Collection<String> tables, Collection<String> columns) {
            this(name, sql, tables, columns, null);
        }

        public Step(String name, String sql, Collection<String> tables, Collection<String> columns,
                    StepVerifier verifier) {
            this.name = name;
            this.sql = sql;
            this.tables = lower(tables);
            this.columns = lower(columns);
            this.verifier = verifier;
        }

        boolean verify(StarRocksAssert sr, String db) {
            return verifier == null || verifier.applied(sr, db);
        }

        @Override
        public String toString() {
            return name + ": " + sql;
        }
    }

    private static Set<String> lower(Collection<String> in) {
        Set<String> out = new LinkedHashSet<>();
        if (in != null) {
            for (String s : in) {
                if (s != null) {
                    out.add(s.toLowerCase(Locale.ROOT));
                }
            }
        }
        return Collections.unmodifiableSet(out);
    }

    // ----------------------------------------------------------- step templates

    /**
     * Text templates that apply to {@code table} as it currently stands. Returns an empty list when the
     * table is not an OLAP table or has no column worth touching — "not applicable" is a normal answer,
     * per the {@code Mutation} convention.
     */
    public static List<Step> candidateSteps(StarRocksAssert sr, String db, String table) {
        List<Step> out = new ArrayList<>();
        Table t = sr.getTable(db, table);
        if (!(t instanceof OlapTable)) {
            return out;
        }
        OlapTable olap = (OlapTable) t;
        String q = "`" + table + "`";

        String valueCol = null;
        String intValueCol = null;
        for (Column c : olap.getBaseSchema()) {
            if (c.isKey() || c.isAggregated()) {
                continue;
            }
            if (valueCol == null) {
                valueCol = c.getName();
            }
            if (intValueCol == null && c.getType() != null && c.getType().isInt()) {
                intValueCol = c.getName();
            }
        }

        out.add(new Step("M8-add-column",
                "ALTER TABLE " + q + " ADD COLUMN `" + ADDED_COLUMN + "` INT NULL",
                Collections.singletonList(table), Collections.singletonList(ADDED_COLUMN),
                (a, d) -> hasColumn(a, d, table, ADDED_COLUMN)));

        if (valueCol != null) {
            final String col = valueCol;
            out.add(new Step("M8-drop-column",
                    "ALTER TABLE " + q + " DROP COLUMN `" + col + "`",
                    Collections.singletonList(table), Collections.singletonList(col),
                    (a, d) -> !hasColumn(a, d, table, col)));
            out.add(new Step("M8-rename-column",
                    "ALTER TABLE " + q + " RENAME COLUMN `" + col + "` TO `" + col + RENAME_SUFFIX + "`",
                    Collections.singletonList(table), Arrays.asList(col, col + RENAME_SUFFIX),
                    (a, d) -> hasColumn(a, d, table, col + RENAME_SUFFIX)));
        }
        if (intValueCol != null) {
            final String col = intValueCol;
            // Needs a data rewrite, so in FE-only mode the verifier is expected to reject it. Kept so
            // the same catalogue works unchanged once the driver is pointed at a real cluster.
            out.add(new Step("M8-modify-column-type",
                    "ALTER TABLE " + q + " MODIFY COLUMN `" + col + "` BIGINT",
                    Collections.singletonList(table), Collections.singletonList(col),
                    (a, d) -> columnIsBigint(a, d, table, col)));
            out.add(new Step("M8-add-bitmap-index",
                    "ALTER TABLE " + q + " ADD INDEX `idx" + RENAME_SUFFIX + "` (`" + col + "`) USING BITMAP",
                    Collections.singletonList(table), Collections.singletonList(col),
                    (a, d) -> hasIndex(a, d, table, "idx" + RENAME_SUFFIX)));
        }

        out.add(new Step("M8-rename-table",
                "ALTER TABLE " + q + " RENAME `" + table + RENAME_SUFFIX + "`",
                Arrays.asList(table, table + RENAME_SUFFIX), Collections.emptyList(),
                (a, d) -> a.getTable(d, table + RENAME_SUFFIX) != null));

        // A brand new object: it names an existing table but is not allowed to change how any existing
        // statement resolves, which makes it the sharpest of the templates.
        out.add(new Step("M8-create-view",
                "CREATE VIEW `v" + RENAME_SUFFIX + "` AS SELECT * FROM " + q,
                Collections.singletonList("v" + RENAME_SUFFIX), Collections.emptyList(),
                (a, d) -> a.getTable(d, "v" + RENAME_SUFFIX) != null));
        out.add(new Step("M8-create-mv",
                "CREATE MATERIALIZED VIEW `mv" + RENAME_SUFFIX + "` REFRESH MANUAL AS SELECT * FROM " + q,
                Arrays.asList("mv" + RENAME_SUFFIX, table), Collections.emptyList(),
                (a, d) -> a.getTable(d, "mv" + RENAME_SUFFIX) != null));

        List<String> partCols = olap.getPartitionColumnNames();
        if (olap.getPartitionInfo() != null && olap.getPartitionInfo().isRangePartition()
                && partCols != null && partCols.size() == 1) {
            String bound = rangeBoundFor(olap, partCols.get(0));
            if (bound != null) {
                out.add(new Step("M8-add-partition",
                        "ALTER TABLE " + q + " ADD PARTITION `p" + RENAME_SUFFIX + "` VALUES LESS THAN (" + bound + ")",
                        Collections.singletonList(table), Collections.emptyList(),
                        (a, d) -> hasPartition(a, d, table, "p" + RENAME_SUFFIX)));
            }
        }
        return out;
    }

    /** Picks one applicable template, or null when none applies. */
    public static Step planStep(StarRocksAssert sr, String db, String table, Random rnd) {
        List<Step> steps = candidateSteps(sr, db, table);
        return steps.isEmpty() ? null : steps.get(rnd.nextInt(steps.size()));
    }

    private static String rangeBoundFor(OlapTable olap, String column) {
        for (Column c : olap.getBaseSchema()) {
            if (!c.getName().equalsIgnoreCase(column) || c.getType() == null) {
                continue;
            }
            if (c.getType().isDateType()) {
                return "'2099-12-31'";
            }
            if (c.getType().isIntegerType()) {
                return "'2147483647'";
            }
            return null;
        }
        return null;
    }

    private static boolean hasColumn(StarRocksAssert sr, String db, String table, String column) {
        Table t = sr.getTable(db, table);
        if (t == null) {
            return false;
        }
        for (Column c : t.getBaseSchema()) {
            if (c.getName().equalsIgnoreCase(column)) {
                return true;
            }
        }
        return false;
    }

    private static boolean columnIsBigint(StarRocksAssert sr, String db, String table, String column) {
        Table t = sr.getTable(db, table);
        if (t == null) {
            return false;
        }
        for (Column c : t.getBaseSchema()) {
            if (c.getName().equalsIgnoreCase(column)) {
                return c.getType() != null && c.getType().isBigint();
            }
        }
        return false;
    }

    private static boolean hasIndex(StarRocksAssert sr, String db, String table, String index) {
        Table t = sr.getTable(db, table);
        if (!(t instanceof OlapTable)) {
            return false;
        }
        OlapTable olap = (OlapTable) t;
        return olap.getIndexes() != null && olap.getIndexes().stream()
                .anyMatch(i -> i.getIndexName() != null && i.getIndexName().equalsIgnoreCase(index));
    }

    private static boolean hasPartition(StarRocksAssert sr, String db, String table, String partition) {
        Table t = sr.getTable(db, table);
        if (!(t instanceof OlapTable)) {
            return false;
        }
        return ((OlapTable) t).getPartition(partition) != null;
    }

    // ------------------------------------------------------------- observations

    /** One probe, evaluated once. */
    private static final class Eval {
        final boolean ok;
        final String deparsed;
        final String error;
        final boolean internal;

        private Eval(boolean ok, String deparsed, String error, boolean internal) {
            this.ok = ok;
            this.deparsed = deparsed;
            this.error = error;
            this.internal = internal;
        }
    }

    public static final class Observation {
        public final String probe;
        public final boolean dependent;
        public final Verdict verdict;
        public final String detail;

        Observation(String probe, boolean dependent, Verdict verdict, String detail) {
            this.probe = probe;
            this.dependent = dependent;
            this.verdict = verdict;
            this.detail = detail;
        }

        @Override
        public String toString() {
            return verdict + (dependent ? " [dependent] " : " [independent] ") + probe
                    + (detail == null ? "" : " -- " + detail);
        }
    }

    /** Outcome of one whole sequence, including the reasons it may have produced nothing. */
    public static final class SequenceResult {
        public final Step step;
        /** Setup DDL that the in-process catalog refused. A sequence with any of these is not observed. */
        public final List<String> setupFailures = new ArrayList<>();
        /** True when the step ran and its effect was verified in the catalog. */
        public boolean applied;
        /** Non-null when the step was rejected; a declared rejection is not a defect. */
        public String ddlError;
        /** The step's handler threw something that is not a declared error. BUG. */
        public boolean ddlInternalError;
        /** The step ran without error but left no trace in the catalog — typically a BE-bound job. */
        public boolean unverified;
        /** Teardown did not remove the database. BUG: the next sequence would inherit it. */
        public String residue;
        public final List<Observation> observations = new ArrayList<>();

        SequenceResult(Step step) {
            this.step = step;
        }

        public boolean hasBug() {
            return ddlInternalError || residue != null
                    || observations.stream().anyMatch(o -> isBug(o.verdict));
        }

        public List<Observation> bugs() {
            List<Observation> out = new ArrayList<>();
            for (Observation o : observations) {
                if (isBug(o.verdict)) {
                    out.add(o);
                }
            }
            return out;
        }

        public String render() {
            StringBuilder sb = new StringBuilder(NAME).append(" / ").append(step);
            sb.append("\n  applied=").append(applied);
            if (!setupFailures.isEmpty()) {
                sb.append(" setupFailures=").append(setupFailures);
            }
            if (ddlError != null) {
                sb.append(" ddlError=").append(ddlError).append(" internal=").append(ddlInternalError);
            }
            if (unverified) {
                sb.append(" (step left no trace in the catalog)");
            }
            if (residue != null) {
                sb.append(" residue=").append(residue);
            }
            for (Observation o : observations) {
                sb.append("\n  ").append(o);
            }
            return sb.toString();
        }
    }

    // -------------------------------------------------------------------- driver

    /**
     * Runs one sequence end to end: build {@code db} from {@code schemaDdl}, baseline {@code probes},
     * splice {@code step} in, re-evaluate the same probes, classify, then tear the database down.
     *
     * <p>The database is created fresh and dropped in a {@code finally}; the caller only has to pass a
     * name it does not use elsewhere. Nothing is thrown for a failed step or a failed teardown — both
     * are recorded on the result, because a fuzzer that aborts on the first oddity stops fuzzing.
     */
    public static SequenceResult runSequence(ConnectContext ctx, StarRocksAssert sr, String db,
                                             List<String> schemaDdl, List<String> probes, Step step) {
        SequenceResult result = new SequenceResult(step);
        try {
            sr.withDatabase(db).useDatabase(db);
        } catch (Throwable t) {
            result.setupFailures.add("create database: " + oneLine(t));
            return result;
        }
        try {
            for (String ddl : schemaDdl) {
                StatementBase ast;
                try {
                    ast = SqlParser.parse(ddl, ctx.getSessionVariable()).get(0);
                } catch (Throwable t) {
                    result.setupFailures.add(ddl + " -> " + oneLine(t));
                    continue;
                }
                if (!CorpusReader.applySchemaSetup(sr, ddl, ast)) {
                    result.setupFailures.add(ddl);
                }
            }
            if (!result.setupFailures.isEmpty()) {
                return result;
            }

            List<Eval> before = new ArrayList<>();
            List<Deps> deps = new ArrayList<>();
            for (String probe : probes) {
                before.add(evaluate(ctx, probe));
                deps.add(dependenciesOf(ctx, probe));
            }

            try {
                sr.ddl(step.sql);
            } catch (Throwable t) {
                result.ddlError = oneLine(t);
                result.ddlInternalError = isInternal(t);
                return result;
            }
            if (!step.verify(sr, db)) {
                result.unverified = true;
                return result;
            }
            result.applied = true;

            for (int i = 0; i < probes.size(); i++) {
                String probe = probes.get(i);
                Eval b = before.get(i);
                if (!b.ok) {
                    result.observations.add(new Observation(probe, false, Verdict.BASELINE_UNUSABLE, b.error));
                    continue;
                }
                Eval a = evaluate(ctx, probe);
                boolean dependent = deps.get(i).touchedBy(step);
                result.observations.add(classify(probe, dependent, b, a));
            }
            return result;
        } finally {
            teardown(sr, db, result);
        }
    }

    private static Observation classify(String probe, boolean dependent, Eval before, Eval after) {
        if (!after.ok && after.internal) {
            return new Observation(probe, dependent, Verdict.BUG_INTERNAL_ERROR, after.error);
        }
        if (!after.ok) {
            return new Observation(probe, dependent,
                    dependent ? Verdict.EXPECTED_BREAK : Verdict.BUG_UNRELATED_BREAK, after.error);
        }
        if (!Objects.equals(before.deparsed, after.deparsed)) {
            return new Observation(probe, dependent,
                    dependent ? Verdict.EXPECTED_DRIFT : Verdict.BUG_UNRELATED_DRIFT,
                    "before: " + before.deparsed + "  |  after: " + after.deparsed);
        }
        return new Observation(probe, dependent, Verdict.OK, null);
    }

    /**
     * Teardown is part of the oracle, not housekeeping. A database that survives its sequence changes
     * what the next sequence sees, and the resulting findings point at the wrong statement entirely.
     */
    private static void teardown(StarRocksAssert sr, String db, SequenceResult result) {
        try {
            sr.ddl("DROP DATABASE IF EXISTS `" + db + "` FORCE");
        } catch (Throwable t) {
            result.residue = "drop failed: " + oneLine(t);
        }
        if (result.residue == null && sr.databaseExist(db)) {
            result.residue = "database still visible after drop";
        }
        // A context still pointing at a dropped database resolves unqualified names against nothing.
        sr.withoutUseDatabase();
    }

    // ------------------------------------------------------------------ evaluate

    private static Eval evaluate(ConnectContext ctx, String sql) {
        StatementBase ast;
        try {
            ast = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        } catch (Throwable t) {
            return new Eval(false, null, "parse: " + oneLine(t), false);
        }
        try {
            Analyzer.analyze(ast, ctx);
        } catch (Throwable t) {
            return new Eval(false, null, oneLine(t), isInternal(t));
        }
        try {
            String s = AstToSQLBuilder.toSQL(ast);
            return new Eval(true, s == null ? "<null>" : s.replace('\n', ' '), null, false);
        } catch (Throwable t) {
            // Deparsing an analyzed tree must not throw; that is a defect of the same tier as an
            // analyzer crash, so it is surfaced as one rather than silently dropping the probe.
            return new Eval(false, null, "deparse: " + oneLine(t), true);
        }
    }

    /**
     * Same tier-C rule as the single-statement driver (plan §3, O2): a declared error is expected, a
     * Guava {@code Preconditions} guard is a declared error wearing the wrong exception type, and
     * anything else is an internal failure.
     */
    static boolean isInternal(Throwable t) {
        Throwable cause = t;
        while (cause != null) {
            if (cause instanceof SemanticException || cause instanceof AnalysisException
                    || cause instanceof StarRocksException || cause instanceof StorageAccessException) {
                return false;
            }
            cause = cause.getCause() == cause ? null : cause.getCause();
        }
        StackTraceElement[] st = t.getStackTrace();
        if (st.length > 0 && st[0].getClassName().startsWith("com.google.common.base.Preconditions")) {
            return false;
        }
        return true;
    }

    private static String oneLine(Throwable t) {
        String m = t.getMessage();
        if (m == null) {
            m = "";
        }
        m = m.replace('\n', ' ');
        if (m.length() > 300) {
            m = m.substring(0, 300) + "...";
        }
        return t.getClass().getSimpleName() + ": " + m;
    }

    // ---------------------------------------------------------------- dependency

    /** Catalog objects a probe resolves against, plus whether it selects a star. */
    static final class Deps {
        final Set<String> tables = new LinkedHashSet<>();
        final Set<String> columns = new LinkedHashSet<>();
        boolean star;

        boolean touchedBy(Step step) {
            boolean tableHit = step.tables.stream().anyMatch(tables::contains);
            if (!tableHit) {
                return false;
            }
            if (step.columns.isEmpty() || star) {
                return true;
            }
            return step.columns.stream().anyMatch(columns::contains);
        }
    }

    /**
     * Table and column names the probe resolves against.
     *
     * <p>Names come from the <b>analyzed</b> tree so that a view contributes its base tables and not
     * just its own name — a text-level check would call {@code SELECT * FROM v} independent of an
     * {@code ALTER TABLE} on the view's base table and report the resulting break as a defect. The star
     * flag comes from the <b>unanalyzed</b> parse, because analysis has already replaced the star with
     * the baseline column list, which is precisely the thing the step is about to change.
     */
    static Deps dependenciesOf(ConnectContext ctx, String sql) {
        Deps deps = new Deps();
        StatementBase raw;
        try {
            raw = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        } catch (Throwable t) {
            return deps;
        }
        new StarFinder(deps).visit(raw);
        StatementBase analyzed;
        try {
            analyzed = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
            Analyzer.analyze(analyzed, ctx);
        } catch (Throwable t) {
            // Fall back to the unanalyzed tree: less precise (views stay opaque), but the caller only
            // uses this for probes whose baseline analysis succeeded, so it is a belt-and-braces path.
            analyzed = raw;
        }
        new NameCollector(deps).visit(analyzed);
        return deps;
    }

    private static final class NameCollector extends AstTraverser<Void, Void> {
        private final Deps deps;

        NameCollector(Deps deps) {
            this.deps = deps;
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            if (node.getName() != null && node.getName().getTbl() != null) {
                deps.tables.add(node.getName().getTbl().toLowerCase(Locale.ROOT));
            }
            // A materialized view resolves as a plain table, so without this it would look independent
            // of any DDL on the table it is built from -- and the first ALTER that disturbed the MV
            // would be reported as an unrelated break. Its base tables are part of what it reads.
            if (node.getTable() instanceof MaterializedView) {
                try {
                    for (BaseTableInfo info : ((MaterializedView) node.getTable()).getBaseTableInfos()) {
                        if (info != null && info.getTableName() != null) {
                            deps.tables.add(info.getTableName().toLowerCase(Locale.ROOT));
                        }
                    }
                } catch (Throwable ignored) {
                    // A dangling MV cannot list its bases; the conservative fallback is its own name.
                }
            }
            return super.visitTable(node, context);
        }

        @Override
        public Void visitView(ViewRelation node, Void context) {
            if (node.getName() != null && node.getName().getTbl() != null) {
                deps.tables.add(node.getName().getTbl().toLowerCase(Locale.ROOT));
            }
            // Descends into the view body, which is what makes dependency transitive.
            return super.visitView(node, context);
        }

        @Override
        public Void visitSlot(SlotRef node, Void context) {
            String c = node.getColumnName();
            if (c != null) {
                deps.columns.add(c.toLowerCase(Locale.ROOT));
            }
            return super.visitSlot(node, context);
        }
    }

    /** Star detection on the unanalyzed tree; also collects the names visible there. */
    private static final class StarFinder extends AstTraverser<Void, Void> {
        private final Deps deps;

        StarFinder(Deps deps) {
            this.deps = deps;
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            if (node.getName() != null && node.getName().getTbl() != null) {
                deps.tables.add(node.getName().getTbl().toLowerCase(Locale.ROOT));
            }
            return super.visitTable(node, context);
        }

        @Override
        public Void visitSelect(SelectRelation node, Void context) {
            if (node.getSelectList() != null && node.getSelectList().getItems() != null) {
                node.getSelectList().getItems().forEach(item -> {
                    if (item.isStar()) {
                        deps.star = true;
                    } else if (item.getExpr() != null) {
                        // Pre-analysis getOutputExpression() is null, so super would not reach these.
                        visit(item.getExpr(), context);
                    }
                });
            }
            return super.visitSelect(node, context);
        }
    }
}
