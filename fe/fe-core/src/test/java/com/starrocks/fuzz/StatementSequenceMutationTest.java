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

import com.starrocks.catalog.MaterializedView;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Proof that M8 works: a sequence runs end to end against the in-process catalog, the differential
 * oracle fires on real state-dependent differences, and the classification separates "the schema change
 * did what it says" from "the schema change reached somewhere it should not have".
 *
 * <p>The two arms of the oracle are proved differently, on purpose:
 * <ul>
 *   <li>The <b>expected</b> arm is proved with genuine schema changes — a dropped column really does
 *       break a query that names it, a star select really does widen after ADD COLUMN. Both are correct
 *       FE behaviour and must not be reported.</li>
 *   <li>The <b>bug</b> arm is proved by fault injection at the one place a real defect would show up:
 *       a step whose declared touch set does not cover what it actually changed. That is the literal
 *       shape of the class of bug M8 exists to catch — "the DDL reached beyond the objects it names" —
 *       and injecting it there tests the classifier without pretending to have found a live defect.</li>
 * </ul>
 */
public class StatementSequenceMutationTest {

    private static ConnectContext ctx;
    private static StarRocksAssert sr;

    private static final List<String> SCHEMA = Arrays.asList(
            "CREATE TABLE a (k int, v int) DUPLICATE KEY(k)"
                    + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')",
            "CREATE TABLE b (k int, w int) DUPLICATE KEY(k)"
                    + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");

    private static final String PROBE_A = "select k, v from a";
    private static final String PROBE_B = "select k, w from b";
    private static final String PROBE_STAR_A = "select * from a";

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        sr = new StarRocksAssert(ctx);
    }

    private static StatementSequenceMutation.Observation observationFor(
            StatementSequenceMutation.SequenceResult r, String probe) {
        for (StatementSequenceMutation.Observation o : r.observations) {
            if (o.probe.equals(probe)) {
                return o;
            }
        }
        return Assertions.fail("no observation for " + probe + "\n" + r.render());
    }

    // ------------------------------------------------------------ end to end

    @Test
    public void testSequenceRunsEndToEnd() {
        StatementSequenceMutation.Step step = new StatementSequenceMutation.Step(
                "M8-add-column", "ALTER TABLE `a` ADD COLUMN `fz_added` INT NULL",
                Collections.singletonList("a"), Collections.singletonList("fz_added"));

        StatementSequenceMutation.SequenceResult r = StatementSequenceMutation.runSequence(
                ctx, sr, "srfuzz_seq_e2e", SCHEMA, Arrays.asList(PROBE_A, PROBE_B), step);

        Assertions.assertTrue(r.setupFailures.isEmpty(), () -> r.render());
        Assertions.assertNull(r.ddlError, () -> r.render());
        Assertions.assertTrue(r.applied, () -> "ADD COLUMN did not land in the catalog\n" + r.render());
        Assertions.assertEquals(2, r.observations.size(), () -> r.render());
        // Adding a column must not move a query that names its columns, on either table.
        for (StatementSequenceMutation.Observation o : r.observations) {
            Assertions.assertEquals(StatementSequenceMutation.Verdict.OK, o.verdict, () -> r.render());
        }
        Assertions.assertFalse(r.hasBug(), () -> r.render());
        Assertions.assertNull(r.residue, () -> r.render());
    }

    @Test
    public void testCandidateStepsAreBuiltFromTextTemplates() throws Exception {
        sr.withDatabase("srfuzz_seq_tpl").useDatabase("srfuzz_seq_tpl");
        try {
            sr.withTable(SCHEMA.get(0));
            List<StatementSequenceMutation.Step> steps =
                    StatementSequenceMutation.candidateSteps(sr, "srfuzz_seq_tpl", "a");
            Assertions.assertFalse(steps.isEmpty());
            List<String> names = new ArrayList<>();
            for (StatementSequenceMutation.Step s : steps) {
                names.add(s.name);
                Assertions.assertNotNull(s.sql);
                Assertions.assertFalse(s.tables.isEmpty(), s.name);
            }
            Assertions.assertTrue(names.contains("M8-add-column"), names::toString);
            Assertions.assertTrue(names.contains("M8-drop-column"), names::toString);
            Assertions.assertTrue(names.contains("M8-create-mv"), names::toString);
            Assertions.assertNotNull(
                    StatementSequenceMutation.planStep(sr, "srfuzz_seq_tpl", "a", new Random(1)));
            // Not applicable is a normal answer, per the Mutation convention.
            Assertions.assertTrue(
                    StatementSequenceMutation.candidateSteps(sr, "srfuzz_seq_tpl", "no_such_table").isEmpty());
        } finally {
            sr.ddl("DROP DATABASE IF EXISTS `srfuzz_seq_tpl` FORCE");
            sr.withoutUseDatabase();
        }
    }

    // -------------------------------------------- the "expected" arm of the oracle

    /**
     * A dropped column really does break a query that names it. This is the FE's contract, not a
     * defect: the alternative — the query keeps working against a column that no longer exists — is
     * the actual bug. Reporting it would also bury every genuine finding, since it happens on every
     * DROP COLUMN step the fuzzer ever schedules.
     */
    @Test
    public void testDropColumnBreaksDependentProbeAsExpected() {
        StatementSequenceMutation.Step step = new StatementSequenceMutation.Step(
                "M8-drop-column", "ALTER TABLE `a` DROP COLUMN `v`",
                Collections.singletonList("a"), Collections.singletonList("v"));

        StatementSequenceMutation.SequenceResult r = StatementSequenceMutation.runSequence(
                ctx, sr, "srfuzz_seq_drop", SCHEMA, Arrays.asList(PROBE_A, PROBE_B), step);

        Assertions.assertTrue(r.applied, () -> r.render());
        StatementSequenceMutation.Observation onA = observationFor(r, PROBE_A);
        Assertions.assertTrue(onA.dependent, () -> r.render());
        Assertions.assertEquals(StatementSequenceMutation.Verdict.EXPECTED_BREAK, onA.verdict, () -> r.render());
        // The break must be a declared rejection. An internal error here would be the real finding.
        Assertions.assertFalse(StatementSequenceMutation.isBug(onA.verdict), () -> r.render());

        // The untouched table is unaffected, which is what makes the independent arm meaningful.
        Assertions.assertEquals(StatementSequenceMutation.Verdict.OK, observationFor(r, PROBE_B).verdict,
                () -> r.render());
        Assertions.assertFalse(r.hasBug(), () -> r.render());
    }

    /**
     * {@code SELECT *} resolves to a different column list after ADD COLUMN. That is the schema change,
     * observed — expected drift, not a defect. It is also why the star flag is read off the unanalyzed
     * parse: analysis has already replaced the star with the baseline column list.
     */
    @Test
    public void testStarProbeDriftsAfterAddColumnAsExpected() {
        StatementSequenceMutation.Step step = new StatementSequenceMutation.Step(
                "M8-add-column", "ALTER TABLE `a` ADD COLUMN `fz_added` INT NULL",
                Collections.singletonList("a"), Collections.singletonList("fz_added"));

        StatementSequenceMutation.SequenceResult r = StatementSequenceMutation.runSequence(
                ctx, sr, "srfuzz_seq_star", SCHEMA, Arrays.asList(PROBE_STAR_A, PROBE_A, PROBE_B), step);

        Assertions.assertTrue(r.applied, () -> r.render());
        StatementSequenceMutation.Observation star = observationFor(r, PROBE_STAR_A);
        Assertions.assertTrue(star.dependent, () -> "star probe must depend on every column\n" + r.render());
        Assertions.assertEquals(StatementSequenceMutation.Verdict.EXPECTED_DRIFT, star.verdict, () -> r.render());
        Assertions.assertTrue(star.detail != null && star.detail.contains("fz_added"),
                () -> "drift detail should show the new column\n" + r.render());

        // Named-column probes on the same table do not move at all.
        Assertions.assertEquals(StatementSequenceMutation.Verdict.OK, observationFor(r, PROBE_A).verdict,
                () -> r.render());
        Assertions.assertFalse(r.hasBug(), () -> r.render());
    }

    /**
     * Dependency is transitive through a view because it is computed on the analyzed tree. A text-level
     * check would call {@code select * from vv} independent of {@code ALTER TABLE a} and report the
     * break as a defect — a false positive on every view in the corpus.
     */
    @Test
    public void testViewDependencyIsTransitive() {
        List<String> schema = new ArrayList<>(SCHEMA);
        schema.add("CREATE VIEW vv AS SELECT k, v FROM a");
        StatementSequenceMutation.Step step = new StatementSequenceMutation.Step(
                "M8-drop-column", "ALTER TABLE `a` DROP COLUMN `v`",
                Collections.singletonList("a"), Collections.singletonList("v"));

        StatementSequenceMutation.SequenceResult r = StatementSequenceMutation.runSequence(
                ctx, sr, "srfuzz_seq_view", schema, Arrays.asList("select * from vv", PROBE_B), step);

        Assertions.assertTrue(r.applied, () -> r.render());
        StatementSequenceMutation.Observation onView = observationFor(r, "select * from vv");
        Assertions.assertTrue(onView.dependent,
                () -> "the view's base table must make it dependent\n" + r.render());
        Assertions.assertEquals(StatementSequenceMutation.Verdict.EXPECTED_BREAK, onView.verdict, () -> r.render());
        Assertions.assertFalse(r.hasBug(), () -> r.render());
    }

    /**
     * A materialized view resolves as an ordinary table, so nothing in the AST connects it to the table
     * it is built from. Without the explicit base-table expansion this probe came back
     * {@code [independent]} of an ALTER on its own base table — one FE change away from a false
     * {@code BUG_UNRELATED_BREAK} on every MV in the corpus.
     */
    @Test
    public void testMaterializedViewDependencyIsResolved() {
        List<String> schema = new ArrayList<>(SCHEMA);
        schema.add("CREATE MATERIALIZED VIEW mvx REFRESH MANUAL AS SELECT k, v FROM a");
        StatementSequenceMutation.Step step = new StatementSequenceMutation.Step(
                "M8-drop-column", "ALTER TABLE `a` DROP COLUMN `v`",
                Collections.singletonList("a"), Collections.singletonList("v"));

        StatementSequenceMutation.SequenceResult r = StatementSequenceMutation.runSequence(
                ctx, sr, "srfuzz_seq_mvx", schema, Arrays.asList("select * from mvx", PROBE_B), step);

        Assertions.assertTrue(r.applied, () -> r.render());
        StatementSequenceMutation.Observation onMv = observationFor(r, "select * from mvx");
        Assertions.assertTrue(onMv.dependent,
                () -> "an MV must depend on its base tables\n" + r.render());
        // The MV holds its own columns, so it keeps analyzing; only the classification had to change.
        Assertions.assertEquals(StatementSequenceMutation.Verdict.OK, onMv.verdict, () -> r.render());
        Assertions.assertFalse(r.hasBug(), () -> r.render());
    }

    /**
     * The metadata bookkeeping M8 exists to stress, pinned directly: dropping a column an MV reads must
     * deactivate the MV with a reason naming the column. Silently leaving it active is how a stale MV
     * ends up answering queries from data that no longer has a source.
     */
    @Test
    public void testDropColumnDeactivatesDependentMaterializedView() throws Exception {
        String db = "srfuzz_seq_mvstate";
        sr.withDatabase(db).useDatabase(db);
        try {
            sr.withTable(SCHEMA.get(0));
            sr.withMaterializedView("CREATE MATERIALIZED VIEW mvy REFRESH MANUAL AS SELECT k, v FROM a");
            MaterializedView mv = (MaterializedView) sr.getTable(db, "mvy");
            Assertions.assertTrue(mv.isActive());

            sr.ddl("ALTER TABLE `a` DROP COLUMN `v`");

            Assertions.assertFalse(mv.isActive(), "MV stayed active after its column was dropped");
            Assertions.assertTrue(mv.getInactiveReason() != null && mv.getInactiveReason().contains("v"),
                    () -> "unhelpful inactive reason: " + mv.getInactiveReason());
        } finally {
            sr.ddl("DROP DATABASE IF EXISTS `" + db + "` FORCE");
            sr.withoutUseDatabase();
        }
    }

    // ------------------------------------------------- the "bug" arm of the oracle

    /**
     * Fault injection at the declared-touch-set boundary: the step says it touches {@code a}, but its
     * text alters {@code b}. The probe over {@code b} analyzes before and fails after, and since the
     * step declared nothing about {@code b} the oracle must call it a defect. This is exactly how a
     * real "the ALTER reached further than it should" bug presents itself to this driver.
     */
    @Test
    public void testUnrelatedBreakIsClassifiedAsBug() {
        StatementSequenceMutation.Step mislabeled = new StatementSequenceMutation.Step(
                "M8-drop-column(mislabeled)", "ALTER TABLE `b` DROP COLUMN `w`",
                Collections.singletonList("a"), Collections.singletonList("v"));

        StatementSequenceMutation.SequenceResult r = StatementSequenceMutation.runSequence(
                ctx, sr, "srfuzz_seq_unrelated_break", SCHEMA, Arrays.asList(PROBE_A, PROBE_B), mislabeled);

        Assertions.assertNull(r.ddlError, () -> r.render());
        StatementSequenceMutation.Observation onB = observationFor(r, PROBE_B);
        Assertions.assertFalse(onB.dependent, () -> r.render());
        Assertions.assertEquals(StatementSequenceMutation.Verdict.BUG_UNRELATED_BREAK, onB.verdict, () -> r.render());
        Assertions.assertTrue(r.hasBug(), () -> r.render());
        Assertions.assertEquals(1, r.bugs().size(), () -> r.render());
        // The probe on the declared table is untouched, so the report points at exactly one statement.
        Assertions.assertEquals(StatementSequenceMutation.Verdict.OK, observationFor(r, PROBE_A).verdict,
                () -> r.render());
    }

    /** Same injection, drift instead of break: an undeclared table's resolved form moved. */
    @Test
    public void testUnrelatedDriftIsClassifiedAsBug() {
        StatementSequenceMutation.Step mislabeled = new StatementSequenceMutation.Step(
                "M8-add-column(mislabeled)", "ALTER TABLE `b` ADD COLUMN `fz_added` INT NULL",
                Collections.singletonList("a"), Collections.singletonList("fz_added"));

        StatementSequenceMutation.SequenceResult r = StatementSequenceMutation.runSequence(
                ctx, sr, "srfuzz_seq_unrelated_drift", SCHEMA,
                Arrays.asList("select * from b", PROBE_A), mislabeled);

        StatementSequenceMutation.Observation onB = observationFor(r, "select * from b");
        Assertions.assertFalse(onB.dependent, () -> r.render());
        Assertions.assertEquals(StatementSequenceMutation.Verdict.BUG_UNRELATED_DRIFT, onB.verdict, () -> r.render());
        Assertions.assertTrue(r.hasBug(), () -> r.render());
    }

    /** A DDL the FE declines is not a finding; only an internal failure of the handler is. */
    @Test
    public void testDeclaredDdlRejectionIsNotABug() {
        StatementSequenceMutation.Step bad = new StatementSequenceMutation.Step(
                "M8-drop-column", "ALTER TABLE `no_such_table` DROP COLUMN `v`",
                Collections.singletonList("no_such_table"), Collections.singletonList("v"));

        StatementSequenceMutation.SequenceResult r = StatementSequenceMutation.runSequence(
                ctx, sr, "srfuzz_seq_baddl", SCHEMA, Arrays.asList(PROBE_A, PROBE_B), bad);

        Assertions.assertFalse(r.applied, () -> r.render());
        Assertions.assertNotNull(r.ddlError, () -> r.render());
        Assertions.assertFalse(r.ddlInternalError, () -> r.render());
        Assertions.assertFalse(r.hasBug(), () -> r.render());
        Assertions.assertTrue(r.observations.isEmpty(), () -> r.render());
    }

    // ------------------------------------------------------------------ teardown

    /**
     * One sequence must not be able to reach the next. The same database name is reused with an
     * incompatible schema: if anything survived the first teardown, the second sequence's baseline
     * would resolve against the old table and every verdict after it would be meaningless.
     */
    @Test
    public void testTeardownLeavesNoResidue() {
        String db = "srfuzz_seq_residue";
        StatementSequenceMutation.Step step = new StatementSequenceMutation.Step(
                "M8-add-column", "ALTER TABLE `a` ADD COLUMN `fz_added` INT NULL",
                Collections.singletonList("a"), Collections.singletonList("fz_added"));

        StatementSequenceMutation.SequenceResult first = StatementSequenceMutation.runSequence(
                ctx, sr, db, SCHEMA, Arrays.asList(PROBE_A, PROBE_B), step);
        Assertions.assertTrue(first.applied, () -> first.render());
        Assertions.assertNull(first.residue, () -> first.render());
        Assertions.assertFalse(sr.databaseExist(db));
        Assertions.assertTrue(ctx.getDatabase() == null || ctx.getDatabase().isEmpty(),
                () -> "current database left dangling: " + ctx.getDatabase());

        List<String> otherSchema = Collections.singletonList(
                "CREATE TABLE a (k int, other int) DUPLICATE KEY(k)"
                        + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        StatementSequenceMutation.SequenceResult second = StatementSequenceMutation.runSequence(
                ctx, sr, db, otherSchema, Collections.singletonList("select k, other from a"), step);

        Assertions.assertTrue(second.setupFailures.isEmpty(), () -> second.render());
        Assertions.assertTrue(second.applied, () -> second.render());
        // Resolving `other` proves the second sequence sees its own table, not the first one's.
        Assertions.assertEquals(StatementSequenceMutation.Verdict.OK,
                observationFor(second, "select k, other from a").verdict, () -> second.render());
        Assertions.assertNull(second.residue, () -> second.render());
        Assertions.assertFalse(sr.databaseExist(db));
    }

    /**
     * Honest scope report for FE-only mode: which of the templates actually land in the catalog with no
     * BE. A template that only enqueues an {@code AlterJobV2} must come back {@code unverified} rather
     * than being observed against a half-applied schema.
     */
    @Test
    public void testFeOnlyReachabilityOfEveryTemplate() throws Exception {
        sr.withDatabase("srfuzz_seq_probe").useDatabase("srfuzz_seq_probe");
        List<StatementSequenceMutation.Step> steps;
        try {
            sr.withTable(SCHEMA.get(0));
            steps = StatementSequenceMutation.candidateSteps(sr, "srfuzz_seq_probe", "a");
        } finally {
            sr.ddl("DROP DATABASE IF EXISTS `srfuzz_seq_probe` FORCE");
            sr.withoutUseDatabase();
        }

        List<String> applied = new ArrayList<>();
        List<String> skipped = new ArrayList<>();
        for (int i = 0; i < steps.size(); i++) {
            StatementSequenceMutation.Step step = steps.get(i);
            StatementSequenceMutation.SequenceResult r = StatementSequenceMutation.runSequence(
                    ctx, sr, "srfuzz_seq_reach_" + i, SCHEMA, Arrays.asList(PROBE_A, PROBE_B), step);
            if (r.applied) {
                applied.add(step.name);
            } else {
                skipped.add(step.name + (r.ddlError != null ? " (rejected: " + r.ddlError + ")"
                        : r.unverified ? " (no catalog trace: needs a BE)" : " (setup failed)"));
            }
            Assertions.assertNull(r.residue, () -> r.render());
            // An unverified step must not produce observations; a half-applied schema is not evidence.
            if (!r.applied) {
                Assertions.assertTrue(r.observations.isEmpty(), () -> r.render());
            }
            Assertions.assertFalse(r.ddlInternalError, () -> "DDL handler failed internally\n" + r.render());
        }
        System.err.println("M8 FE-only reachable steps: " + applied);
        System.err.println("M8 FE-only unreachable steps: " + skipped);
        Assertions.assertTrue(applied.contains("M8-add-column"), applied::toString);
        Assertions.assertTrue(applied.contains("M8-drop-column"), applied::toString);
        Assertions.assertTrue(applied.contains("M8-create-view"), applied::toString);
    }
}
