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
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Proof that M7 does what it claims: fires on real seeds, injects a shape that is still visible after
 * a deparse, produces trees the grammar can express, and hands the analyzer type-hostile input that it
 * rejects by name rather than by falling over.
 *
 * <p>"Returns non-null" would prove none of that. An operator that silently builds text the parser
 * throws on returns null forever and looks healthy; one that builds text the deparser drops looks
 * healthy too, while the fuzzer quietly stops covering complex types. So every assertion here goes
 * through the same chain the driver uses -- deparse, reparse, analyze -- and checks the injected shape
 * on the far side of it.
 */
public class TypeStressMutationTest {

    private static final long SQL_MODE = SqlModeHelper.MODE_DEFAULT;
    private static final String DB = "fuzz_typestress";

    private static ConnectContext ctx;

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        StarRocksAssert a = new StarRocksAssert(ctx);
        a.withDatabase(DB).useDatabase(DB);
        // One table carrying every complex type the operator targets ...
        a.withTable("CREATE TABLE t ("
                + "  k INT,"
                + "  s VARCHAR(32),"
                + "  d DECIMAL(10, 2),"
                + "  dt DATETIME,"
                + "  c_arr ARRAY<INT>,"
                + "  c_map MAP<VARCHAR(16), INT>,"
                + "  c_struct STRUCT<a INT, b VARCHAR(8)>,"
                + "  c_json JSON"
                + ") DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1"
                + " PROPERTIES('replication_num'='1')");
        // ... and one that is entirely scalar, where every wrap is a type error by construction.
        a.withTable("CREATE TABLE p (k INT, v INT) DUPLICATE KEY(k)"
                + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
    }

    // --------------------------------------------------------------- plumbing

    /** One mutant: what the operator said it did, and what the tree looks like on the other side. */
    private static final class Mutant {
        String description;
        String deparsed;
        StatementBase reparsed;

        String shape() {
            return description.substring(0, description.indexOf(' '));
        }

        String label() {
            return description.substring(description.indexOf(" at ") + 4, description.indexOf(": "));
        }

        String before() {
            return description.substring(description.indexOf(": ") + 2, description.indexOf(" -> "));
        }

        String injected() {
            return description.substring(description.indexOf(" -> ") + 4, description.indexOf(" || marker: "));
        }

        String marker() {
            return description.substring(description.indexOf(" || marker: ") + " || marker: ".length());
        }
    }

    private static AstMutationFuzzerTest.Pool poolWith(String... columns) {
        AstMutationFuzzerTest.Pool pool = new AstMutationFuzzerTest.Pool();
        for (String c : columns) {
            pool.columnNames.add(c);
        }
        return pool;
    }

    /**
     * Applies the operator and pushes the result through the driver's grammar-reachability gate.
     *
     * <p>Mirrors {@code AstMutationFuzzerTest#reparseThroughGrammar}, including its tolerance: a mutant
     * that cannot be written down and read back is <b>dropped</b>, not reported. The driver counts those
     * as unreachable rather than as findings, and so does this helper -- {@code reparsed} stays null.
     * Asserting on them here would turn known deparser gaps into failures of the mutation operator.
     *
     * @return null when the operator did not fire; the mutant otherwise
     */
    private static Mutant mutate(String seed, TypeStressMutation.Shape shape, long rngSeed,
                                 AstMutationFuzzerTest.Pool pool) {
        StatementBase ast = SqlParser.parse(seed, ctx.getSessionVariable()).get(0);
        String description = new TypeStressMutation()
                .apply((QueryStatement) ast, pool, new Random(rngSeed), shape);
        if (description == null) {
            return null;
        }
        Mutant m = new Mutant();
        m.description = description;
        try {
            m.deparsed = AstToSQLBuilder.toSQL(ast);
        } catch (Throwable t) {
            m.deparsed = null;
        }
        if (m.deparsed == null || m.deparsed.trim().isEmpty()) {
            m.deparsed = null;
            return m;
        }
        try {
            List<StatementBase> again = SqlParser.parse(m.deparsed, ctx.getSessionVariable());
            m.reparsed = again.isEmpty() ? null : again.get(0);
        } catch (Throwable t) {
            m.reparsed = null;
        }
        return m;
    }

    /** True when the mutant survived the grammar gate and is worth evaluating. */
    private static boolean reachable(Mutant m) {
        return m != null && m.reparsed != null;
    }

    /**
     * Backquotes and line breaks are deparser choices, not part of the injected shape: a statement is
     * rendered with newlines before FROM while a standalone expression is rendered on one line, so the
     * same subtree reads differently in the two contexts.
     */
    private static String norm(String s) {
        return s.replace("`", "").replaceAll("\\s+", " ").trim();
    }

    private static void assertShapeSurvivesDeparse(Mutant m) {
        Assertions.assertTrue(norm(m.deparsed).contains(norm(m.marker())),
                () -> "injected shape " + m.marker() + " is not visible in the deparsed mutant\n"
                        + "  description: " + m.description + "\n  deparsed:    " + m.deparsed);
    }

    /**
     * The analyzer is allowed to say no -- that is the expected outcome for most of what M7 builds. It
     * is not allowed to fall over: an NPE or a ClassCastException from a well-formed tree is a defect,
     * and the same classification the fuzzer uses is applied here so the two agree.
     */
    private static Throwable analyzeExpectingNoInternalError(StatementBase stmt, String context) {
        try {
            Analyzer.analyze(stmt, ctx);
            return null;
        } catch (Throwable t) {
            Assertions.assertTrue(isCleanRejection(t),
                    () -> "analyzer raised an INTERNAL error on a well-formed mutant -- FE defect\n"
                            + "  " + context + "\n  " + t);
            return t;
        }
    }

    private static boolean isCleanRejection(Throwable t) {
        if (t instanceof SemanticException || t instanceof AnalysisException
                || t instanceof StarRocksException || t instanceof StorageAccessException
                || t instanceof ParsingException) {
            return true;
        }
        StackTraceElement[] st = t.getStackTrace();
        return st.length > 0 && st[0].getClassName().startsWith("com.google.common.base.Preconditions");
    }

    // ------------------------------------------------------------------ tests

    @Test
    public void testNameIsStable() {
        Assertions.assertEquals("M7-typestress", new TypeStressMutation().name());
    }

    /**
     * Every entry of every wrap table must be something the parser accepts.
     *
     * <p>A shape the parser rejects is not a weak mutation, it is a dead one: {@code apply} returns null
     * and the operator's firing rate silently drops. Array slices were exactly that -- {@code (e)[1:2]}
     * is in the grammar but {@code AstBuilder#visitArraySlice} throws "unsupported expr", so the whole
     * family was removed rather than left to fail forever.
     */
    @Test
    public void testEveryWrapTableEntryIsGrammatical() {
        String inner = "(`c_arr`)";
        for (String target : TypeStressMutation.CAST_TARGETS) {
            assertParses("CAST(" + inner + " AS " + target + ")");
        }
        for (String idx : TypeStressMutation.SUBSCRIPTS) {
            assertParses(inner + "[" + idx + "]");
        }
        for (String key : TypeStressMutation.MAP_KEYS) {
            assertParses(inner + "[" + key + "]");
        }
        for (String field : TypeStressMutation.SUBFIELD_NAMES) {
            assertParses(inner + "." + field);
        }
        for (String fn : TypeStressMutation.COLLECTION_FNS) {
            assertParses(String.format(fn, inner));
        }
        for (String json : TypeStressMutation.JSON_PATHS) {
            assertParses(String.format(json, inner));
        }
    }

    private static void assertParses(String exprText) {
        try {
            Assertions.assertNotNull(SqlParser.parseSqlToExpr(exprText, SQL_MODE), exprText);
        } catch (Throwable t) {
            Assertions.fail("wrap table entry does not parse: " + exprText + " -> " + t);
        }
    }

    /**
     * Every shape must be reachable through {@code apply} on a real seed, and each must survive the
     * deparse. This is the check that catches a shape whose text parses but whose node the deparser
     * cannot render -- the mutant would be dropped by the driver's gate and the family would go dark.
     */
    @Test
    public void testEveryShapeFiresAndSurvivesTheRoundTrip() {
        String seed = "select c_arr, c_map, c_struct, c_json from t where k > 1";
        AstMutationFuzzerTest.Pool pool = poolWith("k", "c_arr", "a");
        for (TypeStressMutation.Shape shape : TypeStressMutation.Shape.values()) {
            int fired = 0;
            int reached = 0;
            for (long rng = 0; rng < 40; rng++) {
                Mutant m = mutate(seed, shape, rng, pool);
                if (m == null) {
                    continue;
                }
                fired++;
                Assertions.assertEquals(shape.name(), m.shape(), m.description);
                if (!reachable(m)) {
                    continue;
                }
                reached++;
                assertShapeSurvivesDeparse(m);
                analyzeExpectingNoInternalError(m.reparsed, "shape " + shape + ", mutant " + m.deparsed);
            }
            final int firedCount = fired;
            final int reachedCount = reached;
            Assertions.assertEquals(40, firedCount,
                    () -> "shape " + shape + " did not fire on every attempt against " + seed);
            // Not 40: the gate legitimately eats some. STRUCT_FIELD over a numeric literal is the known
            // case -- `(1).a` deparses to "1.`a`", because AST2StringVisitor#visitSubfieldExpr renders
            // its child without parentheses, and "1." then lexes as the start of a decimal. The driver
            // drops those as unreachable; the bar here is that the family still mostly gets through.
            Assertions.assertTrue(reachedCount >= 24,
                    () -> "shape " + shape + " only produced " + reachedCount
                            + " grammar-reachable mutants out of 40 on " + seed);
        }
    }

    /** A table with an ARRAY column: subscripting it is the canonical M7 edit. */
    @Test
    public void testArrayColumnGetsSubscripted() {
        Set<String> injected = new LinkedHashSet<>();
        for (long rng = 0; rng < 60; rng++) {
            Mutant m = mutate("select c_arr from t", TypeStressMutation.Shape.ARRAY_SUBSCRIPT, rng,
                    poolWith("c_arr"));
            if (!reachable(m)) {
                continue;
            }
            Assertions.assertEquals("SelectListItem[0]", m.label(), m.description);
            assertShapeSurvivesDeparse(m);
            injected.add(m.injected());
            analyzeExpectingNoInternalError(m.reparsed, m.deparsed);
        }
        // Not one shape repeated: the index really does vary across the boundary set.
        Assertions.assertTrue(injected.size() >= 4, () -> "subscripts did not vary: " + injected);
        Assertions.assertTrue(injected.stream().anyMatch(s -> s.endsWith("[0]")),
                () -> "the hostile zero index was never produced: " + injected);
        // c_arr[1] is legal, so at least one of these must actually analyze.
        Assertions.assertTrue(
                mutate("select c_arr[1] from t", TypeStressMutation.Shape.COLLECTION_FN, 7L, poolWith())
                        != null,
                "operator did not fire on an already-subscripted array");
    }

    /** MAP and STRUCT columns: key lookup and field access, on a tree with no types populated. */
    @Test
    public void testMapKeyAndStructFieldOnComplexColumns() {
        boolean sawMapKey = false;
        for (long rng = 0; rng < 40; rng++) {
            Mutant m = mutate("select c_map from t", TypeStressMutation.Shape.MAP_KEY, rng, poolWith());
            if (!reachable(m)) {
                continue;
            }
            assertShapeSurvivesDeparse(m);
            analyzeExpectingNoInternalError(m.reparsed, m.deparsed);
            sawMapKey = true;
        }
        Assertions.assertTrue(sawMapKey, "MAP_KEY never fired on a MAP column");

        // `c_struct`.`a` exists, so this one must analyze cleanly rather than merely be rejected.
        boolean sawLegalStructAccess = false;
        for (long rng = 0; rng < 60; rng++) {
            Mutant m = mutate("select c_struct from t", TypeStressMutation.Shape.STRUCT_FIELD, rng,
                    poolWith("a", "b"));
            if (!reachable(m)) {
                continue;
            }
            assertShapeSurvivesDeparse(m);
            Throwable failure = analyzeExpectingNoInternalError(m.reparsed, m.deparsed);
            if (failure == null) {
                sawLegalStructAccess = true;
            }
        }
        Assertions.assertTrue(sawLegalStructAccess,
                "no struct field access ever analyzed; the pool's column names are not being used");
    }

    /**
     * A plain scalar query. Nothing here has a complex type, so every wrap is a type error -- which is
     * precisely the input the operator exists to produce, and it still has to be well-formed SQL.
     */
    @Test
    public void testPlainScalarQueryStillGetsTypeStress() {
        String seed = "select v from p where v > 1 group by v having count(*) > 2 order by v";
        Set<String> labels = new LinkedHashSet<>();
        for (TypeStressMutation.Shape shape : TypeStressMutation.Shape.values()) {
            for (long rng = 0; rng < 30; rng++) {
                Mutant m = mutate(seed, shape, rng, poolWith("v", "k"));
                if (!reachable(m)) {
                    continue;
                }
                labels.add(m.label());
                assertShapeSurvivesDeparse(m);
                analyzeExpectingNoInternalError(m.reparsed, m.deparsed);
            }
        }
        // The clause roots are what make a query like this reachable at all: `v` in the select list is a
        // root with no children, so without them the only sites would be inside the WHERE and HAVING.
        Assertions.assertTrue(labels.contains("SelectListItem[0]"), () -> "select item never chosen: " + labels);
        Assertions.assertTrue(labels.contains("WhereClause"), () -> "WHERE never chosen: " + labels);
        Assertions.assertTrue(labels.contains("HavingClause"), () -> "HAVING never chosen: " + labels);
    }

    /**
     * The operator's whole point: reach the analyzer with a tree that is well-formed but type-hostile.
     * {@code v.a} on an INT column has to come back as a declared rejection, not as an internal error,
     * and it has to deparse and reparse on the way there.
     */
    @Test
    public void testSubfieldAccessOnANonStructIsWellFormedAndCleanlyRejected() {
        Mutant hit = null;
        for (long rng = 0; rng < 60 && hit == null; rng++) {
            Mutant m = mutate("select v from p", TypeStressMutation.Shape.STRUCT_FIELD, rng, poolWith());
            if (reachable(m) && norm(m.deparsed).contains(".a")) {
                hit = m;
            }
        }
        Assertions.assertNotNull(hit, "STRUCT_FIELD never produced `.a` on the scalar table");
        final Mutant found = hit;

        // Well-formed: it deparsed, and mutate() already proved it reparses.
        Assertions.assertTrue(norm(found.deparsed).contains("v.a"), found.deparsed);
        Throwable failure = analyzeExpectingNoInternalError(found.reparsed, found.deparsed);
        Assertions.assertNotNull(failure,
                () -> "expected the analyzer to reject subfield access on an INT column: " + found.deparsed);
        Assertions.assertTrue(isCleanRejection(failure), () -> "not a declared rejection: " + failure);
    }

    /**
     * The cast chains M7 builds, written out by hand: legal syntax, illegal typing. Every one of them
     * has to come back as a declared rejection or a clean analysis, never as an internal error, and at
     * least one has to actually be refused -- otherwise the family is not stressing anything.
     */
    @Test
    public void testHostileCastChainsAreRejectedNotCrashed() {
        String[] hostile = {
                "select CAST(c_arr AS MAP<VARCHAR(16), INT>) from t",
                "select CAST(c_struct AS ARRAY<INT>) from t",
                "select CAST(c_map AS STRUCT<a STRUCT<b STRUCT<c INT>>>) from t",
                "select CAST(CAST(c_json AS ARRAY<MAP<VARCHAR(16), INT>>) AS DECIMAL(38, 38)) from t",
                "select CAST(CAST(s AS DECIMAL(38, 38)) AS ARRAY<STRUCT<a INT, b VARCHAR(8)>>) from t",
        };
        int rejected = 0;
        for (String sql : hostile) {
            StatementBase stmt = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
            if (analyzeExpectingNoInternalError(stmt, "hand-written hostile cast: " + sql) != null) {
                rejected++;
            }
        }
        Assertions.assertTrue(rejected > 0, "not one hostile cast chain was refused");
    }

    /** The description has to be enough to rebuild the mutant from the seed; check it actually is. */
    @Test
    public void testDescriptionIsAccurate() {
        String seed = "select array_length(c_arr) from t where k > 1";
        int checked = 0;
        for (TypeStressMutation.Shape shape : TypeStressMutation.Shape.values()) {
            for (long rng = 0; rng < 10; rng++) {
                Mutant m = mutate(seed, shape, rng, poolWith("k", "c_arr"));
                if (!reachable(m)) {
                    continue;
                }
                checked++;
                Assertions.assertEquals(shape.name(), m.shape(), m.description);
                Assertions.assertFalse(m.label().isEmpty(), m.description);
                // The replaced fragment must come from the seed, and the injected text must contain it:
                // this operator wraps, it never substitutes.
                Assertions.assertTrue(m.injected().contains(m.before()),
                        () -> "injected text does not contain the expression it wrapped: " + m.description);
                // The injected text is a standalone expression the parser accepts.
                assertParses(m.injected());
                // And the site it names really did change.
                Assertions.assertNotEquals(m.before(), m.injected(), m.description);
            }
        }
        final int checkedCount = checked;
        Assertions.assertTrue(checkedCount >= 30,
                () -> "too few mutants to judge the description: " + checkedCount);
    }

    /**
     * Positions {@code mutation-rules.xml} declares off limits must stay off limits.
     *
     * <p>{@code k + (select max(v) from p)} has a {@code Subquery} at child 1, which the
     * subquery-position-is-fixed rule blocks. Child 0 is a plain column and must still be chosen, so an
     * operator that simply never descends would not pass this.
     */
    @Test
    public void testMutationRulesAreHonoured() {
        String seed = "select k + (select max(v) from p) from t";
        Set<String> labels = new LinkedHashSet<>();
        for (TypeStressMutation.Shape shape : TypeStressMutation.Shape.values()) {
            for (long rng = 0; rng < 40; rng++) {
                Mutant m = mutate(seed, shape, rng, poolWith("k", "v"));
                if (m != null) {
                    labels.add(m.label());
                }
            }
        }
        Assertions.assertFalse(labels.contains("ArithmeticExpr[1]"),
                () -> "the mutator entered a blocked Subquery position: " + labels);
        Assertions.assertTrue(labels.contains("ArithmeticExpr[0]"),
                () -> "the unblocked sibling position was never chosen: " + labels);
    }

    /**
     * The broad sweep: many shapes over several seeds, every mutant taken all the way through the chain
     * the fuzzer uses. Nothing here may raise an internal error, and a healthy share must survive the
     * grammar gate -- an operator that mostly builds unreachable trees is not doing any work.
     */
    @Test
    public void testSweepProducesReachableMutantsAndNoInternalErrors() {
        String[] seeds = {
                "select c_arr, c_map, c_struct, c_json from t where k > 1",
                "select c_struct.a from t where c_map['x'] = 1",
                "select k, sum(d) from t group by k having sum(d) > 0",
                "select v from p where v in (select k from t)",
                "select t.k from t join p on t.k = p.k where p.v > 0",
                "with w as (select c_arr from t) select array_length(c_arr) from w",
                "select v from p order by v limit 3",
        };
        AstMutationFuzzerTest.Pool pool = poolWith("k", "v", "c_arr", "c_map", "c_struct", "a", "b");
        List<String> analyzed = new ArrayList<>();
        int applied = 0;
        int reached = 0;
        for (String seed : seeds) {
            for (long rng = 0; rng < 60; rng++) {
                Mutant m = mutate(seed, null, rng, pool);
                if (m == null) {
                    continue;
                }
                applied++;
                if (!reachable(m)) {
                    continue;
                }
                reached++;
                assertShapeSurvivesDeparse(m);
                if (analyzeExpectingNoInternalError(m.reparsed, "seed " + seed + "\n  mutant " + m.deparsed)
                        == null) {
                    analyzed.add(m.deparsed);
                }
            }
        }
        final int appliedCount = applied;
        final int reachedCount = reached;
        Assertions.assertTrue(appliedCount >= seeds.length * 50,
                () -> "operator fired on only " + appliedCount + " of " + (seeds.length * 60) + " attempts");
        // The grammar gate throws mutants away; it must not be throwing away most of them.
        Assertions.assertTrue(reachedCount * 10 >= appliedCount * 8,
                () -> "only " + reachedCount + " of " + appliedCount
                        + " mutants survived the grammar gate");
        // Type stress is supposed to be hostile, but an operator that NEVER analyzes is not stressing
        // the type system, it is only stressing the parser.
        Assertions.assertFalse(analyzed.isEmpty(), "not a single mutant survived analysis");
    }
}
