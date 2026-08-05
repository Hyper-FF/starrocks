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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.MVTransformerContext;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.optimizer.transformer.TransformerContext;
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
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Which optimizer rules does a corpus never fire?
 *
 * The fuzzer is black-box: it mutates SQL and asks an oracle whether the answer looks wrong. Nothing
 * in that loop can tell you which of the optimizer's transformations a corpus actually exercises, so
 * "we ran 300k mutants" and "we covered the optimizer" get quietly conflated. They are not the same
 * claim, and the gap between them is where an unfired rule sits for months.
 *
 * No instrumentation is needed for this: the optimizer already records, per memo group expression,
 * which rules were applied to it ({@link GroupExpression#getAppliedRuleMasks()}), sized by
 * {@link RuleType}. OR those bitsets together across every group of every query and the result is a
 * per-corpus rule-coverage vector, at a cost of a few bitset reads per plan.
 *
 * Rule firing is a far better coverage signal here than line coverage would be. Lines tell you a
 * loop ran again; a fired rule tells you a specific transformation was attempted on a specific
 * shape -- which is the axis optimizer defects actually live on.
 *
 * Run with {@code -Dsrfuzz.rulecov=<file-or-dir>}.
 */
public class RuleCoverageProbeTest {

    private static ConnectContext ctx;
    private static StarRocksAssert srAssert;

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        srAssert = new StarRocksAssert(ctx);
    }

    @Test
    @EnabledIfSystemProperty(named = "srfuzz.rulecov", matches = ".+")
    public void ruleCoverage() throws Exception {
        List<Path> files = new ArrayList<>();
        Path root = Paths.get(System.getProperty("srfuzz.rulecov"));
        if (Files.isDirectory(root)) {
            try (Stream<Path> s = Files.walk(root)) {
                s.filter(Files::isRegularFile).filter(p -> p.toString().endsWith(".sql")).sorted()
                        .forEach(files::add);
            }
        } else {
            files.add(root);
        }

        // Optional synthetic statistics. Without them the optimizer has no reason to enumerate the
        // cost-driven transformations, which is most of the rule set -- measured at 16/263 fired.
        String statsSeed = System.getProperty("srfuzz.stats");
        if (statsSeed != null) {
            GlobalStateMgr.getCurrentState()
                    .setStatisticStorage(new FuzzStatisticStorage(Long.parseLong(statsSeed)));
            System.out.println("synthetic statistics ON (seed " + statsSeed + ")");
        } else {
            System.out.println("synthetic statistics OFF (default storage)");
        }

        BitSet firedAll = new BitSet(RuleType.NUM_RULES.ordinal() + 1);
        Map<String, BitSet> firedPerFile = new LinkedHashMap<>();
        int planned = 0;
        int failed = 0;
        int fileIdx = 0;

        for (Path f : files) {
            String db = "rulecov_db_" + (fileIdx++);
            try {
                srAssert.withDatabase(db).useDatabase(db);
            } catch (Throwable t) {
                continue;
            }
            BitSet firedHere = new BitSet(RuleType.NUM_RULES.ordinal() + 1);
            String text = new String(Files.readAllBytes(f), StandardCharsets.UTF_8);

            for (String sql : CorpusReader.extractStatements(text)) {
                StatementBase ast;
                try {
                    ast = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
                } catch (Throwable t) {
                    continue;
                }
                if (CorpusReader.isSchemaSetup(ast)) {
                    try {
                        srAssert.withTable(sql);
                    } catch (Throwable ignored) {
                        // A fixture whose DDL does not apply simply contributes no coverage.
                    }
                    continue;
                }
                if (!(ast instanceof QueryStatement)) {
                    continue;
                }
                try {
                    Analyzer.analyze(ast, ctx);
                    collect(firedHere, (QueryStatement) ast);
                    planned++;
                } catch (Throwable t) {
                    failed++;
                }
            }
            firedAll.or(firedHere);
            firedPerFile.put(f.getFileName().toString(), firedHere);
        }

        report(firedAll, firedPerFile, planned, failed);
    }

    /**
     * Runs the transform + optimize steps StatementPlanner runs, then reads the memo it leaves
     * behind. Going through StatementPlanner.plan() instead would be simpler but it does not hand
     * back the OptimizerContext, and the memo is the only place the applied-rule bitsets live.
     */
    private static void collect(BitSet into, QueryStatement query) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        TransformerContext tc =
                new TransformerContext(columnRefFactory, ctx, MVTransformerContext.of(ctx, true));
        // The transformer takes the Relation, not the Statement -- StatementPlanner unwraps with
        // getQueryRelation() before calling it.
        LogicalPlan logicalPlan =
                new RelationTransformer(tc).transformWithSelectLimit(query.getQueryRelation());

        OptimizerContext oc = OptimizerFactory.initContext(ctx, columnRefFactory);
        oc.setStatement(query);
        Optimizer optimizer = OptimizerFactory.create(oc);
        optimizer.optimize(logicalPlan.getRoot(), new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()));

        Memo memo = optimizer.getContext().getMemo();
        if (memo == null) {
            // Short-circuit and rule-less paths never build a memo; they contribute no rule coverage
            // rather than zero coverage, and conflating the two would understate the corpus.
            return;
        }
        for (Group g : memo.getGroups()) {
            for (GroupExpression ge : g.getLogicalExpressions()) {
                into.or(ge.getAppliedRuleMasks());
            }
            for (GroupExpression ge : g.getPhysicalExpressions()) {
                into.or(ge.getAppliedRuleMasks());
            }
        }
    }

    private static void report(BitSet all, Map<String, BitSet> perFile, int planned, int failed) {
        RuleType[] rules = RuleType.values();
        int total = RuleType.NUM_RULES.ordinal();
        int hit = 0;
        List<String> never = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            if (all.get(i)) {
                hit++;
            } else {
                never.add(rules[i].name());
            }
        }

        System.out.println("=== optimizer rule coverage ===");
        System.out.println(String.format("queries planned: %d  (failed: %d)", planned, failed));
        System.out.println(String.format("rules fired: %d / %d  (%.1f%%)", hit, total, 100.0 * hit / total));
        System.out.println();
        System.out.println("--- per corpus file ---");
        perFile.forEach((name, bs) ->
                System.out.println(String.format("  %-34s %3d rules", name, bs.cardinality())));
        System.out.println();
        System.out.println(String.format("--- NEVER fired (%d) ---", never.size()));
        never.forEach(n -> System.out.println("  " + n));
    }
}
