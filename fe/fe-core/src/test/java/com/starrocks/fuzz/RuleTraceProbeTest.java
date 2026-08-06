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

import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Proves the three claims {@link RuleTrace} rests on, so a feedback loop is not built on a signal
 * nobody checked.
 *
 * <ol>
 *   <li>The tracer records rules the mask signal cannot see at all.</li>
 *   <li>Different query shapes fire different rule sets -- without this a coverage key is a
 *       constant and steering on it is steering on noise.</li>
 *   <li>{@link RuleFiringTap} does NOT have that property, because it records rules considered
 *       rather than applied. This is the measurement that retires the "178/265 rules covered"
 *       number.</li>
 * </ol>
 *
 * Cheap on purpose: a two-table schema and six queries, no corpus, so it can run as a regression
 * test rather than only as a one-off probe.
 */
public class RuleTraceProbeTest {

    private static ConnectContext ctx;

    /** Six shapes chosen to hit disjoint parts of the RBO rule set. */
    private static final Map<String, String> QUERIES = new LinkedHashMap<>();

    static {
        QUERIES.put("scan", "select k1 from t0");
        QUERIES.put("filter", "select k1 from t0 where k2 > 10 and k1 < 5");
        QUERIES.put("agg", "select k1, sum(k2) from t0 group by k1 having sum(k2) > 3");
        QUERIES.put("join", "select t0.k1 from t0 join t1 on t0.k1 = t1.k1 where t1.k2 > 7");
        QUERIES.put("sortlimit", "select k1 from t0 order by k2 desc limit 10");
        QUERIES.put("union", "select k1 from t0 union all select k1 from t1");
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        StarRocksAssert srAssert = new StarRocksAssert(ctx);
        srAssert.withDatabase("ruletrace_db").useDatabase("ruletrace_db");
        srAssert.withTable("create table t0 (k1 int, k2 int, k3 varchar(64)) "
                + "duplicate key(k1) distributed by hash(k1) buckets 3 "
                + "properties('replication_num'='1')");
        srAssert.withTable("create table t1 (k1 int, k2 int, k3 varchar(64)) "
                + "duplicate key(k1) distributed by hash(k1) buckets 3 "
                + "properties('replication_num'='1')");
    }

    /**
     * Plans one query with the tracer armed and returns what the RBO phase applied.
     *
     * Planning goes through {@code StatementPlanner} rather than a hand-rolled transform+optimize:
     * the point of the tracer is that it reads off the SAME plan the fuzzer already builds, so a
     * probe that optimizes differently would not be measuring the production path.
     */
    private static BitSet planAndTrace(String sql) {
        StatementBase stmt = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        ctx.setThreadLocalInfo();
        Analyzer.analyze(stmt, ctx);
        RuleTrace.arm();
        Assertions.assertTrue(RuleTrace.isArmed(), "tracer did not arm; every reading would be zero");
        List<PlanFragment> ignored = StatementPlanner.plan(stmt, ctx).getFragments();
        Assertions.assertFalse(ignored.isEmpty(), "planning produced no fragments for: " + sql);
        return RuleTrace.fired();
    }

    private static BitSet masksFor(String sql) {
        StatementBase stmt = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        ctx.setThreadLocalInfo();
        Analyzer.analyze(stmt, ctx);
        return RuleCoverage.collect(ctx, (QueryStatement) stmt);
    }

    private static String names(BitSet bs, int limit) {
        RuleType[] rules = RuleType.values();
        StringBuilder sb = new StringBuilder();
        int shown = 0;
        for (int i = bs.nextSetBit(0); i >= 0 && shown < limit; i = bs.nextSetBit(i + 1)) {
            sb.append(shown++ == 0 ? "" : ", ").append(rules[i].name());
        }
        return sb.toString();
    }

    @Test
    public void tracerRecordsRulesTheMaskSignalCannotSee() {
        String sql = QUERIES.get("filter");
        BitSet traced = planAndTrace(sql);
        BitSet masked = masksFor(sql);

        System.out.printf("trace: %d rules -> %s%n", traced.cardinality(), names(traced, 20));
        System.out.printf("masks: %d rules -> %s%n", masked.cardinality(), names(masked, 20));

        Assertions.assertTrue(traced.cardinality() > 0,
                "the tracer recorded nothing at all, which means it is not wired to the product's scopes");

        // Predicate pushdown runs on every query with a WHERE. It lives in logicalRuleRewrite, so the
        // mask signal is structurally unable to report it -- that is the whole point of this class.
        Assertions.assertTrue(traced.get(RuleType.TF_PUSH_DOWN_PREDICATE_SCAN.ordinal()),
                "predicate pushdown did not show up for a query with a WHERE clause");

        BitSet onlyTraced = (BitSet) traced.clone();
        onlyTraced.andNot(masked);
        Assertions.assertTrue(onlyTraced.cardinality() > 0,
                "the tracer added nothing over the masks, so it is not covering the RBO phase");
        System.out.printf("invisible to the masks: %d rules -> %s%n",
                onlyTraced.cardinality(), names(onlyTraced, 20));
    }

    /**
     * The property a feedback key must have: different inputs must produce different readings.
     *
     * A signal that returns the same set for a scan and for a three-way join cannot tell the fuzzer
     * that it reached somewhere new, no matter how many rules it names.
     */
    @Test
    public void differentShapesFireDifferentRules() {
        Map<String, BitSet> byShape = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : QUERIES.entrySet()) {
            byShape.put(e.getKey(), planAndTrace(e.getValue()));
        }

        BitSet union = new BitSet();
        List<Integer> sizes = new ArrayList<>();
        byShape.forEach((shape, bs) -> {
            System.out.printf("  %-10s %3d rules%n", shape, bs.cardinality());
            union.or(bs);
            sizes.add(bs.cardinality());
        });
        System.out.printf("union over %d shapes: %d rules%n", byShape.size(), union.cardinality());

        BitSet scan = byShape.get("scan");
        BitSet join = byShape.get("join");
        Assertions.assertNotEquals(scan, join,
                "a bare scan and a filtered join fired the same rules; the signal does not discriminate");

        // And the union must exceed the largest single query, or the queries are all reaching the
        // same place and the corpus dimension does not exist.
        int largest = sizes.stream().mapToInt(Integer::intValue).max().orElse(0);
        Assertions.assertTrue(union.cardinality() > largest,
                "no query contributed a rule another did not; there is nothing to steer towards");
    }

    /**
     * Why {@link RuleFiringTap} cannot be the key, measured rather than argued.
     *
     * The tap sets a bit for every rule in {@code RewriteTreeTask#applyRules}' argument list, but the
     * product applies a rule only after four guards. So its reading is the phase's static rule list:
     * near-identical for every query, and strictly larger than what actually ran.
     */
    @Test
    public void firingTapReportsRulesConsideredNotApplied() {
        RuleFiringTap.install();

        RuleFiringTap.reset();
        BitSet tracedScan = planAndTrace(QUERIES.get("scan"));
        BitSet tappedScan = RuleFiringTap.fired();

        RuleFiringTap.reset();
        BitSet tracedJoin = planAndTrace(QUERIES.get("join"));
        BitSet tappedJoin = RuleFiringTap.fired();

        System.out.printf("scan: tap %d vs trace %d%n", tappedScan.cardinality(), tracedScan.cardinality());
        System.out.printf("join: tap %d vs trace %d%n", tappedJoin.cardinality(), tracedJoin.cardinality());

        Assertions.assertTrue(tappedScan.cardinality() > tracedScan.cardinality(),
                "the tap did not overcount, so the premise of this test is wrong and RuleTrace's "
                        + "documentation needs revisiting");

        BitSet tapDelta = (BitSet) tappedJoin.clone();
        tapDelta.xor(tappedScan);
        BitSet traceDelta = (BitSet) tracedJoin.clone();
        traceDelta.xor(tracedScan);
        System.out.printf("scan-vs-join differing bits: tap %d, trace %d%n",
                tapDelta.cardinality(), traceDelta.cardinality());

        Assertions.assertTrue(traceDelta.cardinality() > tapDelta.cardinality(),
                "the tap discriminated between a scan and a join at least as well as the tracer, "
                        + "which contradicts the reason it was retired");
    }
}
