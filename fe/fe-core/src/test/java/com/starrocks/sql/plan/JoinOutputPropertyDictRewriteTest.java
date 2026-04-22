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

package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

// Reproduction for the same class of bug as PR #72003 (PruneShuffleColumnRule stale Join
// outputProperty), but for the dict-string column-id rewrite paths:
//
//   fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/tree/lowcardinality/DecodeRewriter.java
//     visitPhysicalDistribution -> exchange.setDistributionSpec(new HashDistributionSpec(...))
//   fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/tree/AddDecodeNodeForDictStringRule.java
//     rewriteDistribution -> exchangeOperator.setDistributionSpec(new HashDistributionSpec(...))
//
// Both run after extractBestPlan, which shares a single PhysicalPropertySet object between the
// child join's outputProperty and the parent join's requiredProperties[i]. The rewrite replaces
// the Exchange's distribution column IDs (string -> dict) by allocating a brand-new
// HashDistributionSpec on the Exchange, but does not rewrite the column IDs inside the shared
// PhysicalPropertySet. The parent join therefore reads stale string column IDs when
// PlanFragmentBuilder calls optExpr.getRequiredProperties().get(0).getDistributionProperty()
// to build probePartitionByExprs, so the pushed-down GRF carries partition_exprs that no longer
// match the shuffle keys emitted by the child Exchange.
public class JoinOutputPropertyDictRewriteTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("CREATE TABLE supplier_dict ( S_SUPPKEY     INTEGER NOT NULL,\n" +
                "                                  S_NAME        CHAR(25) NOT NULL,\n" +
                "                                  S_ADDRESS     VARCHAR(40) NOT NULL,\n" +
                "                                  S_NATIONKEY   INTEGER NOT NULL,\n" +
                "                                  S_PHONE       CHAR(15) NOT NULL,\n" +
                "                                  S_ACCTBAL     double NOT NULL,\n" +
                "                                  S_COMMENT     VARCHAR(101) NOT NULL,\n" +
                "                                  PAD char(1) NOT NULL)\n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`s_suppkey`)\n" +
                "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setSqlMode(2);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        connectContext.getSessionVariable().setCboCteReuse(false);
        connectContext.getSessionVariable().setEnableEliminateAgg(false);
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(false);
        connectContext.getSessionVariable().setEnableMultiColumnsOnGlobbalRuntimeFilter(true);
        connectContext.getSessionVariable().setGlobalRuntimeFilterBuildMaxSize(0);
        connectContext.getSessionVariable().setGlobalRuntimeFilterProbeMinSize(0);
    }

    @AfterAll
    public static void afterClass() {
        FeConstants.USE_MOCK_DICT_MANAGER = false;
        connectContext.getSessionVariable().setSqlMode(0);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
        connectContext.getSessionVariable().setEnableMultiColumnsOnGlobbalRuntimeFilter(false);
    }

    // Nested shuffle join on two dict-eligible string columns. Three supplier_dict scans produce
    // a plan shaped like:
    //
    //     HASH JOIN (outer, shuffle on S_ADDRESS, S_COMMENT)
    //     |--- EXCHANGE (build, HASH on r.S_ADDRESS, r.S_COMMENT)
    //     |--- EXCHANGE (probe, HASH on tt.S_ADDRESS, tt.S_COMMENT)
    //           \--- HASH JOIN (inner, shuffle on S_ADDRESS, S_COMMENT)
    //                |--- EXCHANGE (HASH on m.S_ADDRESS, m.S_COMMENT)
    //                \--- EXCHANGE (HASH on l.S_ADDRESS, l.S_COMMENT)
    //
    // After DecodeRewriter (or AddDecodeNodeForDictStringRule on V1) runs, each Exchange's
    // HashDistributionSpec is rebuilt to reference dict column IDs. But the PhysicalPropertySet
    // shared between the inner join's outputProperty and the outer join's requiredProperties[0]
    // is left untouched, so it still references the original string column IDs.
    //
    // PlanFragmentBuilder.visitPhysicalHashJoin reads optExpr.getRequiredProperties().get(0)
    // for that shared property and feeds its columns into getShuffleExprs, producing
    // probePartitionByExprs with the stale string IDs. The GRF ends up with
    // partition_exprs that disagree with the Exchange's dict shuffle keys, which breaks
    // partitioned GRF routing across the inner Exchange.
    //
    // When the fix is applied, the inner join's outputProperty is synced in the same pass,
    // and the outer join's partition_exprs match the Exchange shuffle keys.
    //
    // Three scans of supplier_dict allocate columns 1..8 (l), 9..16 (m), 17..24 (r).
    // l.S_ADDRESS = 3, l.S_COMMENT = 7.
    // m.S_ADDRESS = 11, m.S_COMMENT = 15.
    // r.S_ADDRESS = 19, r.S_COMMENT = 23.
    // Dict replacement columns are allocated above that range.
    private static final String NESTED_SHUFFLE_JOIN_SQL =
            "select tt.sa, tt.sc, r.S_SUPPKEY from (" +
                    "    select l.S_ADDRESS as sa, l.S_COMMENT as sc, l.S_SUPPKEY" +
                    "    from supplier_dict l" +
                    "    join[shuffle] supplier_dict m" +
                    "      on l.S_ADDRESS = m.S_ADDRESS and l.S_COMMENT = m.S_COMMENT" +
                    ") tt" +
                    "  join[shuffle] supplier_dict r" +
                    "    on tt.sa = r.S_ADDRESS and tt.sc = r.S_COMMENT";

    private static void assertNoStaleStringShuffleKeys(String plan) {
        // Sanity check: the inner join's Exchange shuffle keys have been rewritten to dict
        // column IDs. If the rewriter did not fire, the shuffle keys would still be the
        // original string columns (l.S_ADDRESS = 3, l.S_COMMENT = 7 or m.S_ADDRESS = 11,
        // m.S_COMMENT = 15), and the bug we want to demonstrate would not be reproducible
        // because there would be no dict-vs-string divergence.
        assertNotContains(plan, "partition exprs: [3: S_ADDRESS, VARCHAR, false], [7: S_COMMENT, VARCHAR, false]");
        assertNotContains(plan, "partition exprs: [11: S_ADDRESS, VARCHAR, false], [15: S_COMMENT, VARCHAR, false]");

        // Bug: the outer join's GRF partition_exprs must not reference the stale string column
        // IDs of the inner join's output (l's or m's S_ADDRESS / S_COMMENT). Before the fix,
        // because the inner join's outputProperty still holds the original string column IDs
        // from extractBestPlan, the outer join's probePartitionByExprs is built from those
        // stale IDs, and partition_exprs in the pushed-down GRF ends up as one of these:
        assertNotContains(plan, "partition_exprs = (3: S_ADDRESS,7: S_COMMENT)");
        assertNotContains(plan, "partition_exprs = (11: S_ADDRESS,15: S_COMMENT)");
    }

    // V2 path: LowCardinalityRewriteRule -> DecodeRewriter.visitPhysicalDistribution at lines
    // 536-537 rewrites the Exchange's HashDistributionSpec to dict IDs without syncing the
    // shared PhysicalPropertySet.
    @Test
    public void testNestedShuffleJoinGRFPartitionExprsConsistencyAfterDictRewriteV2() throws Exception {
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(true);
        try {
            String plan = getVerboseExplain(NESTED_SHUFFLE_JOIN_SQL);
            assertNoStaleStringShuffleKeys(plan);
        } finally {
            connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(true);
        }
    }

    // V1 path: AddDecodeNodeForDictStringRule.rewriteDistribution (called from
    // visitPhysicalDistribution, lines 631-649) rewrites the Exchange's HashDistributionSpec
    // to dict IDs without syncing the shared PhysicalPropertySet. V1 only runs when
    // useLowCardinalityOptimizeV2 is disabled.
    @Test
    public void testNestedShuffleJoinGRFPartitionExprsConsistencyAfterDictRewriteV1() throws Exception {
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(false);
        try {
            String plan = getVerboseExplain(NESTED_SHUFFLE_JOIN_SQL);
            assertNoStaleStringShuffleKeys(plan);
        } finally {
            connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(true);
        }
    }
}
