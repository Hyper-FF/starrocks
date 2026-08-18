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

import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * ArrayDistinctAfterAggRule rewrites array_agg into array_agg_distinct when every use of the
 * result is wrapped in array_distinct. array_agg_distinct has no JSON overload, but the rule
 * resolved it with IS_NONSTRICT_SUPERTYPE_OF, so a JSON argument matched the BOOLEAN overload
 * through an implicit cast and the aggregate's type changed from ARRAY&lt;JSON&gt; to
 * ARRAY&lt;BOOLEAN&gt;. The plan validator then rejected the whole plan with a type mismatch,
 * surfaced to the user as an internal "Invalid plan" error.
 *
 * Found by AstMutationFuzzerTest.
 */
public class ArrayAggDistinctJsonRewriteTest extends PlanTestBase {

    @BeforeAll
    public static void beforeAll() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE `ss` (\n" +
                "  `id` int(11) NULL COMMENT \"\",\n" +
                "  `name` varchar(255) NULL COMMENT \"\",\n" +
                "  `subject` varchar(255) NULL COMMENT \"\",\n" +
                "  `score` int(11) NULL COMMENT \"\",\n" +
                "  `arr` array<int>,\n" +
                "  `mmap` map<int,varchar(20)>\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 4\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");
    }

    /** The fuzzer's mutant: a JSON array_agg under a cross join used to fail plan validation. */
    @Test
    public void jsonArrayAggPlansUnderCrossJoin() throws Exception {
        String sql = "SELECT max(ss.id), array_agg(DISTINCT ss.name) FROM ss "
                + "CROSS JOIN (SELECT array_agg(json_object('2:3')) AS c FROM ss) sp "
                + "WHERE ss.id < 2 ORDER BY 1 ASC";
        String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);
        Assertions.assertFalse(plan.contains("array_agg_distinct"),
                "array_agg over a JSON argument must not be rewritten to array_agg_distinct: " + plan);
    }

    /** Same shape without any DISTINCT in the query at all — the rewrite is the optimizer's own. */
    @Test
    public void jsonArrayAggPlansWithoutAnyDistinct() throws Exception {
        String sql = "SELECT max(ss.id), array_agg(ss.name) FROM ss "
                + "CROSS JOIN (SELECT array_agg(json_object('2:3')) AS c FROM ss) sp "
                + "WHERE ss.id < 2";
        UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);
    }

    /** array_distinct(array_agg(json)) must still plan, just without the distinct rewrite. */
    @Test
    public void arrayDistinctOverJsonArrayAggStillPlans() throws Exception {
        String sql = "SELECT array_distinct(array_agg(json_object('2:3'))) FROM ss";
        String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);
        Assertions.assertFalse(plan.contains("array_agg_distinct"),
                "JSON must not reach array_agg_distinct: " + plan);
    }

    /**
     * Negative control: for a groupable type the rewrite is the whole point of the rule and must
     * still fire, otherwise the fix has simply disabled an optimization.
     */
    @Test
    public void groupableTypeStillRewritesToArrayAggDistinct() throws Exception {
        String sql = "SELECT array_distinct(array_agg(name)) FROM ss";
        String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);
        Assertions.assertTrue(plan.contains("array_agg_distinct"),
                "a VARCHAR array_agg should still be rewritten: " + plan);
    }

    @Test
    public void groupableIntStillRewritesToArrayAggDistinct() throws Exception {
        String sql = "SELECT array_distinct(array_agg(id)) FROM ss";
        String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);
        Assertions.assertTrue(plan.contains("array_agg_distinct"),
                "an INT array_agg should still be rewritten: " + plan);
    }
}
