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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ArrayDistinctAfterAggTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @Test
    public void testArrayDistinctAfterAgg() throws Exception {
        String sql = "select array_distinct(array_agg(v2)) from t0 group by v1";
        String sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "array_agg_distinct");

        sql = "select array_length(array_distinct(array_agg(v2))) from t0 group by v1";
        sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "array_agg_distinct");
    }

    @Test
    public void testArrayDistinctAfterAggWithPredicate() throws Exception {
        String sql = "select array_distinct(array_agg(v2)) from t0 group by v1 having " +
                "array_length(array_distinct(array_agg(v2))) > 1";
        String sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "array_agg_distinct");

        sql = "select array_length(array_distinct(array_agg(v2))) from t0 group by v1 having " +
                "array_length(array_distinct(array_agg(v2))) > 1";
        sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "array_agg_distinct");

        sql = "select array_distinct(array_agg(v2)) from t0 group by v1 having array_length(array_agg(v2)) > 1";
        sqlPlan = getFragmentPlan(sql);
        assertNotContains(sqlPlan, "array_agg_distinct");
    }

    @Test
    public void testArrayAggDistinctKeepsElementType() throws Exception {
        // array_agg_distinct is registered as TIME -> ARRAY<DATETIME>, so rewriting array_agg(TIME)
        // into it gave the aggregation a type its output column ref does not have, and the plan was
        // rejected by the type checker. Both the multi-distinct rewrite and the
        // array_distinct(array_agg) rewrite must decline the rewrite in that case.
        String sql = "select array_agg(distinct cast(v3 as time)), count(distinct v1) from t0";
        String sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "array_agg_distinct(CAST(3: v3 AS TIME))");

        // ArrayDistinctAfterAggRule must not swap in array_agg_distinct here either.
        sql = "select array_distinct(array_agg(cast(v3 as time))) from t0 group by v1";
        sqlPlan = getFragmentPlan(sql);
        assertNotContains(sqlPlan, "array_agg_distinct");
    }
}
