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

public class FirstValueRewriteWindowTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE agg_src (" +
                "  k int, c_int int, c_bigint bigint, c_decimal128 decimal128(38,2)" +
                ") DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 " +
                "PROPERTIES('replication_num'='1')");
    }

    @Test
    public void testFirstValueRewriteOverPartition() throws Exception {
        String sql = "SELECT first_value_rewrite(c_decimal128, c_bigint) " +
                "OVER (PARTITION BY k ORDER BY c_int) FROM agg_src";
        String plan = getFragmentPlan(sql);
        System.out.println("=== PLAN OK ===");
        System.out.println(plan);
    }
}
