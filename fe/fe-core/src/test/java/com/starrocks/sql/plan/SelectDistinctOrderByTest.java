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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * SELECT DISTINCT sorts the distinct result, so an ORDER BY column the select list does not produce
 * has nothing to sort on. Without GROUP BY that is already rejected, by the rule that a column must
 * be aggregated or grouped. Adding GROUP BY satisfies that rule, and the DISTINCT rule only covered
 * ORDER BY expressions containing an aggregate, so this shape fell between the two: it was accepted
 * and then failed inside the optimizer while estimating statistics, with
 * "only found column statistics: {4: case}, but missing statistic of col: 1: v1".
 */
public class SelectDistinctOrderByTest extends PlanTestBase {

    private void assertRejected(String sql) {
        Throwable t = Assertions.assertThrows(Throwable.class, () -> getFragmentPlan(sql), sql);
        Assertions.assertTrue(
                String.valueOf(t.getMessage()).contains("ORDER BY expressions must appear in select list"),
                sql + " -> " + t.getMessage());
    }

    @Test
    public void testOrderByColumnTheDistinctDoesNotProduce() {
        // The select list emits case(v1), not v1, so ordering by v1 has nothing to read.
        assertRejected("select distinct case v1 when 1 then 1 else 0 end from t0 group by v1 order by v1");
        assertRejected("select distinct max(v2) from t0 group by v1 order by v1");
        assertRejected("select distinct case v1 when 1 then 1 else 0 end, max(v1), count(*) "
                + "from t0 group by v1 order by v1 limit 1");
    }

    @Test
    public void testOrderByStaysAllowedWhenTheSelectListProducesIt() throws Exception {
        // Listed outright, under an alias, as the input of a listed expression, or by ordinal.
        getFragmentPlan("select distinct v1 from t0 group by v1 order by v1");
        getFragmentPlan("select distinct v1 as a from t0 group by v1 order by a");
        getFragmentPlan("select distinct v1 from t0 group by v1 order by v1 + 1");
        getFragmentPlan("select distinct v1, case v1 when 1 then 1 else 0 end from t0 group by v1 order by v1");
        getFragmentPlan("select distinct v1 cc, v2 cc from t0 group by v1, v2 order by v1");
        getFragmentPlan("select distinct case v1 when 1 then 1 else 0 end from t0 group by v1 order by 1");
        // Without DISTINCT the group key is still available to sort on.
        getFragmentPlan("select case v1 when 1 then 1 else 0 end from t0 group by v1 order by v1");
    }
}
