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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Plans that used to fail in the AST -> plan transformer with an internal error, either because a subquery
 * reached a translator that had been handed no plan builder, or because an outer-scope column reference reached
 * a translator that had been handed no correlation list.
 */
public class PlannerCorrelationBugTest extends PlanTestNoneDBBase {

    private static final String CORRELATION_NOT_SUPPORTED =
            "Only support use correlated columns in the where clause of subqueries";

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
        starRocksAssert.withDatabase("test_correlation_bug").useDatabase("test_correlation_bug");
        starRocksAssert.withTable("create table t1 (v1 int) duplicate key(v1) "
                + "distributed by hash(v1) buckets 1 properties(\"replication_num\"=\"1\")");
        starRocksAssert.withTable("create table tb1 (a int, b int) duplicate key(a) "
                + "distributed by hash(a) buckets 1 properties(\"replication_num\"=\"1\")");
        starRocksAssert.withTable("create table tb2 (c int) duplicate key(c) "
                + "distributed by hash(c) buckets 1 properties(\"replication_num\"=\"1\")");
        starRocksAssert.withTable("create table sk1 (c_key int, c_tinyint tinyint) duplicate key(c_key) "
                + "distributed by hash(c_key) buckets 1 properties(\"replication_num\"=\"1\")");
        starRocksAssert.withTable("create table sk2 (c_key int, c_tinyint tinyint) duplicate key(c_key) "
                + "distributed by hash(c_key) buckets 1 properties(\"replication_num\"=\"1\")");
    }

    private void assertPlans(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        Assertions.assertFalse(plan.isEmpty(), sql);
    }

    private void assertRefused(String sql) {
        Exception e = Assertions.assertThrows(Exception.class, () -> getFragmentPlan(sql), sql);
        Assertions.assertTrue(e.getMessage() != null && e.getMessage().contains(CORRELATION_NOT_SUPPORTED),
                sql + " => " + e.getClass().getName() + ": " + e.getMessage());
    }

    /**
     * A subquery used inside a window function used to reach the translator through the short overload that
     * passes no plan builder, and NPE'd on that null builder.
     */
    @Test
    public void testSubqueryInsideWindowFunction() throws Exception {
        // window function argument
        assertPlans("select sum(v1 in (select v1 from t1)) over () from t1");
        assertPlans("select sum(v1 not in (select v1 from t1)) over () from t1");
        assertPlans("select sum(exists (select v1 from t1)) over () from t1");
        assertPlans("select sum(v1 + (select max(v1) from t1)) over () from t1");
        // partition by / order by
        assertPlans("select sum(v1) over (partition by v1 in (select v1 from t1)) from t1");
        assertPlans("select sum(v1) over (order by v1 in (select v1 from t1)) from t1");
        assertPlans("select sum(v1) over (partition by v1 in (select v1 from t1) "
                + "order by exists (select v1 from t1)) from t1");
        // control: the same expression without the window
        assertPlans("select sum(v1 in (select v1 from t1)) from t1");
    }


    /**
     * An outer-query column referenced from a window's partition by / order by used to have its correlation
     * silently dropped, producing an invalid plan instead of the declared refusal.
     */
    @Test
    public void testCorrelatedColumnInWindow() throws Exception {
        assertRefused("select a from tb1 where b > (select row_number() over (partition by tb1.a) from tb1 t2)");
        assertRefused("select a from tb1 where b > (select row_number() over (order by tb1.a) from tb1 t2)");
        assertRefused("select a from tb1 where b > (select sum(t2.a) over (partition by tb1.a) from tb1 t2)");
        assertRefused("select (select row_number() over (partition by tb1.a) from tb1 t2) from tb1");
        // control: the same correlation without a window
        assertRefused("select a from tb1 where b > (select tb1.a from tb1 t2)");
        // control: an uncorrelated window inside the subquery keeps working
        assertPlans("select a from tb1 where b > (select row_number() over (partition by t2.a) from tb1 t2)");
        // control: a top level window is never correlated
        assertPlans("select row_number() over (partition by a order by b) from tb1");
    }
}
