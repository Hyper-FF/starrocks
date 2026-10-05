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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * column_size, column_compressed_size and flat_json_meta read a column's stored metadata, so the only
 * plan that can evaluate them is the meta scan PushDownAggToMetaScanRule builds. Written any other
 * way, the call used to survive planning and come back from the backend as
 * {@code Invalid agg function plan: column_size with (arg type BIGINT, serde type BIGINT, ...)} --
 * an error naming a backend id and an internal serde type, which tells the reader nothing about what
 * to write instead.
 *
 * <p>Both halves are asserted here. Rejecting the unsupported shapes is worth nothing if it also
 * rejects the supported one, and that is the shape the statistics collector itself issues.
 */
public class MetaOnlyAggregateRejectTest extends PlanTestBase {

    private static final String HINT = "only available in a meta scan";

    private void assertRejected(String sql) {
        Exception e = assertThrows(Exception.class, () -> getFragmentPlan(sql), sql);
        String message = String.valueOf(e.getMessage());
        assertTrue(message.contains(HINT),
                "expected the meta-scan hint in the message for [" + sql + "], got: " + message);
        // The message has to name the function and the form to write, or it is no better than the
        // backend's.
        assertTrue(message.contains("[_META_]"), "expected the usage in the message, got: " + message);
    }

    @Test
    public void testPlainAggregateFormIsRejected() throws Exception {
        assertRejected("select column_size(v1) from t0");
        assertRejected("select column_compressed_size(v1) from t0");
        assertRejected("select column_size(v1), column_compressed_size(v2) from t0");
    }

    @Test
    public void testGroupByIsRejectedEvenWithTheMetaHint() throws Exception {
        // The meta scan reads one value per tablet and has no way to honour a grouping, so the rewrite
        // declines and the call survives. Before this change the hint made no difference here: the
        // statement still reached the backend and failed there.
        assertRejected("select v2, column_size(v1) from t0 [_META_] group by v2");
    }

    @Test
    public void testRejectedWhereverTheCallSits() throws Exception {
        assertRejected("select * from (select column_size(v1) as s from t0) x");
        assertRejected("select column_size(v1) from t0 where v2 > 1");
    }

    @Test
    public void testTheSupportedShapeStillPlans() throws Exception {
        // The negative control, and not a formality: this is what the statistics collector issues, so
        // a rejection that caught it would break dictionary and column-size collection.
        String plan = getFragmentPlan("select column_size(v1) from t0 [_META_]");
        assertTrue(plan.contains("META_SCAN"), "expected a meta scan, got:\n" + plan);
        // The rewrite replaces the call with a plain sum over the per-tablet value, which is exactly
        // why a surviving column_size() means the rewrite did not happen.
        assertTrue(plan.contains("sum("), "expected the rewritten sum(), got:\n" + plan);
        assertEquals(0, countOccurrences(plan, "column_size(1:"),
                "column_size must not survive into the fragment:\n" + plan);

        String compressed = getFragmentPlan("select column_compressed_size(v1) from t0 [_META_]");
        assertTrue(compressed.contains("META_SCAN"), "expected a meta scan, got:\n" + compressed);
    }

    @Test
    public void testOtherMetaAggregatesAreUntouched() throws Exception {
        // dict_merge is in the same meta-scan family but the backend does implement its ordinary
        // aggregate path, so it must keep planning without the hint. Narrowing the rejection to the
        // three functions that have no such path is the whole point.
        String plan = getFragmentPlan("select dict_merge(t1a, 255) from test_all_type");
        assertTrue(plan.contains("dict_merge"), "dict_merge should still plan, got:\n" + plan);
    }

    private static int countOccurrences(String haystack, String needle) {
        int n = 0;
        int i = haystack.indexOf(needle);
        while (i >= 0) {
            n++;
            i = haystack.indexOf(needle, i + needle.length());
        }
        return n;
    }
}
