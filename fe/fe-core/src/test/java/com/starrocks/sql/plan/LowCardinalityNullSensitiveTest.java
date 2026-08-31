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

// Isolated from LowCardinalityTest2: planning dict queries mutates the mock-dict/session state shared
// across methods within a class, which perturbs that class's absolute-id plan assertions. Surefire runs
// each class in its own fork (reuseForks=false), so a separate class keeps this regression self-contained.
public class LowCardinalityNullSensitiveTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("CREATE TABLE `lc_ns` (\n" +
                "  `id` int ,\n" +
                "  `s` varchar(50) \n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 3 \n" +
                "PROPERTIES (\"replication_num\" = \"1\");");
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(true);
        connectContext.getSessionVariable().setCboCteReuse(false);
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
        FeConstants.USE_MOCK_DICT_MANAGER = false;
    }

    @Test
    public void testNullSensitiveOuterOverDerivedDictKeepsIntermediate() throws Exception {
        // `if(v = v, v, 'NX')` maps only the derived dict's synthetic NULL slot (v = v is NULL when v is
        // NULL) to a value the flattened base dict cannot produce. It is non-strict, so it must keep the
        // intermediate dict instead of flattening onto the base -- otherwise BE fails at runtime with
        // "Dict Decode failed, Dict can't take cover all key". IF/CASE are not in the old enumerated
        // null-sensitive list; the fold-based strictness check catches them.
        String kept = getVerboseExplain(
                "SELECT if(v = v, v, 'NX') FROM (SELECT DISTINCT lower(s) v FROM lc_ns) x");
        // Kept: the conditional decodes the intermediate lower dict and runs over the string
        // placeholder. Flattened (the bug) would instead inline lower(...) into the conditional, e.g.
        // "if[(lower[(<place-holder>) ...", building a derived dict the base cannot cover.
        assertContains(kept, "[if[(<place-holder> = <place-holder>, <place-holder>, 'NX')");
        assertNotContains(kept, "if[(lower");

        // CASE normalizes to the same conditional and must be treated identically.
        String caseKept = getVerboseExplain(
                "SELECT (CASE WHEN v = v THEN v ELSE 'NX' END) FROM (SELECT DISTINCT lower(s) v FROM lc_ns) x");
        assertContains(caseKept, "[if[(<place-holder> = <place-holder>, <place-holder>, 'NX')");
        assertNotContains(caseKept, "if[(lower");
    }
}
