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

import com.starrocks.catalog.Table;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarbinaryType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A cast target written as bare VARBINARY/BINARY carries the parser's "unspecified length" sentinel (-1).
 * Nothing on the expression path used to resolve it, so the sentinel ended up both in the thrift the BE
 * receives (which reported types such as VARBINARY(-1)) and in the metadata derived from such expressions.
 */
public class VarbinaryCastTypeTest extends PlanTestBase {

    private static final String DEFAULT_LEN = String.valueOf(TypeFactory.getOlapMaxVarcharLength());

    @Test
    public void testBareVarbinaryCastGetsConcreteLength() throws Exception {
        String plan = getVerboseExplain("select cast(v1 as varbinary) from t0");
        assertTrue(plan.contains("as VARBINARY(" + DEFAULT_LEN + ")"), plan);
    }

    @Test
    public void testBareBinaryCastGetsConcreteLength() throws Exception {
        String plan = getVerboseExplain("select cast(v1 as binary) from t0");
        assertTrue(plan.contains("as VARBINARY(" + DEFAULT_LEN + ")"), plan);
    }

    @Test
    public void testNegativeLengthNeverReachesThrift() throws Exception {
        for (String sql : new String[] {
                "select cast(v1 as varbinary) from t0",
                "select cast(v1 as binary) from t0",
                "select cast(cast(v1 as varbinary) as varbinary) from t0"}) {
            String thrift = getThriftPlan(sql);
            assertFalse(thrift.contains("type:VARBINARY, len:-1"), sql + "\n" + thrift);
        }
    }

    @Test
    public void testExplicitLengthIsPreserved() throws Exception {
        String plan = getVerboseExplain("select cast(v1 as varbinary(10)) from t0");
        assertTrue(plan.contains("as VARBINARY(10)"), plan);
    }

    @Test
    public void testDerivedViewColumnGetsConcreteLength() throws Exception {
        starRocksAssert.withView("create view test_unsized_varbinary_view as "
                + "select cast(v1 as varbinary) as x from t0");
        try {
            Table view = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore()
                    .getDb("test").getTable("test_unsized_varbinary_view");
            assertEquals(TypeFactory.createVarbinary(TypeFactory.getOlapMaxVarcharLength()),
                    view.getBaseSchema().get(0).getType());
        } finally {
            starRocksAssert.dropView("test_unsized_varbinary_view");
        }
    }

    @Test
    public void testNestedUnsizedVarbinaryIsResolved() {
        int defaultLen = TypeFactory.getOlapMaxVarcharLength();
        assertEquals(TypeFactory.createVarbinary(defaultLen),
                TypeFactory.resolveUnsizedVarbinary(VarbinaryType.VARBINARY));
        assertEquals(new ArrayType(TypeFactory.createVarbinary(defaultLen)),
                TypeFactory.resolveUnsizedVarbinary(new ArrayType(VarbinaryType.VARBINARY)));
        assertEquals(new MapType(IntegerType.INT, TypeFactory.createVarbinary(defaultLen)),
                TypeFactory.resolveUnsizedVarbinary(new MapType(IntegerType.INT, VarbinaryType.VARBINARY)));
        assertEquals(new StructType(List.of(new StructField("a", TypeFactory.createVarbinary(defaultLen))), true),
                TypeFactory.resolveUnsizedVarbinary(
                        new StructType(List.of(new StructField("a", VarbinaryType.VARBINARY)), true)));
        // Sized VARBINARY and unrelated types are returned untouched.
        Type sized = TypeFactory.createVarbinary(10);
        assertSame(sized, TypeFactory.resolveUnsizedVarbinary(sized));
        assertSame(IntegerType.INT, TypeFactory.resolveUnsizedVarbinary(IntegerType.INT));
        // The shared singleton must never be mutated in place.
        assertEquals(-1, VarbinaryType.VARBINARY.getLength());
    }
}
