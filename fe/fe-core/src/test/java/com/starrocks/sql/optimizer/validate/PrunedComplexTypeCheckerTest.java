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

package com.starrocks.sql.optimizer.validate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.catalog.OlapTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.thrift.TAccessPathType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The scan hands downstream operators whatever the tablet schema says unless a
 * ColumnAccessPath narrows it, so a plan that declares something narrower is not
 * executable - it has to be rejected here rather than corrupting chunks in the BE.
 */
public class PrunedComplexTypeCheckerTest {

    private static Type arrayOfStruct(String... fieldNames) {
        ArrayList<StructField> fields = Lists.newArrayList();
        for (String name : fieldNames) {
            fields.add(new StructField(name, IntegerType.INT));
        }
        return new ArrayType(new StructType(fields));
    }

    private static OptExpression scanOf(Type declaredType, Type storedType, List<ColumnAccessPath> paths) {
        ColumnRefOperator ref = new ColumnRefOperator(1, declaredType, "c_arr", true);
        Column column = new Column("c_arr", storedType);
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = Maps.newHashMap();
        colRefToColumnMetaMap.put(ref, column);

        OlapTable table = new OlapTable();
        table.setName("t_prune_check");
        PhysicalOlapScanOperator scan = new PhysicalOlapScanOperator(table, colRefToColumnMetaMap, null, -1, null,
                0, Lists.newArrayList(), Lists.newArrayList(), Lists.newArrayList(), Lists.newArrayList(), null,
                false, null);
        scan.setColumnAccessPaths(paths);
        return OptExpression.create(scan);
    }

    private static ColumnAccessPath prunablePathFor(String column) {
        ColumnAccessPath path = new ColumnAccessPath(TAccessPathType.ROOT, column, IntegerType.INT);
        path.addChildPath(new ColumnAccessPath(TAccessPathType.FIELD, "f1", IntegerType.INT));
        return path;
    }

    @Test
    public void testRejectsNarrowedTypeWithoutAccessPath() {
        OptExpression plan = scanOf(arrayOfStruct("f1"), arrayOfStruct("f1", "f2"), ImmutableList.of());
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> PrunedComplexTypeChecker.getInstance().validate(plan, null));
        Assertions.assertTrue(e.getMessage().contains("c_arr"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("no column access path prunes it"), e.getMessage());
    }

    @Test
    public void testAcceptsNarrowedTypeBackedByAccessPath() {
        OptExpression plan = scanOf(arrayOfStruct("f1"), arrayOfStruct("f1", "f2"),
                ImmutableList.of(prunablePathFor("c_arr")));
        PrunedComplexTypeChecker.getInstance().validate(plan, null);
    }

    @Test
    public void testAcceptsPredicateOnlyPathIsNotEnough() {
        ColumnAccessPath path = prunablePathFor("c_arr");
        path.setFromPredicate(true);
        OptExpression plan = scanOf(arrayOfStruct("f1"), arrayOfStruct("f1", "f2"), ImmutableList.of(path));
        // BE skips predicate-only paths when pruning the scan schema, so this is still unsafe.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PrunedComplexTypeChecker.getInstance().validate(plan, null));
    }

    @Test
    public void testSwitchDisablesTheCheck() {
        OptExpression plan = scanOf(arrayOfStruct("f1"), arrayOfStruct("f1", "f2"), ImmutableList.of());
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            context.getSessionVariable().setEnablePrunedComplexTypeCheck(false);
            PrunedComplexTypeChecker.getInstance().validate(plan, null);

            // and back on, the same plan is rejected again
            context.getSessionVariable().setEnablePrunedComplexTypeCheck(true);
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> PrunedComplexTypeChecker.getInstance().validate(plan, null));
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testAcceptsUnnarrowedType() {
        OptExpression plan = scanOf(arrayOfStruct("f1", "f2"), arrayOfStruct("f1", "f2"), ImmutableList.of());
        PrunedComplexTypeChecker.getInstance().validate(plan, null);
    }
}
