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

package com.starrocks.sql.optimizer.transformer;

import com.starrocks.catalog.TableName;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ExpressionMappingTest {

    private static ExpressionMapping twoFieldMapping() {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator a = factory.create("a", IntegerType.INT, true);
        ColumnRefOperator b = factory.create("b", IntegerType.INT, true);
        Scope scope = new Scope(RelationId.anonymous(), new RelationFields(List.of(
                new Field("a", IntegerType.INT, new TableName(null, "t"), null),
                new Field("b", IntegerType.INT, new TableName(null, "t"), null))));
        return new ExpressionMapping(scope, List.of(a, b));
    }

    @Test
    public void testValidIndexesAreReturned() {
        ExpressionMapping mapping = twoFieldMapping();
        Assertions.assertEquals("a", mapping.getColumnRefWithIndex(0).getName());
        Assertions.assertEquals("b", mapping.getColumnRefWithIndex(1).getName());
    }

    /**
     * An index of exactly the length used to pass the guard and fail in the array access, so the
     * caller got {@code ArrayIndexOutOfBoundsException: Index 2 out of bounds for length 2} and no
     * indication of which mapping or index was involved. Real queries reach this boundary.
     */
    @Test
    public void testIndexEqualToLengthReportsTheGuardsError() {
        ExpressionMapping mapping = twoFieldMapping();
        StarRocksPlannerException e = Assertions.assertThrows(StarRocksPlannerException.class,
                () -> mapping.getColumnRefWithIndex(2));
        Assertions.assertTrue(e.getMessage().contains("index 2"), e.getMessage());
        // The length belongs in the message: "out of range" without the range is half an answer.
        Assertions.assertTrue(e.getMessage().contains("length 2"), e.getMessage());
    }

    @Test
    public void testIndexBeyondLengthReportsTheGuardsError() {
        ExpressionMapping mapping = twoFieldMapping();
        StarRocksPlannerException e = Assertions.assertThrows(StarRocksPlannerException.class,
                () -> mapping.getColumnRefWithIndex(1202));
        Assertions.assertTrue(e.getMessage().contains("index 1202"), e.getMessage());
    }

    /** Negative indexes were not covered by the guard at all. */
    @Test
    public void testNegativeIndexReportsTheGuardsError() {
        ExpressionMapping mapping = twoFieldMapping();
        StarRocksPlannerException e = Assertions.assertThrows(StarRocksPlannerException.class,
                () -> mapping.getColumnRefWithIndex(-1));
        Assertions.assertTrue(e.getMessage().contains("index -1"), e.getMessage());
    }
}
