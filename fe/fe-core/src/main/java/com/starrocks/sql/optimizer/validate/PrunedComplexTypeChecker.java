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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.type.ArrayType;
import com.starrocks.type.MapType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;

import java.util.List;
import java.util.Map;

/**
 * Rejects a plan that declares a narrower complex type for an OLAP scan column than the
 * backend will actually materialize for it.
 *
 * <p>The declared type of a scan column is what BE builds every downstream column from -
 * a hash join's probe output, an exchange receiver's chunk, an aggregate's group-by key.
 * What BE actually reads out of the tablet, however, is decided by the pushed-down
 * {@link ColumnAccessPath}: {@code OlapChunkSource} builds its output chunk from the tablet
 * schema and narrows it only in {@code _prune_schema_by_access_paths}, which returns
 * immediately when no path was pushed down for the column.
 *
 * <p>When the two disagree, nothing downstream can work: the serialized chunk encoding is
 * positional, so an exchange receiver walking two sub-columns where the sender wrote four
 * reads the next length field out of unrelated bytes - at best a corrupt column, at worst a
 * memcpy into an unallocated buffer inside {@code serde::read_raw}. A local hash join trips
 * {@code StructColumn::append}'s field-count check instead. Both are silent until they are
 * not, so catch the plan here rather than letting it reach the BE.
 */
public class PrunedComplexTypeChecker implements PlanValidator.Checker {

    private static final PrunedComplexTypeChecker INSTANCE = new PrunedComplexTypeChecker();

    private PrunedComplexTypeChecker() {
    }

    public static PrunedComplexTypeChecker getInstance() {
        return INSTANCE;
    }

    @Override
    public void validate(OptExpression physicalPlan, TaskContext taskContext) {
        // Controlled on its own rather than only through enable_plan_validation: this is a
        // contract between the declared plan and what the storage layer materializes, so it is
        // useful to keep it on (or turn just it off) independently of the other checkers.
        ConnectContext context = ConnectContext.get();
        if (context != null && !context.getSessionVariable().isEnablePrunedComplexTypeCheck()) {
            return;
        }
        physicalPlan.getOp().accept(new Visitor(), physicalPlan, null);
    }

    private static class Visitor extends OptExpressionVisitor<Void, Void> {

        @Override
        public Void visit(OptExpression optExpression, Void context) {
            for (OptExpression input : optExpression.getInputs()) {
                input.getOp().accept(this, input, null);
            }
            return null;
        }

        @Override
        public Void visitPhysicalScan(OptExpression optExpression, Void context) {
            PhysicalScanOperator scanOperator = (PhysicalScanOperator) optExpression.getOp();
            if (OperatorType.PHYSICAL_OLAP_SCAN.equals(scanOperator.getOpType())) {
                for (Map.Entry<ColumnRefOperator, Column> entry : scanOperator.getColRefToColumnMetaMap()
                        .entrySet()) {
                    checkColumn(scanOperator, entry.getKey(), entry.getValue());
                }
            }
            return visit(optExpression, context);
        }

        private void checkColumn(PhysicalScanOperator scanOperator, ColumnRefOperator ref, Column column) {
            Type declared = ref.getType();
            Type stored = column.getType();
            if (!declared.isComplexType() || !isStructurallyNarrower(declared, stored)) {
                return;
            }
            if (hasPrunableAccessPath(scanOperator, column)) {
                return;
            }
            throw new IllegalArgumentException(String.format(
                    "Invalid plan: column %s of table %s is declared as %s but no column access path prunes it, " +
                            "so the scan materializes %s. A narrower declared type than what the scan produces " +
                            "makes downstream operators build columns of the wrong shape.",
                    column.getName(), scanOperator.getTable() == null ? "?" : scanOperator.getTable().getName(),
                    declared, stored));
        }

        private boolean hasPrunableAccessPath(PhysicalScanOperator scanOperator, Column column) {
            for (ColumnAccessPath path : scanOperator.getColumnAccessPaths()) {
                // Mirror what BE prunes by: OlapChunkSource skips predicate-only paths, and
                // prune_field_by_access_paths is a no-op for a path without children.
                if (path.isFromPredicate() || !path.hasChildPath()) {
                    continue;
                }
                if (path.getPath().equalsIgnoreCase(column.getName())) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Whether |declared| drops subfields that |stored| has. Only the nesting structure is
     * compared; scalar widths (varchar length, decimal precision) are irrelevant here.
     */
    private static boolean isStructurallyNarrower(Type declared, Type stored) {
        if (declared == null || stored == null) {
            return false;
        }
        if (declared.isArrayType() && stored.isArrayType()) {
            return isStructurallyNarrower(((ArrayType) declared).getItemType(), ((ArrayType) stored).getItemType());
        }
        if (declared.isMapType() && stored.isMapType()) {
            return isStructurallyNarrower(((MapType) declared).getKeyType(), ((MapType) stored).getKeyType()) ||
                    isStructurallyNarrower(((MapType) declared).getValueType(), ((MapType) stored).getValueType());
        }
        if (declared.isStructType() && stored.isStructType()) {
            List<StructField> declaredFields = ((StructType) declared).getFields();
            List<StructField> storedFields = ((StructType) stored).getFields();
            if (declaredFields.size() != storedFields.size()) {
                return true;
            }
            for (int i = 0; i < declaredFields.size(); i++) {
                if (isStructurallyNarrower(declaredFields.get(i).getType(), storedFields.get(i).getType())) {
                    return true;
                }
            }
        }
        return false;
    }
}
