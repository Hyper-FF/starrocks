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

package com.starrocks.qe;

import com.starrocks.catalog.Column;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.BucketProperty;
import com.starrocks.qe.scheduler.LazyWorkerProvider;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.qe.scheduler.assignment.FragmentAssignmentStrategy;
import com.starrocks.qe.scheduler.assignment.FragmentAssignmentStrategyFactory;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.thrift.TBucketFunction;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Regression test for issue #71680.
 *
 * Verifies that {@link CoordinatorPreprocessor#assignIncrementalScanRangesToFragmentInstances}
 * clears {@code colocatedAssignment.seqToScanRange} between incremental scan range batches.
 * Without the clear, stale scan ranges from previous batches are re-emitted to BE, causing
 * duplicate file scans and inflated query results.
 */
public class CoordinatorPreprocessorIncrementalTest {

    @Test
    public void testAssignIncrementalScanRangesClearsColocateSeqToScanRange() throws Exception {
        // 1. Build a ColocatedBackendSelector.Assignment with pre-populated stale scan ranges,
        //    simulating what would remain after a previous incremental batch.
        BucketProperty bp = new BucketProperty(TBucketFunction.MURMUR3_X86_32, 10,
                new Column("c1", IntegerType.INT));
        ColocatedBackendSelector.Assignment assignment =
                new ColocatedBackendSelector.Assignment(10, 1, Optional.of(List.of(bp)));

        Map<Integer, List<TScanRangeParams>> staleEntry = new HashMap<>();
        TScanRangeParams staleParam = new TScanRangeParams();
        staleParam.setEmpty(false);
        staleEntry.put(1, new ArrayList<>(List.of(staleParam)));
        assignment.getSeqToScanRange().put(0, staleEntry);

        // Pre-condition: stale data is present.
        Assertions.assertFalse(assignment.getSeqToScanRange().isEmpty(),
                "precondition: seqToScanRange should contain stale data");

        // 2. Mock ExecutionFragment to return our pre-populated assignment.
        ExecutionFragment mockFragment = Mockito.mock(ExecutionFragment.class);
        Mockito.when(mockFragment.getColocatedAssignment()).thenReturn(assignment);
        Mockito.when(mockFragment.getScanRangeAssignment()).thenReturn(new FragmentScanRangeAssignment());
        Mockito.when(mockFragment.getInstances()).thenReturn(new ArrayList<>());

        // 3. Mock the factory to return a no-op strategy so the scheduling part doesn't crash.
        FragmentAssignmentStrategyFactory mockFactory = Mockito.mock(FragmentAssignmentStrategyFactory.class);
        FragmentAssignmentStrategy noopStrategy = fragment -> {
        };
        Mockito.when(mockFactory.create(Mockito.any(ExecutionFragment.class), Mockito.any(WorkerProvider.class)))
                .thenReturn(noopStrategy);

        LazyWorkerProvider mockWorkerProvider = Mockito.mock(LazyWorkerProvider.class);
        Mockito.when(mockWorkerProvider.get()).thenReturn(Mockito.mock(WorkerProvider.class));

        // 4. Construct a CoordinatorPreprocessor without calling its constructor (which pulls in
        //    GlobalStateMgr, ConnectContext, etc.) - we only need the two private fields that
        //    assignIncrementalScanRangesToFragmentInstances touches.
        java.lang.reflect.Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        sun.misc.Unsafe unsafe = (sun.misc.Unsafe) unsafeField.get(null);
        CoordinatorPreprocessor preprocessor =
                (CoordinatorPreprocessor) unsafe.allocateInstance(CoordinatorPreprocessor.class);
        Deencapsulation.setField(preprocessor, "fragmentAssignmentStrategyFactory", mockFactory);
        Deencapsulation.setField(preprocessor, "lazyWorkerProvider", mockWorkerProvider);

        // 5. Call the production method under test.
        preprocessor.assignIncrementalScanRangesToFragmentInstances(mockFragment);

        // 6. Assert: seqToScanRange should now be empty.
        //    WITHOUT the fix (clear call removed), this assertion FAILS because
        //    the stale entry we put in step 1 is still there.
        Assertions.assertTrue(assignment.getSeqToScanRange().isEmpty(),
                "seqToScanRange must be cleared between incremental batches (fix for issue #71680)");
    }
}
