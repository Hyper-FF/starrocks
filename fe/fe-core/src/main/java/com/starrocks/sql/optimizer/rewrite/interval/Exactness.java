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


package com.starrocks.sql.optimizer.rewrite.interval;

/**
 * How tightly {@link IntervalMapping#outRange()} bounds the image of the input interval.
 * <p>
 * Neither value promises that every point inside outRange is attained, and no caller may assume it:
 * a monotone function over the integers skips values (x -&gt; 2x never lands on an odd number), so the
 * range is a bounding interval and not the image as a set. What distinguishes the two is only how
 * tight the BOUNDS are.
 * <p>
 * The distinction a caller wanting to DROP the original predicate after the rewrite needs is a
 * different one -- whether f is injective over the interval, which {@link IntervalMapping#plateau()}
 * answers. An interval bound as tightly as possible still admits a second input mapping to the same
 * partition value, and then the kept partitions no longer imply the predicate.
 */
public enum Exactness {
    /**
     * The bounds are tight: each endpoint is attained by the corresponding endpoint of the input
     * (subject to its bound type), and nothing outside the range is attained.
     */
    EXACT,
    /** The bounds are loose: the image lies inside the range, but the endpoints may not be attained. */
    OVER
}
