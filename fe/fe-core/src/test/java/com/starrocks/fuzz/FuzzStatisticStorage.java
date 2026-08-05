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

package com.starrocks.fuzz;

import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Synthetic statistics, so the optimizer has a reason to consider the plans it otherwise never tries.
 *
 * Measured motivation: over 749 planned production queries the fuzzer fired 16 of 263 optimizer
 * rules. The corpus was not the limit -- no single file exceeded 13 rules, and the union barely
 * moved. Almost every transformation past the basic ones is COST-DRIVEN, and against empty tables
 * with default statistics the optimizer has no reason to enumerate them at all. Mutating SQL cannot
 * lift that ceiling; supplying statistics can.
 *
 * <p><b>Values are self-consistent on purpose.</b> It is trivial to crash a cost model with
 * NDV &gt; row count, min &gt; max or a null fraction above 1 -- and equally pointless, because
 * ANALYZE cannot produce those, so nothing found that way is reachable in production. Everything
 * here satisfies the invariants a real collection would, and the interesting values are the extreme
 * but legal ones: a single distinct value, all NULLs, a key column, an empty table, a huge table.
 *
 * <p>The one genuinely valuable inconsistency is a different axis: statistics that are self-
 * consistent but STALE relative to the data. That is the normal production state -- rows change and
 * nobody re-runs ANALYZE -- and it is where a correctness bug of the {@code minmax-cache-swap-stale}
 * kind lives. Stale statistics must change plans and must never change results.
 *
 * <p>Deterministic: every value derives from (seed, table, column), so a finding replays.
 */
public class FuzzStatisticStorage implements StatisticStorage {

    /** Row counts worth planning against: empty, tiny, mid, and big enough to force distribution. */
    private static final long[] ROW_COUNTS = {0L, 1L, 1_000L, 100_000L, 10_000_000L, 2_000_000_000L};

    private final long seed;

    public FuzzStatisticStorage(long seed) {
        this.seed = seed;
    }

    /** Stable, well-mixed hash of the identifying tuple -- not Object#hashCode, which is not stable. */
    private long mix(String a, String b, int salt) {
        long h = seed * 0x9E3779B97F4A7C15L + salt;
        for (String s : new String[] {a, b}) {
            for (int i = 0; i < s.length(); i++) {
                h ^= s.charAt(i);
                h *= 0x100000001B3L;
            }
            h ^= h >>> 29;
        }
        return h == Long.MIN_VALUE ? 0 : Math.abs(h);
    }

    private long rowCountOf(Table table) {
        return ROW_COUNTS[(int) (mix(table.getName(), "#rows", 1) % ROW_COUNTS.length)];
    }

    @Override
    public Map<Long, Optional<Long>> getTableStatistics(Long tableId, Collection<Partition> partitions) {
        long total = ROW_COUNTS[(int) (mix(String.valueOf(tableId), "#rows", 1) % ROW_COUNTS.length)];
        int n = Math.max(1, partitions.size());
        long per = total / n;
        return partitions.stream()
                .collect(Collectors.toMap(Partition::getId, p -> Optional.of(per)));
    }

    @Override
    public ColumnStatistic getColumnStatistic(Table table, String column) {
        long rows = rowCountOf(table);
        long h = mix(table.getName(), column, 2);

        // NDV is constrained to [1, rows]: a column cannot hold more distinct values than rows. The
        // shapes below are the ones that flip plan choice -- a constant column, a boolean-ish column,
        // a mid-cardinality column, and a unique key.
        double ndv;
        switch ((int) (h % 4)) {
            case 0:
                // constant column -> dictionary and single-value paths
                ndv = 1;
                break;
            case 1:
                ndv = Math.min(rows, 2);
                break;
            case 2:
                ndv = Math.max(1, Math.sqrt(Math.max(rows, 1)));
                break;
            default:
                // unique key -> drives join and aggregation choices
                ndv = Math.max(1, rows);
                break;
        }

        // Null fraction stays in [0, 1]; 1.0 (all NULL) is legal and is its own plan-shaping case.
        double nulls;
        switch ((int) ((h >> 3) % 4)) {
            case 0:
                nulls = 0.0;
                break;
            case 1:
                nulls = 0.5;
                break;
            case 2:
                // all NULL is legal and is its own plan-shaping case
                nulls = 1.0;
                break;
            default:
                nulls = 0.01;
                break;
        }

        // min <= max always. The range drives predicate selectivity, which drives nearly every
        // cost-based decision downstream.
        double min;
        double max;
        switch ((int) ((h >> 6) % 3)) {
            case 0:
                // degenerate: a single point
                min = 0;
                max = 0;
                break;
            case 1:
                min = 0;
                max = Math.max(1, ndv);
                break;
            default:
                min = -1e9;
                max = 1e9;
                break;
        }

        return ColumnStatistic.builder()
                .setMinValue(min)
                .setMaxValue(max)
                .setNullsFraction(nulls)
                .setAverageRowSize(1 + (h >> 9) % 64)
                .setDistinctValuesCount(ndv)
                .build();
    }

    @Override
    public void addColumnStatistic(Table table, String column, ColumnStatistic columnStatistic) {
        // Deliberately ignored. Everything this storage returns is derived from (seed, table,
        // column), so accepting a write would make the values depend on execution order and a
        // finding would stop replaying.
    }

    @Override
    public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
        List<ColumnStatistic> out = new ArrayList<>(columns.size());
        for (String c : columns) {
            out.add(getColumnStatistic(table, c));
        }
        return out;
    }
}
