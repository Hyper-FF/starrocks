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

package com.starrocks.metric;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.FlatJsonConfig;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;

import java.util.List;

// Count of OlapTables with flat_json.enable = true.
//
// Backed by a simple TTL cache: get() returns the cached value and
// recomputes via a full catalog walk at most once per REFRESH_INTERVAL_MS.
// The gauge is leader-only (LeaderAwareGaugeMetricLong), so the walk only
// ever runs on the leader. Intentionally avoids hooking DDL lifecycle
// paths (setFlatJsonConfig, onDrop, recoverTable, checkpoint replay)
// — that style drifts when any new code path is added or missed.
public final class FlatJsonTableStats {
    private static final long REFRESH_INTERVAL_MS = 5 * 60 * 1000L;

    private static long cachedValue;
    private static long lastRefreshMs;

    private FlatJsonTableStats() {
    }

    public static synchronized long get() {
        long now = System.currentTimeMillis();
        if (lastRefreshMs == 0L || now - lastRefreshMs >= REFRESH_INTERVAL_MS) {
            cachedValue = compute();
            lastRefreshMs = now;
        }
        return cachedValue;
    }

    private static long compute() {
        GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
        if (gsm == null) {
            return 0L;
        }
        LocalMetastore metastore = gsm.getLocalMetastore();
        if (metastore == null) {
            return 0L;
        }
        long count = 0L;
        List<String> dbNames = metastore.listDbNames(new ConnectContext());
        for (String dbName : dbNames) {
            Database db = metastore.getDb(dbName);
            if (db == null) {
                continue;
            }
            for (Table table : metastore.getTables(db.getId())) {
                if (!(table instanceof OlapTable)) {
                    continue;
                }
                FlatJsonConfig cfg = ((OlapTable) table).getFlatJsonConfig();
                if (cfg != null && cfg.getFlatJsonEnable()) {
                    count++;
                }
            }
        }
        return count;
    }
}
