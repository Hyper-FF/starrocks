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
import java.util.concurrent.atomic.AtomicLong;

// Counts OlapTables with flat_json.enable = true. The catalog walk is cached so that
// metric scrapes do not trigger a full iteration on every request.
final class FlatJsonEnabledTableCounter {
    private static final long REFRESH_INTERVAL_MS = 60_000L;

    private static final AtomicLong CACHED_VALUE = new AtomicLong(0L);
    private static final AtomicLong LAST_REFRESH_MS = new AtomicLong(0L);

    private FlatJsonEnabledTableCounter() {
    }

    static long get() {
        long now = System.currentTimeMillis();
        long last = LAST_REFRESH_MS.get();
        if (now - last >= REFRESH_INTERVAL_MS && LAST_REFRESH_MS.compareAndSet(last, now)) {
            CACHED_VALUE.set(compute());
        }
        return CACHED_VALUE.get();
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
