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

// Incremental counter of OlapTables with flat_json.enable = true.
//
// Seeded from a single catalog walk on leader transition — by then
// the replayer has been stopped and the journal has been fully caught
// up, so the walk sees a committed, quiescent catalog. Must NOT seed
// on followers/observers: replay there is still running and the walk
// would read partially-replayed state. The gauge is leader-only so
// non-leaders don't need a value anyway.
//
// After seeding, the counter is maintained incrementally by
// OlapTable.setFlatJsonConfig()/onDrop() and CatalogRecycleBin
// onRecover hooks. Hooks before the first seed are dropped (the gauge
// reads 0 on non-leader); hooks after a re-seed on failover are
// applied on top of the freshly-seeded value.
public final class FlatJsonTableStats {
    private static long value;
    private static boolean seeded;

    private FlatJsonTableStats() {
    }

    // Recomputes the counter from the catalog. Call only from a quiescent
    // leader-transition point (after replayer stop + journal catch-up,
    // before user DDL is admitted). Safe to call on every leader
    // transition, so the counter is re-seeded after failover.
    public static synchronized void seed() {
        value = compute();
        seeded = true;
    }

    public static synchronized long get() {
        return value;
    }

    public static synchronized void onConfigChange(FlatJsonConfig oldCfg, FlatJsonConfig newCfg) {
        if (!seeded) {
            return;
        }
        boolean was = oldCfg != null && oldCfg.getFlatJsonEnable();
        boolean now = newCfg != null && newCfg.getFlatJsonEnable();
        if (was == now) {
            return;
        }
        value += now ? 1 : -1;
    }

    public static synchronized void onTableDrop(FlatJsonConfig cfg) {
        if (!seeded) {
            return;
        }
        if (cfg != null && cfg.getFlatJsonEnable()) {
            value -= 1;
        }
    }

    public static synchronized void onTableRecover(FlatJsonConfig cfg) {
        if (!seeded) {
            return;
        }
        if (cfg != null && cfg.getFlatJsonEnable()) {
            value += 1;
        }
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
