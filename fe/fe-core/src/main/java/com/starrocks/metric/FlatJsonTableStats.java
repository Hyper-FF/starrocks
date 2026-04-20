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
// Seeded once via a single catalog walk (triggered lazily on first read
// or explicitly by MetricRepo.init()), then maintained incrementally by
// OlapTable.setFlatJsonConfig()/onDrop() hooks so that scrapes are O(1).
//
// All entry points are serialized on the class monitor: incremental
// updates that arrive during the seed walk block briefly so that the
// seed result is consistent with the committed catalog state. Updates
// before seeding are dropped because the eventual seed walk observes
// the same committed state.
public final class FlatJsonTableStats {
    private static long value;
    private static boolean seeded;

    private FlatJsonTableStats() {
    }

    public static synchronized void seed() {
        if (seeded) {
            return;
        }
        value = compute();
        seeded = true;
    }

    public static synchronized long get() {
        if (!seeded) {
            value = compute();
            seeded = true;
        }
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
