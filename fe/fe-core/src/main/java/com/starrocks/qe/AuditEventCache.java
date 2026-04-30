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

import com.starrocks.common.Config;
import com.starrocks.plugin.AuditEvent;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * In-memory LRU cache of recent {@link AuditEvent}s, indexed by query_id.
 * Fed synchronously from {@link AuditEventProcessor#handleAuditEvent} for
 * {@link AuditEvent.EventType#AFTER_QUERY} events so it is independent of
 * the async plugin pipeline and the {@code enable_collect_query_detail_info}
 * gate. Used by builtins such as {@code get_query_dump(query_id)}.
 */
public class AuditEventCache {
    private static final AuditEventCache INSTANCE = new AuditEventCache();

    public static AuditEventCache getInstance() {
        return INSTANCE;
    }

    private final LinkedHashMap<String, AuditEvent> cache =
            new LinkedHashMap<String, AuditEvent>(256, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, AuditEvent> eldest) {
                    return size() > Math.max(1, Config.audit_event_cache_capacity);
                }
            };

    public synchronized void put(AuditEvent event) {
        if (event == null || event.queryId == null || event.queryId.isEmpty()) {
            return;
        }
        cache.put(event.queryId, event);
    }

    public synchronized AuditEvent get(String queryId) {
        if (queryId == null) {
            return null;
        }
        return cache.get(queryId);
    }

    public synchronized int size() {
        return cache.size();
    }

    public synchronized void clear() {
        cache.clear();
    }
}
