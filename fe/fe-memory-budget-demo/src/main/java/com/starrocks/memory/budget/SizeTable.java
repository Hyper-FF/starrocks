package com.starrocks.memory.budget;

import java.lang.instrument.Instrumentation;
import java.util.concurrent.ConcurrentHashMap;

public final class SizeTable {

    private static volatile Instrumentation INST;
    private static final ConcurrentHashMap<Class<?>, Long> CACHE = new ConcurrentHashMap<>();
    // shallow size only — used the first time we see a class, then cached
    private static final long FALLBACK = 64L;

    private SizeTable() {
    }

    public static void setInstrumentation(Instrumentation inst) {
        INST = inst;
    }

    public static long sizeOf(Object obj) {
        if (obj == null) {
            return 0;
        }
        Class<?> cls = obj.getClass();
        Long cached = CACHE.get(cls);
        if (cached != null) {
            return cached;
        }
        Instrumentation i = INST;
        long size = (i == null) ? FALLBACK : safeGetSize(i, obj);
        CACHE.putIfAbsent(cls, size);
        return size;
    }

    private static long safeGetSize(Instrumentation inst, Object o) {
        try {
            return inst.getObjectSize(o);
        } catch (Throwable t) {
            return FALLBACK;
        }
    }
}
