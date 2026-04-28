package com.starrocks.memory.budget;

import java.util.ArrayDeque;
import java.util.Deque;

public final class MemoryBudget implements AutoCloseable {

    private static final ThreadLocal<Deque<MemoryBudget>> STACK = ThreadLocal.withInitial(ArrayDeque::new);

    // soft toggle — flipped at runtime to silence the advice without retransformation
    public static volatile boolean ENABLED = true;

    public static void setEnabled(boolean enabled) {
        ENABLED = enabled;
    }

    public static boolean isEnabled() {
        return ENABLED;
    }

    private final String name;
    private final long limit;
    private long consumed;
    private boolean closed;

    private MemoryBudget(String name, long limit) {
        this.name = name;
        this.limit = limit;
    }

    public static MemoryBudget open(String name, long limit) {
        MemoryBudget b = new MemoryBudget(name, limit);
        STACK.get().push(b);
        return b;
    }

    public static MemoryBudget current() {
        Deque<MemoryBudget> s = STACK.get();
        return s.isEmpty() ? null : s.peek();
    }

    public static void consume(long bytes) {
        MemoryBudget b = current();
        if (b == null || bytes <= 0) {
            return;
        }
        b.consumed += bytes;
        if (b.consumed > b.limit) {
            long over = b.consumed;
            // clamp so a caller that catches and continues won't keep retriggering on every alloc
            b.consumed = b.limit;
            throw new MemoryBudgetExceededException(b.name, over, b.limit);
        }
    }

    public long consumed() {
        return consumed;
    }

    public long limit() {
        return limit;
    }

    public String name() {
        return name;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        Deque<MemoryBudget> s = STACK.get();
        if (s.peek() != this) {
            return;
        }
        s.pop();
        MemoryBudget parent = s.peek();
        if (parent == null) {
            return;
        }
        parent.consumed += consumed;
        if (parent.consumed > parent.limit) {
            long over = parent.consumed;
            parent.consumed = parent.limit;
            throw new MemoryBudgetExceededException(parent.name, over, parent.limit);
        }
    }
}
