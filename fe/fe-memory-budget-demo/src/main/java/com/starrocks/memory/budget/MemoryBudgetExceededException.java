package com.starrocks.memory.budget;

public class MemoryBudgetExceededException extends RuntimeException {

    private final String scope;
    private final long consumed;
    private final long limit;

    public MemoryBudgetExceededException(String scope, long consumed, long limit) {
        super("memory budget exceeded for scope '" + scope + "': consumed=" + consumed + " limit=" + limit);
        this.scope = scope;
        this.consumed = consumed;
        this.limit = limit;
    }

    public String scope() {
        return scope;
    }

    public long consumed() {
        return consumed;
    }

    public long limit() {
        return limit;
    }
}
