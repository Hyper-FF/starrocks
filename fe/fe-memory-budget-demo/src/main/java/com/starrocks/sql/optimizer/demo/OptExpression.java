package com.starrocks.sql.optimizer.demo;

public class OptExpression {

    private final String op;
    private final OptExpression child;
    private long stat;

    public OptExpression(String op, OptExpression child) {
        this.op = op;
        this.child = child;
    }

    public String op() {
        return op;
    }

    public OptExpression child() {
        return child;
    }
}
