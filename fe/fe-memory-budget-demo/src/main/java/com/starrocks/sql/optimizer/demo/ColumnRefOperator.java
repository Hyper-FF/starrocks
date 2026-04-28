package com.starrocks.sql.optimizer.demo;

public class ColumnRefOperator {

    private final int id;
    private final String name;

    public ColumnRefOperator(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int id() {
        return id;
    }

    public String name() {
        return name;
    }
}
