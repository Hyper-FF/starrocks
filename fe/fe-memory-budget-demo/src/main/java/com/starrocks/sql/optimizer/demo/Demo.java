package com.starrocks.sql.optimizer.demo;

import com.starrocks.memory.budget.MemoryBudget;
import com.starrocks.memory.budget.MemoryBudgetExceededException;

public class Demo {

    public static void main(String[] args) {
        runWithinBudget();
        runOverBudget();
        runNested();
        runWithoutScope();
    }

    private static void runWithinBudget() {
        System.out.println("--- within budget ---");
        try (MemoryBudget b = MemoryBudget.open("planner", 64 * 1024)) {
            OptExpression root = null;
            for (int i = 0; i < 100; i++) {
                root = new OptExpression("op_" + i, root);
            }
            System.out.println("ok: consumed=" + b.consumed() + " limit=" + b.limit());
        }
    }

    private static void runOverBudget() {
        System.out.println("--- over budget ---");
        int created = 0;
        try (MemoryBudget b = MemoryBudget.open("planner", 256)) {
            OptExpression root = null;
            for (int i = 0; i < 1000; i++) {
                root = new OptExpression("op_" + i, root);
                created++;
            }
            System.out.println("did not throw, created=" + created);
        } catch (MemoryBudgetExceededException e) {
            System.out.println("aborted after " + created + " allocations: " + e.getMessage());
        }
    }

    private static void runNested() {
        System.out.println("--- nested ---");
        try (MemoryBudget outer = MemoryBudget.open("query", 64 * 1024)) {
            for (int i = 0; i < 5; i++) {
                new OptExpression("outer_" + i, null);
            }
            try (MemoryBudget inner = MemoryBudget.open("rewrite", 4 * 1024)) {
                for (int i = 0; i < 20; i++) {
                    new ColumnRefOperator(i, "c" + i);
                }
                System.out.println("inner consumed=" + inner.consumed());
            }
            System.out.println("outer consumed (after bubble)=" + outer.consumed());
        }
    }

    private static void runWithoutScope() {
        System.out.println("--- no scope (advice is a no-op) ---");
        for (int i = 0; i < 10; i++) {
            new OptExpression("noscope_" + i, null);
        }
        System.out.println("ok");
    }
}
