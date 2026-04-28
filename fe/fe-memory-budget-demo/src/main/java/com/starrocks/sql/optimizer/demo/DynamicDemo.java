package com.starrocks.sql.optimizer.demo;

import com.starrocks.memory.budget.BudgetController;
import com.starrocks.memory.budget.MemoryBudget;
import com.starrocks.memory.budget.MemoryBudgetExceededException;

public class DynamicDemo {

    public static void main(String[] args) throws Exception {
        scenario("before attach (no instrumentation)");

        BudgetController.attach(BudgetController.DEFAULT_PREFIX);
        scenario("attached + enabled");

        MemoryBudget.setEnabled(false);
        scenario("attached + soft-disabled");

        MemoryBudget.setEnabled(true);
        scenario("attached + re-enabled");

        BudgetController.detach();
        scenario("detached (bytecode reverted)");
    }

    private static void scenario(String label) {
        System.out.println("--- " + label + " ---");
        int created = 0;
        try (MemoryBudget b = MemoryBudget.open("planner", 256)) {
            for (int i = 0; i < 100; i++) {
                new OptExpression("op_" + i, null);
                created++;
            }
            System.out.println("ok: created=" + created + " consumed=" + b.consumed()
                    + " attached=" + BudgetController.isAttached()
                    + " enabled=" + MemoryBudget.isEnabled());
        } catch (MemoryBudgetExceededException e) {
            System.out.println("aborted after " + created + ": " + e.getMessage());
        }
    }
}
