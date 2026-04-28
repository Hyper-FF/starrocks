package com.starrocks.memory.budget;

import net.bytebuddy.asm.Advice;

public class MemoryBudgetAdvice {

    // ByteBuddy forbids onThrowable on constructor exits, so this advice runs only on normal
    // completion. A MemoryBudgetExceededException thrown here propagates out of the new-expression.
    @Advice.OnMethodExit
    public static void onConstructorExit(@Advice.This Object self) {
        if (!MemoryBudget.ENABLED) {
            return;
        }
        MemoryBudget.consume(SizeTable.sizeOf(self));
    }
}
