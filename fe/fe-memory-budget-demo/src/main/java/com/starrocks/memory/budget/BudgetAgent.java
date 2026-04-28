package com.starrocks.memory.budget;

import net.bytebuddy.agent.builder.ResettableClassFileTransformer;

import java.lang.instrument.Instrumentation;

public class BudgetAgent {

    public static void premain(String args, Instrumentation inst) {
        install(args, inst);
    }

    public static void agentmain(String args, Instrumentation inst) {
        install(args, inst);
    }

    private static void install(String args, Instrumentation inst) {
        SizeTable.setInstrumentation(inst);
        String prefix = (args == null || args.isEmpty()) ? BudgetController.DEFAULT_PREFIX : args;
        ResettableClassFileTransformer t = BudgetController.build(prefix, inst);
        BudgetController.registerStaticInstall(t, inst);
        System.err.println("[memory-budget-agent] installed for prefix=" + prefix);
    }
}
