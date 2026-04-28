package com.starrocks.memory.budget;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.utility.JavaModule;

import java.lang.instrument.Instrumentation;

public class BudgetAgent {

    private static final String DEFAULT_PREFIX = "com.starrocks.sql.optimizer.";

    public static void premain(String args, Instrumentation inst) {
        install(args, inst);
    }

    public static void agentmain(String args, Instrumentation inst) {
        install(args, inst);
    }

    private static void install(String args, Instrumentation inst) {
        SizeTable.setInstrumentation(inst);
        String prefix = (args == null || args.isEmpty()) ? DEFAULT_PREFIX : args;
        new AgentBuilder.Default()
                .disableClassFormatChanges()
                .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
                .with(AgentBuilder.TypeStrategy.Default.REDEFINE)
                .with(new AgentBuilder.Listener.Adapter() {
                    @Override
                    public void onError(String typeName, ClassLoader cl, JavaModule m, boolean loaded, Throwable t) {
                        System.err.println("[memory-budget-agent] error on " + typeName + ": " + t);
                    }
                })
                .ignore(ElementMatchers.nameStartsWith("net.bytebuddy.")
                        .or(ElementMatchers.nameStartsWith("com.starrocks.memory.budget.")))
                .type(ElementMatchers.nameStartsWith(prefix))
                .transform((builder, type, classLoader, module, protectionDomain) ->
                        builder.visit(Advice.to(MemoryBudgetAdvice.class)
                                .on(ElementMatchers.isConstructor())))
                .installOn(inst);
        System.err.println("[memory-budget-agent] installed for prefix=" + prefix);
    }
}
