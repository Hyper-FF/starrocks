package com.starrocks.memory.budget;

import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.ResettableClassFileTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.utility.JavaModule;

import java.lang.instrument.Instrumentation;

public final class BudgetController {

    public static final String DEFAULT_PREFIX = "com.starrocks.sql.optimizer.";

    private static final Object LOCK = new Object();
    private static volatile ResettableClassFileTransformer TRANSFORMER;
    private static volatile Instrumentation INSTRUMENTATION;

    private BudgetController() {
    }

    public static boolean isAttached() {
        return TRANSFORMER != null;
    }

    /** Attach in-process (no -javaagent flag needed). Idempotent. */
    public static void attach(String prefix) {
        synchronized (LOCK) {
            if (TRANSFORMER != null) {
                return;
            }
            Instrumentation inst = ByteBuddyAgent.install();
            INSTRUMENTATION = inst;
            SizeTable.setInstrumentation(inst);
            TRANSFORMER = build(prefix == null ? DEFAULT_PREFIX : prefix, inst);
        }
    }

    /** Revert all transformations and stop intercepting future class loads. */
    public static void detach() {
        synchronized (LOCK) {
            ResettableClassFileTransformer t = TRANSFORMER;
            Instrumentation inst = INSTRUMENTATION;
            if (t == null || inst == null) {
                return;
            }
            t.reset(inst, AgentBuilder.RedefinitionStrategy.RETRANSFORMATION);
            TRANSFORMER = null;
        }
    }

    /** Used by both BudgetAgent.premain and BudgetController.attach. */
    static ResettableClassFileTransformer build(String prefix, Instrumentation inst) {
        return new AgentBuilder.Default()
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
    }

    static void registerStaticInstall(ResettableClassFileTransformer t, Instrumentation inst) {
        synchronized (LOCK) {
            TRANSFORMER = t;
            INSTRUMENTATION = inst;
        }
    }
}
