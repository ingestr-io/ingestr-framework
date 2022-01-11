package io.ingestr.framework.entities;

public interface TriggerFunction {
    void shutdown();
    void run(TriggerContext context);
}
