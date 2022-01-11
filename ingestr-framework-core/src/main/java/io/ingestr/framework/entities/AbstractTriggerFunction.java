package io.ingestr.framework.entities;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractTriggerFunction implements TriggerFunction {
    private AtomicBoolean shutdown = new AtomicBoolean(false);

    @Override
    public void shutdown() {
        shutdown.set(true);
    }

    protected boolean isShutdown() {
        return shutdown.get();
    }
}
