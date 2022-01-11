package io.ingestr.framework.service.logging.store;

public interface EventConsumer {
    boolean isRunning();

    void start();

    void stop();
}
