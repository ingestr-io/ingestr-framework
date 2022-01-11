package io.ingestr.framework.service.logging.store;

import io.ingestr.framework.service.logging.LogEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class EventConsumerDefault implements EventConsumer {
    private EventBus eventBus;
    private EventStore eventStore;
    private AtomicBoolean shutdown = new AtomicBoolean(false);
    private Thread eventConsumerThread;

    public EventConsumerDefault(EventBus eventBus, EventStore eventStore) {
        this.eventBus = eventBus;
        this.eventStore = eventStore;
    }

    @Override
    public void start() {
        log.info("Starting the Event Consumer Thread...");
        eventConsumerThread = new Thread(
                new EventConsumerRunnable(shutdown, this),
                "event-consumer-thread");
        shutdown.set(false);
        eventConsumerThread.start();
    }

    @Override
    public void stop() {
        log.info("Stopping the Event Consumer Thread...");

        shutdown.set(true);
        try {
            eventConsumerThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

    }

    @Override
    public boolean isRunning() {
        return !shutdown.get();
    }

    void doProcess() {
        for (LogEvent logEvent : eventBus.take()) {
            if (logEvent == null) {
                try {
                    Thread.sleep(10);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            } else {
                log.debug("Processing log event from bus - {}", logEvent);
                eventStore.save(logEvent);
            }
        }
    }

    private static class EventConsumerRunnable implements Runnable {
        private AtomicBoolean shutdown;
        private EventConsumerDefault eventConsumerDefault;

        public EventConsumerRunnable(AtomicBoolean shutdown, EventConsumerDefault eventConsumerDefault) {
            this.shutdown = shutdown;
            this.eventConsumerDefault = eventConsumerDefault;
        }

        @Override
        public void run() {
            while (!shutdown.get()) {
                eventConsumerDefault.doProcess();
            }
        }
    }
}
