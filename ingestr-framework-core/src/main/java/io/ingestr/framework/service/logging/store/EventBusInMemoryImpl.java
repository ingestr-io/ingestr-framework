package io.ingestr.framework.service.logging.store;

import io.ingestr.framework.service.logging.LogEvent;

import java.util.Arrays;
import java.util.List;
import java.util.Queue;

public class EventBusInMemoryImpl implements EventBus {
    private Queue<LogEvent> queue;

    public EventBusInMemoryImpl(Queue<LogEvent> queue) {
        this.queue = queue;
    }

    @Override
    public void send(LogEvent logEvent) {
        queue.add(logEvent);
    }

    @Override
    public List<LogEvent> take() {
        return Arrays.asList(queue.poll());
    }

}
