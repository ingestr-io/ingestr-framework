package io.ingestr.framework.service.logging.store;

import io.ingestr.framework.service.logging.LogEvent;

import java.util.List;

public interface EventBus {
    void send(LogEvent logEvent);

    List<LogEvent> take();
}
