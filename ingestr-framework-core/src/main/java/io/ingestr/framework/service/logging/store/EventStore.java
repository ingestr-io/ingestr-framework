package io.ingestr.framework.service.logging.store;

import io.ingestr.framework.service.logging.LogEvent;

public interface EventStore {

    void save(LogEvent logEvent);

}
