package io.ingestr.framework.service.logging.store;

import io.ingestr.framework.service.logging.LogEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventStoreInMemoryImpl implements EventStore {
    private EventLogDB eventLogDB;

    public EventStoreInMemoryImpl(EventLogDB eventLogDB) {
        this.eventLogDB = eventLogDB;
    }

    @Override
    public void save(LogEvent logEvent) {
        this.eventLogDB.add(logEvent);
    }
}
