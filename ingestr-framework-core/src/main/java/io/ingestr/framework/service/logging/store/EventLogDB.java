package io.ingestr.framework.service.logging.store;

import io.ingestr.framework.service.logging.LogEvent;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class EventLogDB {
    private List<Item> items = new ArrayList<>();
    private AtomicLong sequenceGenerator = new AtomicLong(0);

    public void add(LogEvent logEvent) {
        items.add(new Item(
                sequenceGenerator.getAndAdd(1l),
                logEvent
        ));
    }

    public List<Item> getAll() {
        return this.items;
    }

    @AllArgsConstructor
    @Data
    public static class Item {
        private long sequence;
        private LogEvent logEvent;
    }
}
