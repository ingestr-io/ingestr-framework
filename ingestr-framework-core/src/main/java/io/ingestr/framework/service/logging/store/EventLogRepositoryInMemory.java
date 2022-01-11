package io.ingestr.framework.service.logging.store;

import io.ingestr.framework.service.logging.LogEvent;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.CompareToBuilder;

import javax.validation.Valid;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EventLogRepositoryInMemory implements EventLogRepository {
    private EventLogDB eventLogDB;


    public EventLogRepositoryInMemory(EventLogDB eventLogDB) {
        this.eventLogDB = eventLogDB;
    }


    @Override
    public PartitionExecutionSummary partitionExecutionSummary(@Valid PartitionExecutionSummaryQuery query) {
        EventLogResult results = query(EventLogQuery.builder()
                .loader(query.getLoader())
                .partition(query.getPartition())
                .from(query.getFrom())
                .to(query.getTo())
                .resultLimit(query.getResultLimit())
                .offset(query.getOffset())
                .build());


        PartitionExecutionSummary.PartitionExecutionSummaryBuilder b = PartitionExecutionSummary.builder()
                .partition(query.getPartition())
                .loader(query.getLoader());

        Map<String, PartitionExecutionSummaryItem> items = new HashMap<>();

        for (LogEvent logEvent : results.getLogEvents()) {
            //1. see if the item exists by task id
            if (!items.containsKey(logEvent.getTaskIdentifier())) {
                PartitionExecutionSummaryItem p = new PartitionExecutionSummaryItem();
                p.setTaskIdentifier(logEvent.getTaskIdentifier());
                items.put(logEvent.getTaskIdentifier(), p);
            }
            PartitionExecutionSummaryItem item = items.get(logEvent.getTaskIdentifier());
            item.setTimestamp(logEvent.getTimestamp());


            if (logEvent.getEvent().equalsIgnoreCase("ingestr.ingestion.success")) {
                item.setStatus(ExecutionStatus.Success);
            } else if (logEvent.getEvent().equalsIgnoreCase("ingestr.ingestion.failure")) {
                item.setStatus(ExecutionStatus.Fail);
            }
            if (logEvent.getProperties().containsKey("duration")) {
                try {
                    item.setDuration(Long.parseLong(
                            logEvent.getProperties().get("duration")
                    ));
                } catch (Exception e) {

                }
            }
            if (logEvent.getProperties().containsKey("offset")) {
                item.setOffset(logEvent.getProperties().get("offset"));
            }
        }
        List<PartitionExecutionSummaryItem> it = new ArrayList<>(items.values());
        Collections.sort(it, (o1, o2) -> new CompareToBuilder()
                .append(o1.getTimestamp(), o2.getTimestamp())
                .build());

        return b.items(it)
                .build();
    }

    @Override
    public EventLogResult query(EventLogQuery query) {
        Stream<EventLogDB.Item> ls = this.eventLogDB.getAll().stream();
        if (query.getLoader() != null) {
            ls = ls.filter(l -> StringUtils.equalsIgnoreCase(query.getLoader(), l.getLogEvent().getLoader()));
        }
        if (query.getPartition() != null) {
            ls = ls.filter((l -> StringUtils.equalsIgnoreCase(query.getPartition(),
                    l.getLogEvent().getProperties().getOrDefault("partition", ""))));
        }
        if (query.getFrom() != null) {
            ls = ls.filter(l -> l.getLogEvent().getTimestamp().equals(query.getFrom()) || l.getLogEvent().getTimestamp().isAfter(query.getFrom()));
        }
        if (query.getTo() != null) {
            ls = ls.filter(l -> l.getLogEvent().getTimestamp().equals(query.getTo()) || l.getLogEvent().getTimestamp().isBefore(query.getTo()));
        }
        if (query.getEvent() != null) {
            ls = ls.filter(l -> StringUtils.containsIgnoreCase(l.getLogEvent().getEvent(), query.getEvent()));
        }
        if (query.getContext() != null) {
            ls = ls.filter(l ->
                    StringUtils.equalsIgnoreCase(
                            l.getLogEvent().getContext() != null ? l.getLogEvent().getContext().name() : "", query.getContext()));
        }
        if (query.getTaskIdentifier() != null) {
            ls = ls.filter(l ->
                    StringUtils.equalsIgnoreCase(l.getLogEvent().getTaskIdentifier(),
                            query.getTaskIdentifier()));
        }

        List<LogEvent> res = ls.map(EventLogDB.Item::getLogEvent)
                .collect(Collectors.toList());

        if (query.getResultLimit() != null) {
            if (res.size() > query.getResultLimit()) {
                res = res.subList(0, query.getResultLimit());
            }
        }
        return EventLogResult.builder()
                .logEvents(res)
                .build();
    }

}
