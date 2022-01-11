package io.ingestr.framework.api;

import io.ingestr.framework.service.logging.store.EventLogRepository;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.time.Instant;

@Controller("/event-logs")
@Slf4j
public class EventLoggerController {
    private EventLogRepository eventLogRepository;

    public EventLoggerController(EventLogRepository eventLogRepository) {
        this.eventLogRepository = eventLogRepository;
    }

    @Get(produces = MediaType.APPLICATION_JSON)
    public EventLogRepository.EventLogResult findAll(
            @QueryValue("loader") @Nullable String loader,
            @Nullable @QueryValue("partition") String partition,
            @Nullable @QueryValue("context") String context,
            @Nullable @QueryValue("taskIdentifier") String taskIdentifier,
            @Nullable @QueryValue("event") String event,
            @Nullable @QueryValue("from") Instant from,
            @Nullable @QueryValue("to") Instant to,
            @Nullable @QueryValue("size") Integer size,
            @Nullable @QueryValue("offset") String offset
    ) {

        EventLogRepository.EventLogResult res = eventLogRepository.query(EventLogRepository.EventLogQuery.builder()
                .from(Instant.now().minusSeconds(100))
                .loader(loader)
                .taskIdentifier(taskIdentifier)
                .context(context)
                .event(event)
                .partition(partition)
                .resultLimit(size == null ? 10 : size)
                .offset(offset)
                .build());
        return res;
    }
}

