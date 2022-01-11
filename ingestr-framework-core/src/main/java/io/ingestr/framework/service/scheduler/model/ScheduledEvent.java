package io.ingestr.framework.service.scheduler.model;

import java.time.Instant;
import java.time.ZonedDateTime;

public interface ScheduledEvent {
    /**
     * The actual timestamp this schedule event was triggered
     *
     * @return
     */
    Instant executionTimestamp();

    /**
     * The scheduled ZoneDateTime this schedule event was scheduled to run
     *
     * @return
     */
    ZonedDateTime scheduledTimestamp();

    Schedule schedule();

    void commitExecution();
}
