package io.ingestr.framework.service.scheduler.model;

import java.util.List;

public interface ScheduleListener {

    void notify(List<ScheduledEvent> events);
}
