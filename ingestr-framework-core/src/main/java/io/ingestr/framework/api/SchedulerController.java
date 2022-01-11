package io.ingestr.framework.api;


import io.ingestr.framework.service.scheduler.Scheduler;
import io.ingestr.framework.service.scheduler.model.Schedule;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.runtime.context.scope.Refreshable;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Controller("/scheduler")
@Requires(beans = {Scheduler.class})
@Slf4j
@Refreshable
public class SchedulerController {
    @Inject
    private Scheduler scheduler;

    public SchedulerController(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Get(produces = MediaType.APPLICATION_JSON)
    public List<Schedule> findAll() {
        return scheduler.getSchedules();
    }

    @Get(uri = "/{id}")
    public Schedule findById(
            @PathVariable("id") String scheduleId
    ) {
        return scheduler.getById(scheduleId)
                .orElse(null);
    }

    @Get(uri = "/{id}/disable")
    public Schedule disable(
            @PathVariable("id") String scheduleId
    ) {
        return scheduler.setScheduleEnabled(scheduleId, false)
                .orElse(null);
    }

    @Get(uri = "/{id}/enable")
    public Schedule enable(
            @PathVariable("id") String scheduleId
    ) {
        return scheduler.setScheduleEnabled(scheduleId, true)
                .orElse(null);

    }
}