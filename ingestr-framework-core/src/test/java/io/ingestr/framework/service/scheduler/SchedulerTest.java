package io.ingestr.framework.service.scheduler;

import io.ingestr.framework.entities.LoaderDefinition;
import io.ingestr.framework.service.db.LoaderDefinitionServices;
import io.ingestr.framework.service.scheduler.model.Schedule;
import io.ingestr.framework.service.scheduler.model.ScheduledEvent;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@MicronautTest
class SchedulerTest {
    @Inject
    private Scheduler scheduler;
    @Inject
    private LoaderDefinitionServices loaderDefinitionServices;

    @Test
    void shouldSchedule() {
        loaderDefinitionServices.setLoaderDefinition(new LoaderDefinition("test", "v1"));

        Clock clock = Clock.fixed(Instant.ofEpochSecond(1), ZoneId.of("UTC"));
        AtomicReference<List<ScheduledEvent>> s = new AtomicReference<>();
        scheduler.setClock(clock);

        //given a scheduler
        //with a schedule
        scheduler.addSchedule(
                Schedule.newSchedule(
                        "sch1",
                        "ing",
                        "0 5 * * * ? *",
                        ZoneId.of("UTC"),
                        clock
                )
        );
        scheduler.addListener(events -> s.set(events));

        //and we set the clock to the 5 seconds mark
        clock = Clock.fixed(Instant.ofEpochSecond(0).plus(5, ChronoUnit.MINUTES), ZoneId.of("UTC"));
        scheduler.setClock(clock);

        //when we executed the schedules
        scheduler.executeSchedules();

        assertEquals(1, s.get().size());
        assertEquals(clock.instant().atZone(ZoneId.of("UTC")), s.get().get(0).schedule().getLastExecution());

        //when we executed the schedules again without commitment, there should be no change
        scheduler.executeSchedules();

        assertEquals(1, s.get().size());
        assertEquals(clock.instant().atZone(ZoneId.of("UTC")), s.get().get(0).schedule().getLastExecution());

    }

    @Test
    void shouldStartScheduler() throws InterruptedException {
        loaderDefinitionServices.setLoaderDefinition(new LoaderDefinition("test", "v1"));

        AtomicReference<List<ScheduledEvent>> s = new AtomicReference<>();
        AtomicInteger counter = new AtomicInteger(0);

        //with a schedule
        scheduler.addSchedule(
                Schedule.newSchedule(
                        "sch1",
                        "ing",
                        "*/2 * * * * ? *",
                        ZoneId.of("UTC")
                )
        );
        scheduler.addListener(events -> {
            s.set(events);
            counter.incrementAndGet();
        });

        scheduler.start();

        Thread.sleep(5_000);

        scheduler.stop();

        assertEquals(3, counter.get());

    }

}