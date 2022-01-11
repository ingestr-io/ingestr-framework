package io.ingestr.framework.service.scheduler;

import io.ingestr.framework.service.db.LoaderDefinitionServices;
import io.ingestr.framework.service.scheduler.model.Schedule;
import io.ingestr.framework.service.scheduler.model.ScheduleListener;
import io.ingestr.framework.service.scheduler.model.ScheduledEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.time.Clock;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Singleton
public class Scheduler {
    private List<Schedule> schedules = new ArrayList<>();
    private AtomicInteger schedulesCount = new AtomicInteger(0);
    private Clock clock;
    private Thread schedulerThread = null;
    private ScheduleRunner scheduleRunner = null;
    private List<ScheduleListener> listeners = new ArrayList<>();

    private MeterRegistry meterRegistry;
    private LoaderDefinitionServices loaderDefinitionServices;

    @Inject
    public Scheduler(
            MeterRegistry meterRegistry,
            LoaderDefinitionServices loaderDefinitionServices
    ) {
        this.clock = Clock.systemUTC();
        this.meterRegistry = meterRegistry;
        this.loaderDefinitionServices = loaderDefinitionServices;
    }

    public Clock getClock() {
        return this.clock;
    }

    public List<Schedule> getSchedules() {
        return schedules;
    }

    public Optional<Schedule> getById(String scheduleId) {
        return schedules.stream()
                .filter(s -> StringUtils.equalsIgnoreCase(scheduleId, s.getIdentifier()))
                .findFirst();
    }

    public Optional<Schedule> setScheduleEnabled(String scheduleId, boolean enabled) {
        Optional<Schedule> s = getById(scheduleId);
        if (s.isPresent()) {
            log.info("Setting schedule {} to enabled={}", scheduleId, enabled);

            s.get().setEnabled(enabled);
        }
        return s;
    }

    /**
     * Initializes the Scheduler from the beginning
     */
    public void init() {
        schedules.clear();
        schedulesCount.set(0);
        listeners.clear();

        //setup the gauge
        Gauge
                .builder("ingestr.scheduler.schedules", () -> schedulesCount)
                .description("The total number of schedules managed by the scheduler") // optional
                .tag("loader", this.loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                .register(this.meterRegistry);
    }

    public void start() {
        log.info("Initiating the start of the Scheduler...");
        if (schedulerThread != null) {
            throw new IllegalStateException("Scheduler is already started!");
        }
        scheduleRunner = new ScheduleRunner(this);
        schedulerThread = new Thread(scheduleRunner, "ingestr-scheduler");
        schedulerThread.start();
        log.info("Completed the start of the Scheduler...");
    }

    public void join() throws InterruptedException {
        schedulerThread.join();
    }

    public void stop() {
        log.info("Initiating shutdown of the Scheduler...");

        if (scheduleRunner == null) {
            throw new IllegalStateException("Cannot stop a Scheduler that is not running!");
        }
        log.info("Initiating shutdown of Scheduler Thread...");
        scheduleRunner.shutdown();
        try {
            schedulerThread.join();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        log.info("Completed shutdown of Scheduler");
    }


    /**
     * Adds a Listener for Scheduled Events
     *
     * @param listener
     */
    public void addListener(ScheduleListener listener) {
        this.listeners.add(listener);
    }

    /**
     * Removes all Schedules related to a Particular Context
     *
     * @param context
     */
    public void removeScheduleByContext(String context) {
        synchronized (this) {
            List<Schedule> ss = new ArrayList<>();
            for (Schedule schedule : this.schedules) {
                if (StringUtils.equalsIgnoreCase(context, schedule.getContext())) {
                    ss.add(schedule);
                }
            }
            this.schedules.removeAll(ss);
        }
    }

    public void addSchedule(Schedule schedule) {
        log.debug("Adding Schedule - {}", schedule);
        synchronized (this) {
            this.schedules.add(schedule);
        }
    }

    void syncStats() {
        this.schedulesCount.set(this.schedules.size());
    }

    void executeSchedules() {
        Instant now = clock.instant().minusMillis(1);

        log.trace("Executing Schedules at now={}...", now);
        List<ScheduledEvent> events = new ArrayList<>();
        synchronized (this) {
            for (Schedule schedule : schedules) {
                Optional<ZonedDateTime> nextEx = schedule.getNextExecution();
                if (nextEx.isEmpty()) {
                    continue;
                }
                Instant nextExIns = nextEx.get().toInstant();

                if (nextExIns.isBefore(now) || nextExIns.equals(now)) {

                    Counter.builder("ingestr.scheduler.executions")
                            .tag("loader", this.loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                            .tag("schedule", schedule.getIdentifier())
                            .description("The total number of Executions done by the Scheduler")
                            .register(this.meterRegistry)
                            .increment();

                    events.add(new ScheduledEvent() {
                        @Override
                        public Instant executionTimestamp() {
                            return now;
                        }

                        @Override
                        public ZonedDateTime scheduledTimestamp() {
                            return nextEx.get();
                        }

                        @Override
                        public Schedule schedule() {
                            return schedule;
                        }

                        @Override
                        public void commitExecution() {
                            log.trace("Committing execution - {}", nextEx.get());
                            schedule.setLastExecution(nextEx.get());
                        }
                    });
                }
            }
        }
        if (!events.isEmpty()) {
            for (ScheduleListener listener : this.listeners) {
                try {
                    listener.notify(events);
                } catch (Throwable t) {
                    log.error(t.getMessage(), t);
                } finally {
                }
            }
            //commit the schedules
            for (ScheduledEvent e : events) {
                log.debug("Committing Scheduled Execution for schedule - {}", e.schedule());
                e.commitExecution();
            }
        }
    }

    private static class ScheduleRunner implements Runnable {
        private Scheduler scheduler;
        private boolean shutdown = false;

        private ScheduleRunner(Scheduler scheduler) {
            this.scheduler = scheduler;
        }

        void shutdown() {
            shutdown = true;
        }

        @Override
        public void run() {
            log.info("Starting ScheduleRunner Thread...");
            while (true) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
                if (shutdown) {
                    log.error("Shutting down ScheduleRunner...");
                    break;
                }
                scheduler.syncStats();
                scheduler.executeSchedules();
            }
            log.info("Stopped ScheduleRunner Thread");
        }

    }

    void setClock(Clock clock) {
        this.clock = clock;
    }
}
