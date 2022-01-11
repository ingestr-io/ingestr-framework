package io.ingestr.framework.service.workers;

import io.ingestr.framework.service.db.LoaderDefinitionServices;
import io.ingestr.framework.service.logging.LogContext;
import io.ingestr.framework.service.logging.LogEvent;
import io.ingestr.framework.service.logging.EventLogger;
import io.ingestr.framework.service.queue.QueueConsumer;
import io.ingestr.framework.service.queue.QueueExecutor;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Singleton
public class TaskExecutionThread extends Thread {
    private AtomicBoolean shutdown = new AtomicBoolean(false);
    private final EventLogger eventLogger;
    private QueueExecutor queueExecutor;
    private final QueueConsumer queueConsumer;
    private final LoaderDefinitionServices loaderDefinitionServices;
    private final ApplicationContext applicationContext;
    private final MeterRegistry meterRegistry;


    public TaskExecutionThread(
            EventLogger eventLogger, QueueConsumer queueConsumer,
            LoaderDefinitionServices loaderDefinitionServices,
            ApplicationContext applicationContext,
            MeterRegistry meterRegistry) {
        this.eventLogger = eventLogger;
        this.queueConsumer = queueConsumer;
        this.loaderDefinitionServices = loaderDefinitionServices;
        this.applicationContext = applicationContext;
        this.meterRegistry = meterRegistry;
    }

    void init() {
        eventLogger.log(
                LogEvent.info("Initialising the Task Workers ...")
                        .context(LogContext.WORKER)
                        .property("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                        .event("taskWorker.init.started"));

        this.queueExecutor = new QueueExecutor(
                this.queueConsumer,
                this.loaderDefinitionServices,
                this.applicationContext
        );

        this.queueExecutor.start();

        eventLogger.log(
                LogEvent.info("Initialising the Task Workers Finished")
                        .property("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                        .event("taskWorker.init.finished"));
    }


    public void shutdown() {
        log.info("Initiating shutdown of the Task Executions...");
        shutdown.set(true);
        queueExecutor.shutdown();
    }

    @Override
    public void run() {
        init();
        AtomicInteger runningTriggers = new AtomicInteger(0);

        Gauge runningGauge = Gauge
                .builder("ingestr.taskExecutor.running", () -> runningTriggers)
                .description("The total number of triggers that are running") // optional
                .tag("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                .register(meterRegistry);


        eventLogger.log(
                LogEvent.info("Starting the Task Execution Thread ...")
                        .context(LogContext.WORKER)
                        .property("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                        .event("ingestr.taskExecutor.running"));


        try {
            while (shutdown.get()) {
                runningTriggers.set(1);
                try {
                    Thread.sleep(500);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        } finally {
            runningTriggers.set(0);
        }
        eventLogger.log(
                LogEvent.info("Completed the Task Execution Thread")
                        .context(LogContext.WORKER)
                        .property("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                        .event("ingestr.taskExecutor.finished"));
    }
}
