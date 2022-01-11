package io.ingestr.framework.service.workers.triggers;

import io.ingestr.framework.repositories.PartitionRepository;
import io.ingestr.framework.service.db.LoaderDefinitionServices;
import io.ingestr.framework.service.logging.EventLogger;
import io.ingestr.framework.service.queue.QueueProducer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Singleton
public class TriggersService {
    private final MeterRegistry meterRegistry;
    private final LoaderDefinitionServices loaderDefinitionServices;
    private final EventLogger eventLogger;
    private final PartitionRepository partitionRepository;


    private List<TriggerThread> triggerFunctions;

    private ExecutorService triggersExecutors;
    private AtomicInteger runningTriggers;
    private AtomicInteger totalTriggers;
    private AtomicInteger running = new AtomicInteger(-1);
    private Thread gaugeSync;

    @Inject
    public TriggersService(MeterRegistry meterRegistry, LoaderDefinitionServices loaderDefinitionServices, EventLogger eventLogger, PartitionRepository partitionRepository) {
        this.meterRegistry = meterRegistry;
        this.loaderDefinitionServices = loaderDefinitionServices;
        this.eventLogger = eventLogger;
        this.partitionRepository = partitionRepository;
        this.triggerFunctions = new ArrayList<>();
    }


    public void run(QueueProducer taskQueue) {
        if (running.get() == 0) {
            throw new IllegalStateException("Trigger Service is already in a Running State!");
        }
        log.info("Starting up Trigger Service...");
        running.set(0);
        runningTriggers = new AtomicInteger(0);
        totalTriggers = new AtomicInteger(0);


        Gauge totalGauge = Gauge
                .builder("ingestr.triggers.total", () -> totalTriggers)
                .description("The total number of triggers that have been configured") // optional
                .tag("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                .register(meterRegistry);


        Gauge runningGauge = Gauge
                .builder("ingestr.triggers.running", () -> runningTriggers)
                .description("The total number of triggers that are running") // optional
                .tag("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                .register(meterRegistry);

        //setup a thread to sync the gauges
        gaugeSync = new Thread(() -> {
            try {
                while (running.get() == 0) {
                    Thread.sleep(500);
                    int runningCount = 0;
                    for (TriggerThread tf : triggerFunctions) {
                        if (tf.isRunning()) {
                            runningCount += 1;
                        }
                    }
                    runningTriggers.set(runningCount);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        });
        gaugeSync.start();

        //re-initialise the list in case we run, stop, run (we want an empty list)
        this.triggerFunctions = new ArrayList<>();

        //4. Setups the triggers
        loaderDefinitionServices.getLoaderDefinition()
                .getIngestions().forEach(i -> i.getTriggers().forEach(tr -> {
                    this.triggerFunctions.add(new TriggerThread(i, tr,
                            taskQueue,
                            eventLogger,
                            meterRegistry,
                            loaderDefinitionServices,
                            partitionRepository));
                    this.totalTriggers.incrementAndGet();
                }
        ));


        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("trigger-service-thread-%d")
                .daemon(true)
                .priority(Thread.NORM_PRIORITY)
                .build();

        this.triggersExecutors = Executors.newFixedThreadPool(this.triggerFunctions.size(), factory);
        for (TriggerThread tf : this.triggerFunctions) {
            log.info("Initialised Trigger - {}", tf.getTrigger().getName());
            this.triggersExecutors.submit(tf);
        }

    }

    public void stop() {
        if (running.get() == 1) {
            throw new IllegalStateException("Already in a Stopped State!");
        }
        log.info("Initiating shutdown of the Trigger Service...");
        running.set(-1);

        for (TriggerThread tf : this.triggerFunctions) {
            tf.shutdown();
        }

        triggersExecutors.shutdown();

        try {
            triggersExecutors.awaitTermination(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


    public List<TriggerThread> getTriggerFunctions() {
        return triggerFunctions;
    }


    public boolean hasInitialised() {
        return running.get() > -1;
    }

    public boolean isRunning() {
        return running.get() == 0;
    }

    public boolean isShutdown() {
        return running.get() == 1;
    }
}
