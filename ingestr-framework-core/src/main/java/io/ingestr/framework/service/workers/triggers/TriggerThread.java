package io.ingestr.framework.service.workers.triggers;

import io.ingestr.framework.IngestrFunctions;
import io.ingestr.framework.entities.*;
import io.ingestr.framework.repositories.PartitionRepository;
import io.ingestr.framework.service.db.LoaderDefinitionServices;
import io.ingestr.framework.service.logging.LogContext;
import io.ingestr.framework.service.logging.LogEvent;
import io.ingestr.framework.service.logging.EventLogger;
import io.ingestr.framework.service.queue.QueueProducer;
import io.ingestr.framework.service.queue.model.IngestPartitionQueueItem;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class TriggerThread extends Thread {
    private final Trigger trigger;
    private final Ingestion ingestion;
    private TriggerFunction triggerFunction;
    private AtomicBoolean running = new AtomicBoolean();
    private QueueProducer taskQueue;
    private EventLogger eventLogger;

    private MeterRegistry meterRegistry;
    private LoaderDefinitionServices loaderDefinitionServices;
    private PartitionRepository partitionRepository;

    public TriggerThread(Ingestion ingestion,
                         Trigger trigger,
                         QueueProducer taskQueue,
                         EventLogger eventLogger,
                         MeterRegistry meterRegistry,
                         LoaderDefinitionServices loaderDefinitionServices,
                         PartitionRepository partitionRepository) {
        this.trigger = trigger;
        this.ingestion = ingestion;
        this.taskQueue = taskQueue;
        this.meterRegistry = meterRegistry;
        this.loaderDefinitionServices = loaderDefinitionServices;
        this.partitionRepository = partitionRepository;
        //set the name of the thread
        setName("ingestr-trigger-" + trigger.getName());
        this.eventLogger = eventLogger;
    }

    public void shutdown() {
        triggerFunction.shutdown();
        eventLogger.log(
                LogEvent.info("Trigger Thread Stopped Execution")
                        .event("ingestr.triggerExecutions.stopped")
                        .context(LogContext.TRIGGER)
                        .property("trigger", trigger.getName())
                        .property("ingestion", ingestion.getIdentifier())
        );
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public Ingestion getIngestion() {
        return ingestion;
    }

    @Override
    public void run() {
        this.triggerFunction = trigger.getTrigger().get();
        this.running.set(true);
        eventLogger.log(
                LogEvent.info("Trigger Thread Began Execution")
                        .context(LogContext.TRIGGER)
                        .event("ingestr.triggerExecutions.started")
                        .property("trigger", trigger.getName())
                        .property("ingestion", ingestion.getIdentifier())
        );
        try {

            TriggerContext tc = new TriggerContext() {
                @Override
                public List<Partition> partitions() {
                    Stream<Partition> sp = partitionRepository
                            .findByDataDescriptorIdentifier(ingestion.getDataDescriptorIdentifier()).stream();

                    if (trigger.getPartitionFilters() != null && !trigger.getPartitionFilters().isEmpty()) {
                        for (PartitionFilter pf : trigger.getPartitionFilters()) {
                            sp = sp.filter(pf::apply);
                        }
                    }
                    return sp.collect(Collectors.toList());
                }

                @Override
                public void notify(TriggeredPartition triggeredPartition) {
                    triggerIngestion(ingestion, triggeredPartition);
                }

                @Override
                public void notify(TriggeredPartition.TriggeredPartitionBuilder triggeredPartition) {
                    notify(triggeredPartition.build());
                }
            };
            try {
                triggerFunction.run(tc);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                eventLogger.log(
                        LogEvent.error("Trigger Failed Execution")
                                .context(LogContext.TRIGGER)
                                .body("Triggered Function Failed Execution - " + e.getMessage(), e)
                                .event("ingestr.triggerExecutions.failed")
                                .property("trigger", trigger.getName())
                                .property("function", triggerFunction.getClass().getSimpleName())
                                .property("ingestion", ingestion.getIdentifier())

                );
                Counter.builder("ingestr.triggerExecutions.failed")
                        .tag("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                        .tag("trigger", trigger.getName())
                        .tag("ingestion", ingestion.getIdentifier())
                        .tag("dataDescriptor", ingestion.getDataDescriptorIdentifier())
                        .description("Counter of the Failed Triggers")
                        .register(meterRegistry)
                        .increment();
            }
        } finally {
            running.set(false);
        }
    }


    void triggerIngestion(
            Ingestion ingestion,
            TriggeredPartition triggeredPartition
    ) {
        log.debug("Triggering Ingestion={} partition={}",
                ingestion.getIdentifier(),
                triggeredPartition);

        //Construct Partition based on the composite keys present in the Triggered Partition
        final Partition targetPartition = IngestrFunctions.newPartition(triggeredPartition.getPartitionEntries())
                .dataDescriptorIdentifier(ingestion.getDataDescriptorIdentifier())
                .build();

        //lookup the database to see if we have an existing partition
        Optional<Partition> existingPartitionOpt = partitionRepository.findByKey(targetPartition.getKey());

        if (existingPartitionOpt.isEmpty()) {
            eventLogger.log(
                    LogEvent.warn("Skipping execution of unregistered Partition")
                            .context(LogContext.TRIGGER)
                            .event("ingestr.triggerExecutions.skipped")
                            .property("trigger", trigger.getName())
                            .property("partition", targetPartition.toString())
                            .property("ingestion", ingestion.getIdentifier())
            );
            Counter.builder("ingestr.triggerExecutions.skipped")
                    .tag("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                    .tag("trigger", trigger.getName())
                    .tag("ingestion", ingestion.getIdentifier())
                    .tag("dataDescriptor", ingestion.getDataDescriptorIdentifier())
                    .description("Counter of the Skipped Triggered Ingestions")
                    .register(meterRegistry)
                    .increment();
            return;
        }

        Partition existingPartition = existingPartitionOpt.get();
        if (existingPartition.getDeleted() == Boolean.TRUE) {
            eventLogger.log(
                    LogEvent.warn("Skipping execution of deleted Partition")
                            .context(LogContext.TRIGGER)
                            .event("ingestr.triggerExecutions.skipped")
                            .property("trigger", trigger.getName())
                            .property("partition", targetPartition.toString())
                            .property("ingestion", ingestion.getIdentifier())
            );
            Counter.builder("ingestr.triggerExecutions.skipped")
                    .tag("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                    .tag("trigger", trigger.getName())
                    .tag("ingestion", ingestion.getIdentifier())
                    .tag("dataDescriptor", ingestion.getDataDescriptorIdentifier())
                    .description("Counter of the Skipped Triggered Ingestions")
                    .register(meterRegistry)
                    .increment();
            return;
        }

        if (existingPartition.getEnabled() == Boolean.FALSE) {
            eventLogger.log(
                    LogEvent.info("Skipping execution of disabled Partition")
                            .context(LogContext.TRIGGER)
                            .event("ingestr.triggerExecutions.skipped")
                            .property("trigger", trigger.getName())
                            .property("partition", targetPartition.toString())
                            .property("ingestion", ingestion.getIdentifier())
            );
            Counter.builder("ingestr.triggerExecutions.skipped")
                    .tag("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                    .tag("trigger", trigger.getName())
                    .tag("ingestion", ingestion.getIdentifier())
                    .tag("dataDescriptor", ingestion.getDataDescriptorIdentifier())
                    .description("Counter of the Skipped Triggered Ingestions")
                    .register(meterRegistry)
                    .increment();
            return;
        }

        log.debug("Queueing Execution of Triggered Partition - {}", targetPartition);

        taskQueue.queue(IngestPartitionQueueItem.builder()
                .ingestionIdentifier(ingestion.getIdentifier())
                .partition(targetPartition)
                .executionTimestamp(Instant.now())
                .triggerName(trigger.getName())
                .properties(triggeredPartition.getProperties())
                .build());

        Counter.builder("ingestr.triggerExecutions.success")
                .tag("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                .tag("trigger", trigger.getName())
                .tag("ingestion", ingestion.getIdentifier())
                .tag("dataDescriptor", ingestion.getDataDescriptorIdentifier())
                .description("Counter of the Successfully Triggered Ingestions")
                .register(meterRegistry)
                .increment();
    }

    public boolean isRunning() {
        return running.get();
    }
}
