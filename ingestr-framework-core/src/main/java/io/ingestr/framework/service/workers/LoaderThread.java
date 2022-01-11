package io.ingestr.framework.service.workers;

import io.ingestr.framework.entities.*;
import io.ingestr.framework.repositories.*;
import io.ingestr.framework.service.db.LoaderDefinitionServices;
import io.ingestr.framework.service.db.RepositoryService;
import io.ingestr.framework.service.gateway.CommandGateway;
import io.ingestr.framework.service.gateway.CommandHandler;
import io.ingestr.framework.service.gateway.CommandProcessor;
import io.ingestr.framework.service.gateway.commands.Command;
import io.ingestr.framework.service.gateway.commands.TriggerPartitionRegistrationCommand;
import io.ingestr.framework.service.gateway.exceptions.CommandGatewayException;
import io.ingestr.framework.service.gateway.model.CommandProcessListener;
import io.ingestr.framework.service.gateway.model.CommandSendRequest;
import io.ingestr.framework.service.logging.LogContext;
import io.ingestr.framework.service.logging.LogEvent;
import io.ingestr.framework.service.logging.EventLogger;
import io.ingestr.framework.service.queue.QueueProducer;
import io.ingestr.framework.service.queue.model.IngestPartitionQueueItem;
import io.ingestr.framework.service.scheduler.Scheduler;
import io.ingestr.framework.service.scheduler.model.Schedule;
import io.ingestr.framework.service.scheduler.model.ScheduledEvent;
import io.ingestr.framework.service.workers.lock.LoaderLock;
import io.ingestr.framework.service.workers.triggers.TriggersService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Singleton
public class LoaderThread extends Thread {
    private final Scheduler scheduler;
    private final EventLogger eventLogger;
    private final QueueProducer queueProducer;
    private final TriggersService triggersService;
    private final CommandGateway commandGateway;
    private final CommandProcessor commandProcessor;
    private final CommandHandler commandHandler;
    private final LoaderDefinitionServices loaderDefinitionServices;

    private final MeterRegistry meterRegistry;

    private final RepositoryService repositoryService;
    private final PartitionRepository partitionRepository;
    private final LoaderRepository loaderRepository;
    private final DataDescriptorRepository dataDescriptorRepository;
    private final IngestionRepository ingestionRepository;

    private final LoaderLock loaderLock;

    private AtomicBoolean shutdown = new AtomicBoolean(false);


    public LoaderThread(Scheduler scheduler,
                        EventLogger eventLogger,
                        QueueProducer queueProducer,
                        TriggersService triggersService,
                        CommandGateway commandGateway, CommandProcessor commandProcessor,
                        CommandHandler commandHandler,
                        LoaderDefinitionServices loaderDefinitionServices,
                        MeterRegistry meterRegistry,
                        RepositoryService repositoryService, PartitionRepository partitionRepository,
                        LoaderRepository loaderRepository,
                        DataDescriptorRepository dataDescriptorRepository,
                        IngestionRepository ingestionRepository,
                        LoaderLock loaderLock) {
        this.scheduler = scheduler;
        this.eventLogger = eventLogger;
        this.queueProducer = queueProducer;
        this.triggersService = triggersService;
        this.commandGateway = commandGateway;
        this.commandProcessor = commandProcessor;
        this.commandHandler = commandHandler;
        this.loaderDefinitionServices = loaderDefinitionServices;
        this.meterRegistry = meterRegistry;
        this.repositoryService = repositoryService;
        this.partitionRepository = partitionRepository;
        this.loaderRepository = loaderRepository;
        this.dataDescriptorRepository = dataDescriptorRepository;
        this.ingestionRepository = ingestionRepository;
        this.loaderLock = loaderLock;

    }


    public void shutdown() {
        log.info("Initiating shutdown of the Loader...");
        shutdown.set(true);
        scheduler.stop();
        commandProcessor.stop();
        queueProducer.shutdown();
        triggersService.stop();
    }

    public void init() {
        eventLogger.log(
                LogEvent.info("Initialising the Loader Thread ...")
                        .context(LogContext.MAIN)
                        .property("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                        .event("ingestr.loader.initStarted"));

        scheduler.init();

        commandProcessor.removeListeners();

        //setup the listeners
        commandProcessor.register(new CommandProcessListener() {
            @Override
            public void onRecordsProcessed(int count) {
                Counter.builder("ingestr.commands.recordCount")
                        .tag("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                        .description("Total number of records processed by the command processor")
                        .register(meterRegistry)
                        .increment(count);
            }

            @Override
            public void onCommandProcessed(Class<? extends Command> aClass) {
                Counter.builder("ingestr.commands.commandCount")
                        .tag("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                        .tag("command", aClass.getSimpleName())
                        .description("Total number of commands processed by the command processor")
                        .register(meterRegistry)
                        .increment();
            }
        });

        //begin listening for commands

        commandProcessor.start();

        eventLogger.log(
                LogEvent.info("Initialising the Loader Main Thread Finished")
                        .property("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                        .event("ingestr.loader.initComplete"));
    }

    @Override
    public void run() {
        log.info("Beginning the Main Loader Thread attempting lock to ensure single execution...");


        //try to get a lock to see if this thread should run as the main thread

        while (true) {
            if (loaderLock.getLock().tryLock()) {
                log.info("Loader Thread won the lock and will begin execution");
                break;
            }
            try {
                Thread.sleep(3000);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }


        init();

        log.info("Started Running of the Main Loader Thread...");

        //1. Update the Descriptor Definitions

        loaderRepository.save(Loader.builder()
                .identifier(loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                .name(loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                .build());


        for (DataDescriptor dd : loaderDefinitionServices.getLoaderDefinition().getDataDescriptors()) {
            dataDescriptorRepository.save(dd);
        }

        //2. Initialize the Scheduler
        for (Ingestion ingestion : loaderDefinitionServices.getLoaderDefinition().getIngestions()) {
            ingestionRepository.save(ingestion);
            try {
                syncSchedules(ingestion);
            } catch (Throwable t) {
                eventLogger.log(
                        LogEvent.error("Error Synchronising the Schedules for ingestion={}", ingestion)
                                .event("ingestr.loader.configError")
                                .context(LogContext.MAIN)
                                .body(t.getMessage(), t)
                                .property("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                                .property("ingestion", ingestion.getIdentifier())
                );
                log.error(t.getMessage(), t);
            }
        }

        for (DataDescriptor dd : loaderDefinitionServices.getLoaderDefinition().getDataDescriptors()) {
            syncPartitionRegistrator(dd);

            List<Partition> dds = partitionRepository.findByDataDescriptorIdentifier(dd.getIdentifier());
            if (dds.isEmpty()) {
                log.info("No partitions found for data descriptor={}", dd.getIdentifier());

                try {
                    commandGateway.send(CommandSendRequest.builder()
                            .command(new TriggerPartitionRegistrationCommand(dd.getIdentifier()))
                            .build());
                } catch (CommandGatewayException cge) {
                    log.error(cge.getMessage(), cge);
                }
            }
        }

        //3. Start the scheduler
        scheduler.start();

        scheduler.addListener(events -> {

            //Setup a hash to prevent double execution of the same ingestion
            Set<String> scheduledIngestionIds = new HashSet<>();

            for (ScheduledEvent event : events) {
                String[] ctx = StringUtils.split(event.schedule().getContext(), ":");

                if (StringUtils.equalsIgnoreCase(ctx[0], "ingestion")) {
                    Ingestion ingestion = loaderDefinitionServices.getLoaderDefinition().findByIngestionId(ctx[1])
                            .orElseThrow(() -> new IllegalArgumentException("Could not find Ingestion for id " + ctx[1]));
                    if (!scheduledIngestionIds.contains(ingestion.getIdentifier())) {
                        executeIngestion(
                                event.executionTimestamp(),
                                event.scheduledTimestamp(),
                                loaderDefinitionServices.getLoaderDefinition().findByScheduleId(event.schedule().getIdentifier())
                                        .orElseThrow(() -> new IllegalArgumentException("Could not find Schedule for Id " + event.schedule().getIdentifier())),
                                ingestion
                        );
                        //now update the set of ingestion ids
                        scheduledIngestionIds.add(ingestion.getIdentifier());
                    }
                } else if (StringUtils.equalsIgnoreCase(ctx[0], "partitionRegistrator")) {
                    try {
                        commandGateway.send(CommandSendRequest.builder()
                                .command(new TriggerPartitionRegistrationCommand(ctx[1]))
                                .build());
                    } catch (CommandGatewayException cge) {
                        log.error(cge.getMessage(), cge);
                    }
                }
            }
        });

        triggersService.run(queueProducer);


        //now wait for the Scheduler to die
        while (!shutdown.get()) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {
            }
        }

        //wait for scheduler to finish
        try {
            scheduler.join();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        eventLogger.log(
                LogEvent.error("Finished running the Loader Main Thread")
                        .property("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                        .event("ingestr.loader.stopped"));
    }

    void syncPartitionRegistrator(DataDescriptor dataDescriptor) {
        if (dataDescriptor.getPartitionRegister() == null) {
            return;
        }
        //if no schedule dont bother
        if (dataDescriptor.getPartitionRegister().getSchedule() == null) {
            return;
        }
        String context = "partitionRegistrator:" + dataDescriptor.getIdentifier();

        scheduler.removeScheduleByContext(context);

        scheduler.addSchedule(
                Schedule.newSchedule(
                        dataDescriptor.getIdentifier() + "-partitionRegistrator",
                        context,
                        dataDescriptor.getPartitionRegister().getSchedule(),
                        ZoneId.of(dataDescriptor.getPartitionRegister().getScheduleTimeZone()),
                        Clock.systemUTC()
                )
        );
    }

    void syncSchedules(Ingestion ingestion) {
        //remove all existing schedules related to the ingestion
        String context = "ingestion:" + ingestion.getIdentifier();
        scheduler.removeScheduleByContext(context);

        List<Schedule> schedules = new ArrayList<>();

        //1. Create all the schedules in the 1st step
        for (IngestionSchedule ingestionSchedule : ingestion.getIngestionSchedules()) {
            try {
                schedules.add(Schedule.newSchedule(
                        ingestionSchedule.getIdentifier(),
                        context,
                        ingestionSchedule.getSchedule(),
                        ZoneId.of(ingestionSchedule.getScheduleTimeZone()),
                        Clock.systemUTC()
                ));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Could not register Ingestion Schedule '" + ingestionSchedule.getIdentifier() + "' : " + e.getMessage(), e);
            }
        }

        //2. Once all the schedules are created, lets add them to the scheduler to ensure that all the schedule definitions are valid before adding them
        for (Schedule schedule : schedules) {
            scheduler.addSchedule(
                    schedule
            );
        }
    }

    void executeIngestion(
            Instant executionTimestamp,
            ZonedDateTime scheduledExecution,
            IngestionSchedule ingestionSchedule,
            Ingestion ingestion) {
        log.debug("Executing Scheduled Ingestion - {}", ingestion);

        //find all relevant partitions
        List<Partition> partitions = partitionRepository.findByDataDescriptorIdentifier(ingestion.getDataDescriptorIdentifier());
        Stream<Partition> stream = partitions.stream()
                .filter(p -> p.getEnabled() == null || p.getEnabled() == Boolean.TRUE);

        if (ingestionSchedule.getPartitionFilters() != null) {
            for (PartitionFilter filt : ingestionSchedule.getPartitionFilters()) {
                stream = stream.filter(filt::apply);
            }
        }
        List<Partition> ps = stream.collect(Collectors.toList());

        if (ps.isEmpty()) {
            log.warn("Found 0 partitions out of {} for ingestion '{}'",
                    partitions.size(),
                    ingestion.getIdentifier());
        }

        for (Partition partition : ps) {
            log.debug("Queueing Execution of partition - {}", ps);
            queueProducer.queue(IngestPartitionQueueItem.builder()
                    .ingestionIdentifier(ingestion.getIdentifier())
                    .partition(partition)
                    .scheduleIdentifier(ingestionSchedule.getIdentifier())
                    .scheduledExecution(scheduledExecution)
                    .executionTimestamp(executionTimestamp)
                    .build());
        }
    }
}
