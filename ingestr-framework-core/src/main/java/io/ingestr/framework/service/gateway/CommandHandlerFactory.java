package io.ingestr.framework.service.gateway;

import io.ingestr.framework.entities.Partition;
import io.ingestr.framework.repositories.DataDescriptorRepository;
import io.ingestr.framework.repositories.IngestionRepository;
import io.ingestr.framework.repositories.LoaderRepository;
import io.ingestr.framework.repositories.PartitionRepository;
import io.ingestr.framework.service.db.LoaderDefinitionServices;
import io.ingestr.framework.service.gateway.commands.PartitionDisableCommand;
import io.ingestr.framework.service.gateway.commands.PartitionEnableCommand;
import io.ingestr.framework.service.gateway.commands.PartitionTraceCommand;
import io.ingestr.framework.service.gateway.commands.TriggerPartitionRegistrationCommand;
import io.ingestr.framework.service.gateway.model.CommandHandlerTask;
import io.ingestr.framework.service.logging.EventLogger;
import io.ingestr.framework.service.workers.tasks.PartitionRegistrationTask;
import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Factory
public class CommandHandlerFactory {

    private final PartitionRepository partitionRepository;
    private final LoaderRepository loaderRepository;
    private final DataDescriptorRepository dataDescriptorRepository;
    private final IngestionRepository ingestionRepository;
    private final MeterRegistry meterRegistry;
    private final EventLogger eventLogger;
    private final LoaderDefinitionServices loaderDefinitionServices;


    public CommandHandlerFactory(
            PartitionRepository partitionRepository,
            LoaderRepository loaderRepository,
            DataDescriptorRepository dataDescriptorRepository,
            IngestionRepository ingestionRepository,
            MeterRegistry meterRegistry,
            EventLogger eventLogger,
            LoaderDefinitionServices loaderDefinitionServices) {
        this.partitionRepository = partitionRepository;
        this.loaderRepository = loaderRepository;
        this.dataDescriptorRepository = dataDescriptorRepository;
        this.ingestionRepository = ingestionRepository;
        this.meterRegistry = meterRegistry;
        this.eventLogger = eventLogger;
        this.loaderDefinitionServices = loaderDefinitionServices;
    }

    @Singleton
    public CommandHandler commandHandler() {
        CommandHandler commandHandler = new CommandHandlerImpl();

        commandHandler.register(
                TriggerPartitionRegistrationCommand.class,
                () -> (CommandHandlerTask<TriggerPartitionRegistrationCommand>) command -> {
                    return new PartitionRegistrationTask(
                            partitionRepository,
                            meterRegistry,
                            eventLogger,
                            loaderDefinitionServices.getLoaderDefinition()
                                    .findByDescriptorId(command.getDataDescriptorIdentifier())
                                    .orElseThrow(() -> new IllegalArgumentException("Could not find Data Descriptor for id " +
                                            command.getDataDescriptorIdentifier())),
                            loaderDefinitionServices
                    );
                });


        //Command to disable the partition
        commandHandler.register(
                PartitionDisableCommand.class,
                () -> (CommandHandlerTask<PartitionDisableCommand>) command -> {
                    return (Runnable) () -> {
                        Partition partition = partitionRepository.findByKey(command.getKey())
                                .orElseThrow(() -> new IllegalArgumentException("Could not find Partition for key " + command.getKey()));
                        partition.setEnabled(false);
                        partitionRepository.save(partition);
                    };
                }
        );

        //Command to enable the partition
        commandHandler.register(
                PartitionEnableCommand.class,
                () -> (CommandHandlerTask<PartitionEnableCommand>) command -> {
                    return (Runnable) () -> {
                        Partition partition = partitionRepository.findByKey(command.getKey())
                                .orElseThrow(() -> new IllegalArgumentException("Could not find Partition for key " + command.getKey()));
                        partition.setEnabled(true);
                        partitionRepository.save(partition);
                    };
                }
        );


        //Command to enable the partition
        commandHandler.register(
                PartitionTraceCommand.class,
                () -> (CommandHandlerTask<PartitionTraceCommand>) command -> {
                    return (Runnable) () -> {
                        Validate.notNull(command.getKey(), "PartitionKey cannot be null");

                        Partition partition = partitionRepository.findByKey(command.getKey())
                                .orElseThrow(() -> new IllegalArgumentException("Could not find Partition for key " + command.getKey()));

                        if (command.getTraceUntil() != null) {
                            if (command.getTraceUntil().isAfter(Instant.now().plus(7, ChronoUnit.DAYS))) {
                                throw new IllegalArgumentException("Cannot setup a trace that is longer than 7 days into the future");
                            }
                        }
                        Instant tracingTil = command.getTraceUntil();

                        if (command.getTraceFor() != null) {
                            try {
                                int amount =
                                        Integer.parseInt(
                                                command.getTraceFor().replaceAll("[^\\d.]", "")
                                        );
                                //default to minutes
                                ChronoUnit unit = ChronoUnit.MINUTES;
                                if (StringUtils.containsIgnoreCase("s", command.getTraceFor())) {
                                    unit = ChronoUnit.SECONDS;
                                } else if (StringUtils.containsIgnoreCase("m", command.getTraceFor())) {
                                    unit = ChronoUnit.MINUTES;
                                } else if (StringUtils.containsIgnoreCase("h", command.getTraceFor())) {
                                    unit = ChronoUnit.HOURS;
                                } else if (StringUtils.containsIgnoreCase("d", command.getTraceFor())) {
                                    unit = ChronoUnit.DAYS;
                                } else if (StringUtils.containsIgnoreCase("w", command.getTraceFor())) {
                                    unit = ChronoUnit.WEEKS;
                                }
                                tracingTil = Instant.now().plus((long) amount, unit);
                            } catch (Exception e) {
                                throw new IllegalArgumentException("Could not parse 'TraceFor' string " + command.getTraceFor());
                            }
                        }

                        Validate.notNull(tracingTil, "Either TraceUntil or TraceFor must be set");

                        partition.setTracingEnabledUntil(tracingTil);

                        partitionRepository.save(partition);
                    };
                }
        );
        return commandHandler;
    }
}
