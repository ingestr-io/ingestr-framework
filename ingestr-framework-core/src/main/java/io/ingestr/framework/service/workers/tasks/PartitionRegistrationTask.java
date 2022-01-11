package io.ingestr.framework.service.workers.tasks;

import io.ingestr.framework.entities.*;
import io.ingestr.framework.repositories.PartitionRepository;
import io.ingestr.framework.service.db.LoaderDefinitionServices;
import io.ingestr.framework.service.logging.LogContext;
import io.ingestr.framework.service.logging.LogEvent;
import io.ingestr.framework.service.logging.EventLogger;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class PartitionRegistrationTask implements Runnable {
    private DataDescriptor dataDescriptor;
    private PartitionRepository partitionRepository;
    private MeterRegistry meterRegistry;
    private EventLogger eventLogger;
    private LoaderDefinitionServices loaderDefinitionServices;

    public PartitionRegistrationTask(
            PartitionRepository partitionRepository,
            MeterRegistry meterRegistry,
            EventLogger eventLogger,
            DataDescriptor dataDescriptor,
            LoaderDefinitionServices loaderDefinitionServices) {
        this.dataDescriptor = dataDescriptor;
        this.partitionRepository = partitionRepository;
        this.meterRegistry = meterRegistry;
        this.eventLogger = eventLogger;
        this.loaderDefinitionServices = loaderDefinitionServices;
    }

    @Override
    public void run() {
        StopWatch watch = new StopWatch();
        watch.start();

        try {
            PartitionRegistrator reg = dataDescriptor.getPartitionRegister().getPartitionRegistratorSupplier()
                    .get();
            PartitionRegistrator.ParitionRegistratorResult result = new PartitionRegistrator.ParitionRegistratorResult();

            reg.discover(PartitionRegistrator.ParitionRegistratorRequest
                            .newParitionRegistratorRequest().build(),
                    result);

            log.debug("Found {} discovered partitions", result.getPartitions().size());

            //1. Determine which partitions need updating/creating
            for (Partition partition : result.getPartitions()) {
                //set the descriptor identifier
                partition.setDataDescriptorIdentifier(dataDescriptor.getIdentifier());
                //Set the registration flag as we have found it
                partition.setRegistered(true);

                Optional<Partition> partitionDb = partitionRepository.findByKey(partition.getKey());
                if (partitionDb.isPresent()) {
                    if (!partitionDb.get().equals(partition)) {
                        eventLogger.log(LogEvent.info("Updating Partition - {}", partition.toString())
                                .event("ingestr.partitionRegistration.updated")
                                .context(LogContext.PARTITION_REGISTRATION)
                                .property("loader", this.loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                                .property("partitionKey", partition.getKey())
                                .property("dataDescriptor", dataDescriptor.getIdentifier())
                        );
                        partitionRepository.save(partition);

                        Counter.builder("ingestr.partitionRegistration.updated")
                                .tag("loader", this.loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                                .tag("dataDescriptor", dataDescriptor.getIdentifier())
                                .description("The total number of updated partitions performed by Partition Registration")
                                .register(this.meterRegistry)
                                .increment();
                    }
                } else {
                    eventLogger.log(LogEvent.info("Creating new Partition - {}", partition.toString())
                            .event("ingestr.partitionRegistration.created")
                            .context(LogContext.PARTITION_REGISTRATION)
                            .property("loader", this.loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                            .property("partitionKey", partition.getKey())
                            .property("dataDescriptor", dataDescriptor.getIdentifier())
                    );
                    Counter.builder("ingestr.partitionRegistration.created")
                            .tag("loader", this.loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                            .tag("dataDescriptor", dataDescriptor.getIdentifier())
                            .description("The total number of created partitions performed by Partition Registration")
                            .register(this.meterRegistry)
                            .increment();
                    partitionRepository.save(partition);
                }
            }

            //2. Determine which partitions need disabling because we no long have discovered them
            List<Partition> existingPartitions = partitionRepository.findByDataDescriptorIdentifier(dataDescriptor.getIdentifier())
                    .stream()
                    .filter(p -> p.getDeleted() == Boolean.FALSE)
                    .filter(p -> p.getEnabled() == Boolean.TRUE)
                    .filter(p -> p.getRegistered() == Boolean.TRUE)
                    .collect(Collectors.toList());

            List<Partition> dereg = new ArrayList<>();
            for (Partition existingPartition : existingPartitions) {
                boolean found = false;
                for (Partition partition : result.getPartitions()) {
                    if (existingPartition.getKey().equalsIgnoreCase(partition.getKey())) {
                        found = true;
                    }
                }
                if (!found) {
                    dereg.add(existingPartition);
                }
            }

            log.debug("Found {} candidate Partitions for De-registration", dereg.size());

            for (Partition partition : dereg) {
                //act according to the deregistration method
                if (dataDescriptor.getPartitionRegister().getDeregistrationMethod() == DeregistrationMethod.DEREGISTER) {
                    partition.setRegistered(false);
                    partitionRepository.save(partition);
                } else if (dataDescriptor.getPartitionRegister().getDeregistrationMethod() == DeregistrationMethod.DISABLE) {
                    partition.setEnabled(false);
                    partition.setRegistered(false);
                    partitionRepository.save(partition);
                } else if (dataDescriptor.getPartitionRegister().getDeregistrationMethod() == DeregistrationMethod.DELETE) {
                    partitionRepository.delete(partition);
                }

                eventLogger.log(LogEvent.info("Deregistering with method {} for Partition - {}",
                                dataDescriptor.getPartitionRegister().getDeregistrationMethod(),
                                partition.toString())
                        .event("ingestr.partitionRegistration.deregistered")
                        .context(LogContext.PARTITION_REGISTRATION)
                        .property("loader", this.loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                        .property("partitionKey", partition.getKey())
                        .property("dataDescriptor", dataDescriptor.getIdentifier())
                );
                Counter.builder("ingestr.partitionRegistration.deregistered")
                        .tag("loader", this.loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                        .tag("dataDescriptor", dataDescriptor.getIdentifier())
                        .description("The total number of de-registered partitions performed by Partition Registration")
                        .register(this.meterRegistry)
                        .increment();
            }
        } finally {
            watch.stop();
            eventLogger.log(LogEvent.info("Completed Partition Registration Task")
                    .event("ingestr.partitionRegistration.deregistered")
                    .context(LogContext.PARTITION_REGISTRATION)

                    .property("loader", this.loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                    .property("dataDescriptor", dataDescriptor.getIdentifier())
            );

            Timer.builder("ingestr.partitionRegistration.duration")
                    .tag("loader", this.loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                    .tag("dataDescriptor", dataDescriptor.getIdentifier())
                    .description("The amount of time taken to perform Partition Registration")
                    .register(meterRegistry)
                    .record(watch.getTime(), TimeUnit.MILLISECONDS);

        }
    }
}
