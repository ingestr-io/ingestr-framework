package io.ingestr.framework.api;


import io.ingestr.framework.api.model.MessageDTO;
import io.ingestr.framework.entities.DataDescriptor;
import io.ingestr.framework.repositories.DataDescriptorRepository;
import io.ingestr.framework.service.gateway.CommandGateway;
import io.ingestr.framework.service.gateway.commands.TriggerPartitionRegistrationCommand;
import io.ingestr.framework.service.gateway.exceptions.CommandGatewayException;
import io.ingestr.framework.service.gateway.model.CommandSendRequest;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Controller("/data-descriptor")
@Requires(beans = {DataDescriptorRepository.class})
@Slf4j
public class DataDescriptorController {
    @Inject
    private DataDescriptorRepository dataDescriptorRepository;
    @Inject
    private CommandGateway commandGateway;

    public DataDescriptorController(DataDescriptorRepository dataDescriptorRepository,
                                    CommandGateway commandGateway) {
        this.dataDescriptorRepository = dataDescriptorRepository;
        this.commandGateway = commandGateway;
    }

    @Get(produces = MediaType.APPLICATION_JSON)
    public List<DataDescriptor> findAll() {
        return dataDescriptorRepository.findAll();
    }


    @Get(uri = "/{dataDescriptorIdentifier}/partition-registrator/trigger", produces = MediaType.APPLICATION_JSON)
    public MessageDTO triggerPartitionRegistration(
            @PathVariable("dataDescriptorIdentifier") String dataDescriptorIdentifier
    ) throws CommandGatewayException {

        log.info("Triggering Refresh of the Partition Registration processes on the Data Descriptor {}", dataDescriptorIdentifier);

        commandGateway.send(CommandSendRequest.builder()
                .command(TriggerPartitionRegistrationCommand.builder()
                        .dataDescriptorIdentifier(dataDescriptorIdentifier)
                        .build())
                .build());

        return MessageDTO.ok();
    }
}