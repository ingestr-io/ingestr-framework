package io.ingestr.framework.api;


import io.ingestr.framework.api.model.MessageDTO;
import io.ingestr.framework.api.model.PartitionTraceDTO;
import io.ingestr.framework.entities.Partition;
import io.ingestr.framework.repositories.PartitionRepository;
import io.ingestr.framework.service.db.LoaderDefinitionServices;
import io.ingestr.framework.service.gateway.CommandGateway;
import io.ingestr.framework.service.gateway.commands.PartitionDisableCommand;
import io.ingestr.framework.service.gateway.commands.PartitionEnableCommand;
import io.ingestr.framework.service.gateway.commands.PartitionTraceCommand;
import io.ingestr.framework.service.gateway.exceptions.CommandGatewayException;
import io.ingestr.framework.service.gateway.model.CommandSendRequest;
import io.ingestr.framework.service.logging.store.EventLogRepository;
import io.ingestr.framework.service.utils.DateParser;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.*;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

@Controller("/data-descriptor/{dataDescriptorIdentifier}/partitions")
@Requires(beans = {PartitionRepository.class})
@Slf4j
public class PartitionsController {
    @Inject
    private PartitionRepository partitionRepository;
    @Inject
    private CommandGateway commandGateway;
    private EventLogRepository eventLogRepository;
    private LoaderDefinitionServices loaderDefinitionServices;


    public PartitionsController(PartitionRepository partitionRepository, CommandGateway commandGateway, EventLogRepository eventLogRepository, LoaderDefinitionServices loaderDefinitionServices) {
        this.partitionRepository = partitionRepository;
        this.commandGateway = commandGateway;
        this.eventLogRepository = eventLogRepository;
        this.loaderDefinitionServices = loaderDefinitionServices;
    }

    @Get(uri = "/{partitionKey}", produces = MediaType.APPLICATION_JSON)
    public Optional<Partition> findOne(@PathVariable("dataDescriptorIdentifier") String dataDescriptorIdentifier, @PathVariable("partitionKey") String partitionKey) {
        return partitionRepository.findByKey(partitionKey);
    }

    @Get(produces = MediaType.APPLICATION_JSON)
    public List<Partition> findAll(
            @PathVariable("dataDescriptorIdentifier") String dataDescriptorIdentifier,
            @QueryValue(value = "includeDeleted", defaultValue = "false") Boolean includeDeleted) {
        return partitionRepository.findByDataDescriptorIdentifier(dataDescriptorIdentifier, includeDeleted);
    }

    @Delete(uri = "/{partitionKey}", produces = MediaType.APPLICATION_JSON)
    public Optional<Partition> delete(
            @PathVariable("dataDescriptorIdentifier") String dataDescriptorIdentifier,
            @PathVariable("partitionKey") String partitionKey) {
        Optional<Partition> p = partitionRepository.findByKey(partitionKey);

        p.ifPresent(partition -> partitionRepository.delete(partition));

        return p;
    }

    @Post(uri = "/{partitionKey}/undelete", produces = MediaType.APPLICATION_JSON)
    public Optional<Partition> undelete(@PathVariable("dataDescriptorIdentifier") String dataDescriptorIdentifier, @PathVariable("partitionKey") String partitionKey) {
        Optional<Partition> p = partitionRepository.findByKey(partitionKey);

        p.ifPresent(partition -> partitionRepository.unDelete(partition));

        return p;
    }

    @Get(uri = "/{partitionKey}/execution-summary")
    public EventLogRepository.PartitionExecutionSummary partitionExecutionSummary(
            @PathVariable("dataDescriptorIdentifier") String dataDescriptorIdentifier,
            @PathVariable("partitionKey") String partitionKey,
            @Nullable @QueryValue("from") String from,
            @Nullable @QueryValue("to") String to
    ) {
        return eventLogRepository.partitionExecutionSummary(
                EventLogRepository.PartitionExecutionSummaryQuery.builder()
                        .loader(loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                        .partition(partitionKey)
                        .from(DateParser.parseInstant(from))
                        .to(DateParser.parseInstant(to))
                        .build()
        );
    }

    @Get(uri = "/{partitionKey}/enable", produces = MediaType.APPLICATION_JSON)
    public MessageDTO enable(
            @PathVariable("dataDescriptorIdentifier") String dataDescriptorIdentifier,
            @PathVariable("partitionKey") String partitionKey) throws CommandGatewayException {

        commandGateway.send(CommandSendRequest.builder().command(new PartitionEnableCommand(partitionKey)).build());

        return new MessageDTO("ok", "Partition '" + partitionKey + "' enabled");
    }

    @Get(uri = "/{partitionKey}/disable", produces = MediaType.APPLICATION_JSON)
    public MessageDTO disable(@PathVariable("dataDescriptorIdentifier") String dataDescriptorIdentifier, @PathVariable("partitionKey") String partitionKey) throws CommandGatewayException {

        commandGateway.send(CommandSendRequest.builder().command(new PartitionDisableCommand(partitionKey)).build());

        return new MessageDTO("ok", "Partition '" + partitionKey + "' disabled");
    }

    @Post(uri = "/{partitionKey}/trace", produces = MediaType.APPLICATION_JSON)
    public MessageDTO trace(@PathVariable("dataDescriptorIdentifier") String dataDescriptorIdentifier, @PathVariable("partitionKey") String partitionKey, @Body PartitionTraceDTO partitionTraceDTO) throws CommandGatewayException {

        if (partitionTraceDTO.getTraceFor() != null) {
            commandGateway.send(CommandSendRequest.builder().command(new PartitionTraceCommand(partitionKey, partitionTraceDTO.getTraceFor())).build());
        } else if (partitionTraceDTO.getTraceUntil() != null) {
            commandGateway.send(CommandSendRequest.builder().command(new PartitionTraceCommand(partitionKey, partitionTraceDTO.getTraceUntil())).build());
        } else {
            throw new IllegalArgumentException("Either TraceFor or TraceUntil must be specified");
        }
        return new MessageDTO("ok", "Partition '" + partitionKey + "' tracing enabled");
    }

}