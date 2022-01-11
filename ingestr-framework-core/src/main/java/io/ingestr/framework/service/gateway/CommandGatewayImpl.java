package io.ingestr.framework.service.gateway;

import io.ingestr.framework.service.gateway.exceptions.CommandBusException;
import io.ingestr.framework.service.gateway.exceptions.CommandGatewayException;
import io.ingestr.framework.service.gateway.model.CommandSendRequest;
import io.ingestr.framework.service.gateway.model.CommandSendResult;
import lombok.extern.slf4j.Slf4j;

import java.time.ZonedDateTime;
import java.util.UUID;

@Slf4j
public class CommandGatewayImpl implements CommandGateway {
    private CommandBus commandBus;

    public CommandGatewayImpl(
            CommandBus commandBus) {
        this.commandBus = commandBus;
    }

    @Override
    public CommandSendResult send(CommandSendRequest commandSendRequest) throws CommandGatewayException {
        log.debug("Sending Command - {}", commandSendRequest);
        String id = UUID.randomUUID().toString();
        ZonedDateTime createdAt = ZonedDateTime.now();

        try {
            commandBus.send(commandSendRequest.getCommand());
        } catch (CommandBusException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        return CommandSendResult.builder()
                .identifier(id)
                .createdAt(createdAt)
                .build();
    }
}
