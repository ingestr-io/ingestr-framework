package io.ingestr.framework.service.gateway;

import io.ingestr.framework.service.gateway.commands.Command;
import io.ingestr.framework.service.gateway.exceptions.CommandBusException;

public interface CommandBus {

    void send(Command command) throws CommandBusException;

    Command receive() throws CommandBusException;
}
