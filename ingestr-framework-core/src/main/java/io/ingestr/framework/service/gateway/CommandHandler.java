package io.ingestr.framework.service.gateway;

import io.ingestr.framework.service.gateway.commands.Command;
import io.ingestr.framework.service.gateway.exceptions.CommandHandlerException;
import io.ingestr.framework.service.gateway.model.CommandHandlerTask;

import java.util.function.Supplier;

public interface CommandHandler {

    CommandHandlerTask process(Command command) throws CommandHandlerException;

    <T extends Command> void register(Class<T> commandClass, Supplier<CommandHandlerTask<T>> handle);
}
