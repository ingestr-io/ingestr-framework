package io.ingestr.framework.service.gateway;

import io.ingestr.framework.service.gateway.commands.Command;
import io.ingestr.framework.service.gateway.exceptions.CommandHandlerException;
import io.ingestr.framework.service.gateway.model.CommandHandlerTask;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

@Slf4j
public class CommandHandlerImpl implements CommandHandler {
    private Map<Class<? extends Command>, Supplier<?>> commandHandlers = new HashMap<>();

    @Override
    public CommandHandlerTask process(Command command) throws CommandHandlerException {
        try {
            log.info("Processing Command - {}", command);
            CommandHandlerTask cht = (CommandHandlerTask) commandHandlers.get(command.getClass()).get();
            return cht;
        } catch (Exception e) {
            throw new CommandHandlerException(e.getMessage(), e);
        }
    }

    @Override
    public <T extends Command> void register(Class<T> commandClass, Supplier<CommandHandlerTask<T>> handle) {
        this.commandHandlers.put(commandClass, handle);
    }

}
