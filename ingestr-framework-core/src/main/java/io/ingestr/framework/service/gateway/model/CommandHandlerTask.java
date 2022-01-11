package io.ingestr.framework.service.gateway.model;


import io.ingestr.framework.service.gateway.commands.Command;

public interface CommandHandlerTask<T extends Command> {
    Runnable handle(T command);
}
