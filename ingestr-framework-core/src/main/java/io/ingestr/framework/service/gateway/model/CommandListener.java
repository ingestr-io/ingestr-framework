package io.ingestr.framework.service.gateway.model;


import io.ingestr.framework.service.gateway.commands.Command;

public interface CommandListener<T extends Command> {
    void notify(T command);
}
