package io.ingestr.framework.service.gateway;

import io.ingestr.framework.service.gateway.commands.Command;
import io.ingestr.framework.service.gateway.model.CommandProcessListener;

public interface CommandProcessor {
    void start();

    void stop();

    void trigger(Command command);

    void register(CommandProcessListener listener);

    void removeListeners();
}
