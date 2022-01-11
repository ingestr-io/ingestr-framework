package io.ingestr.framework.service.gateway.model;


import io.ingestr.framework.service.gateway.commands.Command;

public interface CommandProcessListener {

    void onRecordsProcessed(int count);

    void onCommandProcessed(Class<? extends Command> aClass);

}
