package io.ingestr.framework.service.gateway;

import io.ingestr.framework.service.gateway.commands.Command;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class CommandBusMemoryImpl implements CommandBus {
    private Queue<Command> commands = new ArrayBlockingQueue<>(1000);

    @Override
    public void send(Command command) {
        commands.add(command);
    }


    @Override
    public Command receive() {
        return commands.poll();
    }
}
