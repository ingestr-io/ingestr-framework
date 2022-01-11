package io.ingestr.framework.service.gateway;

import io.ingestr.framework.service.gateway.commands.Command;
import io.ingestr.framework.service.gateway.exceptions.CommandBusException;
import io.ingestr.framework.service.gateway.exceptions.CommandHandlerException;
import io.ingestr.framework.service.gateway.model.CommandProcessListener;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class CommandProcessorThreadImpl implements CommandProcessor {
    private CommandBus commandBus;
    private CommandHandler commandHandler;
    private List<CommandProcessListener> processListeners;
    private Thread processorThread;
    private ProcessorWorker processorWorker;
    private ExecutorService taskExecutors;

    public CommandProcessorThreadImpl(
            int concurrency,
            CommandHandler commandHandler,
            CommandBus commandBus) {
        this.commandBus = commandBus;
        this.commandHandler = commandHandler;
        this.processorWorker = new ProcessorWorker();
        this.processorThread = new Thread(processorWorker, "command-processor");
        this.processListeners = new ArrayList<>();
        this.taskExecutors = Executors.newFixedThreadPool(concurrency);

    }

    @Override
    public void start() {
        log.info("Initiating the Command Processor Worker Thread...");
        this.processorThread.start();
    }

    @Override
    public void stop() {
        log.info("Initiating shutdown of Command Processor Worker Thread...");
        this.processorWorker.shutdown();
        this.processListeners = new ArrayList<>();
    }

    @Override
    public void removeListeners() {
        this.processListeners.clear();
    }

    @Override
    public void register(CommandProcessListener listener) {
        this.processListeners.add(listener);
    }

    @Override
    public void trigger(Command command) {
        if (command != null) {
            try {
                taskExecutors.submit(commandHandler.process(command).handle(command));

            } catch (Exception | CommandHandlerException e) {
                log.error(e.getMessage(), e);
            }
        }
        for (CommandProcessListener pl : processListeners) {
            pl.onRecordsProcessed(1);
        }
    }

    void poll() throws CommandBusException {
        Command command = commandBus.receive();
        trigger(command);
    }

    private class ProcessorWorker implements Runnable {
        private AtomicBoolean shutdown = new AtomicBoolean(false);

        void shutdown() {
            this.shutdown.set(true);
        }

        @Override
        public void run() {
            while (!shutdown.get()) {
                try {
                    Thread.sleep(1);
                    poll();
                } catch (Exception | CommandBusException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
        }
    }
}
