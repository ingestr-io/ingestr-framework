package io.ingestr.framework.service.workers.tasks;

public abstract class AbstractWorkerTask implements Runnable {

    abstract void execute();

    @Override
    public void run() {
        execute();
    }
}
