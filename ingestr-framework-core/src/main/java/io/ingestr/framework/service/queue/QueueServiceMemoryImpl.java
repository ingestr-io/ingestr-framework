package io.ingestr.framework.service.queue;

import io.ingestr.framework.service.queue.exceptions.QueueBusException;
import io.ingestr.framework.service.queue.model.QueueItem;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class QueueServiceMemoryImpl implements QueueService {
    private final BlockingQueue<QueueItem> queue = new ArrayBlockingQueue<>(100);

    @Override
    public void add(QueueItem queueItem) {
        queue.add(queueItem);
    }

    @Override
    public QueueItem poll(long timeout, TimeUnit unit) throws QueueBusException {
        try {
            return queue.poll(timeout, unit);
        } catch (InterruptedException e) {
            throw new QueueBusException(e.getMessage(), e);
        }
    }
}
