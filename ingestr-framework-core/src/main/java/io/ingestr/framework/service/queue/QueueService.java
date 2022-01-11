package io.ingestr.framework.service.queue;

import io.ingestr.framework.service.queue.exceptions.QueueBusException;
import io.ingestr.framework.service.queue.model.QueueItem;

import java.util.concurrent.TimeUnit;

public interface QueueService {
    void add(QueueItem queueItem);

    QueueItem poll(long timeout, TimeUnit unit) throws QueueBusException;
}
