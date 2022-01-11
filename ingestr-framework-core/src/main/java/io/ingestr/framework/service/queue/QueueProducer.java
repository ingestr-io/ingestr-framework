package io.ingestr.framework.service.queue;

import io.ingestr.framework.service.queue.model.QueueItem;

public interface QueueProducer {
    void shutdown();

    void queue(QueueItem queueItem);
}
