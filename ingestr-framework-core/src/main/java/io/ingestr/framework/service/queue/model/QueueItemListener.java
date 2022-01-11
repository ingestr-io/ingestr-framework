package io.ingestr.framework.service.queue.model;

public interface QueueItemListener<T extends QueueItem> {
    Class<T> on();

    void notify(T queueItem);
}
