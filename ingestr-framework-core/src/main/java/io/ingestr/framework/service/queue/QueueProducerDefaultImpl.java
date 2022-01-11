package io.ingestr.framework.service.queue;

import io.ingestr.framework.service.queue.model.QueueItem;

public class QueueProducerDefaultImpl implements QueueProducer {
    private final QueueService queueService;

    public QueueProducerDefaultImpl(QueueService queueService) {
        this.queueService = queueService;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public void queue(QueueItem queueItem) {
        queueService.add(queueItem);
    }
}
