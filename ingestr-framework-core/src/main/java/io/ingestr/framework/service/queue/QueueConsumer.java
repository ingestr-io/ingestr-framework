package io.ingestr.framework.service.queue;

import io.ingestr.framework.service.queue.model.QueuedResponse;

import java.util.List;

public interface QueueConsumer {
    void shutdown();

    List<QueuedResponse> receive();

    void commit(QueuedResponse response);

    List<QueuedResponse> receive(long milliseconds);
}
