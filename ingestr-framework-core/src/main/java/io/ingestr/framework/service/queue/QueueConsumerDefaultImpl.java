package io.ingestr.framework.service.queue;

import io.ingestr.framework.service.queue.exceptions.QueueBusException;
import io.ingestr.framework.service.queue.model.QueueItem;
import io.ingestr.framework.service.queue.model.QueuedResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class QueueConsumerDefaultImpl implements QueueConsumer {
    private final QueueService queue;

    public QueueConsumerDefaultImpl(QueueService queue) {
        this.queue = queue;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public List<QueuedResponse> receive() {
        return receive(0);
    }

    @Override
    public void commit(QueuedResponse response) {
        //In memory Queue only supports Auto Commit
    }

    @Override
    public List<QueuedResponse> receive(long milliseconds) {
        List<QueuedResponse> results = new ArrayList<>();


        try {
            QueueItem qi = queue.poll(milliseconds, TimeUnit.MILLISECONDS);
            if (qi != null) {
                QueuedResponse qr = new QueuedResponse();
                qr.setQueueItem(qi);
                qr.setOffset("-1");
                results.add(qr);
            }
        } catch (QueueBusException e) {
            log.error(e.getMessage(), e);
        }


        return results;
    }
}
