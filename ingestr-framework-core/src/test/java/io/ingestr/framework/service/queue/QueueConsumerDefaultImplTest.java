package io.ingestr.framework.service.queue;

import io.ingestr.framework.service.queue.model.QueueItem;
import io.ingestr.framework.service.queue.model.QueuedResponse;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QueueConsumerDefaultImplTest {

    @Test
    void shouldConsumeWithTimeout() {
//        ArrayBlockingQueue<QueueItem> queue = new ArrayBlockingQueue<>(10);
        QueueServiceMemoryImpl queue = new QueueServiceMemoryImpl();

        QueueConsumer qc = new QueueConsumerDefaultImpl(queue);

        queue.add(new TestQueueItem("Test"));

        List<QueuedResponse> responses = qc.receive(10);

        assertFalse(responses.isEmpty());
    }


    @Data
    @AllArgsConstructor
    public static class TestQueueItem implements QueueItem {
        private String name;

    }

}