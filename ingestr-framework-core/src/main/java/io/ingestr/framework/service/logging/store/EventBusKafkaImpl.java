package io.ingestr.framework.service.logging.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ingestr.framework.kafka.KafkaRecordHeaderBuilder;
import io.ingestr.framework.kafka.ObjectMapperFactory;
import io.ingestr.framework.service.logging.LogEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.concurrent.Future;

@Slf4j
public class EventBusKafkaImpl implements EventBus {
    private ObjectMapper objectMapper = ObjectMapperFactory.kafkaMessageObjectMapper();
    private Future<Producer<String, String>> producer;
    private Future<Consumer<String, String>> consumer;
    private int partition;
    private String topic;

    public EventBusKafkaImpl(Future<Producer<String, String>> producer, Future<Consumer<String, String>> consumer, int partition, String topic) {
        this.producer = producer;
        this.consumer = consumer;
        this.partition = partition;
        this.topic = topic;
    }

    @Override
    public void send(LogEvent logEvent) {

        try {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                    topic,
                    partition,
                    logEvent.getTaskIdentifier(),
                    objectMapper.writeValueAsString(logEvent)
            );

            KafkaRecordHeaderBuilder headerb = KafkaRecordHeaderBuilder.of(record)
                    .add("loader", logEvent.getLoader())
                    .add("context", logEvent.getContext() == null ? null : logEvent.getContext().name())
                    .add("event", logEvent.getEvent())
                    .add("taskIdentifier", logEvent.getTaskIdentifier());

            for (Map.Entry<String, String> e : logEvent.getProperties().entrySet()) {
                headerb.add(e.getKey(), e.getValue());
            }
            producer.get().send(record);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            //swallow the exception as its only a log even
        }
    }

    @Override
    public List<LogEvent> take() {
        return null;
    }
}
