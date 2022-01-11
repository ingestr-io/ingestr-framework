package io.ingestr.framework.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;

public class KafkaRecordHeaderBuilder {
    private ProducerRecord<String, String> rec;

    private KafkaRecordHeaderBuilder(ProducerRecord<String, String> rec) {
        this.rec = rec;
    }

    public static KafkaRecordHeaderBuilder of(ProducerRecord<String, String> record) {
        return new KafkaRecordHeaderBuilder(record);
    }

    public KafkaRecordHeaderBuilder add(String key, String value) {
        if (key == null || value == null) {
            return this;
        }
        rec.headers().add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
        return this;
    }
}
