package io.ingestr.framework.kafka;


import io.ingestr.framework.kafka.builders.ConsumerBuilder;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.text.NumberFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class KafkaQueryHandler {
    private String kafkaBootstrapServers;

    public KafkaQueryHandler(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    public KafkaQueryResult query(KafkaQuery kafkaQuery) {
        StopWatch watch = new StopWatch();
        watch.start();
        long timeLimit = kafkaQuery.getTimeLimitMs() == null ? 15_000l : kafkaQuery.getTimeLimitMs();

        ConsumerBuilder kb = Kafka.consumer()
                .bootstrapServers(kafkaBootstrapServers)
                .groupId("kafka-query-" + RandomStringUtils.randomNumeric(10))
                .enableAutoCommit(false);
        if (kafkaQuery.getPartition() != null) {
            kb.topicPartition(kafkaQuery.getTopic(), kafkaQuery.getPartition());
        } else {
            kb.topic(kafkaQuery.getTopic());
        }
        Consumer<String, String> consumer = kb.build();
        KafkaQueryResult.KafkaQueryResultBuilder res = KafkaQueryResult.builder();
        int count = 0;
        int processCount = 0;
        KafkaOffset ko = null;
        try {
            while (consumer.assignment().isEmpty()) {
                consumer.poll(Duration.ZERO);
            }
            if (kafkaQuery.getOffset() != null) {
                KafkaOffset kos = KafkaOffset.of(consumer, kafkaQuery.getOffset());
                kos.seekToOffset(); //adjust the topic consumer to match the offset

            } else if (kafkaQuery.getFrom() != null) {
                log.debug("Adjusting Consumer offset to Timestamp {}", kafkaQuery.getFrom());

                Set<TopicPartition> tps = consumer.assignment();
                Map<TopicPartition, Long> offsetsForTimes = new HashMap<>();
                for (TopicPartition tp : tps) {
                    offsetsForTimes.put(tp, kafkaQuery.getFrom().getEpochSecond());
                }
                consumer.offsetsForTimes(offsetsForTimes);
            } else {
                consumer.seekToBeginning(consumer.assignment());
            }


            ko = KafkaOffset.of(consumer);

            log.debug("Determined Offset - {}", ko.asCode());

            records_loop:
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    break;
                }
                if (watch.getTime() > timeLimit) {
                    log.info("Query Exceeded the assigned Query Time limit {} > {}", watch.getTime(), timeLimit);
                    break;
                }
                for (ConsumerRecord<String, String> record : records) {
                    boolean add = true;
                    processCount += 1;
                    if (kafkaQuery.getHeaderFilters() != null) {
                        for (KafkaHeaderValue filter : kafkaQuery.getHeaderFilters()) {
                            if (!KafkaUtils.hasRecordHeaderMatching(record, filter.getName(), filter.getValue())) {
                                add = false;
                            }
                        }
                    }
                    if (add) {
                        res.record(record);
                        count += 1;
                        ko.adjust(record.partition(), record.offset());
                    }
                    if (count >= kafkaQuery.getResultLimit()) {
                        break records_loop;
                    }
                }
            }
        } finally {
            consumer.close();
            watch.stop();
        }


        log.debug("Query took {}ms", NumberFormat.getIntegerInstance().format(watch.getTime()));
        return res
                .duration(watch.getTime())
                .processedRecords(processCount)
                .offset(ko.asCode())
                .build();
    }

    @Data
    @Builder
    @ToString
    public static class KafkaQuery {
        private String topic;
        private Integer partition;

        private String offset;
        private Instant from;
        private Instant to;
        @Builder.Default
        private Integer resultLimit = 100;

        @Builder.Default
        private Long timeLimitMs = 15_000l;
        @Singular
        private List<KafkaHeaderValue> headerFilters;
    }

    @Data
    @Builder
    @ToString
    public static class KafkaHeaderValue {
        private String name;
        private String value;

        public static KafkaHeaderValue of(String name, String value) {
            return KafkaHeaderValue.builder()
                    .name(name)
                    .value(value)
                    .build();
        }
    }

    @Data
    @Builder
    @ToString
    public static class KafkaQueryResult {
        @Singular
        private List<ConsumerRecord<String, String>> records;
        private long duration;
        private long processedRecords;
        private String offset;

    }

}
