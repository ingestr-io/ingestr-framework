package io.ingestr.framework.kafka;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

@Slf4j
public class KafkaOffset {
    private List<TopicPartitionOffset> offsets = new ArrayList<>();
    private Consumer consumer;
    private String topic;

    private KafkaOffset(
            Consumer consumer,
            List<TopicPartitionOffset> offsets,
            String topic) {
        this.offsets = offsets;
        this.consumer = consumer;
        this.topic = topic;
    }

    public static KafkaOffset of(Consumer consumer) {
        List<TopicPartitionOffset> offsets = new ArrayList<>();
        Set<TopicPartition> partitions = consumer.assignment();
        String topic = partitions.stream().findFirst().get().topic();

        for (TopicPartition partition : partitions) {
            offsets.add(TopicPartitionOffset
                    .builder()
                    .partition(partition.partition())
                    .offset(consumer.position(partition))
                    .topic(topic)
                    .build());
        }

        return new KafkaOffset(consumer, offsets, topic);
    }

    public static KafkaOffset of(Consumer consumer, String offsetCode) {

        Set<TopicPartition> partitions = consumer.assignment();
        String topic = partitions.stream().findFirst().get().topic();

        List<TopicPartitionOffset> offsets = new ArrayList<>();

        for (String s : StringUtils.split(offsetCode, ",")) {
            String[] os = StringUtils.split(s, ":");
            offsets.add(TopicPartitionOffset.builder()
                    .topic(topic)
                    .partition(Integer.parseInt(os[0]))
                    .offset(Long.parseLong(os[1]))
                    .build());
        }
        return new KafkaOffset(consumer, offsets, topic);
    }

    public void adjust(int partition, long offset) {
        for (TopicPartitionOffset of : this.offsets) {
            if (of.getPartition() == partition) {
                of.setOffset(offset);
            }
        }
    }

    public void seekToOffset() {
        for (TopicPartitionOffset offset : this.offsets) {
            consumer.seek(new TopicPartition(offset.getTopic(), offset.getPartition()), offset.getOffset());
        }
    }

    public void commit() {
        log.debug("Commit offset - {}", asCode());
        Map<TopicPartition, OffsetAndMetadata> req = new HashMap<>();
        for (TopicPartitionOffset offset : this.offsets) {
            req.put(new TopicPartition(topic, offset.partition), new OffsetAndMetadata(offset.offset));
        }
        consumer.commitSync(req);
    }

    public String asCode() {
        assert this.offsets != null;

        StringBuilder sb = new StringBuilder();

        if (this.offsets.isEmpty()) {
            return "";
        }
        for (TopicPartitionOffset partition : this.offsets) {
            sb
                    .append(",")
                    .append(partition.getPartition()).append(":").append(partition.getOffset());
        }
        return sb.substring(1);
    }

    @Data
    @ToString
    @Builder
    private static class TopicPartitionOffset {
        private String topic;
        private int partition;
        private long offset;
    }
}
