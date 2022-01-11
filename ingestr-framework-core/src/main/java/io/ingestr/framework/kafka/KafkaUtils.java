package io.ingestr.framework.kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Utils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaUtils {

    public static Integer partitionForKey(String key, int numPartitions) {
        assert StringUtils.isNotBlank(key);
        return Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % numPartitions;
    }

    public static Optional<String> headerValue(ConsumerRecord record, String header) {
        assert StringUtils.isNotBlank(header);

        for (Header h : record.headers()) {
            if (StringUtils.equalsIgnoreCase(h.key(), header)) {
                return Optional.of(new String(h.value(), StandardCharsets.UTF_8));
            }
        }
        return Optional.empty();
    }

    public static boolean hasRecordHeaderMatching(ConsumerRecord record, String header, String value) {
        for (Header h : record.headers()) {
            if (StringUtils.equalsIgnoreCase(h.key(), header)) {
                if (StringUtils.equalsIgnoreCase(new String(h.value(), StandardCharsets.UTF_8), value)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static void assignToOffset(Consumer consumer, String topic, String offset) {
        List<Pair<TopicPartition, Long>> partitionsAndOffsets = new ArrayList<>();

        for (String s : StringUtils.split(offset, ",")) {
            String[] os = StringUtils.split(s, ":");
            TopicPartition tp = new TopicPartition(topic, Integer.parseInt(os[0]));
            partitionsAndOffsets.add(
                    Pair.of(tp, Long.parseLong(os[1]))
            );
        }
        consumer.assign(
                partitionsAndOffsets.stream().map(f -> f.getLeft()).collect(Collectors.toList())
        );
        for (Pair<TopicPartition, Long> po : partitionsAndOffsets) {
            consumer.seek(po.getLeft(), po.getRight());
        }

        while (consumer.assignment().isEmpty()) {
            consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
        }
    }

    public static String describeOffset(Consumer consumer) {
        Set<TopicPartition> partitions = consumer.assignment();
        return describeOffset(consumer, partitions);
    }

    public static String describeOffset(Consumer consumer, Set<TopicPartition> partitions) {
        StringBuilder sb = new StringBuilder();

        if (partitions == null || partitions.isEmpty()) {
            return "";
        }
        for (TopicPartition partition : partitions) {
            sb
                    .append(",")
                    .append(partition.partition()).append(":").append(consumer.position(partition));
        }
        return sb.substring(1);
    }
}
