package io.ingestr.framework.kafka.builders;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Slf4j
public class ConsumerBuilder {
    private String bootstrapServers;
    private String groupId;
    private String schemaRegistryUrl;
    private List<String> topics = new ArrayList<>();
    private List<TopicPartition> topicPartitions = new ArrayList<>();
    private Boolean earliest;
    private Boolean seekBeginning;
    private Boolean enableAutoCommit;
    private Integer autoCommitIntervalMs;
    private Integer heartbeatIntervalMs;
    private Integer maxPollIntervalMs;
    private Integer maxPollRecords;

    public ConsumerBuilder groupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public ConsumerBuilder schemaRegistryUrl(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        return this;
    }

    public ConsumerBuilder topic(String topic) {
        this.topics.add(topic);
        return this;
    }


    public ConsumerBuilder topics(String... topics) {
        for (String topic : topics) {
            this.topics.add(topic);
        }
        return this;
    }

    public ConsumerBuilder topics(List<String> topics) {
        for (String topic : topics) {
            this.topics.add(topic);
        }
        return this;
    }

    public ConsumerBuilder topicPartition(String topic, Integer partition) {
        this.topicPartitions.add(new TopicPartition(topic, partition));
        return this;
    }

    public ConsumerBuilder earliest(Boolean earliest) {
        this.earliest = earliest;
        return this;
    }

    public ConsumerBuilder enableAutoCommit(Boolean enableAutoCommit) {
        this.enableAutoCommit = enableAutoCommit;    //default value  true
        return this;
    }

    public ConsumerBuilder autoCommitIntervalMs(Integer autoCommitIntervalMs) {
        this.autoCommitIntervalMs = autoCommitIntervalMs;  // default value 5000  (ms)
        return this;
    }

    public ConsumerBuilder heartbeatIntervalMs(Integer heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;  // default value 5000  (ms)
        return this;
    }

    public ConsumerBuilder maxPollIntervalMs(Integer maxPollIntervalMs) {
        this.maxPollIntervalMs = maxPollIntervalMs;  // default value 300000  (5 min)
        return this;
    }

    public ConsumerBuilder maxPollRecords(Integer maxPollRecords) {
        this.maxPollRecords = maxPollRecords;  // default value 300000  (5 min)
        return this;
    }

    /**
     * Sets the Bootstrap Server.  Use bootstrapServers to set the default
     *
     * @param bootstrapServers
     * @return
     */
    public ConsumerBuilder bootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    public ConsumerBuilder seekBeginning(Boolean seekBeginning) {
        this.seekBeginning = seekBeginning;
        return this;
    }

    public <T> Consumer<String, T> build() {

        String bs = this.bootstrapServers;

        Validate.notBlank(bs, "BootstrapServers must be set");
        Validate.notBlank(groupId, "GroupId cannot be null");

        log.info("Creating Kafka Consumer. broker={} groupId={}",
                bs,
                groupId);


        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bs);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        if (earliest == Boolean.TRUE) {
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }
        if (enableAutoCommit == Boolean.FALSE) {
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        }
        if (autoCommitIntervalMs != null) {
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitIntervalMs.toString());
        }
        if (heartbeatIntervalMs != null) {
            props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs.toString());
        }
        if (maxPollIntervalMs != null) {
            props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs.toString());
        }
        if (maxPollRecords != null) {
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString());
        }

        log.info("Using String KeyDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        log.info("Using String ValueDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        KafkaConsumer<String, T> consumer = new KafkaConsumer<>(props);

        if (topicPartitions != null && !topicPartitions.isEmpty()) {
            log.info("Subscribing to topic with partition assignment {}", StringUtils.join(topicPartitions, ","));
            consumer.assign(topicPartitions);
        } else if (!topics.isEmpty()) {
            log.info("Subscribing to topic {}", StringUtils.join(topics, ","));
            consumer.subscribe(topics);
        }

        if (seekBeginning == Boolean.TRUE) {
            int count = 0;
            while (consumer.assignment().isEmpty()) {
                consumer.poll(Duration.of(1000l, ChronoUnit.MILLIS));
                log.info("Resetting offset for topic {} position to beginning.   assignment={}",
                        StringUtils.join(topics, " ,"),
                        consumer.assignment());
                if (count >= 100) {
                    throw new IllegalStateException("Failed to reset offset for topic " +
                            StringUtils.join(topics, " ,")
                            + " after 100 retries");
                }
                count += 1;
            }
            consumer.seekToBeginning(consumer.assignment());
        }

        return consumer;
    }
}
