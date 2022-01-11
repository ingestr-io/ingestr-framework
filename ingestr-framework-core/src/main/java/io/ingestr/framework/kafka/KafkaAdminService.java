package io.ingestr.framework.kafka;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaFuture;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

@Slf4j
@Singleton
public class KafkaAdminService {
    public final static Integer DEFAULT_PARTITIONS = 50;
    private Map<String, CompletableFuture<Producer>> kafkaProducers = new HashMap<>();
    private String bootstrapServers;

    @Inject
    public KafkaAdminService(
            @Value("${kafka.bootstrapServers}")
                    String bootstrapServers
    ) {
        this.bootstrapServers = bootstrapServers;
    }


    private Admin admin() {
        Properties properties = new Properties();
        properties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
        );
        return Admin.create(properties);
    }


    @Getter
    @Builder
    @ToString
    public static class TopicDescriptionResult {
        private String topic;
        private Integer partitionCount;
    }

    public synchronized TopicDescriptionResult describeTopic(String topic) {
        try (Admin admin = admin()) {
            DescribeTopicsResult describeTopicsResult = admin.describeTopics(Collections.singleton(topic));
            Map<String, TopicDescription> descriptionMap = describeTopicsResult.all()
                    .get();


            return TopicDescriptionResult.builder()
                    .topic(topic)
                    .partitionCount(descriptionMap.get(topic).partitions().size())
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public synchronized void deleteTopic(String topic) {
        try (Admin admin = admin()) {
            DeleteTopicsResult res = admin.deleteTopics(Collections.singleton(topic));
            res.all().get();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public boolean containsTopic(String topicName, Collection<TopicListing> listings) {
        for (TopicListing topicListing : listings) {
            if (StringUtils.equalsIgnoreCase(topicName, topicListing.name())) {
                return true;
            }
        }
        return false;
    }


    public void createTopicIfNotExists(
            String topicName, int partitions, short replicationFactor) throws ExecutionException, InterruptedException {
        try (Admin admin = admin()) {
            Collection<TopicListing> topicListings = admin.listTopics().listings().get();
            log.info("Listing existing Kafka Topics...");

            if (!containsTopic(topicName, topicListings)) {
                log.info("Creating Topic - {}", topicName);
                createTopic(admin, topicName, partitions, replicationFactor);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void createTopic(String topicName, int partitions, short replicationFactor) throws ExecutionException, InterruptedException {
        createTopic(admin(), topicName, partitions, replicationFactor);
    }

    void createTopic(Admin admin, String topicName, int partitions, short replicationFactor) throws ExecutionException, InterruptedException {
        createTopic(admin, topicName, partitions, replicationFactor, false);
    }

    public void createTopic(Admin admin, String topicName, int partitions, short replicationFactor, boolean compaction) throws ExecutionException, InterruptedException {

        Map<String, String> cfg = new HashMap<>();

        if (compaction) {
            cfg.put("cleanup.policy", "compact");
            cfg.put("delete.retention.ms", "100");
            cfg.put("segment.ms", "100");
            cfg.put("min.cleanable.dirty.ratio", "0.01");
        }
        createTopic(admin, topicName, partitions, replicationFactor, cfg);
    }

    public void createTopic(Admin admin, String topicName, int partitions, short replicationFactor, Map<String, String> cfg) throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);

        if (!cfg.isEmpty()) {
            newTopic.configs(cfg);
        }

        CreateTopicsResult result = admin.createTopics(
                Collections.singleton(newTopic)
        );
        KafkaFuture<Void> future = result.values().get(topicName);
        future.get();
    }

    public synchronized CompletableFuture<Producer> kafkaProducerDefault() {
        return kafkaProducerDefault(this.bootstrapServers);
    }

    public synchronized CompletableFuture<Producer> kafkaProducerDefault(String bootstrapServers) {
        Validate.notBlank(bootstrapServers, "Bootstrap servers cannot be null");

        return kafkaProducer(
                "default",
                () ->
                        Kafka.producer()
                                .bootstrapServers(bootstrapServers)
                                .clientId("default")
                                .build()
        );
    }

    public synchronized CompletableFuture<Producer> kafkaProducer(
            String clientId,
            Supplier<Producer> producerSupplier) {
        if (kafkaProducers.containsKey(clientId)) {
            return kafkaProducers.get(clientId);
        }
        log.info("Initialising new Kafka Producer for client - {}", clientId);

        CompletableFuture<Producer> producerCompletableFuture = CompletableFuture.supplyAsync(producerSupplier);
        kafkaProducers.put(clientId, producerCompletableFuture);
        return producerCompletableFuture;
    }


}
