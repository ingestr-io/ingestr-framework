package io.ingestr.framework.service.logging.config;

import io.ingestr.framework.kafka.Kafka;
import io.ingestr.framework.kafka.KafkaAdminService;
import io.ingestr.framework.kafka.KafkaQueryHandler;
import io.ingestr.framework.kafka.KafkaUtils;
import io.ingestr.framework.service.db.LoaderDefinitionServices;
import io.ingestr.framework.service.logging.EventLogger;
import io.ingestr.framework.service.logging.EventLoggerLogImpl;
import io.ingestr.framework.service.logging.store.*;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

@Slf4j
public abstract class AbstractLoggerServiceKafkaFactory {

    @Singleton
    public EventLogger loggerService(
            LoaderDefinitionServices loaderDefinitionServices,
            EventBus eventBus) {
        log.info("Initializing In Kafka Logger Service with Config");
        return new EventLoggerLogImpl(
                eventBus,
                loaderDefinitionServices.getLoaderDefinition().getLoaderName()
        );
    }

    @Singleton
    public EventBus eventBus(
            KafkaAdminService kafkaAdminService,
            LoaderDefinitionServices loaderDefinitionServices,
            @Value("${kafka.bootstrapServers}") String kafkaBootstrapServers,
            @Value("${logger.topic}") String topic,
            @Value("${logger.partitions}") Integer partitions
    ) {

        //setup a future which does the topic initialisation and producer setup
        CompletableFuture<Producer<String, String>> res = CompletableFuture.supplyAsync(() -> {

            //1. Initialise the topic
            Properties properties = new Properties();
            properties.put(
                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers
            );

            try (Admin admin = Admin.create(properties)) {
                Collection<TopicListing> topicListings = admin.listTopics().listings().get();

                if (!kafkaAdminService.containsTopic(topic, topicListings)) {
                    log.info("Creating Log Repository Topic - {}", topic);
                    kafkaAdminService.createTopic(admin, topic, partitions, (short) 1,
                            new HashMap<>() {{
                                put("delete.retention.ms", "604800000"); //1 week
                                put("segment.ms", "604800000");
                            }});
                }

            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }


            return Kafka.producer()
                    .bootstrapServers(kafkaBootstrapServers)
                    .clientId("ingestr-logger-" +
                            RandomStringUtils.randomNumeric(10))
                    .build();
        });

        //determine the partition we should be using
        Integer partition = KafkaUtils.partitionForKey(loaderDefinitionServices.getLoaderDefinition().getLoaderName(), partitions);

        CompletableFuture<Consumer<String, String>> cons = res.thenApply((s) -> {
            return Kafka.consumer()
                    .bootstrapServers(kafkaBootstrapServers)
                    .topicPartition(topic, partition)
                    .build();
        });

        return new EventBusKafkaImpl(
                res,
                cons,
                partition,
                topic
        );
    }

    @Singleton
    public KafkaQueryHandler kafkaQueryHandler(
            @Value("${kafka.bootstrapServers}") String kafkaBootstrapServers
    ) {
        log.info("Initializing KafkaQueryHandler for host={}", kafkaBootstrapServers);

        return new KafkaQueryHandler(kafkaBootstrapServers);
    }

    @Singleton
    public EventLogRepository eventLogRepository(
            LoaderDefinitionServices loaderDefinitionServices,
            KafkaQueryHandler kafkaQueryHandler,
            @Value("${logger.topic}") String topic,
            @Value("${logger.partitions}") Integer partitions
    ) {
        log.info("Initializing EventLogRepository for Kafka for topic {}", topic);
        Integer partition = KafkaUtils.partitionForKey(loaderDefinitionServices.getLoaderDefinition().getLoaderName(), partitions);

        return new EventLogRepositoryKafka(kafkaQueryHandler,
                topic,
                partition
        );
    }

    @Singleton
    public EventConsumer eventConsumer(EventBus eventBus, EventStore eventStore) {
        EventConsumerDefault d = new EventConsumerDefault(eventBus, eventStore);
        return d;
    }


}
