package io.ingestr.framework.service.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ingestr.framework.kafka.Kafka;
import io.ingestr.framework.kafka.KafkaAdminService;
import io.ingestr.framework.kafka.KafkaUtils;
import io.ingestr.framework.kafka.ObjectMapperFactory;
import io.ingestr.framework.kafka.builders.ConsumerBuilder;
import io.ingestr.framework.service.db.model.EntityPayload;
import io.ingestr.framework.service.db.model.SaveEntityRequest;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class acts as an in memory database for the fastest lookup/processing times of the loaders.
 * In particular, offsets and partitions can be updated very frequently and can benefit from
 * being accessible in memory for faster updates.
 * <p>
 * There is an async process to send the
 */
@Slf4j
@Singleton
@Requires(property = "repository.source", value = "kafka", defaultValue = "memory")
public class RepositoryServiceKafkaImpl implements RepositoryService {
    //In Memory Hashmap of all the entities
    private Map<String, Map<String, Object>> db = Collections.synchronizedMap(new HashMap<>());
    private Lock lock = new ReentrantLock();
    private ObjectMapper objectMapper = ObjectMapperFactory.kafkaMessageObjectMapper();
    private Future<Producer<String, String>> producer;

    private Thread syncThread = null;
    private AtomicBoolean shutdown = new AtomicBoolean(false);
    private AtomicBoolean initialised = new AtomicBoolean(false);
    private AtomicLong counter = new AtomicLong(0);
    private RepositoryServiceConfig repositoryServiceConfig;
    private String id = RandomStringUtils.randomNumeric(10);
    private boolean continuousExecution = false;
    private Integer partition = null;
    private KafkaAdminService kafkaAdminService;

    @Data
    @Builder
    public static class RepositoryServiceConfig {
        private String kafkaBootstrapServers;
        private String context;
        private Integer partitionCount;
        private String topic;
    }

    public RepositoryServiceKafkaImpl(
            RepositoryServiceConfig repositoryServiceConfig,
            CompletableFuture<Producer<String, String>> producer) {
        this.repositoryServiceConfig = repositoryServiceConfig;
        this.partition = KafkaUtils.partitionForKey(repositoryServiceConfig.getContext(), repositoryServiceConfig.getPartitionCount());
        this.producer = producer;
    }


    @Override
    public void sync() {
        this.lock.lock();
        this.initialised.set(false);

        syncThread = new Thread(() -> {
            log.info("Loading Entity Data...");
            Consumer<String, String> consumer = null;
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            try {
                ConsumerBuilder kb = Kafka.consumer()
                        .bootstrapServers(repositoryServiceConfig.getKafkaBootstrapServers())
                        .groupId(repositoryServiceConfig.getContext() + "-" + repositoryServiceConfig.getTopic() + "-" + id)
                        .topicPartition(repositoryServiceConfig.getTopic(), partition) //this is a performance optimsation that shaves off 3 seconds if we guarantee only a single partition exists
                        .enableAutoCommit(false);

                consumer = kb.build();
                while (consumer.assignment().isEmpty()) {
                    consumer.poll(Duration.ZERO);
                }
                consumer.seekToBeginning(consumer.assignment());

                while (!shutdown.get()) {

                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(200));
                    counter.addAndGet(records.count());

                    if (!this.initialised.get() && records.count() == 0) {
                        stopWatch.stop();
                        log.info("Finished Initialising {} Entities took {} ms",
                                counter.get(),
                                stopWatch.getTime());
                        this.initialised.set(true);
                        this.lock.unlock();

                        if (!continuousExecution) {
                            log.info("Continuous Execution is Disabled!");
                            shutdown.set(true);
                        }
                    }
                    for (ConsumerRecord<String, String> record : records) {
                        //skip records not part of the context
                        if (this.repositoryServiceConfig.getContext() != null &&
                                !KafkaUtils.hasRecordHeaderMatching(record, "CONTEXT", repositoryServiceConfig.getContext())) {
                            continue;
                        }
                        try {
                            EntityPayload entityPayload = objectMapper.readValue(record.value(), EntityPayload.class);

                            //load up the database
                            try {
                                Class clazz = Class.forName(entityPayload.getClassName());

                                String persistenceKey = clazz.getSimpleName();
                                Object entity = objectMapper.treeToValue(entityPayload.getPayload(), clazz);

                                if (!db.containsKey(persistenceKey)) {
                                    db.put(persistenceKey, Collections.synchronizedMap(new HashMap<>()));
                                }
                                db.get(clazz.getSimpleName()).put(entityPayload.getIdentifier(), entity);
                            } catch (Exception e) {
                                log.error(e.getMessage(), e);
                            }

                        } catch (Exception e) {
                            log.warn(e.getMessage(), e);
                        }
                    }
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                consumer.close();
                log.info("Finished Entity Loader!");
            }
        }, repositoryServiceConfig.getContext() + "-" + repositoryServiceConfig.getTopic());
        syncThread.start();
    }

    @Override
    public void persist(SaveEntityRequest request) {
        assert request.getVersion() != null;
        assert request.getEntity() != null;
        assert request.getIdentifier() != null;

        lock.lock();

        EntityPayload payload = EntityPayload.builder()
                .identifier(request.getIdentifier())
                .className(request.getEntity().getClass().getName())
                .version(request.getVersion())
                .createdAt(ZonedDateTime.now())
                .payload(objectMapper.valueToTree(request.getEntity()))
                .build();

        String key = request.getEntity().getClass().getSimpleName() + ":" + request.getIdentifier();

        //save the record to the in memory database
        if (!db.containsKey(request.persistenceKey())) {
            db.put(request.persistenceKey(), Collections.synchronizedMap(new HashMap<>()));
        }
        db.get(request.persistenceKey()).put(request.getIdentifier(), request.getEntity());
        lock.unlock();

        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    repositoryServiceConfig.getTopic(),
                    partition,
                    key,
                    objectMapper.writeValueAsString(payload),
                    Arrays.asList(
                            new RecordHeader("ENTITY", payload.getClassName().getBytes(StandardCharsets.UTF_8)),
                            new RecordHeader("ENTITY_VER", payload.getVersion().getBytes(StandardCharsets.UTF_8)),
                            new RecordHeader("CONTEXT", repositoryServiceConfig.getContext() == null ? "default".getBytes(StandardCharsets.UTF_8) : repositoryServiceConfig.getContext().getBytes(StandardCharsets.UTF_8))
                    )
            );
            producer.get().send(record);
        } catch (JsonProcessingException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
        }
    }

    @Override
    public <T> Optional<T> findById(Class<T> entityClass, String id) {
        lock.lock();
        try {
            return (Optional<T>) Optional.ofNullable(
                    this.db.getOrDefault(entityClass.getSimpleName(), new HashMap<>())
                            .getOrDefault(id, null)
            );
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <T> List<T> findAll(Class<T> entityClass) {
        lock.lock();
        try {
            if (!this.db.containsKey(entityClass.getSimpleName())) {
                return new ArrayList<>();
            }
            List<T> results = new ArrayList<>();
            for (Object va : this.db.get(entityClass.getSimpleName())
                    .values()) {
                results.add((T) va);
            }
            return results;
        } finally {
            lock.unlock();
        }
    }


}
