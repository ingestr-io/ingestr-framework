package io.ingestr.framework.service;

import io.ingestr.framework.kafka.KafkaAdminService;
import io.ingestr.framework.service.db.LoaderDefinitionServices;
import io.ingestr.framework.service.logging.EventLogger;
import io.ingestr.framework.service.logging.store.EventConsumer;
import io.ingestr.framework.service.workers.LoaderThread;
import io.ingestr.framework.service.workers.TaskExecutionThread;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.env.Environment;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

@Singleton
@Slf4j
public class LoaderService {
    private final LoaderDefinitionServices loaderDefinitionServices;
    private final LoaderThread loaderThread;
    private final EventLogger eventLogger;
    private final KafkaAdminService kafkaAdminService;
    private final TaskExecutionThread taskExecutionThread;
    private final EventConsumer eventConsumer;
    private final Environment environment;

    private String ingestionTopicPattern;
    private String bootstrapServers;

    @Inject
    public LoaderService(
            LoaderDefinitionServices loaderDefinitionServices,
            LoaderThread loaderThread,
            TaskExecutionThread taskExecutionThread,
            EventLogger eventLogger,
            KafkaAdminService kafkaAdminService,
            Environment environment,
            EventConsumer eventConsumer,
            @Value("${ingestion.topicPattern}")
                    String ingestionTopicPattern,
            @Value("${kafka.bootstrapServers}")
                    String bootstrapServers) {
        this.loaderDefinitionServices = loaderDefinitionServices;
        this.loaderThread = loaderThread;
        this.eventLogger = eventLogger;
        this.kafkaAdminService = kafkaAdminService;
        this.ingestionTopicPattern = ingestionTopicPattern;
        this.bootstrapServers = bootstrapServers;
        this.taskExecutionThread = taskExecutionThread;
        this.environment = environment;
        this.eventConsumer = eventConsumer;
    }

    public void start() {
        log.info("Starting Loader Services [{}]...", environment.getActiveNames());


        loaderThread.start();
        taskExecutionThread.start();

        eventConsumer.start();

        try {
            loaderThread.join();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        try {
            taskExecutionThread.join();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void stop() {

    }

}
