package io.ingestr.framework.service.logging.store;

import io.ingestr.framework.entities.LoaderDefinition;
import io.ingestr.framework.service.db.LoaderDefinitionServices;
import io.ingestr.framework.service.logging.EventLogger;
import io.ingestr.framework.service.logging.EventLoggerLogImpl;
import io.ingestr.framework.service.logging.LogContext;
import io.ingestr.framework.service.logging.LogEvent;
import io.ingestr.framework.service.logging.config.AbstractLoggerServiceKafkaFactory;
import io.micronaut.context.annotation.Factory;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@MicronautTest(environments = "local-kafka")
class EventLogRepositoryKafkaIT {

    @Inject
    EventLogRepository eventLogRepository;

    @Inject
    EventLogger eventLogger;

    @Inject
    LoaderDefinitionServices loaderDefinitionServices;


    @BeforeEach
    public void setup() {
    }

    @Factory
    public static class TestConfig extends AbstractLoggerServiceKafkaFactory {

        @Singleton
        public LoaderDefinitionServices loaderDefinitionServices() {
            LoaderDefinition ld = new LoaderDefinition("test-loader", "1.0.0");

            LoaderDefinitionServices definitionServices = new LoaderDefinitionServices(
                    ld
            );

            return definitionServices;
        }
    }

    @Test
    void shouldQueryKafkaForEventLogs() throws InterruptedException {
        List<String> ingestionIds = new ArrayList<>() {{
            add("ing-" + RandomStringUtils.randomNumeric(10));
            add("ing-" + RandomStringUtils.randomNumeric(10));
            add("ing-" + RandomStringUtils.randomNumeric(10));
            add("ing-" + RandomStringUtils.randomNumeric(10));
            add("ing-" + RandomStringUtils.randomNumeric(10));
        }};
        List<String> partitionIds = new ArrayList<>();
        for (int x = 0; x < 1000; x++) {
            partitionIds.add("part-" + UUID.randomUUID().toString());
        }

        //disable the log output temporarily
        ((EventLoggerLogImpl) eventLogger).setLogEvent(false);


        //given a topic full of messages

        for (int x = 0; x < 100_000; x++) {
            String taskIdentifier = UUID.randomUUID().toString();
            Collections.shuffle(ingestionIds);
            Collections.shuffle(partitionIds);

            String ingestionId = ingestionIds.get(0);

            eventLogger.log(LogEvent.debug("Ingestion was successful")
                    .event(Math.random() < .05 ? "ingestr.ingestion.failure" : "ingestr.ingestion.success")
                    .taskIdentifier(taskIdentifier)
                    .context(LogContext.INGESTION_TASK)
                    .property("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                    .property("ingestion", ingestionId)
                    .property("offset", "offset-" + x)
                    .property("partition", partitionIds.get(0))
                    .property("duration", RandomStringUtils.randomNumeric(3))
            );
        }
        ((EventLoggerLogImpl) eventLogger).setLogEvent(true);


        //Then. I search events by Partition in parallel
        Collections.shuffle(partitionIds);

        ExecutorService es = Executors.newFixedThreadPool(20);

        for (int x = 0; x < 20; x++) {
            final String partitionId = partitionIds.get(x);
            es.submit(new Runnable() {
                @Override
                public void run() {

                    EventLogRepository.EventLogResult logResults = eventLogRepository.query(EventLogRepository.EventLogQuery.builder()
                            .partition(partitionId)
                            .resultLimit(20)
//                            .from(Instant.now().minusSeconds(30))
                            .build());
                    assertEquals(20, logResults.getLogEvents().size());

                    for (LogEvent logEvent : logResults.getLogEvents()) {
                        log.info("LogEvent - {}", logEvent);
                        assertEquals(partitionId, logEvent.getProperties().get("partition"));
                    }

                }
            });
        }

        es.shutdown();
        es.awaitTermination(1, TimeUnit.DAYS);

//
//        EventLogRepository.PartitionExecutionSummary summary = eventLogRepository.partitionExecutionSummary(
//                EventLogRepository.PartitionExecutionSummaryQuery.builder()
//                        .loader(loaderDefinitionServices.getLoaderDefinition().getLoaderName())
//                        .partition(partitionId)
//                        .build()
//        );
//
//
//        for (EventLogRepository.PartitionExecutionSummaryItem item : summary.getItems()) {
//            log.info("Item - {}", item);
//
//        }
    }

}