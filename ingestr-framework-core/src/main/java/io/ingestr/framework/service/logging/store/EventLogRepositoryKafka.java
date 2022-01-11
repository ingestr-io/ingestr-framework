package io.ingestr.framework.service.logging.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ingestr.framework.kafka.KafkaQueryHandler;
import io.ingestr.framework.kafka.ObjectMapperFactory;
import io.ingestr.framework.service.logging.LogEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.*;

@Slf4j
public class EventLogRepositoryKafka implements EventLogRepository {
    private ObjectMapper objectMapper = ObjectMapperFactory.kafkaMessageObjectMapper();
    private KafkaQueryHandler kafkaQueryHandler;
    private String topic;
    private Integer topicPartition;

    public EventLogRepositoryKafka(KafkaQueryHandler kafkaQueryHandler, String topic, Integer topicPartition) {
        this.kafkaQueryHandler = kafkaQueryHandler;
        this.topic = topic;
        this.topicPartition = topicPartition;
    }

    public EventLogRepositoryKafka(KafkaQueryHandler kafkaQueryHandler, String topic) {
        this.kafkaQueryHandler = kafkaQueryHandler;
        this.topic = topic;
        this.topicPartition = null;
    }

    @Override
    public PartitionExecutionSummary partitionExecutionSummary(PartitionExecutionSummaryQuery query) {
        EventLogResult results = query(EventLogQuery.builder()
                .loader(query.getLoader())
                .partition(query.getPartition())
                .from(query.getFrom())
                .to(query.getTo())
                .resultLimit(query.getResultLimit())
                .offset(query.getOffset())
                .build());


        PartitionExecutionSummary.PartitionExecutionSummaryBuilder b = PartitionExecutionSummary.builder()
                .partition(query.getPartition())
                .loader(query.getLoader());

        Map<String, PartitionExecutionSummaryItem> items = new HashMap<>();

        for (LogEvent logEvent : results.getLogEvents()) {
            //1. see if the item exists by task id
            if (!items.containsKey(logEvent.getTaskIdentifier())) {
                PartitionExecutionSummaryItem p = new PartitionExecutionSummaryItem();
                p.setTaskIdentifier(logEvent.getTaskIdentifier());
                items.put(logEvent.getTaskIdentifier(), p);
            }
            PartitionExecutionSummaryItem item = items.get(logEvent.getTaskIdentifier());
            item.setTimestamp(logEvent.getTimestamp());


            if (logEvent.getEvent().equalsIgnoreCase("ingestr.ingestion.success")) {
                item.setStatus(ExecutionStatus.Success);
            } else if (logEvent.getEvent().equalsIgnoreCase("ingestr.ingestion.failure")) {
                item.setStatus(ExecutionStatus.Fail);
            }
            if (logEvent.getProperties().containsKey("duration")) {
                try {
                    item.setDuration(Long.parseLong(
                            logEvent.getProperties().get("duration")
                    ));
                } catch (Exception e) {

                }
            }
            if (logEvent.getProperties().containsKey("offset")) {
                item.setOffset(logEvent.getProperties().get("offset"));
            }
        }
        List<PartitionExecutionSummaryItem> it = new ArrayList<>(items.values());
        Collections.sort(it, (o1, o2) -> new CompareToBuilder()
                .append(o1.getTimestamp(), o2.getTimestamp())
                .build());

        return b.items(it)
                .build();
    }

    @Override
    public EventLogResult query(EventLogQuery query) {
        log.debug("Performing Event Log Query - {}", query);
        KafkaQueryHandler.KafkaQuery.KafkaQueryBuilder kqb = KafkaQueryHandler.KafkaQuery.builder()
                .topic(topic)
                .partition(topicPartition)
                .resultLimit(query.getResultLimit())
                .from(query.getFrom())
                .to(query.getTo());
        if (query.getTimeLimitMs() != null) {
            kqb.timeLimitMs(query.getTimeLimitMs());
        }

        if (query.getLoader() != null) {
            kqb.headerFilter(KafkaQueryHandler.KafkaHeaderValue.of("loader", query.getLoader()));
        }
        if (query.getPartition() != null) {
            kqb.headerFilter(KafkaQueryHandler.KafkaHeaderValue.of("partition", query.getPartition()));
        }
        if (query.getOffset() != null) {
            kqb.offset(query.getOffset());
        }

        KafkaQueryHandler.KafkaQueryResult results = kafkaQueryHandler.query(kqb.build());

        EventLogResult.EventLogResultBuilder res = EventLogResult.builder();
        for (ConsumerRecord<String, String> record : results.getRecords()) {
            try {
                res.logEvent(
                        objectMapper.readValue(record.value(), LogEvent.class)
                );
                //catch but ignore any errors, we simply skip these results
            } catch (JsonProcessingException e) {
                log.error(e.getMessage(), e);
            }
        }

        return res
                .build();
    }


}
