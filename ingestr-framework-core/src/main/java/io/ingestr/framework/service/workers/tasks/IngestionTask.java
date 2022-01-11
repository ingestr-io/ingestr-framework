package io.ingestr.framework.service.workers.tasks;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ingestr.framework.entities.*;
import io.ingestr.framework.kafka.ObjectMapperFactory;
import io.ingestr.framework.model.BatchJob;
import io.ingestr.framework.repositories.OffsetRepository;
import io.ingestr.framework.service.db.LoaderDefinitionServices;
import io.ingestr.framework.service.logging.LogContext;
import io.ingestr.framework.service.logging.LogEvent;
import io.ingestr.framework.service.logging.EventLogger;
import io.ingestr.framework.service.producer.IngestrProducer;
import io.ingestr.framework.service.producer.IngestrProducerRecord;
import io.ingestr.framework.service.queue.model.IngestPartitionQueueItem;
import io.ingestr.framework.validation.ParameterDescriptorParser;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micronaut.context.annotation.Prototype;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.commons.lang3.time.StopWatch;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@Prototype
public class IngestionTask implements Runnable {
    private IngestPartitionQueueItem ingestPartitionQueueItem;
    private final OffsetRepository offsetRepository;
    private final EventLogger eventLogger;
    private final MeterRegistry meterRegistry;
    private final IngestrProducer producer;
    private final LoaderDefinitionServices loaderDefinitionServices;
    private ObjectMapper objectMapper = ObjectMapperFactory.kafkaMessageObjectMapper();
    private final String ingestionTopicPattern;

    private Boolean debug;

    @Inject
    public IngestionTask(
            OffsetRepository offsetRepository,
            EventLogger eventLogger,
            MeterRegistry meterRegistry,
            IngestrProducer producer,
            LoaderDefinitionServices loaderDefinitionServices,
            @Value("${ingestion.topicPattern}") String ingestionTopicPattern,
            @Value("${ingestion.debug:false}") Boolean debug) {
        this.offsetRepository = offsetRepository;
        this.eventLogger = eventLogger;
        this.meterRegistry = meterRegistry;
        this.producer = producer;
        this.loaderDefinitionServices = loaderDefinitionServices;
        this.ingestionTopicPattern = ingestionTopicPattern;
        this.debug = debug;
    }

    public void setIngestPartitionQueueItem(IngestPartitionQueueItem ingestPartitionQueueItem) {
        this.ingestPartitionQueueItem = ingestPartitionQueueItem;
    }


    @Override
    public void run() {
        //Create a taskExecution Identifier
        String taskIdentifier = UUID.randomUUID().toString();
        boolean trace = debug;
        if (ingestPartitionQueueItem.getPartition().getTracingEnabledUntil() != null) {
            //check to see if the trace has expired for this partition otherwise enabled tracing
            if (Instant.now().isBefore(ingestPartitionQueueItem.getPartition().getTracingEnabledUntil())) {
                trace = true;
            }
        }
        String ingestionEvent = "";
        //determine the relevant bits for this queue item
        log.debug("Executing Ingestion Task item={}", ingestPartitionQueueItem);
        if (trace) {
            eventLogger.log(LogEvent
                    .debug("Assigned Partition Queue Item for processing")
                    .taskIdentifier(taskIdentifier)
                    .context(LogContext.INGESTION_TASK)
                    .event("ingestr.ingestion.ingestPartitionQueueItem")
                    .property("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                    .property("ingestion", ingestPartitionQueueItem.getIngestionIdentifier())
                    .property("partition", ingestPartitionQueueItem.getPartition().getKey())
                    .subject(ingestPartitionQueueItem)
            );
        }
        Ingestion ingestion = loaderDefinitionServices
                .getLoaderDefinition()
                .findByIngestionId(ingestPartitionQueueItem.getIngestionIdentifier())
                .orElseThrow(() -> new IllegalStateException("Could not find an Ingestion by identifier = " + ingestPartitionQueueItem.getIngestionIdentifier()));

        DataDescriptor dataDescriptor = loaderDefinitionServices
                .getLoaderDefinition()
                .findByDescriptorId(ingestPartitionQueueItem.getPartition().getDataDescriptorIdentifier())
                .orElseThrow(() -> new IllegalStateException("Could not find a Data Descriptor by identifier = " +
                        ingestPartitionQueueItem.getPartition().getDataDescriptorIdentifier()));


        Optional<Offset> offset;
        if (ingestPartitionQueueItem.getOverrideOffset() == null) {
            offset = offsetRepository.findByPartitionKey(ingestPartitionQueueItem.getPartition().getKey());
        } else {
            offset = Optional.of(ingestPartitionQueueItem.getOverrideOffset());
        }

        Optional<IngestionSchedule> schedule = Optional.empty();
        if (ingestPartitionQueueItem.getScheduleIdentifier() != null) {
            schedule = loaderDefinitionServices.getLoaderDefinition()
                    .findByScheduleId(ingestPartitionQueueItem.getScheduleIdentifier());
        }

        //Create the parameters from the Data Descriptor Level
        Parameters parameters = ParameterDescriptorParser
                .parse(dataDescriptor.getParameterDescriptors(), ingestPartitionQueueItem.getParameters());

        //Create the parameters from the scheduler level if a scheduler invoked this execution
        if (ingestPartitionQueueItem.getScheduleIdentifier() != null) {
            //merge these parameters ingestion parameters
            parameters.merge(ParameterDescriptorParser
                    .parse(schedule.get().getParameterDescriptors(), ingestPartitionQueueItem.getParameters())
            );
        }

        StopWatch watch = new StopWatch();
        watch.start();

        IngestionRequest ingestionRequest = IngestionRequest.builder()
                .dataDescriptor(dataDescriptor)
                .ingestion(ingestion)
                .partition(ingestPartitionQueueItem.getPartition())
                .schedule(schedule)
                .lastOffset(offset)
                .properties(ingestPartitionQueueItem.getProperties() != null ? ingestPartitionQueueItem.getProperties() : new HashMap<>())
                .parameters(parameters)
                .build();
        if (trace) {
            eventLogger.log(LogEvent
                    .debug("Invoking job with IngestionRequest for handling")
                    .taskIdentifier(taskIdentifier)
                    .context(LogContext.INGESTION_TASK)
                    .event("ingestr.ingestion.ingestionRequest")
                    .property("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                    .property("ingestion", ingestion.getIdentifier())
                    .property("partition", ingestPartitionQueueItem.getPartition().getKey())
                    .subject(ingestionRequest)
            );
        }

        BatchJob job = (BatchJob) ingestion.getJob().get();
        //Assign the log information needed to the job so the job can also send event log messages
        job.setEventLogger(
                eventLogger,
                ingestion.getIdentifier(),
                taskIdentifier,
                loaderDefinitionServices.getLoaderDefinition().getLoaderName(),
                ingestPartitionQueueItem.getPartition().getKey());

        IngestionResult result = null;

        try {
            result = job.ingest(ingestionRequest);

            if (trace) {
                eventLogger.log(LogEvent
                        .debug("Ingestion Result post processing")
                        .taskIdentifier(taskIdentifier)
                        .context(LogContext.INGESTION_TASK)
                        .event("ingestr.ingestion.ingestionResult")
                        .property("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                        .property("ingestion", ingestion.getIdentifier())
                        .property("partition", ingestPartitionQueueItem.getPartition().getKey())
                        .subject(result)
                );
            }


            boolean sendResult = true;

            //1. If there is a previous offset, compare to current
            if (offset.isPresent()) {
                if (result.getOffset().getOffsetEntries().equals(offset.get().getOffsetEntries())) {

                    //offsets have not moved from before and after the execution
                    //likely means the data from source has not changed since last we executed
                    boolean isDuplicate = true;

                    //As a fall back for scenarios where the data has changed, but the offset has not,
                    //check the update hash for differences at a data level
                    if (result.getOffset().getUpdateHash().isPresent() && offset.get().getUpdateHash().isPresent()) {

                        log.debug("Found matching update hash");

                        isDuplicate = result.getOffset().getUpdateHash().get().equals(offset.get().getUpdateHash().get());
                    }

                    if (isDuplicate) {
                        eventLogger.log(LogEvent
                                .info("Ingestion contains duplication, offset has not changed between execution")
                                .taskIdentifier(taskIdentifier)
                                .event("ingestr.ingestion.offsetDuplication")
                                .context(LogContext.INGESTION_TASK)
                                .property("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                                .property("ingestion", ingestion.getIdentifier())
                                .property("offset", result.getOffset().toString())
                                .property("dataDescriptor", dataDescriptor.getIdentifier())
                                .property("schedule", schedule.isEmpty() ? "" : schedule.get().getIdentifier())
                                .property("partition", ingestPartitionQueueItem.getPartition().getKey())
                                .property("duration", String.valueOf(watch.getTime()))
                                .subject(offset));

                        Counter.builder("ingestr.ingestion.offsetDuplication")
                                .tag("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                                .tag("ingestion", ingestion.getIdentifier())
                                .tag("dataDescriptor", dataDescriptor.getIdentifier())
                                .tag("schedule", schedule.isEmpty() ? "" : schedule.get().getIdentifier())
                                .description("Counter of Ingestion result contains duplication as the offset has not changed between execution")
                                .register(meterRegistry)
                                .increment();

                        sendResult = false;
                    } else {
                        eventLogger.log(LogEvent
                                .info("Ingestion offset unchanged but data changes were detected")
                                .taskIdentifier(taskIdentifier)
                                .event("ingestr.ingestion.dataChanged")
                                .context(LogContext.INGESTION_TASK)
                                .property("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                                .property("ingestion", ingestion.getIdentifier())
                                .property("offset", result.getOffset().toString())
                                .property("dataDescriptor", dataDescriptor.getIdentifier())
                                .property("schedule", schedule.isEmpty() ? null : schedule.get().getIdentifier())
                                .property("partition", ingestPartitionQueueItem.getPartition().getKey())
                                .property("duration", String.valueOf(watch.getTime()))
                                .subject(offset)
                        );

                        Counter.builder("ingestr.ingestion.dataChanged")
                                .tag("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                                .tag("ingestion", ingestion.getIdentifier())
                                .tag("schedule", schedule.isEmpty() ? "" : schedule.get().getIdentifier())
                                .tag("dataDescriptor", dataDescriptor.getIdentifier())
                                .description("Counter of Ingestion offset unchanged but data changes detected")
                                .register(meterRegistry)
                                .increment();

                        //continue sending the data as the updateHash tells us that the data is uniquely different despite
                        //having the same offset
                        sendResult = true;
                    }
                }
            }


            if (sendResult) {

                StrSubstitutor strSubstitutor = new StrSubstitutor(new HashMap<>() {{
                    put("loaderName", loaderDefinitionServices.getLoaderDefinition().getLoaderName());
                    if (StringUtils.isNotBlank(dataDescriptor.getTopic())) {
                        put("topic", dataDescriptor.getTopic());
                    } else {
                        put("topic", dataDescriptor.getIdentifier());
                    }
                }}, "{", "}");

                String topic = strSubstitutor.replace(ingestionTopicPattern);

                String value = null;
                if (result.getData().isPresent()) {
                    value = objectMapper.writeValueAsString(result.getData().get());
                } else if (result.getRawLargeData().isPresent()) {
                    value = "";
                } else {
                    throw new IllegalStateException("IngestionResult does not contain 'data' or 'rawLargeData'");
                }

                //send the result to the topic
                IngestrProducerRecord.IngestrProducerRecordBuilder recBuilder = IngestrProducerRecord.builder()
                        .topic(topic)
                        .partition(null)
                        .key(result.getKey() == null || result.getKey().isEmpty() ? null : result.getKey().get())
                        .header("data_descriptor", dataDescriptor.getIdentifier())
                        .header("partition", ingestPartitionQueueItem.getPartition().toStringShort())
                        .header("offset", result.getOffset().toStringShort());

                if (result.getMeta() != null && !result.getMeta().isEmpty()) {
                    for (Map.Entry<String, String> e : result.getMeta().entrySet()) {
                        recBuilder.header(e.getKey(), e.getValue());
                    }
                }
                producer.send(recBuilder.build());

                offsetRepository.save(result.getOffset());
            }

            Counter.builder("ingestr.ingestion.success")
                    .tag("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                    .tag("ingestion", ingestion.getIdentifier())
                    .tag("dataDescriptor", dataDescriptor.getIdentifier())
                    .description("Counter of the failed Ingestions")
                    .register(meterRegistry)
                    .increment();

            eventLogger.log(LogEvent.debug("Ingestion was successful")
                    .event("ingestr.ingestion.success")
                    .taskIdentifier(taskIdentifier)
                    .context(LogContext.INGESTION_TASK)
                    .property("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                    .property("ingestion", ingestion.getIdentifier())
                    .property("offset", result == null ? null : result.getOffset().toString())
                    .property("dataDescriptor", dataDescriptor.getIdentifier())
                    .property("schedule", schedule.isEmpty() ? "" : schedule.get().getIdentifier())
                    .property("partition", ingestPartitionQueueItem.getPartition().getKey())
                    .property("duration", String.valueOf(watch.getTime()))
                    .subject(offset));

        } catch (Throwable t) {
            Counter.builder("ingestr.ingestion.failure")
                    .tag("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                    .tag("ingestion", ingestion.getIdentifier())
                    .tag("dataDescriptor", dataDescriptor.getIdentifier())
                    .description("Counter of the failed Ingestions")
                    .register(meterRegistry)
                    .increment();

            eventLogger
                    .log(LogEvent.error("Ingestion Failed")
                            .body(t.getMessage(), t)
                            .taskIdentifier(taskIdentifier)
                            .context(LogContext.INGESTION_TASK)
                            .event("ingestr.ingestion.failure")
                            .property("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                            .property("ingestion", ingestion.getIdentifier())
                            .property("dataDescriptor", dataDescriptor.getIdentifier())
                            .property("partition", ingestPartitionQueueItem.getPartition().getKey())
                            .property("duration", String.valueOf(watch.getTime()))
                            .subject(offset));

        } finally {
            Timer.builder("ingestr.ingestion.completed")
                    .tag("loader", loaderDefinitionServices.getLoaderDefinition().getLoaderName())
                    .tag("ingestion", ingestion.getIdentifier())
                    .tag("dataDescriptor", dataDescriptor.getIdentifier())
                    .description("The amount of time taken to perform an Ingestion")
                    .register(meterRegistry)
                    .record(watch.getTime(), TimeUnit.MILLISECONDS);
        }
    }
}
