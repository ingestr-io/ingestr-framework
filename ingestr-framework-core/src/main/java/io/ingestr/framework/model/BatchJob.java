package io.ingestr.framework.model;


import io.ingestr.framework.entities.IngestionBatchJob;
import io.ingestr.framework.entities.IngestionRequest;
import io.ingestr.framework.entities.IngestionResult;
import io.ingestr.framework.exception.NoResultsFoundException;
import io.ingestr.framework.service.logging.LogContext;
import io.ingestr.framework.service.logging.LogEvent;
import io.ingestr.framework.service.logging.EventLogger;

import java.util.Map;

public abstract class BatchJob implements LoaderJob, IngestionBatchJob {
    private EventLogger eventLogger;
    private String taskExecutionIdentifier;
    private String loader;
    private String partitionKey;
    private String ingestionIdentifier;

    public void setEventLogger(
            EventLogger eventLogger,
            String ingestionIdentifier,
            String taskExecutionIdentifier,
            String loader,
            String partitionKey
    ) {
        this.eventLogger = eventLogger;
        this.taskExecutionIdentifier = taskExecutionIdentifier;
        this.loader = loader;
        this.partitionKey = partitionKey;
        this.ingestionIdentifier = ingestionIdentifier;
    }

    public void logEvent(BatchJobEvent.BatchJobEventBuilder jobEvent) {
        BatchJobEvent e = jobEvent.build();

        LogEvent.LogEventBuilder eb = LogEvent
                .level(e.getLogLevel(), e.getMessage(), e.getProps())
                .taskIdentifier(taskExecutionIdentifier)
                .context(LogContext.INGESTION_TASK)
                .event("ingestr.ingestion.custom")
                .property("loader", loader)
                .property("ingestion", ingestionIdentifier)
                .property("partition", partitionKey);

        for (Map.Entry<String, String> en : e.getProperties().entrySet()) {
            eb.property(en.getKey(), en.getValue());
        }
        eventLogger.log(eb);
    }

    public abstract IngestionResult ingest(IngestionRequest request)
            throws NoResultsFoundException;

}
