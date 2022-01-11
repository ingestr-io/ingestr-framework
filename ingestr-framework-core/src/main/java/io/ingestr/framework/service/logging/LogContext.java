package io.ingestr.framework.service.logging;

public enum LogContext {
    MAIN,
    WORKER,
    TRIGGER,
    PARTITION_REGISTRATION,
    INGESTION_TASK;
}
