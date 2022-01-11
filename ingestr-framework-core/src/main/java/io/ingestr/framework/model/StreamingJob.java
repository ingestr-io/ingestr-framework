package io.ingestr.framework.model;

import io.ingestr.framework.entities.IngestionResult;

public abstract class StreamingJob implements LoaderJob {
    private boolean running = false;

    public abstract void ingest(StreamingJobContext context);

    public boolean isRunning() {
        return running;
    }

    public void shutdown() {
        this.running = false;
    }

    public interface StreamingJobContext {
        void emitResult(IngestionResult ingestionResult);
    }
}
