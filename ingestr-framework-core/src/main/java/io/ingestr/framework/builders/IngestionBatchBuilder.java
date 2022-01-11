package io.ingestr.framework.builders;


import io.ingestr.framework.entities.Ingestion;
import io.ingestr.framework.entities.IngestionBatchJob;
import io.ingestr.framework.entities.IngestionSchedule;
import io.ingestr.framework.entities.Trigger;

import java.util.function.Supplier;

public class IngestionBatchBuilder {
    private Ingestion ingestion;

    public IngestionBatchBuilder( String identifier, String name,
                                 Supplier<? extends IngestionBatchJob> job) {
        this.ingestion = new Ingestion();
        this.ingestion.setIdentifier(identifier);
        this.ingestion.setName(name);
        this.ingestion.setJob(job);
    }

    public IngestionBatchBuilder description(String description) {
        this.ingestion.setDescription(description);
        return this;
    }


    public IngestionBatchBuilder addSchedule(IngestionSchedule.IngestionScheduleBuilder schedule) {
        this.ingestion.getIngestionSchedules().add(schedule.build());
        return  this;
    }


    public IngestionBatchBuilder addTrigger(Trigger.TriggerBuilder trigger) {
        this.ingestion.getTriggers().add(trigger.build());
        return this;
    }

    public Ingestion build() {
        return ingestion;
    }
}
