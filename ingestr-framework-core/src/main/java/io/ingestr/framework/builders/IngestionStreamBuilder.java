package io.ingestr.framework.builders;


import io.ingestr.framework.entities.Ingestion;
import io.ingestr.framework.entities.IngestionStreamJob;

import java.util.function.Supplier;

public class IngestionStreamBuilder {
    private Ingestion ingestion;

    public IngestionStreamBuilder(String identifier, String name,
                                  Supplier<? extends IngestionStreamJob> job) {
        this.ingestion = new Ingestion();
        this.ingestion.setIdentifier(identifier);
        this.ingestion.setName(name);
        this.ingestion.setJob(job);
    }

    public IngestionStreamBuilder description(String description) {
        this.ingestion.setDescription(description);
        return this;
    }


    public Ingestion build() {
        return ingestion;
    }
}
