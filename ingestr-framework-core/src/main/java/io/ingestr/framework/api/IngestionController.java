package io.ingestr.framework.api;


import io.ingestr.framework.entities.Ingestion;
import io.ingestr.framework.repositories.IngestionRepository;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.runtime.context.scope.Refreshable;
import jakarta.inject.Inject;

import java.util.List;

@Controller("/data-descriptor/{dataDescriptorIdentifier}/ingestions")
@Requires(beans = {IngestionRepository.class})
@Refreshable
public class IngestionController {
    @Inject
    private IngestionRepository ingestionRepository;

    public IngestionController(IngestionRepository ingestionRepository) {
        this.ingestionRepository = ingestionRepository;
    }

    @Get(produces = MediaType.APPLICATION_JSON)
    public List<Ingestion> findAll(
            @PathVariable("dataDescriptorIdentifier") String dataDescriptorIdentifier
    ) {
        return ingestionRepository.findByDataDescriptorIdentifier(dataDescriptorIdentifier);
    }
}