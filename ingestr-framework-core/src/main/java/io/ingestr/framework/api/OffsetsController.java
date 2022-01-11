package io.ingestr.framework.api;


import io.ingestr.framework.entities.Offset;
import io.ingestr.framework.repositories.OffsetRepository;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.runtime.context.scope.Refreshable;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Controller("/data-descriptor/{dataDescriptorIdentifier}/partitions/{key}/offset")
@Requires(beans = {OffsetRepository.class})
@Slf4j
@Refreshable
public class OffsetsController {
    @Inject
    private OffsetRepository offsetRepository;

    public OffsetsController(OffsetRepository offsetRepository) {
        this.offsetRepository = offsetRepository;
    }

    @Get(produces = MediaType.APPLICATION_JSON)
    public Offset findByKey(
            @PathVariable("dataDescriptorIdentifier") String dataDescriptorIdentifier,
            @PathVariable("key") String key
    ) {
        log.debug("Looking up offset for {}", key);
        return offsetRepository.findByPartitionKey(key)
                .orElse(null);
    }
}