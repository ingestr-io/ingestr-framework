package io.ingestr.framework.repositories;

import io.ingestr.framework.entities.Offset;
import io.ingestr.framework.service.db.PersistenceRepository;
import io.ingestr.framework.service.db.RepositoryService;
import io.ingestr.framework.service.db.model.SaveEntityRequest;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.Validate;

import java.time.ZonedDateTime;
import java.util.*;

@Singleton
public class OffsetRepository implements PersistenceRepository<Offset> {
    private RepositoryService repositoryService;

    @Inject
    public OffsetRepository(RepositoryService repositoryService) {
        this.repositoryService = repositoryService;
    }

    public void save(Offset entity) {
        Validate.notBlank(entity.getPartitionKey(), "Must contain a Partition Key");
        Validate.notNull(entity.getOffsetEntries(), "Must contain a set of Offset Entries");

        entity.setUpdatedAt(ZonedDateTime.now());
        //update kafka
        repositoryService.persist(
                SaveEntityRequest.builder()
                        .entity(entity)
                        .version("1.0.0")
                        .identifier(entity.getPartitionKey())
                        .build()
        );
    }

    public List<Offset> findAll() {
        return new ArrayList<>(this.repositoryService.findAll(Offset.class));
    }


    public Optional<Offset> findByPartitionKey(String partitionKey) {
        return this.repositoryService.findById(Offset.class, partitionKey);
    }

}
