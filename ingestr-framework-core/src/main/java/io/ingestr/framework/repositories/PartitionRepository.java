package io.ingestr.framework.repositories;

import io.ingestr.framework.entities.Partition;
import io.ingestr.framework.service.db.RepositoryService;
import io.ingestr.framework.service.db.PersistenceRepository;
import io.ingestr.framework.service.db.model.SaveEntityRequest;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Singleton
public class PartitionRepository implements PersistenceRepository<Partition> {
    private RepositoryService repositoryService;

    @Inject
    public PartitionRepository(RepositoryService repositoryService) {
        this.repositoryService = repositoryService;
    }

    public void save(Partition entity) {
        Validate.notBlank(entity.getDataDescriptorIdentifier(), "Data Descriptor Identifier must be set");
        Validate.notNull(entity.getPartitionEntries(), "Must contain a set of Partition Entries");

        if (entity.getEnabled() == null) {
            entity.setEnabled(true);
        }
        entity.setUpdatedAt(ZonedDateTime.now());

        //update kafka
        repositoryService.persist(
                SaveEntityRequest.builder()
                        .entity(entity)
                        .version("1.0.0")
                        .identifier(entity.getKey())
                        .build()
        );
    }

    public void delete(Partition entity) {
        Validate.notBlank(entity.getKey(), "Partition Key cannot be blank");

        //Set the Delete flag and save the entity (so its committed)
        Partition p = repositoryService.findById(Partition.class, entity.getKey())
                .orElseThrow(() -> new IllegalArgumentException("Could not find and Existing partition for " + entity.getKey()));

        p.setDeleted(true);
        p.setDeletedAt(ZonedDateTime.now());
        save(p);
    }

    public void unDelete(Partition entity) {
        Validate.notBlank(entity.getKey(), "Partition Key cannot be blank");

        //Set the Delete flag and save the entity (so its committed)
        Partition p = repositoryService.findById(Partition.class, entity.getKey())
                .orElseThrow(() -> new IllegalArgumentException("Could not find and Existing partition for " + entity.getKey()));

        p.setDeleted(false);
        save(p);
    }

    public Optional<Partition> findByKey(String key) {
        return repositoryService.findById(Partition.class, key);
    }

    public List<Partition> findAll() {
        return findAll(false);
    }

    public List<Partition> findAll(boolean includeDeleted) {
        return this.repositoryService.findAll(Partition.class)
                .stream()
                .filter(p -> !includeDeleted ? (p.getDeleted() == null || !p.getDeleted()) : true)
                .collect(Collectors.toList());
    }

    public List<Partition> findByDataDescriptorIdentifier(String dataDescriptorIdentifier) {
        return findByDataDescriptorIdentifier(dataDescriptorIdentifier, false);
    }

    public List<Partition> findByDataDescriptorIdentifier(String dataDescriptorIdentifier, boolean includeDeleted) {
        return findAll(includeDeleted)
                .stream()
                .filter(p -> StringUtils.equalsIgnoreCase(p.getDataDescriptorIdentifier(), dataDescriptorIdentifier))
                .collect(Collectors.toList());
    }
}
