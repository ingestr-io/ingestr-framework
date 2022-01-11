package io.ingestr.framework.repositories;

import io.ingestr.framework.entities.Ingestion;
import io.ingestr.framework.service.db.PersistenceRepository;
import io.ingestr.framework.service.db.RepositoryService;
import io.ingestr.framework.service.db.model.SaveEntityRequest;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Singleton
public class IngestionRepository implements PersistenceRepository<Ingestion> {
    private RepositoryService repositoryService;

    @Inject
    public IngestionRepository(
            RepositoryService repositoryService) {
        this.repositoryService = repositoryService;
    }

    public void save(Ingestion entity) {
        Validate.notBlank(entity.getIdentifier(), "Must contain an Identifier");
        Validate.notBlank(entity.getName(), "Must contain a Name");
        Validate.notBlank(entity.getDataDescriptorIdentifier(), "Must contain a Data Descriptor Identifier");

        entity.setUpdatedAt(ZonedDateTime.now());

        //update the localdb
        //update kafka
        repositoryService.persist(
                SaveEntityRequest.builder()
                        .entity(entity)
                        .version("1.0.0")
                        .identifier(entity.getIdentifier())
                        .build()
        );
    }

    public List<Ingestion> findAll() {
        return new ArrayList<>(this.repositoryService.findAll(Ingestion.class));
    }

    public List<Ingestion> findByDataDescriptorIdentifier(String dataDescriptorIdentifier) {
        return findAll()
                .stream()
                .filter(i -> StringUtils.equalsIgnoreCase(i.getDataDescriptorIdentifier(), dataDescriptorIdentifier))
                .collect(Collectors.toList());
    }


    public Optional<Ingestion> findByIdentifier(String identifier) {
        return repositoryService.findById(Ingestion.class, identifier);
    }
}
