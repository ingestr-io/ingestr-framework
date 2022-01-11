package io.ingestr.framework.repositories;

import io.ingestr.framework.entities.DataDescriptor;
import io.ingestr.framework.service.db.PersistenceRepository;
import io.ingestr.framework.service.db.RepositoryService;
import io.ingestr.framework.service.db.model.SaveEntityRequest;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.Validate;

import java.time.ZonedDateTime;
import java.util.*;

@Singleton
public class DataDescriptorRepository implements PersistenceRepository<DataDescriptor> {
    @Inject
    private RepositoryService repositoryService;

    public DataDescriptorRepository(RepositoryService repositoryService) {
        this.repositoryService = repositoryService;
    }

    public void save(DataDescriptor entity) {
        Validate.notBlank(entity.getIdentifier(), "Must have an Identifier");
        Validate.notBlank(entity.getName(), "Must have a Name");

        entity.setUpdatedAt(ZonedDateTime.now());

        //update kafka
        repositoryService.persist(
                SaveEntityRequest.builder()
                        .entity(entity)
                        .version("1.0.0")
                        .identifier(entity.getIdentifier())
                        .build()
        );
    }

    public List<DataDescriptor> findAll() {
        return new ArrayList<>(this.repositoryService.findAll(DataDescriptor.class));
    }


    public Optional<DataDescriptor> findByIdentifier(String identifier) {
        return
                this.repositoryService.findById(DataDescriptor.class, identifier);
    }
}
