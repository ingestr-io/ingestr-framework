package io.ingestr.framework.repositories;

import io.ingestr.framework.entities.Loader;
import io.ingestr.framework.service.db.RepositoryService;
import io.ingestr.framework.service.db.PersistenceRepository;
import io.ingestr.framework.service.db.model.SaveEntityRequest;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.List;

@Singleton
public class LoaderRepository implements PersistenceRepository<Loader> {
    private RepositoryService repositoryService;

    @Inject
    public LoaderRepository(RepositoryService repositoryService) {
        this.repositoryService = repositoryService;
    }

    public void save(Loader entity) {
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

    public List<Loader> findAll() {
        return this.repositoryService
                .findAll(Loader.class);
    }
}
