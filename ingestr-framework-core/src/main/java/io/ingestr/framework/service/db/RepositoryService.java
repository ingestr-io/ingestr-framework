package io.ingestr.framework.service.db;

import io.ingestr.framework.service.db.model.SaveEntityRequest;

import java.util.List;
import java.util.Optional;

public interface RepositoryService {

    void sync();

    void persist(SaveEntityRequest request);

    <T> Optional<T> findById(Class<T> entityClass, String id);

    <T> List<T> findAll(Class<T> entityClass);
}
