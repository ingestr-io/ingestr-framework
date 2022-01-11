package io.ingestr.framework.service.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ingestr.framework.kafka.ObjectMapperFactory;
import io.ingestr.framework.service.db.model.SaveEntityRequest;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class acts as an in memory only database for the fastest lookup/processing times of the loaders.
 * <p>
 * Ideal for testing
 */
@Slf4j
public class RepositoryServiceInMemoryImpl implements RepositoryService {
    private Map<String, Map<String, Object>> db = Collections.synchronizedMap(new HashMap<>());
    private Lock lock = new ReentrantLock();
    private ObjectMapper objectMapper = ObjectMapperFactory.kafkaMessageObjectMapper();

    @Override
    public void persist(SaveEntityRequest request) {
        assert request.getVersion() != null;
        assert request.getEntity() != null;
        assert request.getIdentifier() != null;

        lock.lock();

        //save the record to the in memory database
        if (!db.containsKey(request.persistenceKey())) {
            db.put(request.persistenceKey(), Collections.synchronizedMap(new HashMap<>()));
        }
        db.get(request.persistenceKey()).put(request.getIdentifier(), request.getEntity());
        lock.unlock();
    }

    @Override
    public <T> Optional<T> findById(Class<T> entityClass, String id) {
        lock.lock();
        try {
            return (Optional<T>) Optional.ofNullable(
                    this.db.getOrDefault(entityClass.getSimpleName(), new HashMap<>())
                            .getOrDefault(id, null)
            );
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <T> List<T> findAll(Class<T> entityClass) {
        lock.lock();
        try {
            if (!this.db.containsKey(entityClass.getSimpleName())) {
                return new ArrayList<>();
            }
            List<T> results = new ArrayList<>();
            for (Object va : this.db.get(entityClass.getSimpleName())
                    .values()) {
                results.add((T) va);
            }
            return results;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void sync() {
        //nothing to do here, we
    }
}
