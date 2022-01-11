package io.ingestr.framework.repositories;

import io.ingestr.framework.entities.Loader;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@MicronautTest
class LoaderRepositoryTest {

    @Inject
    LoaderRepository loaderRepository;

    @Test
    void shouldLoadFromEntityTopic() {
        assertTrue(loaderRepository.findAll().isEmpty());

        Loader l = Loader.builder()
                .identifier("l1")
                .name("Loader 1")
                .build();
        loaderRepository.save(l);

        //expect that there is nothing in it
        assertFalse(loaderRepository.findAll().isEmpty());



    }

}