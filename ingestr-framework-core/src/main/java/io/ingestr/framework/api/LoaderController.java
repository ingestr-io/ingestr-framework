package io.ingestr.framework.api;


import io.ingestr.framework.entities.Loader;
import io.ingestr.framework.repositories.LoaderRepository;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.runtime.context.scope.Refreshable;
import jakarta.inject.Inject;

import java.util.List;

@Controller("/loaders")
@Requires(beans = {LoaderRepository.class})
@Refreshable
public class LoaderController {
    @Inject
    private LoaderRepository loaderRepository;

    public LoaderController(LoaderRepository loaderRepository) {
        this.loaderRepository = loaderRepository;
    }

    @Get(produces = MediaType.APPLICATION_JSON)
    public List<Loader> findAll() {
        return loaderRepository.findAll();
    }
}