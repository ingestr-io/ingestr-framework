package io.ingestr.framework.entities;

public interface IngestionContext {

    Object getConfig(String key);
}
