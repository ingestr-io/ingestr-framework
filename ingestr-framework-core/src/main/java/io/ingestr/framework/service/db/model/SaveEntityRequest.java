package io.ingestr.framework.service.db.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SaveEntityRequest {
    private String identifier;
    private Object entity;
    private String version;


    public String persistenceKey() {
        return getEntity().getClass().getSimpleName();
    }
}