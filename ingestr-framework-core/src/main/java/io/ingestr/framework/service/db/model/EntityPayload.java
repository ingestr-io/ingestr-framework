package io.ingestr.framework.service.db.model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.*;

import java.time.ZonedDateTime;

@Getter
@Builder
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class EntityPayload {
    private String identifier;
    private String className;
    private String version;
    private ZonedDateTime createdAt;
    private JsonNode payload;
}
