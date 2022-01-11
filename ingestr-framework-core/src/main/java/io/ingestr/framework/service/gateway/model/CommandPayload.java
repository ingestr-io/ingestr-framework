package io.ingestr.framework.service.gateway.model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.*;

import java.time.ZonedDateTime;

@Getter
@Builder
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class CommandPayload {
    private String identifier;
    private String className;
    private ZonedDateTime createdAt;
    private JsonNode payload;
}
