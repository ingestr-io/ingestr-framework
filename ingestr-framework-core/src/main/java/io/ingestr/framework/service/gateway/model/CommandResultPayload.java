package io.ingestr.framework.service.gateway.model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.*;

import java.time.ZonedDateTime;

@Getter
@Builder
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class CommandResultPayload {
    private String identifier;
    private String commandIdentifier;
    private String className;
    private ZonedDateTime createdAt;
    private String commandGroup;
    private JsonNode payload;
}
