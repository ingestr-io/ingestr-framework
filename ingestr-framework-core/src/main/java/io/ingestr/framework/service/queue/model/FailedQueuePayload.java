package io.ingestr.framework.service.queue.model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.*;

import java.util.Map;

@Getter
@Builder
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class FailedQueuePayload {
    private String reason;
    private String message;
    private Map<String, String> properties;
    private String className;
    private JsonNode payload;
}
