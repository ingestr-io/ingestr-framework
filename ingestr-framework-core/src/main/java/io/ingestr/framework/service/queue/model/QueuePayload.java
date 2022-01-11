package io.ingestr.framework.service.queue.model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.*;

@Getter
@Builder
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class QueuePayload {
    private String id;
    private String className;
    private JsonNode payload;
}
