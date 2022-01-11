package io.ingestr.framework.service.gateway.model;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import java.time.ZonedDateTime;

@Data
@Builder
@ToString
public class EventSendResult {
    private String identifier;
    private ZonedDateTime createdAt;
}
