package io.ingestr.framework.service.gateway.model;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import java.time.ZonedDateTime;

@Data
@ToString
@Builder
public class CommandSendResult {
    private String identifier;
    private ZonedDateTime createdAt;
}
