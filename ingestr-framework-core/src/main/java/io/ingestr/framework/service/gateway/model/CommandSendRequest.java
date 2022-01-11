package io.ingestr.framework.service.gateway.model;

import io.ingestr.framework.service.gateway.commands.Command;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import java.util.Optional;

@Data
@ToString
@Builder
public class CommandSendRequest {
    private Command command;
    @Builder.Default
    private Optional<String> context = Optional.empty();
}
