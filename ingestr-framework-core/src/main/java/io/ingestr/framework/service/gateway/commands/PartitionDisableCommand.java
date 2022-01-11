package io.ingestr.framework.service.gateway.commands;

import lombok.*;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class PartitionDisableCommand implements Command {
    private String context;
    private String key;

    public PartitionDisableCommand(String key) {
        this.key = key;
    }

    @Override
    public void setContext(String context) {
        this.context = context;
    }
}
