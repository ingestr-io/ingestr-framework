package io.ingestr.framework.service.gateway.commands;

import lombok.*;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class PartitionEnableCommand implements Command {
    private String context;
    private String key;

    public PartitionEnableCommand(String key) {
        this.key = key;
    }

    @Override
    public void setContext(String context) {
        this.context = context;
    }
}
