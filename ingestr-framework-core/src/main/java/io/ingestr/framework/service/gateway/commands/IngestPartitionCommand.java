package io.ingestr.framework.service.gateway.commands;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@Builder
public class IngestPartitionCommand implements Command {
    private String context;
    private String dataSourceIdentifier;
    private String partitionKey;

    @Override
    public void setContext(String context) {
        this.context = context;
    }
}
