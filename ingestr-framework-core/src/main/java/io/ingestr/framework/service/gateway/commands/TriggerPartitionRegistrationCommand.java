package io.ingestr.framework.service.gateway.commands;

import lombok.*;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class TriggerPartitionRegistrationCommand implements Command {
    private String dataDescriptorIdentifier;
    private String context;

    public TriggerPartitionRegistrationCommand(String dataDescriptorIdentifier) {
        this.dataDescriptorIdentifier = dataDescriptorIdentifier;
    }

    @Override
    public void setContext(String context) {
        this.context = context;
    }
}
