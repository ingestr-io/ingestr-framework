package io.ingestr.framework.service.gateway.commands;

import lombok.*;

import java.time.Instant;

/**
 * This command setups up a Trace on the Partition that will be enabled up until some time in the near future
 */
@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class PartitionTraceCommand implements Command {
    private String context;
    private String key;
    private Instant traceUntil;
    /**
     * A String representation of how long the trace should be set for e.g. 15m, 1h, 2d 1w (max)
     */
    private String traceFor;

    public PartitionTraceCommand(String key, Instant traceUntil) {
        this.key = key;
        this.traceUntil = traceUntil;
    }

    public PartitionTraceCommand(String key, String traceFor) {
        this.key = key;
        this.traceFor = traceFor;
    }

    @Override
    public void setContext(String context) {
        this.context = context;
    }
}
