package io.ingestr.framework.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PartitionTraceDTO {
    private Instant traceUntil;
    private String traceFor;
}
