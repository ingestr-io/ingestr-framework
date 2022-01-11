package io.ingestr.framework.service.queue.model;

import io.ingestr.framework.entities.Offset;
import io.ingestr.framework.entities.Partition;
import lombok.*;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Map;

@Getter
@Builder
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class IngestPartitionQueueItem implements QueueItem {
    private String ingestionIdentifier;
    private Partition partition;
    /**
     * Optional Offset to target.
     * <p>
     * This is used by the backfill process which determines the offset to target, otherwise, the last offset will be used
     */
    private Offset overrideOffset;

    /**
     * Optional Schedule Identifier which links to the schedule that triggered this process
     */
    private String scheduleIdentifier;

    private ZonedDateTime scheduledExecution;
    private Instant executionTimestamp;

    private String triggerName;

    private Map<String, String> parameters;
    private Map<String, String> properties;


}
