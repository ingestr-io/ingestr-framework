package io.ingestr.framework.entities;


import lombok.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Getter
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class IngestionRequest {
    /**
     * If this ingestion was triggered by scheduling, this will be configured with the schedule that triggered it
     */
    private Optional<IngestionSchedule> schedule;
    /**
     * The full definition of the originating Ingestion behind this request
     */
    private Ingestion ingestion;
    /**
     * The full definition of the originating DataDescriptor behind this request
     */
    private DataDescriptor dataDescriptor;
    /**
     * The Data Partition that is the subject of this request and the target of the Source
     */
    private Partition partition;
    /**
     * (Optional) The previous offset that was stored of the last successful Ingestion that took place
     */
    private Optional<Offset> lastOffset;

    /**
     * The set of Parameters and associated values after being merged
     */
    private Parameters parameters;
    /**
     * Any arbitrary properties that may have been provided at execution time
     */
    private Map<String, String> properties = new HashMap<>();

}
