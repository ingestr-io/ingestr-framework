package io.ingestr.framework.entities;

import lombok.*;

import java.io.InputStream;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;

@Getter
@ToString
@Builder
public class IngestionResult {

    @NonNull
    @Builder.Default
    private final ZonedDateTime executionTimestamp = ZonedDateTime.now();

    /**
     * (Mandatory) Returns the offset of where we are up to with the data processing (inclusive)
     */
    private Offset offset;

    /**
     * (Optional) The InputStream containing the excessive payload.  If Data Storage is configured, then the
     * loader will automatically store in the Data Storage, and send a kafka topic message pointing the location;
     */
    private Optional<InputStream> rawLargeData;
    /**
     * (Optional) The Raw String payload to be sent to kafka.
     * Either rawLargeData or data object must be set
     */
    private Optional<String> data;

    /**
     * Specify any additional "Meta-Data" that should accompany the data collected which will be attached to the header
     * information of the kafka messages
     */
    @Singular(value = "meta")
    private Map<String, String> meta;

    /**
     * (Optional) Specify the key of this record as it goes into the Kafka Topic.  This will influence the partitioning
     * of messages into the queue.  If this is not set, then null (random) topic assignment will be used.
     */
    private Optional<String> key;

}
