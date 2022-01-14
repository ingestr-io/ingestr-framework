package io.ingestr.framework.entities;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Builder
@ToString
@Getter
public class LoaderConfiguration {
    /**
     * The amount of 'concurrency' of each loader instance.  i.e. How many threads should be dedicated
     * to processing Ingestion tasks.  (default = 1)
     */
    @Builder.Default
    private int concurrency = 1;
    /**
     * The 'defaultPartitionCount' of the ingestion topic in Kafka.  This will affect the number of partitions in the Kafka Ingestion
     * topic at a cost of ordering guarantees.
     * <p>
     * Please read Kafka documentation on how to configure this value properly
     * Kafka only supports increasing this number, decreases will be ignored. (default = 2)
     */
    @Builder.Default
    private int defaultPartitionCount = 2;


    public static class LoaderConfigurationBuilder {}

}
