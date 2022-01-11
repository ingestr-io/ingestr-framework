package io.ingestr.framework.entities;

public enum DeregistrationMethod {
    /**
     * Effectively flags the Data Partition as de-registered indicating that the
     * Data Partition was not discovered during the last Data Registration discovery process
     */
    DEREGISTER,
    /**
     * Indicates that the affected Data Partition should be disabled if during the Data Registration discovery process
     * we no long find this Data Partition.  Disabled Data Partitions will no longer be attempted for data ingestion
     */
    DISABLE,
    /**
     * Indicates that the affected Data Partition should be completed deleted if during the Data Registration discovery process
     * we no long find this Data Partition. This is the most drastic measure and removes the existince of the Data Partition
     */
    DELETE;
}
