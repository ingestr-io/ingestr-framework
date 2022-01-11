package io.ingestr.framework.entities;

import java.util.List;

public interface TriggerContext {

    /**
     * The list of Partitions this Trigger should target for
     *
     * @return
     */
    List<Partition> partitions();


    /**
     * Notify the Trigger which Partitions should be executed
     *
     * @param triggeredPartition
     */
    void notify(TriggeredPartition triggeredPartition);

    void notify(TriggeredPartition.TriggeredPartitionBuilder triggeredPartition);
}
