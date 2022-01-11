package io.ingestr.framework.entities;

public interface PartitionFilter {
    boolean apply(Partition partition);
}
