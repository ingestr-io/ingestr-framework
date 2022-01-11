package io.ingestr.framework.entities;

import lombok.*;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Data
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TriggeredPartition implements Serializable {
    @Singular
    private Set<PartitionEntry> partitionEntries = new HashSet<>();
    @Singular
    private Map<String, String> properties;


}
