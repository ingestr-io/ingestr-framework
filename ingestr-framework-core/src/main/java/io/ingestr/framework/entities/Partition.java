package io.ingestr.framework.entities;

import io.ingestr.framework.Entity;
import lombok.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Partition implements Entity {
    private String dataDescriptorIdentifier;
    @Singular
    private Set<PartitionEntry> partitionEntries;
    @Builder.Default
    private Boolean registered = Boolean.FALSE;
    @Builder.Default
    private Boolean enabled = Boolean.TRUE;
    @Builder.Default
    private Boolean deleted = Boolean.FALSE;
    private ZonedDateTime deletedAt;
    private ZonedDateTime updatedAt;
    @Builder.Default
    private Priority priority = Priority.NORMAL;
    private Props props;
    private Set<String> tags = new HashSet<>();
    @Singular(value = "meta")
    private Map<String, String> meta = new HashMap<>();

    /**
     * If the timestamp is set here, then detailed tracing will be enabled
     */
    public Instant tracingEnabledUntil;


    public enum Priority {
        LOW,
        NORMAL,
        HIGH,
        CRITICAL;
    }


    /**
     * Creates a Key based on this partition information
     *
     * @return
     */
    public String getKey() {
        assert partitionEntries != null && !partitionEntries.isEmpty();
        assert StringUtils.isNotBlank(dataDescriptorIdentifier);
        return DigestUtils.md5Hex(toString());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();

        partitionEntries.stream().sorted((o1, o2) -> new CompareToBuilder()
                .append(o1.getName(), o2.getName())
                .build())
                .forEach(p -> sb.append(",").append(p.getName()).append(":").append(p.getValue()));

        return dataDescriptorIdentifier + ":" + sb.substring(1);
    }

    public String toStringShort() {
        final StringBuilder sb = new StringBuilder();

        partitionEntries.stream().sorted((o1, o2) -> new CompareToBuilder()
                .append(o1.getName(), o2.getName())
                .build())
                .forEach(p -> sb.append(",").append(p.getName()).append(":").append(p.getValue()));
        return sb.substring(1);
    }

    public Optional<PartitionEntry> getByName(String name) {
        return partitionEntries.stream()
                .filter(p -> StringUtils.equalsIgnoreCase(name, p.getName()))
                .findFirst();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        Partition partition = (Partition) o;


        return new EqualsBuilder()
                .append(dataDescriptorIdentifier, partition.dataDescriptorIdentifier)
                .append(getKey(), partition.getKey())
                .append(tags, partition.getTags())
                .append(enabled, partition.getEnabled())
                .append(deleted, partition.getDeleted())
                .append(registered, partition.getRegistered())
                .append(this.priority, partition.getPriority())
                .append(this.props, partition.getProps())
                .append(this.meta, partition.getMeta())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(44117, 12137)
                .append(dataDescriptorIdentifier)
                .append(getKey())
                .toHashCode();
    }

    public static class PartitionBuilder {
        private Set<String> tags = new HashSet<>();

        public PartitionBuilder tags(String... tags) {
            for (String tag : tags) {
                this.tags.add(tag.toLowerCase());
            }
            return this;
        }
    }
}
