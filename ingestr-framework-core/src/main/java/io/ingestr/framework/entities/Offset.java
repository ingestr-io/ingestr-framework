package io.ingestr.framework.entities;

import io.ingestr.framework.Entity;
import lombok.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.CompareToBuilder;

import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.Set;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Offset implements Entity {
    private String partitionKey;

    @Singular
    private Set<OffsetEntry> offsetEntries;

    private Props properties;

    /**
     * (Optional) This hash is used as a secondary key of uniqueness alongside the Offset to determine if the Data for
     * this offset has changed.
     * <p>
     * This hash allows Ingestr to determine if the currently loaded data matches the previous loaded data even if the
     * offset has not changed.  I.e. Late landing data arrives without affecting the offset.
     * <p>
     * If the Offset + updateHash match of the current Offset with the newly ingested data, then this data can be considered duplicate
     * <p>
     * The hash does not have to be a hash, but any string that makes sense to the Ingestion Process, it could be a date that
     * naturally occurs in the data (e.g. a last udpated timestamp, or it could be a hash of some key pieces of data).  Only
     * equality is checked
     */
    @Builder.Default
    private final Optional<String> updateHash = Optional.empty();

    /**
     * Represents the Update Timestamp of when this record was created/updated
     */
    private ZonedDateTime updatedAt;

    public Optional<String> getOffsetValue(String name) {
        return offsetEntries.stream()
                .filter(e -> StringUtils.equalsIgnoreCase(e.getName(), name))
                .map(OffsetEntry::getValue)
                .findFirst();
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();

        offsetEntries.stream().sorted((o1, o2) -> new CompareToBuilder()
                .append(o1.getName(), o2.getName())
                .build())
                .forEach(p -> sb.append(",").append(p.getName()).append(":").append(p.getValue()));

        return partitionKey + ":" + sb.substring(1);
    }

    public String toStringShort() {
        final StringBuilder sb = new StringBuilder();

        offsetEntries.stream().sorted((o1, o2) -> new CompareToBuilder()
                .append(o1.getName(), o2.getName())
                .build())
                .forEach(p -> sb.append(",").append(p.getName()).append(":").append(p.getValue()));

        return sb.substring(1);
    }

    public static class OffsetBuilder {
        private Optional<String> updateHash = Optional.empty();

        public OffsetBuilder updateHash(String hash) {
            this.updateHash = Optional.ofNullable(hash);
            return this;
        }
    }
}
