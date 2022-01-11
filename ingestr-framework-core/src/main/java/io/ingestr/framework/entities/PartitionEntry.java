package io.ingestr.framework.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class PartitionEntry implements Serializable {
    private String name;
    private String value;

    public static PartitionEntry newEntry(String name, String value) {
        return new PartitionEntry(name, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        PartitionEntry that = (PartitionEntry) o;

        return new EqualsBuilder()
                .append(name, that.name)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(232117, 33137)
                .append(name)
                .toHashCode();
    }
}
