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
public class OffsetEntry implements Serializable {
    private String name;
    private String value;

    public static OffsetEntry of(String name, String value) {
        return new OffsetEntry(name, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        OffsetEntry that = (OffsetEntry) o;

        return new EqualsBuilder()
                .append(name, that.name)
                .append(value, that.value)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(3232117, 3322137)
                .append(name)
                .append(value)
                .toHashCode();
    }
}
