package io.ingestr.framework.entities;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

@Slf4j
@Data
@ToString
@Builder(access = AccessLevel.PRIVATE, builderClassName = "B")
@AllArgsConstructor
@NoArgsConstructor
public class PartitionRegistration {
    private Partition partition;


    public static PartitionRegistrationBuilder newRegistration() {
        return new PartitionRegistrationBuilder(PartitionRegistration.builder());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        PartitionRegistration that = (PartitionRegistration) o;

        return new EqualsBuilder()
                .append(partition.getKey(), that.partition.getKey())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(411217, 315517)
                .append(partition.getKey())
                .toHashCode();
    }


    public static class PartitionRegistrationBuilder {
        private B builder;

        public PartitionRegistrationBuilder(B builder) {
            this.builder = builder;
        }


        public PartitionRegistrationBuilder partition(Partition.PartitionBuilder partition) {
            this.builder.partition(partition.build());
            return this;
        }

        public PartitionRegistration build() {
            return builder.build();
        }
    }
}
