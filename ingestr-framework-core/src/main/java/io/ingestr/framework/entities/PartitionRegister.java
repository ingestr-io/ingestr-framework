package io.ingestr.framework.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;

import java.util.function.Supplier;

@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PartitionRegister {
    @JsonIgnore
    private Supplier<? extends PartitionRegistrator> partitionRegistratorSupplier;
    private String schedule;
    private String scheduleTimeZone;
    @Builder.Default
    private DeregistrationMethod deregistrationMethod = DeregistrationMethod.DEREGISTER;

    public PartitionRegister(Supplier<? extends PartitionRegistrator> partitionRegistratorSupplier, DeregistrationMethod deregistrationMethod) {
        this.partitionRegistratorSupplier = partitionRegistratorSupplier;
        this.deregistrationMethod = deregistrationMethod;
    }

    public static class PartitionRegisterBuilder {
        public PartitionRegisterBuilder schedule(String schedule, String scheduleTimeZone) {
            this.schedule = schedule;
            this.scheduleTimeZone = scheduleTimeZone;
            return this;
        }
    }
}
