package io.ingestr.framework.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@Data
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class ReprocessorDescriptor {
    @JsonIgnore
    private Supplier<? extends Reprocessor> reprocessor;
    @Singular
    private List<ParameterDescriptor> parameterDescriptors = new ArrayList<>();

    public static class ReprocessorDescriptorBuilder {}
}
