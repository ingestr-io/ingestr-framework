package io.ingestr.framework.entities;

import lombok.*;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@Builder
public class ParameterValueDescriptor extends AbstractField {
    private String identifier;
    private String defaultValue;


    public static class ParameterValueDescriptorBuilder {}
}
