package io.ingestr.framework.entities;

import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@Builder
public class ConfigurationDescriptor extends AbstractField {
    private String identifier;
    private String label;
    private String hint;
    private FieldType fieldType;
    private DataType dataType;
    private String validation;
    @Singular
    private List<String> allowedValues;
    private String defaultValue;

    @Builder.Default
    private Boolean nullable = Boolean.FALSE;
    @Builder.Default
    private Boolean updatable = Boolean.TRUE;


    public static class ConfigurationDescriptorBuilder {
    }
}
