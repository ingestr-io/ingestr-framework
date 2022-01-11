package io.ingestr.framework.entities;

import lombok.*;

import java.util.Arrays;
import java.util.List;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@Builder
public class ParameterDescriptor extends AbstractField {
    private String identifier;
    private String label;
    private String hint;
    private FieldType fieldType;
    private DataType dataType;
    private String validation;
    private List<String> allowedValues;
    private String defaultValue;
    private Boolean nullable = Boolean.FALSE;
    private Boolean updatable = Boolean.TRUE;


    public static class ParameterDescriptorBuilder {
        private List<String> allowedValues;

        public ParameterDescriptorBuilder allowedValues(String ... values) {
            this.allowedValues.addAll(Arrays.asList(values));
            return this;
        }
    }
}
