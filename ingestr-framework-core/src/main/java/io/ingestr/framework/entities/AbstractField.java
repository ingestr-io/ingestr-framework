package io.ingestr.framework.entities;

import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@SuperBuilder
public abstract class AbstractField {
    private String identifier;
    private String label;
    private String hint;
    private FieldType fieldType;
    private DataType dataType;
    private String validation;
    @Singular
    private List<String> allowedValues;
    private String defaultValue;
}
