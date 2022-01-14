package io.ingestr.framework.entities;

import lombok.*;

import java.util.List;

@Data
@ToString
@AllArgsConstructor
@Builder
public class PartitionKeyDescriptor {
    private String identifier;
    private String label;
    private String hint;
    private FieldType fieldType;
    private DataType dataType;
    private String validation;
    @Singular
    private List<String> allowedValues;
    private String defaultValue;


    public static class PartitionKeyDescriptorBuilder {}
}
