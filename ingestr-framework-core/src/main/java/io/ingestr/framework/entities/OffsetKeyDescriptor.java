package io.ingestr.framework.entities;

import lombok.*;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OffsetKeyDescriptor {
    private String identifier;
    private DataType dataType;

    public static class OffsetKeyDescriptorBuilder {}
}
