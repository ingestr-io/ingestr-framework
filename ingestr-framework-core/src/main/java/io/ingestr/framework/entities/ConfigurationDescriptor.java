package io.ingestr.framework.entities;

import lombok.*;
import lombok.experimental.SuperBuilder;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
public class ConfigurationDescriptor extends AbstractField {
    @Builder.Default
    private Boolean nullable = Boolean.FALSE;
    @Builder.Default
    private Boolean updatable = Boolean.TRUE;


}
