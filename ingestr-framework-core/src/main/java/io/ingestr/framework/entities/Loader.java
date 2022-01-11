package io.ingestr.framework.entities;

import io.ingestr.framework.Entity;
import lombok.*;

@ToString
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Loader implements Entity {
    private String identifier;
    private String name;

}
