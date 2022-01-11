package io.ingestr.framework.entities;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.time.ZonedDateTime;

@Getter
@ToString
@Builder(toBuilder = true)
public class DataSource {
    @NonNull
    private final String identifier;
    @NonNull
    private final String name;
    private final String description;

    private final Props properties;

    private final ZonedDateTime createdAt;
    private final ZonedDateTime updatedAt;

}
