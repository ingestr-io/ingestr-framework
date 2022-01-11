package io.ingestr.framework.model;


import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.time.ZonedDateTime;
import java.util.Optional;

@Getter
@ToString
@Builder(toBuilder = true)
public class LoadResult {

    @NonNull
    @Builder.Default
    private final ZonedDateTime executionTimestamp = ZonedDateTime.now();


    /**
     * (Optional) This hash is used to determine uniqueness of the load result.
     * If the result matches a previous load hash, then the result is considered to be stale and not fresh
     */
    @Builder.Default
    private final Optional<String> loadHash = Optional.empty();

}
