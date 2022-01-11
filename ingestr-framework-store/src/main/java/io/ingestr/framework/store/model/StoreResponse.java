package io.ingestr.framework.store.model;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.time.ZonedDateTime;

@Slf4j
@Data
@Builder
public class StoreResponse {
    private ZonedDateTime storedAt;
}
