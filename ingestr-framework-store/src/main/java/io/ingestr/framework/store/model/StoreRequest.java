package io.ingestr.framework.store.model;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;

@Slf4j
@Data
@Builder
public class StoreRequest {
    private String key;
    private InputStream data;
}
