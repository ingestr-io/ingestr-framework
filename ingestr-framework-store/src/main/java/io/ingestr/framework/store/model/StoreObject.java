package io.ingestr.framework.store.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.InputStream;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class StoreObject {
    private String key;
    private InputStream inputStream;
}
