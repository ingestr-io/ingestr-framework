package io.ingestr.framework.store.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.InputStream;
import java.io.OutputStream;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ReceiveObject {
    private String key;
    private InputStream inputStream;
}
