package io.ingestr.framework.service.producer;

import lombok.Builder;
import lombok.Data;
import lombok.Singular;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

@Data
@Builder
@ToString
public class IngestrProducerRecord {
    private String topic;
    private Integer partition;
    private String key;
    @Singular
    private Map<String, String> headers = new HashMap<>();
    private String value;
}
