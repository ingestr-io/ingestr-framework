package io.ingestr.framework.kafka.model;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.*;

@ConfigurationProperties("kafka")
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class KafkaConfiguration {
    private String bootstrapServers = "localhost:9092";
    private String schemaRegistry;
}
