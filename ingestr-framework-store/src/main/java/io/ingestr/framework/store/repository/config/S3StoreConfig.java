package io.ingestr.framework.store.repository.config;

import lombok.*;

import java.util.Optional;
import java.util.Properties;

@Data
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class S3StoreConfig {
    @Builder.Default
    private Optional<String> accessKey = Optional.empty();
    @Builder.Default
    private Optional<String> secretKey = Optional.empty();
    @Builder.Default
    private Optional<String> region = Optional.empty();

    private String basePath = "/";
    private String bucketName;

    public static S3StoreConfig fromProperties(Properties properties) {
        S3StoreConfigBuilder b = S3StoreConfig.builder()
                .accessKey(Optional.ofNullable(properties.getProperty("s3.accessKey", null)))
                .secretKey(Optional.ofNullable(properties.getProperty("s3.secretKey", null)))
                .bucketName(properties.getProperty("s3.bucket"))
                .region(Optional.ofNullable(properties.getProperty("s3.region", null)));

        return b.build();
    }
}
