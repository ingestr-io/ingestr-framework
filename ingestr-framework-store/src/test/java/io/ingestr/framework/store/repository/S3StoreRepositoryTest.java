package io.ingestr.framework.store.repository;

import io.ingestr.framework.store.model.StoreObject;
import io.ingestr.framework.store.repository.config.S3StoreConfig;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;

class S3StoreRepositoryTest {

    @Test
    void shouldUploadToS3() throws IOException {
        Properties props = new Properties();
        props.load(S3StoreRepositoryTest.class.getResourceAsStream("/application.properties"));
        S3StoreConfig s3StoreConfig = S3StoreConfig.fromProperties(props);

        S3StoreRepository rep = S3StoreRepository.newStore(s3StoreConfig);

        rep.store(
                StoreObject.builder()
                        .key("test.key")
                        .inputStream(
                                IOUtils.toInputStream("Something Special - " + RandomStringUtils.randomAlphanumeric(100), Charset.defaultCharset())
                        )
                        .build()
        );


    }

}