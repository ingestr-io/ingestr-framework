package io.ingestr.framework.store.utils;

import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

class CompressedIOTest {

    @Test
    void shouldStreamToCompressInput() throws IOException {

        CompressedIO c = CompressedIO.compressed(
                new BufferedInputStream(
                        new InputStream() {
                            @Override
                            public int read() {
                                for (int x = 0; x < 2_000_000_000; x++) {
                                    return 1;
                                }
                                return 0;
                            }
                        }
                )
        );

        c.streamTo(new FileOutputStream("/tmp/test.snappy"));

        c.join();


    }

}