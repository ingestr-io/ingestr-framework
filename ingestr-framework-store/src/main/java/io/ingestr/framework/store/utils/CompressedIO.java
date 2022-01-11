package io.ingestr.framework.store.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.util.zip.GZIPOutputStream;

@Slf4j
public class CompressedIO {
    private Thread thread;
    private PipedOutputStream pos;
    private PipedInputStream pis;

    public enum CompressMethod {
        GZIP,
        SNAPPY;
    }

    public CompressedIO(PipedOutputStream out, PipedInputStream in, Thread thread) {
        this.thread = thread;
        this.pos = out;
        this.pis = in;
    }

    public static CompressedIO compressed(
            InputStream inputStream
    ) throws IOException {
        return compressed(inputStream, CompressMethod.GZIP);
    }

    public static CompressedIO compressed(
            InputStream inputStream,
            CompressMethod method
    ) throws IOException {
        PipedOutputStream pos = new PipedOutputStream();
        PipedInputStream pis = new PipedInputStream(pos);

        Thread t = new Thread(() -> {
            OutputStream gos = null;

            try {
                if (method == CompressMethod.GZIP) {
                    gos = new GZIPOutputStream(new BufferedOutputStream(pos));
                } else if (method == CompressMethod.SNAPPY) {
                    gos = new SnappyOutputStream(new BufferedOutputStream(pos));
                }

                // write the original OutputStream to the PipedOutputStream
                // note that in order for the below method to work, you need
                // to ensure that the data has finished writing to the
                // ByteArrayOutputStream

                IOUtils.copyLarge(new BufferedInputStream(inputStream), gos);

                log.debug("Finished Copying stream...");

            } catch (IOException e) {
                // logging and exception handling should go here
                log.error(e.getMessage(), e);
            } finally {
                // close the PipedOutputStream here because we're done writing data
                // once this thread has completed its run
                IOUtils.closeQuietly(gos);
            }
        });
        return new CompressedIO(pos, pis, t);
    }

    public InputStream stream() {
        this.thread.start();
        return this.pis;
    }

    public void streamTo(OutputStream outputStream) throws IOException {
        log.debug("Starting to Stream to Compressed InputStream...");
        this.thread.start();

        try {
            IOUtils.copy(pis, outputStream);
        } finally {
            IOUtils.closeQuietly(pis);
            IOUtils.closeQuietly(outputStream);
        }
    }

    public void join() {
        try {
            this.thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
