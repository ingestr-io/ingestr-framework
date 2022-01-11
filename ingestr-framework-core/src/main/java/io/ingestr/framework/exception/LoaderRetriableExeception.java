package io.ingestr.framework.exception;

public class LoaderRetriableExeception extends RuntimeException {
    public LoaderRetriableExeception(String message) {
        super(message);
    }

    public LoaderRetriableExeception(String message, Throwable cause) {
        super(message, cause);
    }
}
