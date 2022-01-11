package io.ingestr.framework.exception;

public class LoaderException extends Exception {
    public LoaderException(String message) {
        super(message);
    }

    public LoaderException(String message, Throwable cause) {
        super(message, cause);
    }
}
