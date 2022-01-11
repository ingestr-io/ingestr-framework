package io.ingestr.framework.exception;

public class NoResultsFoundException extends LoaderException {
    public NoResultsFoundException(String message) {
        super(message);
    }

    public NoResultsFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
