package io.ingestr.framework.service.queue.exceptions;

public class QueueBusException extends Exception {
    public QueueBusException() {
    }

    public QueueBusException(String message) {
        super(message);
    }

    public QueueBusException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueueBusException(Throwable cause) {
        super(cause);
    }

    public QueueBusException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
