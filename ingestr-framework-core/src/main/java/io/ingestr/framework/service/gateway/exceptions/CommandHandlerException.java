package io.ingestr.framework.service.gateway.exceptions;

public class CommandHandlerException extends Throwable {
    public CommandHandlerException() {
    }

    public CommandHandlerException(String message) {
        super(message);
    }

    public CommandHandlerException(String message, Throwable cause) {
        super(message, cause);
    }

    public CommandHandlerException(Throwable cause) {
        super(cause);
    }

    public CommandHandlerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
