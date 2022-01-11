package io.ingestr.framework.service.gateway.exceptions;

public class CommandBusException extends Throwable {
    public CommandBusException(String message, Exception e) {
        super(message, e);
    }
}
