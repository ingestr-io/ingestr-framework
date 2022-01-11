package io.ingestr.framework.service.gateway.exceptions;

public class CommandGatewayException extends Throwable {
    public CommandGatewayException(String message, Exception e) {
        super(message, e);
    }
}
