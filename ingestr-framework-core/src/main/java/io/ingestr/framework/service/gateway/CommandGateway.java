package io.ingestr.framework.service.gateway;

import io.ingestr.framework.service.gateway.exceptions.CommandGatewayException;
import io.ingestr.framework.service.gateway.model.CommandSendRequest;
import io.ingestr.framework.service.gateway.model.CommandSendResult;

public interface CommandGateway {
    CommandSendResult send(CommandSendRequest commandSendRequest) throws CommandGatewayException;
}
