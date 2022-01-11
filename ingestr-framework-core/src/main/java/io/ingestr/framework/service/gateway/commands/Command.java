package io.ingestr.framework.service.gateway.commands;

import java.io.Serializable;

/**
 * A command targets 1 or multiple instances of services clustered within the same Command Group and expects that
 * only 1 of these instances acts on the command.
 * <p>
 * E.g. A command is expected to be executed only once on a single instance within an Command Group
 */
public interface Command extends Serializable {
    String getContext();

    void setContext(String context);
}
