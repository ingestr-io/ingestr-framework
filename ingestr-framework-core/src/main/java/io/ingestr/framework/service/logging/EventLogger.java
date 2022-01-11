package io.ingestr.framework.service.logging;

public interface EventLogger {
    void log(LogEvent.LogEventBuilder logEvent);

}
