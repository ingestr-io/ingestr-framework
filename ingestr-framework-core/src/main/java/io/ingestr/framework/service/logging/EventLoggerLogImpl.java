package io.ingestr.framework.service.logging;

import io.ingestr.framework.service.logging.store.EventBus;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class EventLoggerLogImpl implements EventLogger {

    private EventBus eventBus;
    private String loaderName;
    private boolean logEvent = true;

    public EventLoggerLogImpl(EventBus eventBus, String loaderName) {
        this.eventBus = eventBus;
        this.loaderName = loaderName;
    }

    public boolean isLogEvent() {
        return logEvent;
    }

    public void setLogEvent(boolean logEvent) {
        this.logEvent = logEvent;
    }

    @Override
    public void log(LogEvent.LogEventBuilder logEvent) {
        log(logEvent.build());
    }

    void log(LogEvent logEvent) {
        logEvent.setLoader(loaderName);

        if (isLogEvent()) {
            StringBuilder sb = new StringBuilder();
            if (logEvent.getEvent() != null) {
                sb.append("[").append(logEvent.getEvent()).append("] ");
            }
            sb.append(logEvent.getMessage());
            if (logEvent.getBody() != null) {
                sb.append(" - ").append(logEvent.getBody());
            }
            for (Map.Entry<String, String> e : logEvent.getProperties().entrySet()) {
                sb.append(" ").append(e.getKey()).append("=").append(e.getValue());
            }
            if (logEvent.getLogLevel() == LogLevel.ERROR) {
                log.error(sb.toString());
            } else if (logEvent.getLogLevel() == LogLevel.INFO) {
                log.info(sb.toString());
            } else if (logEvent.getLogLevel() == LogLevel.WARN) {
                log.warn(sb.toString());
            } else if (logEvent.getLogLevel() == LogLevel.DEBUG) {
                log.debug(sb.toString());
            }
        }

        eventBus.send(logEvent);
    }

}
