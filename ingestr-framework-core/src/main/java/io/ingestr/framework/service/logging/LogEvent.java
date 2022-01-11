package io.ingestr.framework.service.logging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import io.ingestr.framework.kafka.ObjectMapperFactory;
import lombok.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties
@ToString
public class LogEvent {
    @Builder.Default
    private String id = UUID.randomUUID().toString();
    private String taskIdentifier;
    private String loader;
    private LogContext context;
    private String event;
    private String message;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private String body;
    private Instant timestamp;
    private LogLevel logLevel;
    @Singular
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Map<String, String> properties;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private JsonNode subject;


    static String conv(String message) {
        return StringUtils.replace(message, "{}", "%s");
    }

    public static LogEventBuilder level(LogLevel logLevel, String message, Object... props) {
        Validate.notNull(logLevel, "Log Level cannot be null");

        if (logLevel == LogLevel.ERROR) {
            return error(message, props);
        } else if (logLevel == LogLevel.DEBUG) {
            return debug(message, props);
        } else if (logLevel == LogLevel.INFO) {
            return info(message, props);
        } else if (logLevel == LogLevel.WARN) {
            return warn(message, props);
        }
        throw new UnsupportedOperationException("Did not know how to handle level " + logLevel);
    }

    public static LogEventBuilder debug(String message, Object... props) {
        return LogEvent.builder()
                .logLevel(LogLevel.DEBUG)
                .message(String.format(conv(message), props))
                .timestamp(Instant.now());
    }

    public static LogEventBuilder info(String message, Object... props) {
        return LogEvent.builder()
                .logLevel(LogLevel.INFO)
                .message(String.format(conv(message), props))
                .timestamp(Instant.now());
    }

    public static LogEventBuilder warn(String message, Object... props) {
        return LogEvent.builder()
                .logLevel(LogLevel.WARN)
                .message(String.format(conv(message), props))
                .timestamp(Instant.now());
    }


    public static LogEventBuilder error(String message, Object... props) {
        return LogEvent.builder()
                .logLevel(LogLevel.ERROR)
                .message(String.format(conv(message), props))
                .timestamp(Instant.now());
    }

    public static class LogEventBuilder {

        public LogEventBuilder body(String body) {
            this.body = body;
            return this;
        }

        public LogEventBuilder subject(Object o) {

            if (o != null) {
                try {
                    this.subject = ObjectMapperFactory.kafkaMessageObjectMapper().valueToTree(o);
                } catch (Exception e) {

                }
            }
            return this;
        }

        public LogEventBuilder body(String message, Throwable t) {
            StringBuilder sb = new StringBuilder();
            sb.append(message).append("\n")
                    .append(ExceptionUtils.getStackTrace(t));
            this.body = sb.toString();
            return this;
        }
    }
}
