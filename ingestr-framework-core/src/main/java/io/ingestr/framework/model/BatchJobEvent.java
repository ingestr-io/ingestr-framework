package io.ingestr.framework.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.ingestr.framework.service.logging.LogLevel;
import lombok.*;

import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class BatchJobEvent {
    private String event;
    private String message;
    private Object[] props;
    private String body;
    private LogLevel logLevel;
    @Singular
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Map<String, String> properties;

    public static BatchJobEvent.BatchJobEventBuilder debug(String message, Object... props) {
        return BatchJobEvent.builder()
                .logLevel(LogLevel.DEBUG)
                .message(message)
                .props(props);
    }

    public static BatchJobEvent.BatchJobEventBuilder info(String message, Object... props) {
        return BatchJobEvent.builder()
                .logLevel(LogLevel.INFO)
                .message(message)
                .props(props);
    }

    public static BatchJobEvent.BatchJobEventBuilder warn(String message, Object... props) {
        return BatchJobEvent.builder()
                .logLevel(LogLevel.WARN)
                .message(message)
                .props(props);
    }


    public static BatchJobEvent.BatchJobEventBuilder error(String message, Object... props) {
        return BatchJobEvent.builder()
                .logLevel(LogLevel.ERROR)
                .message(message)
                .props(props);
    }


    public static class BatchJobEventBuilder {}

}

