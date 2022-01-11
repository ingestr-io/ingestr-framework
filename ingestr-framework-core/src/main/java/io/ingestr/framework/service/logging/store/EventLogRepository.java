package io.ingestr.framework.service.logging.store;

import io.ingestr.framework.service.logging.LogEvent;
import lombok.*;

import javax.validation.constraints.NotNull;
import java.time.Instant;
import java.util.List;

public interface EventLogRepository {
    EventLogResult query(EventLogQuery query);

    PartitionExecutionSummary partitionExecutionSummary(PartitionExecutionSummaryQuery query);

    @Data
    @ToString
    @Builder
    class PartitionExecutionSummaryQuery {
        @NotNull(message = "Loader cannot be null")
        private String loader;

        @NotNull(message = "Partition cannot be null")
        private String partition;

        private Instant from;
        private Instant to;
        private Integer resultLimit;
        private String offset;

    }

    @Data
    @ToString
    @Builder
    class PartitionExecutionSummary {
        private String partition;
        private String loader;
        @Singular
        private List<PartitionExecutionSummaryItem> items;
    }

    @Data
    @ToString
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    class PartitionExecutionSummaryItem {
        private String taskIdentifier;
        private Instant timestamp;
        private Long duration;
        private ExecutionStatus status;
        private String offset;
    }

    enum ExecutionStatus {
        Success,
        Fail
    }

    @Data
    @ToString
    @Builder
    class EventLogQuery {
        private String taskIdentifier;
        private String loader;
        private String partition;
        private String context;
        private String event;
        private Instant from;
        private Instant to;

        private Integer resultLimit;
        private String offset;
        private Long timeLimitMs;
    }

    @Data
    @ToString
    @Builder
    class EventLogResult {
        @Singular
        private List<LogEvent> logEvents;
        private String offset;
    }
}
