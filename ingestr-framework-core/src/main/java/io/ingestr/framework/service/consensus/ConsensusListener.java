package io.ingestr.framework.service.consensus;

import lombok.*;

import java.time.Instant;

public interface ConsensusListener {

    void onElectionEvent(ElectionEvent event);

    interface ElectionEvent {

    }

    @Getter
    @ToString
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    class ElectionCreatedEvent implements ElectionEvent {
        private String consensusGroupId;
        private Instant timestamp;
    }

    @Getter
    @ToString
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    class FailedHeartbeatEvent implements ElectionEvent {
        private String consensusGroupId;
        private Instant timestamp;
        private String leader;
        private boolean isLeader;

    }


    @Getter
    @ToString
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    class WorkerThreadStarted implements ElectionEvent {
        private String consensusGroupId;
        private Instant timestamp;

    }


    @Getter
    @ToString
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    class WorkerThreadEnded implements ElectionEvent {
        private String consensusGroupId;
        private Instant timestamp;

    }
}
