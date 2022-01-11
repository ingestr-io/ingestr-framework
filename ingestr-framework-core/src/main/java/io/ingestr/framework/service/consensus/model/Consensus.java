package io.ingestr.framework.service.consensus.model;


import io.ingestr.framework.service.consensus.ConsensusListener;
import io.ingestr.framework.service.consensus.ConsensusWorker;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

@Getter
@ToString
@Slf4j
public class Consensus {
    public static final Integer DEFAULT_HEARTBEAT_INVALIDATION_AGE_SECONDS = 3;

    private String consensusGroup;
    private String identifier;
    private String hostname;
    private HeartBeat lastHeartBeat;
    private Clock clock;
    private ConsensusElection election = null;

    /**
     * This is populated when consensus elects this the leader
     */
    private ConsensusWorker consensusWorker;
    private Supplier<ConsensusWorker> consensusWorkerSupplier;

    private List<ConsensusListener> listeners = new ArrayList<>();

    public Consensus(
            String consensusGroup,
            String identifier,
            String hostname,
            Supplier<ConsensusWorker> consensusWorkerSupplier) {
        this(Clock.systemUTC(), consensusGroup, identifier, hostname, consensusWorkerSupplier);
    }

    public Consensus(
            Clock clock,
            String consensusGroup,
            String identifier,
            String hostname,
            Supplier<ConsensusWorker> consensusWorkerSupplier) {
        this.consensusGroup = consensusGroup;
        this.identifier = identifier;
        this.clock = clock;
        this.consensusWorkerSupplier = consensusWorkerSupplier;
        this.hostname = hostname;

    }

    void setClock(Clock clock) {
        this.clock = clock;
    }

    public void addListener(ConsensusListener listener) {
        this.listeners.add(listener);
    }

    public ConsensusElection getOrCreateElection(Election election) {
        log.warn("Initialising new Election for {}", this.consensusGroup);
        Validate.isTrue(StringUtils.equalsIgnoreCase(election.getConsensusGroup(), this.consensusGroup));

        if (this.election == null) {
            //no prior election has taken place
            this.election = new ConsensusElection(election.getIdentifier(), election.getTimestamp());
        } else {
            //determine if we have a current election process and we should ignore this request to start a new election process
            if (this.election.isCurrentElection(election.getTimestamp())) {
                //do nothing its current
            } else {
                this.election = new ConsensusElection(election.getIdentifier(), election.getTimestamp());
            }
        }
        return this.election;
    }

    public void castVote(Vote vote) {
        if (this.election == null) {
            throw new IllegalStateException("Cannot cast a vote when there is no election");
        }
        if (this.election.castVote(vote)) {
            //we won
            log.info("Won election {} with vote {}", election, vote);

            //create a new Consumer Worker
            this.consensusWorker = consensusWorkerSupplier.get();

            //wrap the listeners together
            this.consensusWorker.addListener(event -> {
                if (event instanceof ConsensusWorker.ConsensusWorkerEndedEvent) {
                    notifyAllListeners(new ConsensusListener.WorkerThreadEnded(
                            consensusGroup,
                            Instant.now()
                    ));
                } else if (event instanceof ConsensusWorker.ConsensusWorkerStartedEvent) {
                    notifyAllListeners(new ConsensusListener.WorkerThreadStarted(
                            consensusGroup,
                            Instant.now()
                    ));
                }
            });
            this.consensusWorker.start();
        }
    }

    public String getConsensusGroup() {
        return consensusGroup;
    }

    public String getIdentifier() {
        return identifier;
    }

    public boolean hasCurrentElection() {
        if (this.election == null) {
            return false;
        }
        return this.election.isCurrentElection(Instant.now());
    }

    public Optional<Instant> lastHeartbeat() {
        if (lastHeartBeat == null) {
            return Optional.empty();
        }
        return Optional.of(lastHeartBeat.getTimestamp());
    }

    public String leader() {
        if (election != null) {
            return election.getLeader().getIdentifier();
        }
        return null;
    }

    public String hostname() {
        return hostname;
    }

    public boolean isHealthy() {
        if (lastHeartBeat == null) {
            if (election.getLeader().getTimestamp().plusSeconds(DEFAULT_HEARTBEAT_INVALIDATION_AGE_SECONDS).isAfter(clock.instant())) {
//                    log.debug("No Heartbeat received but within grace period group={}", consensusGroup);
                return true;
            }
            return false;
        }
        return lastHeartBeat.getTimestamp().plusSeconds(DEFAULT_HEARTBEAT_INVALIDATION_AGE_SECONDS).isAfter(clock.instant());
    }

    public boolean validateLeader() {
        if (election == null || election.getLeader() == null) {
            return false;
        }

        //if there is no heart beat now, we do not have a valid leader
        if (lastHeartBeat == null) {
            //Check if the last election result was recent, and ignore the heartbeats for a short period as we may
            //not have received a heartbeat after the recent election
            if (election.getLeader().getTimestamp().plusSeconds(DEFAULT_HEARTBEAT_INVALIDATION_AGE_SECONDS).isAfter(clock.instant())) {
                log.debug("No Heartbeat received but within grace period group={}", consensusGroup);
                return true;
            }

            log.info("No heartbeat received for consensus group '{}' after {} seconds ", consensusGroup, DEFAULT_HEARTBEAT_INVALIDATION_AGE_SECONDS);
            notifyFailHeartBeat();
            return false;
        }

        //Validate the last heartbeat
        if (lastHeartBeat.getTimestamp().plusSeconds(DEFAULT_HEARTBEAT_INVALIDATION_AGE_SECONDS).isAfter(clock.instant())) {
            return true;
        }

        notifyFailHeartBeat();
        return false;
    }


    /**
     * Determines if the Current Node is the elected leader
     *
     * @return
     */
    public boolean isLeader() {
        if (this.election == null) {
            return false;
        }
        if (StringUtils.equalsIgnoreCase(this.election.getLeader().getIdentifier(), this.identifier)) {
            return true;
        }
        return false;
    }

    public void heartBeat(HeartBeat beat) {
        if (!this.consensusGroup.equalsIgnoreCase(beat.getConsensusGroup())) {
            throw new IllegalStateException("Cannot register Heartbeat for this Consensus Group " + beat.getConsensusGroup());
        }
        log.trace("Processing Heartbeat - {}", beat);
        this.lastHeartBeat = beat;
    }


    void notifyFailHeartBeat() {
        notifyAllListeners(ConsensusListener.FailedHeartbeatEvent.builder()
                .timestamp(Instant.now())
                .consensusGroupId(consensusGroup)
                .leader(election.getLeader().getIdentifier())
                .isLeader(isLeader())
                .build());
    }

    void notifyAllListeners(ConsensusListener.ElectionEvent event) {
        for (ConsensusListener l : this.listeners) {
            l.onElectionEvent(event);
        }
    }
}
