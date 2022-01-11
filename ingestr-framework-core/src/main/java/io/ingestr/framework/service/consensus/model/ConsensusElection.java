package io.ingestr.framework.service.consensus.model;


import java.time.Instant;

public class ConsensusElection {
    private Vote leader = null;
    private Instant electionTimestamp;
    private String electionIdentifier;
    public static final Integer DEFAULT_ELECTION_WINDOW_SECONDS = 3;


    public ConsensusElection(String electionIdentifier, Instant electionTimestamp) {
        this.electionIdentifier = electionIdentifier;
        this.electionTimestamp = electionTimestamp;
    }

    /**
     * Only 1 election should be permitted in a 30 second period with the results being final for this duration
     *
     * @param timestamp
     * @return
     */
    public boolean isCurrentElection(Instant timestamp) {
        if (electionTimestamp.plusSeconds(DEFAULT_ELECTION_WINDOW_SECONDS).isAfter(timestamp)) {
            return true;
        }
        return false;
    }

    public String getElectionIdentifier() {
        return electionIdentifier;
    }

    /**
     * Cast vote for leadership returning true if successful
     *
     * @param vote
     * @return
     */
    public boolean castVote(Vote vote) {
        synchronized (this) {
            if (this.leader == null) {
                this.leader = vote;
                return true;
            }
        }
        return false;
    }

    public Vote getLeader() {
        return leader;
    }
}

