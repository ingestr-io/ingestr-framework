package io.ingestr.framework.service.consensus;

import io.ingestr.framework.service.consensus.model.Consensus;
import io.ingestr.framework.service.consensus.model.ConsensusRegistration;

import java.util.List;

public interface ConsensusService {
    public static final Long DEFAULT_HEARTBEAT_INTERVAL = 500l;


    List<Consensus> listConsensus();

    void registerConsensus(ConsensusRegistration request);

    void start();

    void stop();

    void sendHeartBeat(String consensusGroup) throws ConsensusException;
}
