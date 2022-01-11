package io.ingestr.framework.service.consensus;

import io.ingestr.framework.service.consensus.model.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConsensusServiceInMemoryImpl implements ConsensusService {
    private Map<String, Consensus> consensusMap = new HashMap<>();


    @Override
    public List<Consensus> listConsensus() {
        return new ArrayList<>(this.consensusMap.values());
    }

    @Override
    public void registerConsensus(ConsensusRegistration request) {
        assert request.getConsensusGroup() != null;

        Consensus consensus = new Consensus(
                request.getConsensusGroup(),
                "node",
                "localhost",
                () -> new ConsensusWorker(
                        request.getConsensusGroup(),
                        this,
                        request.getConsensusSupplier()
                ));


        if (request.getListeners() != null) {
            for (ConsensusListener listener : request.getListeners()) {
                consensus.addListener(listener);
            }
        }
        consensusMap.put(request.getConsensusGroup(), consensus);

        //trigger the election and vote instantly
        ConsensusElection election = consensus.getOrCreateElection(new Election(request.getConsensusGroup(), "node", Instant.now()));

        Vote vote = new Vote();
        vote.setConsensusGroup(request.getConsensusGroup());
        vote.setIdentifier("node");
        vote.setTimestamp(Instant.now());
        vote.setElectionIdentifier(election.getElectionIdentifier());

        consensus.castVote(vote);
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public void sendHeartBeat(String consensusGroup) throws ConsensusException {

    }
}
