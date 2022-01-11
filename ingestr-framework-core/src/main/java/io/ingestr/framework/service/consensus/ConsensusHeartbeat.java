package io.ingestr.framework.service.consensus;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConsensusHeartbeat implements Runnable {
    private boolean shutdown = false;
    private long interval;
    private ConsensusService consensusService;
    private String consensusGroup;

    public ConsensusHeartbeat(
            ConsensusService consensusService,
            String consensusGroup,
            long interval
    ) {
        this.interval = interval;
        this.consensusGroup = consensusGroup;
        this.consensusService = consensusService;
    }

    public void shutdown() {
        log.info("Shutting down the Heartbeat thread for {}", consensusGroup);
        this.shutdown = true;
    }

    @Override
    public void run() {
        log.info("Starting the Consensus Heartbeat Thread Monitor for {}...", consensusGroup);
        while (!shutdown) {
            try {
                Thread.sleep(interval);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
            try {
                consensusService.sendHeartBeat(consensusGroup);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                shutdown();
            }
        }
        log.info("Finished the Consensus Heartbeat Thread Monitor for {}...", consensusGroup);
    }

}
