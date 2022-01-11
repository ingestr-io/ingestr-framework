package io.ingestr.framework.service.consensus;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@Slf4j
public class ConsensusWorker extends Thread {
    private ConsensusService consensusService;
    private Supplier<ConsensusRunnable> workerSupplier;

    private String consensusGroup;
    private boolean shutdown = false;

    private Thread workerThread;
    private Thread heartbeatThread;
    private ConsensusHeartbeat heartbeat;

    private ConsensusRunnable worker;
    private Instant startedAt;
    private Instant endedAt;

    private List<ConsensusWorkerListener> listeners = new ArrayList<>();

    public ConsensusWorker(
            String consensusGroup,
            ConsensusService consensusService,
            Supplier<ConsensusRunnable> workerSupplier) {
        this.consensusService = consensusService;
        this.consensusGroup = consensusGroup;
        this.workerSupplier = workerSupplier;
        this.heartbeat = new ConsensusHeartbeat(
                consensusService,
                consensusGroup,
                ConsensusService.DEFAULT_HEARTBEAT_INTERVAL);
        this.heartbeatThread = new Thread(heartbeat, consensusGroup + "-heartbeat");
        this.setName(consensusGroup + "-worker");
    }

    public void addListener(ConsensusWorkerListener listener) {
        this.listeners.add(listener);
    }

    public void shutdown() {
        if (this.worker == null) {
            throw new IllegalStateException("Cannot shutdown something that has not been created!");
        }
        shutdown = true;
        log.info("Shutting down Worker Thread for consumer group {}", consensusGroup);
        this.worker.shutdown();
        try {
            if (this.workerThread.isAlive()) {
                while (true) {
                    log.info("Waiting for Worker Thread to finish for consumer group {}", consensusGroup);
                    this.workerThread.join(3_000);
                    if (!this.workerThread.isAlive()) {
                        break;
                    }
                }
                log.info("Worker Thread finished for consumer group {}", consensusGroup);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        log.info("Shutting down Heartbeat Thread for consumer group {}", consensusGroup);
        try {
            this.heartbeat.shutdown();
            if (this.heartbeatThread.isAlive()) {
                while (true) {
                    log.info("Waiting for Heartbeat Thread to finish for consumer group {}", consensusGroup);
                    this.heartbeatThread.join(3_000);
                    if (!this.heartbeatThread.isAlive()) {
                        break;
                    }
                }
                log.info("Heartbeat Thread finished for consumer group {}", consensusGroup);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    protected Instant getStartedAt() {
        return startedAt;
    }

    @Override
    public void run() {
        log.info("Starting Consensus Worker for consumer group {}", this.consensusGroup);

        shutdown = false;
        startedAt = Instant.now();
        endedAt = null;

        log.debug("Notifying {} listeners of Worker Start...", this.listeners.size());
        for (ConsensusWorkerListener l : listeners) {
            l.onWorkerEvent(new ConsensusWorkerStartedEvent(this.consensusGroup));
        }
        try {
            //Create a new Consensus Worker Object and wrap it into a Thread
            worker = workerSupplier.get();
            workerThread = new Thread(worker, this.consensusGroup);

            //trigger the Init method
            worker.init();

            //start the thread
            this.workerThread.start();
            //start the heartbeat thread
            this.heartbeatThread.start();


            while (!shutdown) {
                try {
                    Thread.sleep(500);
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage(), e);
                }

                //check the health of both the threads
                if (!this.workerThread.isAlive()) {
                    log.warn("Detected Consensus Worker Thread is no longer alive.  Terminating the Heartbeat...");
                    shutdown();
                    break;
                }
                if (!this.heartbeatThread.isAlive()) {
                    log.warn("Detected Consensus Worker Heartbeat thread is no longer alive.  Terminating the Worker Thread...");
                    shutdown();
                    break;
                }
            }

        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            worker.onFail(t.getMessage());
        } finally {
            log.info("Ended Consensus Worker Thread for consumer group {}", consensusGroup);

            //make sure we stop the heartbeats
            endedAt = Instant.now();
            //fire the OnShutdownEvent
            worker.shutdown();
            for (ConsensusWorkerListener l : listeners) {
                l.onWorkerEvent(new ConsensusWorkerEndedEvent());
            }
        }
    }

    public interface ConsensusWorkerListener {
        void onWorkerEvent(ConsensusWorkerEvent event);
    }

    public interface ConsensusWorkerEvent {

    }

    @AllArgsConstructor
    @Getter
    public class ConsensusWorkerStartedEvent implements ConsensusWorkerEvent {
        private String consensusGroup;

    }

    public class ConsensusWorkerEndedEvent implements ConsensusWorkerEvent {

    }
}
