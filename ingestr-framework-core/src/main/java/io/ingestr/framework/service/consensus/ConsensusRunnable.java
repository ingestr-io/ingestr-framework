package io.ingestr.framework.service.consensus;

public interface ConsensusRunnable extends Runnable {
    void shutdown();

    void init();

    void onFail(String reason);
}
