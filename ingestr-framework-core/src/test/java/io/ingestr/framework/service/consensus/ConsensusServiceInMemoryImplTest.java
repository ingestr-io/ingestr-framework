package io.ingestr.framework.service.consensus;

import io.ingestr.framework.service.consensus.model.ConsensusRegistration;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class ConsensusServiceInMemoryImplTest {

    @Test
    void shouldWinConsensus() {
        ConsensusServiceInMemoryImpl mi = new ConsensusServiceInMemoryImpl();
        mi.registerConsensus(ConsensusRegistration.of(
                "group-1",
                TestCR.class,
                TestCR::new
        ));
    }

    public static class TestCR implements ConsensusRunnable {
        @Override
        public void shutdown() {

        }

        @Override
        public void init() {

        }

        @Override
        public void onFail(String reason) {

        }

        @Override
        public void run() {
            log.info("Run");

        }
    }
}