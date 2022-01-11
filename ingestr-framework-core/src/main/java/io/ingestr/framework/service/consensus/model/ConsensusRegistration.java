package io.ingestr.framework.service.consensus.model;

import io.ingestr.framework.service.consensus.ConsensusListener;
import io.ingestr.framework.service.consensus.ConsensusRunnable;
import lombok.Data;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

@Data
@ToString
public class ConsensusRegistration {
    private Class<? extends ConsensusRunnable> consensusRunnable;
    private Supplier<ConsensusRunnable> consensusSupplier;
    private String consensusGroup;
    private List<ConsensusListener> listeners;

    public ConsensusRegistration(
            Class<? extends ConsensusRunnable> consensusRunnable,
            Supplier<ConsensusRunnable> consensusSupplier,
            String consensusGroup,
            List<ConsensusListener> listeners) {
        this.consensusRunnable = consensusRunnable;
        this.consensusSupplier = consensusSupplier;
        this.consensusGroup = consensusGroup;
        this.listeners = listeners;
    }

    public static ConsensusRegistration of(
            String consensusGroup,
            Class<? extends ConsensusRunnable> consensusRunnable,
            Supplier<ConsensusRunnable> consensusSupplier) {
        return new ConsensusRegistration(consensusRunnable, consensusSupplier, consensusGroup, new ArrayList<>());
    }

    public static ConsensusRegistration of(
            String consensusGroup,
            Class<? extends ConsensusRunnable> consensusRunnable,
            Supplier<ConsensusRunnable> consensusSupplier,
            List<ConsensusListener> listeners) {
        return new ConsensusRegistration(consensusRunnable, consensusSupplier, consensusGroup, listeners);
    }

    public static ConsensusRegistration of(
            String consensusGroup,
            Class<? extends ConsensusRunnable> consensusRunnable,
            Supplier<ConsensusRunnable> consensusSupplier,
            ConsensusListener listener) {
        return new ConsensusRegistration(consensusRunnable, consensusSupplier, consensusGroup, Arrays.asList(listener));
    }

}
