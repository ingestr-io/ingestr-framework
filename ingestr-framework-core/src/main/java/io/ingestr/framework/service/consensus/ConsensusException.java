package io.ingestr.framework.service.consensus;

public class ConsensusException extends Exception {
    public ConsensusException() {
    }

    public ConsensusException(String message) {
        super(message);
    }

    public ConsensusException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConsensusException(Throwable cause) {
        super(cause);
    }

    public ConsensusException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
