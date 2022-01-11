package io.ingestr.framework.entities;


import lombok.*;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface Reprocessor {

    ReprocessorResponse reprocess(ReprocessorRequest request);

    @Getter
    @ToString
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    class ReprocessorRequest {
        private Partition partition;
        @Singular
        private Map<String, String> parameters;
        private Optional<Offset> from;
        private Optional<Offset> to;
    }

    @Getter
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    class ReprocessorResponse {
        @Singular
        private Set<Offset> offsets = new HashSet<>();
    }
}
