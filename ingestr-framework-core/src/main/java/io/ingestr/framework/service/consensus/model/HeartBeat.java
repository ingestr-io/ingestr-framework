package io.ingestr.framework.service.consensus.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.Instant;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class HeartBeat {
    private String identifier;
    private String consensusGroup;
    @JsonIgnore
    private Instant timestamp;
    @JsonIgnore
    private Long sequence;
}