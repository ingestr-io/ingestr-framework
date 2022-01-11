package io.ingestr.framework.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.ingestr.framework.Entity;
import io.ingestr.framework.builders.IngestionBatchBuilder;
import io.ingestr.framework.builders.IngestionStreamBuilder;
import lombok.*;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@Data
@ToString(exclude = {"job"})
@Builder
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(of = {"identifier"})
public class Ingestion implements Entity {
    private String identifier;
    private String name;
    private String description;
    private String dataDescriptorIdentifier;
    private Props properties;

    @JsonIgnore
    private Supplier<? extends IngestionJob> job;
    private List<IngestionSchedule> ingestionSchedules = new ArrayList<>();
    private List<Trigger> triggers = new ArrayList<>();

    private ZonedDateTime updatedAt;

    public static IngestionBatchBuilder batch(String identifier, String name,
                                              Supplier<? extends IngestionBatchJob> job) {
        return new IngestionBatchBuilder(identifier, name, job);
    }


    public static IngestionStreamBuilder stream(String identifier, String name,
                                                Supplier<? extends IngestionStreamJob> job) {
        return new IngestionStreamBuilder(identifier, name, job);
    }
}
