package io.ingestr.framework.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@Data
@NoArgsConstructor
@ToString
@Builder
@AllArgsConstructor
public class Trigger implements Serializable {
    private String name;
    @Singular
    @JsonIgnore
    private List<PartitionFilter> partitionFilters = new ArrayList<>();

    @JsonIgnore
    private transient Supplier<? extends TriggerFunction> trigger;




    public static class TriggerBuilder {

        public TriggerBuilder partitionFilterByPriority(Partition.Priority priority) {
            this.partitionFilter(partition -> partition.getPriority() == priority);
            return this;
        }

        public TriggerBuilder partitionFilterByTag(String... tags) {
            this.partitionFilter(partition -> {
                for (String tag : tags) {
                    if (partition.getTags().contains(tag.toLowerCase())) {
                        return true;
                    }
                }
                return false;
            });
            return this;
        }
    }
}
