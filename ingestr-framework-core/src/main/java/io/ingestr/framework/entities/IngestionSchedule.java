package io.ingestr.framework.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@ToString(exclude = {"partitionFilters", "properties"})
@EqualsAndHashCode(of = {"identifier", "name", "schedule", "scheduleTimeZone", "properties", "parameterDescriptors"})
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@NoArgsConstructor
@Builder
public class IngestionSchedule {
    private String identifier;
    private String name;
    private String schedule;
    private String scheduleTimeZone;
    @Singular
    private Map<String, String> properties = new HashMap<>();
    private List<ParameterDescriptor> parameterDescriptors = new ArrayList<>();
    @JsonIgnore
    private List<PartitionFilter> partitionFilters;


    public static class IngestionScheduleBuilder {
        private List<ParameterDescriptor> parameterDescriptors = new ArrayList<>();
        private List<ParameterValueDescriptor> parameterValueDescriptors = new ArrayList<>();
        private List<PartitionFilter> partitionFilters = new ArrayList<>();

//        public IngestionScheduleBuilder addParameter(ParameterDescriptor.ParameterDescriptorBuilder parameterDescriptor) {
//            parameterDescriptors.add(parameterDescriptor.build());
//            return this;
//        }

        public IngestionScheduleBuilder setParameter(ParameterValueDescriptor.ParameterValueDescriptorBuilder parameterValueDescriptorBuilder) {
            parameterValueDescriptors.add(parameterValueDescriptorBuilder.build());
            return this;
        }


        public IngestionScheduleBuilder partitionFilterByTag(String... tags) {
            partitionFilters.add(partition -> {
                for (String tag : tags) {
                    if (partition.getTags().contains(StringUtils.lowerCase(tag.toLowerCase()))) {
                        return true;
                    }
                }
                return false;
            });
            return this;
        }


        public IngestionScheduleBuilder partitionFilterByPriority(Partition.Priority priority) {
            partitionFilters.add(partition -> partition.getPriority() == priority);
            return this;
        }

        public IngestionScheduleBuilder partitionFilter(PartitionFilter partitionFilter) {
            partitionFilters.add(partitionFilter);
            return this;
        }
    }
}
