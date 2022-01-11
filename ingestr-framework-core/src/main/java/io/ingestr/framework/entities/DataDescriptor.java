package io.ingestr.framework.entities;

import io.ingestr.framework.Entity;
import lombok.*;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DataDescriptor implements Entity {
    private String identifier;
    private String name;
    private String description;
    private String version;
    private String topic;
    private Integer topicPartitionCount;

    private Props properties;

    @Singular
    private List<PartitionKeyDescriptor> partitionKeyDescriptors = new ArrayList<>();
    @Singular
    private List<ParameterDescriptor> parameterDescriptors = new ArrayList<>();
    @Singular
    private List<OffsetKeyDescriptor> offsetKeyDescriptors = new ArrayList<>();
    private PartitionRegister partitionRegister;

    private ReprocessorDescriptor reprocessorDescriptor;

    private String requiredVersion;

    private ZonedDateTime updatedAt;

    @Builder
    public DataDescriptor(String identifier, String name, String description, String version, String requiredVersion, ZonedDateTime updatedAt) {
        this.identifier = identifier;
        this.name = name;
        this.description = description;
        this.version = version;
        this.requiredVersion = requiredVersion;
        this.updatedAt = updatedAt;
    }

    public static class DataDescriptorBuilder {

        public DataDescriptorBuilder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public DataDescriptorBuilder topic(String topic, Integer topicPartitionCount) {
            this.topic = topic;
            this.topicPartitionCount = topicPartitionCount;
            return this;
        }

        public DataDescriptorBuilder addPartitionKey(PartitionKeyDescriptor.PartitionKeyDescriptorBuilder partitionDescriptorBuilder) {
            assert partitionDescriptorBuilder != null;
            if (this.partitionKeyDescriptors == null) {
                this.partitionKeyDescriptors = new ArrayList<>();
            }
            this.partitionKeyDescriptors.add(partitionDescriptorBuilder.build());
            return this;
        }

        public DataDescriptorBuilder addOffsetKey(OffsetKeyDescriptor.OffsetKeyDescriptorBuilder offset) {
            assert offset != null;
            if (this.offsetKeyDescriptors == null) {
                this.offsetKeyDescriptors = new ArrayList<>();
            }
            this.offsetKeyDescriptors.add(offset.build());
            return this;
        }

        public DataDescriptorBuilder addParameter(ParameterDescriptor.ParameterDescriptorBuilder parameter) {
            assert parameter != null;
            if (this.parameterDescriptors == null) {
                this.parameterDescriptors = new ArrayList<>();
            }
            this.parameterDescriptors.add(parameter.build());
            return this;
        }

        public DataDescriptorBuilder partitionRegistrator(PartitionRegister.PartitionRegisterBuilder partitionRegister) {
            this.partitionRegister = partitionRegister.build();
            return this;
        }

        public DataDescriptorBuilder reprocessor(ReprocessorDescriptor.ReprocessorDescriptorBuilder reprocessorDescriptor) {
            this.reprocessorDescriptor = reprocessorDescriptor.build();
            return this;
        }
    }
}
