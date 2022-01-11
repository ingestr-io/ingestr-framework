package io.ingestr.framework;

import io.ingestr.framework.entities.*;

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;


public class IngestrFunctions {

    public static LoaderConfiguration.LoaderConfigurationBuilder newLoaderConfiguration() {
        return LoaderConfiguration.builder();
    }

    public static ConfigurationDescriptor.ConfigurationDescriptorBuilder newConfig(String identifier, FieldType fieldType, DataType dataType) {
        return ConfigurationDescriptor.builder()
                .identifier(identifier)
                .fieldType(fieldType)
                .dataType(dataType);
    }


    public static DataDescriptor.DataDescriptorBuilder newDataDescriptor(
            String identifier,
            String name
    ) {
        return DataDescriptor.builder()
                .identifier(identifier)
                .name(name);
    }

    public static PartitionKeyDescriptor.PartitionKeyDescriptorBuilder newPartitionKey(String identifier, FieldType fieldType, DataType dataType) {
        return PartitionKeyDescriptor.builder()
                .identifier(identifier)
                .fieldType(fieldType)
                .dataType(dataType);
    }

    public static OffsetKeyDescriptor.OffsetKeyDescriptorBuilder newOffsetKey(String identifier, DataType dataType) {
        return OffsetKeyDescriptor.builder()
                .identifier(identifier)
                .dataType(dataType);
    }

    public static ParameterDescriptor.ParameterDescriptorBuilder newParameter(String identifier, FieldType fieldType, DataType dataType) {
        return ParameterDescriptor.builder()
                .identifier(identifier)
                .fieldType(fieldType)
                .dataType(dataType);
    }

    public static ParameterValueDescriptor.ParameterValueDescriptorBuilder newParameterValue(String identifier, String value) {
        return ParameterValueDescriptor.builder()
                .identifier(identifier)
                .defaultValue(value);
    }


    public static PartitionRegister.PartitionRegisterBuilder newPartitionRegister(Supplier<? extends PartitionRegistrator> partitionRegistratorSupplier) {
        return PartitionRegister.builder()
                .partitionRegistratorSupplier(partitionRegistratorSupplier);
    }


    public static ReprocessorDescriptor.ReprocessorDescriptorBuilder newReprocessor(Supplier<? extends Reprocessor> reprocessor) {
        return ReprocessorDescriptor.builder()
                .reprocessor(reprocessor);
    }


    public static Offset.OffsetBuilder newOffsetKey(OffsetEntry offsetEntry) {
        return Offset.builder()
                .offsetEntry(offsetEntry);
    }

    public static Trigger.TriggerBuilder newTrigger(String name, Supplier<? extends TriggerFunction> trigger) {
        return Trigger.builder()
                .name(name)
                .trigger(trigger);
    }

    public static Partition.PartitionBuilder newPartition(Set<PartitionEntry> entries) {
        return newPartition(entries.toArray(new PartitionEntry[0]));
    }

    public static Partition.PartitionBuilder newPartition(PartitionEntry... entries) {
        Partition.PartitionBuilder pb = Partition.builder();
        for (PartitionEntry e : entries) {
            pb = pb.partitionEntry(e);
        }
        return pb;
    }

    public static IngestionResult.IngestionResultBuilder newIngestionResult(
            IngestionRequest request, String data, Offset.OffsetBuilder offsetBuilder) {
        Offset offset = offsetBuilder.build();
        offset.setPartitionKey(request.getPartition().getKey());
        return IngestionResult.builder()
                .offset(offset)
                .data(Optional.of(data));
    }


    public static TriggeredPartition.TriggeredPartitionBuilder newTriggeredPartition() {
        return TriggeredPartition.builder();
    }

    public static TriggeredPartition.TriggeredPartitionBuilder newTriggeredPartition(Partition partition) {
        return TriggeredPartition.builder()
                .partitionEntries(partition.getPartitionEntries());
    }

    public static TriggeredPartition.TriggeredPartitionBuilder newTriggeredPartition(String k1, String v1) {
        return TriggeredPartition.builder()
                .partitionEntry(
                        PartitionEntry.newEntry(k1, v1)
                );
    }

    public static TriggeredPartition.TriggeredPartitionBuilder newTriggeredPartition(String k1, String v1, String k2, String v2) {
        return TriggeredPartition.builder()
                .partitionEntry(PartitionEntry.newEntry(k1, v1))
                .partitionEntry(PartitionEntry.newEntry(k2, v2));
    }

    public static TriggeredPartition.TriggeredPartitionBuilder newTriggeredPartition(String k1, String v1, String k2, String v2, String k3, String v3) {
        return TriggeredPartition.builder()
                .partitionEntry(PartitionEntry.newEntry(k1, v1))
                .partitionEntry(PartitionEntry.newEntry(k2, v2))
                .partitionEntry(PartitionEntry.newEntry(k3, v3));
    }


    public static IngestionSchedule.IngestionScheduleBuilder newSchedule(String identifier, String name, String schedule, String scheduleTimeZone) {
        return IngestionSchedule.builder()
                .identifier(identifier)
                .schedule(schedule)
                .scheduleTimeZone(scheduleTimeZone);
    }
}
