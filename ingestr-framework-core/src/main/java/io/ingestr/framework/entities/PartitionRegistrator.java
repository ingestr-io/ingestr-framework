package io.ingestr.framework.entities;

import lombok.*;

import java.util.ArrayList;
import java.util.List;

public interface PartitionRegistrator {

//    ParitionRegistratorResult discover(ParitionRegistratorRequest request);
    void discover(ParitionRegistratorRequest request, ParitionRegistratorResult result);


    @Data
    @ToString
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder(access = AccessLevel.PRIVATE, builderClassName = "B")
    class ParitionRegistratorRequest {
        private IngestionContext ingestionContext;


        public static ParitionRegistratorRequestBuilder newParitionRegistratorRequest() {
            return new ParitionRegistratorRequestBuilder(ParitionRegistratorRequest.builder());
        }

        public static class ParitionRegistratorRequestBuilder {
            private B builder;

            private ParitionRegistratorRequestBuilder(B builder) {
                this.builder = builder;
            }

            public ParitionRegistratorRequest build() {
                return builder.build();
            }
        }
    }

//    @Data
//    @ToString
//    @AllArgsConstructor
//    @NoArgsConstructor
//    @Builder(access = AccessLevel.PRIVATE, builderClassName = "B")
//    class ParitionRegistratorResultOld {
//        @Singular
//        private Set<PartitionRegistration> partitionRegistrations;
//
//
//        public static ParitionRegistratorResultBuilder newPartitionRegistratorResult() {
//            return new ParitionRegistratorResultBuilder(ParitionRegistratorResult.builder());
//        }
//
//
//        public static class ParitionRegistratorResultBuilder {
//            private B builder;
//
//            public ParitionRegistratorResultBuilder(B builder) {
//                this.builder = builder;
//            }
//
//            public ParitionRegistratorResultBuilder partitionRegistration(PartitionRegistration.PartitionRegistrationBuilder partitionRegistration) {
//                builder.partitionRegistration(partitionRegistration.build());
//                return this;
//            }
//
//            public ParitionRegistratorResult build() {
//                return builder.build();
//            }
//        }
//    }

    class ParitionRegistratorResult {
        private List<Partition> partitions = new ArrayList<>();

        public ParitionRegistratorResult addPartition(Partition.PartitionBuilder partition) {
            this.partitions.add(partition.build());
            return this;
        }

        public List<Partition> getPartitions() {
            return partitions;
        }
    }
}
