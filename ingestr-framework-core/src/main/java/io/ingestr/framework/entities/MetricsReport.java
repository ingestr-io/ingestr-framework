package io.ingestr.framework.entities;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.time.ZonedDateTime;
import java.util.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class MetricsReport {
    private String loaderName;
    private String hostname;
    private String host;
    @Singular
    private List<Metric> metrics;
    private ZonedDateTime reportTimestamp;


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class Metric {
        private String name;
        private String type;
        private String description;
        private String baseUnit;

        @Builder.Default
        private List<MetricMeasurement> measurements = new ArrayList<>();
//        @Builder.Default
//        private Set<MetricValue> totals = new HashSet<>();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    @EqualsAndHashCode
    @ToString
    public static class MetricMeasurement {
        private Set<MetricValue> metricValues;
        @Singular
        private Set<MetricTag> tags = new HashSet<>();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    @EqualsAndHashCode(exclude = "value")
    @ToString
    public static class MetricValue {
        private String name;
        private Double value;

    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    @EqualsAndHashCode
    @ToString
    public static class MetricTag {
        private String name;
        private String value;
    }

}
