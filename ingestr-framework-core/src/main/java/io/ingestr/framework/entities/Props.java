package io.ingestr.framework.entities;

import lombok.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.*;

@Data
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Props {
    @Singular
    private List<Prop> properties;


    public boolean containsKey(String key) {
        for (Props.Prop pr : properties) {
            if (StringUtils.equalsIgnoreCase(pr.getIdentifier(), key)) {
                return true;
            }
        }
        return false;
    }

    public boolean isEqual(String key, String value) {
        for (Props.Prop pr : properties) {
            if (StringUtils.equalsIgnoreCase(pr.getIdentifier(), key)) {
                return StringUtils.equalsIgnoreCase(pr.getValue(), value);
            }
        }
        return false;
    }

    public Optional<Prop> getProp(String key) {
        for (Props.Prop pr : properties) {
            if (StringUtils.equalsIgnoreCase(pr.getIdentifier(), key)) {
                return Optional.of(pr);
            }
        }
        return Optional.empty();
    }

    public static PropsBuilder newProps() {
        return Props.builder();
    }


    public static PropsBuilder newProps(String k1, String v1) {
        return Props.builder()
                .set(k1, v1);
    }

    public static PropsBuilder newProps(String k1, String v1, String k2, String v2) {
        return Props.builder()
                .set(k1, v1)
                .set(k2, v2)
                ;
    }


    public static PropsBuilder newProps(String k1, String v1, String k2, String v2, String k3, String v3) {
        return Props.builder()
                .set(k1, v1)
                .set(k2, v2)
                .set(k3, v3);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        Props props = (Props) o;

        if (properties.size() != props.properties.size()) {
            return false;
        }

        Collections.sort(properties, (o1, o2) -> o1.getIdentifier().compareTo(o2.getIdentifier()));
        Collections.sort(props.properties, (o1, o2) -> o1.getIdentifier().compareTo(o2.getIdentifier()));

        return new EqualsBuilder().append(properties, props.properties).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(174123, 37512).append(properties).toHashCode();
    }

    @Data
    @ToString
    @Builder(toBuilder = true)
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Prop {
        private String identifier;
        private String value;
        private List<String> values;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            Prop prop = (Prop) o;

            return new EqualsBuilder()
                    .append(identifier, prop.identifier).append(value, prop.value).append(values, prop.values).isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(512317, 553117).append(identifier).append(value).append(values).toHashCode();
        }
    }

    public static class PropsBuilder {

        public PropsBuilder set(String identifier, String value) {
            this.property(Prop.builder()
                    .identifier(identifier)
                    .value(value)
                    .build());
            return this;
        }

        public PropsBuilder set(String identifier, String... values) {
            this.property(Prop.builder()
                    .identifier(identifier)
                    .values(Arrays.asList(values))
                    .build());
            return this;
        }

        public PropsBuilder set(String identifier, List<String> values) {
            this.property(Prop.builder()
                    .identifier(identifier)
                    .values(values)
                    .build());

            return this;
        }
    }
}
