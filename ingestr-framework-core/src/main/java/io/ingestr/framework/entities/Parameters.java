package io.ingestr.framework.entities;


import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;


@AllArgsConstructor
@NoArgsConstructor
public class Parameters {
    private Map<String, String> parameters = new HashMap<>();


    public Parameters merge(Parameters parameters) {
        for (Map.Entry<String, String> e : parameters.parameters.entrySet()) {
            this.parameters.put(e.getKey(), e.getValue());
        }
        return this;
    }

    public String get(String key) {
        return get(key, null);
    }

    public String get(String key, String defaultValue) {
        return parameters.getOrDefault(key, defaultValue);
    }

    public Integer getInteger(String key) {
        return getInteger(key, null);
    }

    public Integer getInteger(String key, Integer defaultValue) {
        String val = parameters.getOrDefault(key, defaultValue == null ? null : defaultValue.toString());
        if (val == null) {
            return null;
        }
        return Integer.parseInt(val);
    }

    public Float getFloat(String key) {
        return getFloat(key, null);
    }

    public Float getFloat(String key, Float defaultValue) {
        String val = parameters.getOrDefault(key, defaultValue == null ? null : defaultValue.toString());
        if (val == null) {
            return null;
        }
        return Float.parseFloat(val);
    }

    public LocalDate getDate(String key) {
        return getDate(key, null);
    }

    public LocalDate getDate(String key, LocalDate defaultValue) {
        String val = parameters.getOrDefault(key, defaultValue == null ? null : defaultValue.toString());
        if (val == null) {
            return null;
        }
        return LocalDate.parse(val);
    }

    public ZonedDateTime getTimestamp(String key) {
        return getTimestamp(key, null);
    }

    public ZonedDateTime getTimestamp(String key, ZonedDateTime defaultValue) {
        String val = parameters.getOrDefault(key, defaultValue == null ? null : defaultValue.toString());
        if (val == null) {
            return null;
        }
        return ZonedDateTime.parse(val);
    }


    @Override
    public String toString() {
        if (parameters == null) {
            return "";
        }
        return parameters.toString();
    }
}
