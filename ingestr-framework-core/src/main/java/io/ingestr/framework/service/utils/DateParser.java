package io.ingestr.framework.service.utils;

import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.time.format.DateTimeParseException;

public class DateParser {

    public static Instant parseInstant(String dateString) {
        if (dateString == null) {
            return null;
        }
        //go the easy approach
        try {
            return Instant.parse(dateString);
        } catch (DateTimeParseException e) {
        }
        if (dateString.contains("-")) {
            if (!dateString.contains("T")) {
                try {
                    return Instant.parse(dateString + "T00:00:00.000Z");
                } catch (DateTimeParseException e) {
                }
            } else {
                if (!dateString.contains(".")) {
                    return Instant.parse(dateString + ".000Z");
                }
                if (!dateString.contains("Z")) {
                    return Instant.parse(dateString + "Z");
                }
            }
        }
        //is numbers only, could be epoch
        if (StringUtils.isNumeric(dateString)) {
            if (dateString.length() >= 12) {
                return Instant.ofEpochMilli(Long.parseLong(dateString));
            } else {
                return Instant.ofEpochSecond(Long.parseLong(dateString));
            }
        }
        return null;
    }

}
