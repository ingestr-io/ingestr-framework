package io.ingestr.framework.validation;

import io.ingestr.framework.entities.DataType;
import io.ingestr.framework.entities.ParameterDescriptor;
import io.ingestr.framework.entities.Parameters;
import io.ingestr.framework.exception.ValidationException;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParameterDescriptorParser {

    public static Parameters parse(
            List<ParameterDescriptor> parameterDescriptors,
            Map<String, String> parameters
    ) {
        //make sure that the parameters are nullsafe
        if (parameters == null) {
            parameters = new HashMap<>();
        }
        //update the map to include lowercase versions of the same key/value for standardisation
        Map<String, String> lowerParams = new HashMap<>();

        for (Map.Entry<String, String> e : parameters.entrySet()) {
            lowerParams.put(e.getKey().toLowerCase(), e.getValue());
        }

        Map<String, String> results = new HashMap<>();
        for (ParameterDescriptor pd : parameterDescriptors) {
            String value = lowerParams.getOrDefault(pd.getIdentifier().toLowerCase(), null);
            //if the value is null
            if (value == null) {
                if (pd.getDefaultValue() != null) {
                    value = pd.getDefaultValue();
                }
                if (value == null && pd.getNullable() == Boolean.FALSE) {
                    throw new ValidationException(
                            "Invalid field id = '" + pd.getIdentifier() + "' is not nullable and no value is set "
                    );
                }
            }

            /**
             * Validate the allowed values
             */
            if (pd.getAllowedValues() != null && !pd.getAllowedValues().isEmpty()) {
                //make sure that the value is in the allowed values
                boolean found = false;
                for (String allowedValue : pd.getAllowedValues()) {
                    if (StringUtils.equalsIgnoreCase(allowedValue, value)) {
                        found = true;
                        //override the value with the allowedValue to standardise the value
                        value = allowedValue;
                    }
                }
                if (!found) {
                    throw new ValidationException("The value '" + value + "' was not found in the allowed values list '" +
                            StringUtils.join(pd.getAllowedValues(), ",") + "'");
                }
            }

            /**
             *
             */
            if (pd.getDataType() == DataType.FLOAT) {
                try {
                    Float.parseFloat(value);
                } catch (NumberFormatException e) {
                    throw new ValidationException("The value '" + value + "' is not parseable to a Float");
                }
            }
            if (pd.getDataType() == DataType.INTEGER) {
                try {
                    Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    throw new ValidationException("The value '" + value + "' is not parseable to an Integer");
                }
            }

            if (pd.getDataType() == DataType.TIMESTAMP) {
                try {
                    ZonedDateTime.parse(value);
                } catch (DateTimeParseException e) {
                    throw new ValidationException("The value '" + value + "' is not parseable to a Time Stamp expected: " +
                            DateTimeFormatter.ISO_ZONED_DATE_TIME);
                }
            }

            if (pd.getDataType() == DataType.DATE) {
                try {
                    LocalDate.parse(value);
                } catch (DateTimeParseException e) {
                    throw new ValidationException("The value '" + value + "' is not parseable to a Date expected: " +
                            DateTimeFormatter.ISO_LOCAL_DATE);
                }
            }
            results.put(pd.getIdentifier(), value);
        }

        return new Parameters(results);
    }
}
