package io.ingestr.framework.validation;

import io.ingestr.framework.entities.DataType;
import io.ingestr.framework.entities.FieldType;
import io.ingestr.framework.entities.ParameterDescriptor;
import io.ingestr.framework.entities.Parameters;
import io.ingestr.framework.IngestrFunctions;
import io.ingestr.framework.exception.ValidationException;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ParameterDescriptorParserTest {

    @Test
    void shouldValidateNotNullableField() {
        List<ParameterDescriptor> parameterDescriptors = new ArrayList<>();
        parameterDescriptors.add(
                IngestrFunctions.newParameter("p1", FieldType.SINGLE, DataType.STRING)
                        .nullable(false)
                        .build()
        );

        ParameterDescriptorParser v = new ParameterDescriptorParser();
        try {
            v.parse(parameterDescriptors, new HashMap<>());
            fail("Expected validation Exception");
        } catch (ValidationException ve) {
        }
    }


    @Test
    void shouldValidateNullableField() {
        List<ParameterDescriptor> parameterDescriptors = new ArrayList<>();
        parameterDescriptors.add(
                IngestrFunctions.newParameter("p1", FieldType.SINGLE, DataType.STRING)
                        .nullable(true)
                        .build()
        );

        ParameterDescriptorParser v = new ParameterDescriptorParser();
        Parameters p =
                v.parse(parameterDescriptors, new HashMap<>());

        assertEquals(null, p.get("p1", null));
    }


    @Test
    void shouldApplyDefaultValue() {
        List<ParameterDescriptor> parameterDescriptors = new ArrayList<>();
        parameterDescriptors.add(
                IngestrFunctions.newParameter("p1", FieldType.SINGLE, DataType.STRING)
                        .defaultValue("test")
                        .build()
        );

        ParameterDescriptorParser v = new ParameterDescriptorParser();
        Parameters p =
                v.parse(parameterDescriptors, new HashMap<>());

        assertEquals("test", p.get("p1"));
    }

    @Test
    void shouldValidateFromList() {
        List<ParameterDescriptor> parameterDescriptors = new ArrayList<>();
        parameterDescriptors.add(
                IngestrFunctions.newParameter("p1", FieldType.SINGLE, DataType.STRING)
                        .allowedValues("v1", "v2")
                        .build()
        );

        ParameterDescriptorParser v = new ParameterDescriptorParser();
        Parameters p =
                v.parse(parameterDescriptors, new HashMap<>() {{
                    put("P1", "V1");
                }});

        //make sure the values have been standardised
        assertEquals("v1", p.get("p1"));
    }


    @Test
    void shouldFailIfNotInAllowedList() {
        List<ParameterDescriptor> parameterDescriptors = new ArrayList<>();
        parameterDescriptors.add(
                IngestrFunctions.newParameter("p1", FieldType.SINGLE, DataType.STRING)
                        .allowedValues("v1", "v2")
                        .build()
        );

        ParameterDescriptorParser v = new ParameterDescriptorParser();

        try {
            Parameters p =
                    v.parse(parameterDescriptors, new HashMap<>() {{
                        put("P1", "v3");
                    }});
            fail("Expected Validation Error");
        } catch (ValidationException ve) {
        }
    }


    @Test
    void shouldParseFloat() {
        List<ParameterDescriptor> parameterDescriptors = new ArrayList<>();
        parameterDescriptors.add(
                IngestrFunctions.newParameter("p1", FieldType.SINGLE, DataType.FLOAT)
                        .build()
        );

        ParameterDescriptorParser v = new ParameterDescriptorParser();

        try {
            Parameters p =
                    v.parse(parameterDescriptors, new HashMap<>() {{
                        put("P1", "v3");
                    }});
            fail("Expected Validation Error");
        } catch (ValidationException ve) {
        }


        Parameters p =
                v.parse(parameterDescriptors, new HashMap<>() {{
                    put("P1", "3");
                }});

        assertEquals(3, p.getFloat("p1"));
    }

    @Test
    void shouldParseInteger() {
        List<ParameterDescriptor> parameterDescriptors = new ArrayList<>();
        parameterDescriptors.add(
                IngestrFunctions.newParameter("p1", FieldType.SINGLE, DataType.INTEGER)
                        .build()
        );

        ParameterDescriptorParser v = new ParameterDescriptorParser();

        try {
            Parameters p =
                    v.parse(parameterDescriptors, new HashMap<>() {{
                        put("P1", "v3");
                    }});
            fail("Expected Validation Error");
        } catch (ValidationException ve) {
        }


        Parameters p =
                v.parse(parameterDescriptors, new HashMap<>() {{
                    put("P1", "3");
                }});

        assertEquals("3", p.get("p1"));
    }


    @Test
    void shouldParseTimestamp() {
        List<ParameterDescriptor> parameterDescriptors = new ArrayList<>();
        parameterDescriptors.add(
                IngestrFunctions.newParameter("p1", FieldType.SINGLE, DataType.TIMESTAMP)
                        .build()
        );

        ParameterDescriptorParser v = new ParameterDescriptorParser();

        try {
            Parameters p =
                    v.parse(parameterDescriptors, new HashMap<>() {{
                        put("P1", "asdf");
                    }});
            fail("Expected Validation Error");
        } catch (ValidationException ve) {
        }

        ZonedDateTime now = ZonedDateTime.now();
        Parameters p =
                v.parse(parameterDescriptors, new HashMap<>() {{
                    put("P1", now.toString());
                }});

        assertEquals(now, p.getTimestamp("p1"));
    }


}