package io.ingestr.framework.kafka;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

public class ObjectMapperFactory {
    private static ObjectMapper kakfaObjectMapper = null;
    private final static Object lock = new Object();

    public static ObjectMapper kafkaMessageObjectMapper() {
        if (ObjectMapperFactory.kakfaObjectMapper != null) {
            return kakfaObjectMapper;
        }
        synchronized (lock) {
            ObjectMapperFactory.kakfaObjectMapper = new ObjectMapper();
            ObjectMapperFactory.kakfaObjectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
            ObjectMapperFactory.kakfaObjectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
            ObjectMapperFactory.kakfaObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            ObjectMapperFactory.kakfaObjectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
            ObjectMapperFactory.kakfaObjectMapper.registerModule(new ParameterNamesModule());
            ObjectMapperFactory.kakfaObjectMapper.registerModule(new Jdk8Module());
            ObjectMapperFactory.kakfaObjectMapper.registerModule(new JavaTimeModule());
            return ObjectMapperFactory.kakfaObjectMapper;
        }
    }
}
