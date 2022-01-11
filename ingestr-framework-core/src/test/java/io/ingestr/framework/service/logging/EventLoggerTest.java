package io.ingestr.framework.service.logging;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@MicronautTest
class EventLoggerTest {
    @Inject
    EventLogger eventLogger;

    @Test
    void shouldLogEvents() {
        eventLogger.log(LogEvent.
                info("Test")
                .event("test_event"));
    }

}