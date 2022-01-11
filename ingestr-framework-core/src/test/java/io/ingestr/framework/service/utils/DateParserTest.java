package io.ingestr.framework.service.utils;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

class DateParserTest {

    @Test
    void shouldParseDate() {
        Instant instant =
                DateParser.parseInstant("1641761381");
        assertEquals(Instant.ofEpochSecond(1641761381), instant);

        instant = DateParser.parseInstant("1641761381012");
        assertEquals(Instant.ofEpochMilli(1641761381012l), instant);


        instant = DateParser.parseInstant("2020-01-01");
        assertEquals(
                LocalDateTime.of(2020, 1, 1, 0, 0, 0, 0)
                        .toInstant(ZoneOffset.UTC)
                , instant);


        instant = DateParser.parseInstant("2020-01-01T00:00:01");
        assertEquals(
                LocalDateTime.of(2020, 1, 1, 0, 0, 1, 0)
                        .toInstant(ZoneOffset.UTC)
                , instant);

        instant = DateParser.parseInstant("2020-01-01T00:00:01.012");
        assertEquals(
                LocalDateTime.of(2020, 1, 1, 0, 0, 1, 12000000)
                        .toInstant(ZoneOffset.UTC)
                , instant);


        instant = DateParser.parseInstant("2020-01-01T00:00:01.999Z");
        assertEquals(
                LocalDateTime.of(2020, 1, 1, 0, 0, 1, 999000000)
                        .toInstant(ZoneOffset.UTC)
                , instant);


        instant = DateParser.parseInstant("2020-01-01T00:00:03Z");
        assertEquals(
                LocalDateTime.of(2020, 1, 1, 0, 0, 3, 0)
                        .toInstant(ZoneOffset.UTC)
                , instant);


    }

}