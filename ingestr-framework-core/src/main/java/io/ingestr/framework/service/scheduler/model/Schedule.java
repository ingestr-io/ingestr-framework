package io.ingestr.framework.service.scheduler.model;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.ToString;

import java.time.Clock;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

@Data
@ToString
public class Schedule {
    private String identifier;
    private ZoneId scheduledZone;
    private ZonedDateTime lastExecution;
    @JsonIgnore
    private static final CronDefinition cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ);
    @JsonIgnore
    private final Cron cron;

    private boolean enabled;
    private String context;

    private Schedule(
            String identifier,
            Cron cron,
            ZoneId scheduledZone,
            boolean enabled,
            String context,
            ZonedDateTime lastExecution) {
        this.identifier = identifier;
        this.cron = cron;
        this.scheduledZone = scheduledZone;
        this.lastExecution = lastExecution;
        this.enabled = enabled;
        this.context = context;
    }

    public static Schedule newSchedule(
            String identifier,
            String context,
            String cron,
            ZoneId scheduledZone) {
        return newSchedule(identifier, context, cron, scheduledZone, ZonedDateTime.now().withZoneSameInstant(scheduledZone));
    }

    public static Schedule newSchedule(
            String identifier,
            String context,
            String cron,
            ZoneId scheduledZone,
            Clock clock) {
        return newSchedule(identifier, context, cron, scheduledZone, clock.instant().atZone(scheduledZone));
    }

    public static Schedule newSchedule(String identifier, String context, String cron, ZoneId scheduledZone, ZonedDateTime lastExecution) {
        CronParser cp = new CronParser(cronDefinition);
        try {
            return new Schedule(identifier, cp.parse(cron), scheduledZone, true, context, lastExecution);
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not parse Cron Expression '" + cron + "' : " + e.getMessage());
        }
    }

    public void setLastExecution(ZonedDateTime executionTimestamp) {
        synchronized (this) {
            this.lastExecution = executionTimestamp;
        }
    }

    public Optional<ZonedDateTime> getNextExecution() {
        if (!enabled) {
            return Optional.empty();
        }
        synchronized (this) {
            ExecutionTime executionTime = ExecutionTime.forCron(cron);
            Optional<ZonedDateTime> nextExecution = executionTime.nextExecution(lastExecution);
            return nextExecution;
        }
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }


    @JsonProperty("cron")
    public String getCronString() {
        return this.cron.asString();
    }
}
