package io.ingestr.framework.service.gateway.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ingestr.framework.kafka.ObjectMapperFactory;
import io.ingestr.framework.service.gateway.commands.Command;
import io.ingestr.framework.service.gateway.model.CommandPayload;

import java.time.ZonedDateTime;

public class CommandSerializer {
    private ObjectMapper objectMapper = ObjectMapperFactory.kafkaMessageObjectMapper();


    public String serialize(
            String commandId,
            ZonedDateTime createdAt,
            Command command) throws JsonProcessingException {
        assert createdAt != null;
        assert command != null;

        CommandPayload payload = CommandPayload.builder()
                .identifier(commandId)
                .createdAt(createdAt)
                .className(command.getClass().getName())
                .payload(objectMapper.valueToTree(command))
                .build();

        return objectMapper.writeValueAsString(payload);
    }

    public <T extends Command> T deserialize(String payload) throws JsonProcessingException, ClassNotFoundException {
        assert payload != null;
        assert !payload.isBlank();

        CommandPayload cp = objectMapper.readValue(payload, CommandPayload.class);
        Object res = objectMapper.treeToValue(cp.getPayload(), Class.forName(cp.getClassName()));
        return (T) res;
    }
}
