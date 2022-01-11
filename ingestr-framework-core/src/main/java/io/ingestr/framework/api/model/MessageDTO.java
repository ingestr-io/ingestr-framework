package io.ingestr.framework.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MessageDTO {
    private String status;
    private String message;

    public static MessageDTO ok() {
        return new MessageDTO("ok", "Success");
    }
}
