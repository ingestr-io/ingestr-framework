package io.ingestr.framework.service.queue.model;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class QueuedResponse {
    private String id;
    private QueueItem queueItem;
    private String offset;
}
