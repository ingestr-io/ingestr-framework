package io.ingestr.framework.service.producer;

public interface IngestrProducer {
    void send(IngestrProducerRecord record);
}
