package io.ingestr.framework.service.producer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IngestrProducerInMemoryImpl implements IngestrProducer {

    @Override
    public void send(IngestrProducerRecord record) {
        log.info("Saving Record - {}", record);
    }
}
