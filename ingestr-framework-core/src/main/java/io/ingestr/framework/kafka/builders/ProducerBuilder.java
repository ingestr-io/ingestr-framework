package io.ingestr.framework.kafka.builders;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerBuilder {
    private String bootstrapServers;
    private String clientId;
    private String schemaRegistryUrl;


    public ProducerBuilder bootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    public ProducerBuilder schemaRegistryUrl(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        return this;
    }

    public ProducerBuilder clientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public <T> Producer<String, T> build() {

        String bs = this.bootstrapServers;

        Validate.notBlank(bs, "BootsrapServers must be set");
        Validate.notBlank(clientId, "ClientId cannot be null");

        log.info("Creating Kafka Producer. broker={} client={}",
                bs,
                clientId);

        final Properties props = new Properties();


        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bs);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, T>(props);
    }

}
