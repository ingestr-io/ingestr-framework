package io.ingestr.framework.kafka;


import io.ingestr.framework.kafka.builders.ConsumerBuilder;
import io.ingestr.framework.kafka.builders.ProducerBuilder;

public class Kafka {
    /**
     * Sets the default SchemaRegistryURL ;
     */
    public static String schemaRegistryUrl;
    /**
     * Sets the default bootstrap Kafka Servers;
     */
    public static String bootstrapServers;


    private Kafka() {
    }

    public static ConsumerBuilder consumer() {
        return new ConsumerBuilder();
    }

    public static ProducerBuilder producer() {
        return new ProducerBuilder();
    }


}
