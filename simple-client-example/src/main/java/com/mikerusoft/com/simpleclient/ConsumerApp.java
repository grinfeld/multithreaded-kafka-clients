package com.mikerusoft.com.simpleclient;

import com.mikerusoft.kafka.clients.KafkaProperties;
import com.mikerusoft.kafka.clients.consumer.KafkaConsumerManager;
import com.mikerusoft.kafka.clients.serializers.KeyValueDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class ConsumerApp {

    public static void main(String...args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test1" + System.currentTimeMillis());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // latest, earliest
        KafkaProperties kafkaProps = KafkaProperties.builder()
                .topic("_schemas")
                .properties(props)
            .build();

        KafkaConsumerManager<String, String> consumer = new KafkaConsumerManager<>(1, kafkaProps, keyValueDeserializer, null);
        consumer.startConsume((key, value, metaData, commander) -> {
            System.out.println(key + " ---- " + value);
        });
    }

    private static final KeyValueDeserializer<String, String> keyValueDeserializer = new KeyValueDeserializer<String, String>() {
        @Override
        public Deserializer<String> keyDeSerializer() {
            return new StringDeserializer();
        }

        @Override
        public Deserializer<String> valueDeSerializer() {
            return new StringDeserializer();
        }
    };

}
