package com.dy.kafka.clients.consumer.model;

import com.dy.kafka.clients.consumer.CustomDeserializer;

public interface MetaData {
    String key();
    default byte[] value() {
        return value(input -> input);
    }
    <T> T value(CustomDeserializer<byte[], T> deserializer);
}
