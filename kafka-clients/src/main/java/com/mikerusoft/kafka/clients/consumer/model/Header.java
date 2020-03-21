package com.mikerusoft.kafka.clients.consumer.model;

import com.mikerusoft.kafka.clients.consumer.CustomDeserializer;

import java.nio.charset.StandardCharsets;

public interface Header {
    String key();
    default byte[] value() {
        return value(input -> input);
    }
    <T> T value(CustomDeserializer<byte[], T> deserializer);
    default String stringValue() {
        return new String(value(), StandardCharsets.UTF_8);
    }
}
