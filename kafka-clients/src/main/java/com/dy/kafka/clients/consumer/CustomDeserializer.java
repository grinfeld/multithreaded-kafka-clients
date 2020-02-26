package com.dy.kafka.clients.consumer;

@FunctionalInterface
public interface CustomDeserializer<I, O> {
    O deserialize(I input);
}
