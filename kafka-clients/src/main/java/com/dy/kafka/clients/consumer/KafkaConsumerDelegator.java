package com.dy.kafka.clients.consumer;

import java.util.function.BiConsumer;

public interface KafkaConsumerDelegator<K, T> {

    void startConsume(BiConsumer<K, T> consumer);
    void stopConsume();

}
