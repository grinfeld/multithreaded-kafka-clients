package com.dy.kafka.clients.consumer;

import com.dy.kafka.clients.consumer.model.Worker;

public interface KafkaConsumerDelegator<K, T> {

    void startConsume(Worker<K, T> consumer);
    void stopConsume();

}
