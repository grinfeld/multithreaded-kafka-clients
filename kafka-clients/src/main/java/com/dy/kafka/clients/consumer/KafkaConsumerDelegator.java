package com.dy.kafka.clients.consumer;

import com.dy.kafka.clients.consumer.model.Worker;

import java.io.Closeable;

public interface KafkaConsumerDelegator<K, T> extends Closeable {

    void startConsume(Worker<K, T> consumer);

}
