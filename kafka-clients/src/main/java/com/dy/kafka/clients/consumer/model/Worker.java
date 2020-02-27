package com.dy.kafka.clients.consumer.model;

public interface Worker<T, U> {
    void accept(T t, U u, Iterable<MetaData> metaData, Commander commander);
}
