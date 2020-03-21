package com.mikerusoft.kafka.clients.consumer.model;

public interface Worker<T, U> {
    void accept(T t, U u, MetaData metaData, Commander commander);
}
