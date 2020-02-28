package com.dy.kafka.clients.consumer.model;

public interface MetaData {
    Iterable<Header> getHeaders();
}
