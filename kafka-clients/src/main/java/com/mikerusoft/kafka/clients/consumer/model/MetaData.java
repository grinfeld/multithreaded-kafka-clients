package com.mikerusoft.kafka.clients.consumer.model;

public interface MetaData {
    Iterable<Header> getHeaders();
}
