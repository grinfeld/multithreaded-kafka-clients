package com.mikerusoft.kafka.clients.consumer.model;

@FunctionalInterface
public interface ExternalWorker {
    void work(MetaData metaData);
}
