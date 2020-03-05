package com.dy.kafka.clients.consumer.model;

@FunctionalInterface
public interface ExternalWorker {
    void work(MetaData metaData);
}
