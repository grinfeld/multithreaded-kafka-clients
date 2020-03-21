package com.mikerusoft.kafka.clients.metrics.functions;

@FunctionalInterface
public interface CallExecution<V> {
    V execute();
}
