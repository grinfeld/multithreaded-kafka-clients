package com.dy.kafka.clients.consumer;

public interface FlowErrorHandler {

    default void doOnError(Throwable e) {
        // do nothing
    }
}
