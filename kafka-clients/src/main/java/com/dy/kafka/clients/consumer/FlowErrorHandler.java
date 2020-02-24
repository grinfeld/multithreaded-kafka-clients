package com.dy.kafka.clients.consumer;

public interface FlowErrorHandler {

    default FlowErrorHandler doOnError(Exception e) {
        return this;
    }

    default boolean reThrowException() {
        return false;
    }
}
