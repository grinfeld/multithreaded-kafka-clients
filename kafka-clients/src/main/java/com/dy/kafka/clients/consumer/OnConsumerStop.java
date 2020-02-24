package com.dy.kafka.clients.consumer;

@FunctionalInterface
public interface OnConsumerStop {
    void onStop();
}
