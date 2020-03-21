package com.mikerusoft.kafka.clients.consumer;

@FunctionalInterface
public interface OnConsumerStop {
    void onStop();
}
