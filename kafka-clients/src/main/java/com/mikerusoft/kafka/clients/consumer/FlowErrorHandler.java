package com.mikerusoft.kafka.clients.consumer;

public interface FlowErrorHandler {

    // it could application exception and Kafka exception, for example KafkaException (and specifically KafkaException subset - SerializationException)
    default void doOnError(Throwable e) {
        // do nothing
    }
}
