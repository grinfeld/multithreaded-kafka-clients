package com.dy.kafka.clients.consumer;

import com.dy.metrics.utils.Utils;

public interface DeserializationErrorHandler {
    default void onError(Exception e) {
        Utils.rethrowRuntime(e);
    }
}
