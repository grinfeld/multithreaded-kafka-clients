package com.mikerusoft.kafka.clients.consumer;

import com.mikerusoft.kafka.clients.metrics.utils.Utils;

public interface DeserializationErrorHandler {
    default void onError(Exception e) {
        Utils.rethrowRuntime(e);
    }
}
