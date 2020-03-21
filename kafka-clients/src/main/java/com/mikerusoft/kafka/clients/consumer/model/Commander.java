package com.mikerusoft.kafka.clients.consumer.model;

import java.time.Duration;

public interface Commander {
    default void pause(MetaData metaData) {
        pause(metaData, null);
    }
    void resume(MetaData metaData);
    void pause(MetaData metaData, Duration duration);
}
