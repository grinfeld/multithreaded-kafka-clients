package com.dy.kafka.clients.consumer.model;

import java.time.Duration;

public interface Commander {
    default void pause() {
        pause(null);
    }
    void resume();
    void pause(Duration duration);
}
