package com.dy.metrics.functions;

@FunctionalInterface
public interface CallExecution<V> {
    V execute();
}
