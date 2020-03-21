package com.mikerusoft.kafka.clients.metrics.utils;

public class Utils {
    // this method with return value to make it's easy to use
    public static <T> T rethrowRuntime(Throwable t) {
        if (t instanceof Error)
            throw (Error)t;
        else if (t instanceof RuntimeException)
            throw (RuntimeException)t;

        throw new RuntimeException(t);
    }
}
