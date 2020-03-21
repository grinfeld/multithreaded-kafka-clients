package com.mikerusoft.kafka.clients.metrics;

import com.mikerusoft.kafka.clients.metrics.functions.CallExecution;
import com.mikerusoft.kafka.clients.metrics.functions.Execution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.function.Supplier;

public interface MetricStore {

    default void increaseCounter(String name, Execution runnable) {
        runnable.execute();
    }
    default void increaseCounter(String name, Boolean error) {
        // do nothing
    }
    default void increaseCounter(String name) {
        // do nothing
    }
    default <V> V increaseCounter(String name, CallExecution<V> callable) {
        return callable.execute();
    }
    default void recordTime(String name, Execution runnable) {
        runnable.execute();
    }
    default void recordTime(String name, Boolean error, Long start) {
        // do nothing
    }
    default <V> V recordTime(String name, CallExecution<V> callable) {
        return callable.execute();
    }

    default void gauge(String name, Supplier<Double> supplier, Boolean error) {
        // do nothing
    }

    default void removeGauge(String name, Boolean error) {
        // do nothing
    }


    Logger log = LoggerFactory.getLogger(MetricStore.class);

    static String getDashesHostname() {
        String hostname = "localhost";
        try {
            InetAddress ip = InetAddress.getLocalHost();
            hostname = ip.getHostName();
        } catch (Exception e) {
            log.warn("Failed to find host, using localhost", e);
        }
        return hostname.replaceAll("\\.", "-");
    }
}
