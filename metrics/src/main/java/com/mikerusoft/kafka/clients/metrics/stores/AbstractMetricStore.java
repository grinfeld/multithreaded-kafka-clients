package com.mikerusoft.kafka.clients.metrics.stores;

import com.mikerusoft.kafka.clients.metrics.functions.CallExecution;
import com.mikerusoft.kafka.clients.metrics.utils.Utils;
import com.mikerusoft.kafka.clients.metrics.MetricStore;
import com.mikerusoft.kafka.clients.metrics.functions.Execution;

public class AbstractMetricStore implements MetricStore {

    public static final String SUCCESS_PREFIX = "succ";
    public static final String ERROR_PREFIX = "err";

    @Override
    public void increaseCounter(String name) {
        increaseCounter(name, (Boolean)null);
    }

    @Override
    public void increaseCounter(String name, Execution runnable) {
        increaseCounter(name, () -> {
            runnable.execute();
            return null;
        });
    }

    @Override
    public <V> V increaseCounter(String name, CallExecution<V> callable) {
        try {
            V call = callable.execute();
            increaseCounter(name, false);
            return call;
        } catch (Throwable t) {
            increaseCounter(name, true);
            return Utils.rethrowRuntime(t);
        }
    }

    @Override
    public void recordTime(String name, Execution runnable) {
        recordTime(name, () -> {
            runnable.execute();
            return null;
        });
    }

    @Override
    public <V> V recordTime(String name, CallExecution<V> callable) {
        long start = System.currentTimeMillis();
        try {
            V call = callable.execute();
            recordTime(name, false, start);
            return call;
        } catch (Throwable t) {
            recordTime(name, true, start);
            return Utils.rethrowRuntime(t);
        }
    }

    protected static String buildName(String name, Boolean error) {
        StringBuilder sb = new StringBuilder(name);
        if (error != null) {
            if (error) sb.append(".").append(ERROR_PREFIX);
            else sb.append(".").append(SUCCESS_PREFIX);
        }
        return sb.toString();
    }
}
