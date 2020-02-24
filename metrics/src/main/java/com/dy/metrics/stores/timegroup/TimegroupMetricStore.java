package com.dy.metrics.stores.timegroup;

import com.dy.metrics.stores.AbstractMetricStore;
import com.timgroup.statsd.StatsDClient;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Supplier;

@Slf4j
public class TimegroupMetricStore extends AbstractMetricStore {

    private StatsDClient client;

    public TimegroupMetricStore(StatsDClient client) {
        this.client = client;
    }

    @Override
    public void increaseCounter(String name, Boolean error) {
        if (clientDoesNotExist())
            return;

        try {
            client.count(buildName(name, error), 1);
        } catch (Exception e) {
            log.error("", e);
        }
    }


    @Override
    public void recordTime(String name, Boolean error, Long start) {
        if (clientDoesNotExist())
            return;
        if (start == null)
            return;

        try {
            client.recordExecutionTime(buildName(name, error), System.currentTimeMillis() - start);
        } catch (Exception e) {
            log.error("", e);
        }
    }

    @Override
    public void gauge(String name, Supplier<Double> supplier, Boolean error) {
        if (clientDoesNotExist())
            return;
        try {
            client.gauge(buildName(name, error), supplier.get());
        } catch (Exception e) {
            log.error("", e);
        }
    }

    private boolean clientDoesNotExist() {
        if (client != null)
            return false;

        log.warn("No client  - check configuration");
        return true;
    }
}
