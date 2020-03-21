package com.mikerusoft.kafka.clients.metrics.stores.micrometer;

import com.mikerusoft.kafka.clients.metrics.stores.AbstractMetricStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Slf4j
public class MicrometerMetricStore extends AbstractMetricStore {


    private MeterRegistry meterRegistry;

    public MicrometerMetricStore(MeterRegistry meterRegistry) {
        this.meterRegistry = Optional.ofNullable(meterRegistry).orElseGet(SimpleMeterRegistry::new);
    }

    @Override
    public void increaseCounter(String name, Boolean error) {
        try {
            meterRegistry.counter(buildName(name, error)).increment();
        } catch (Exception e) {
            log.error("", e);
        }
    }

    @Override
    public void recordTime(String name, Boolean error, Long start) {
        if (start == null)
            return;

        try {
            meterRegistry.timer(buildName(name, error)).record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.error("", e);
        }
    }

    @Override
    public void gauge(String name, Supplier<Double> supplier, Boolean error) {
        try {
            meterRegistry.gauge(buildName(name, error), supplier.get());
        } catch (Exception e) {
            log.error("", e);
        }
    }
}
