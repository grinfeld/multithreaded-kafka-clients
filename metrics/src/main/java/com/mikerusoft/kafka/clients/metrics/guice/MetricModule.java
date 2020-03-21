package com.mikerusoft.kafka.clients.metrics.guice;

import com.mikerusoft.kafka.clients.metrics.annotations.AddMetric;
import com.mikerusoft.kafka.clients.metrics.MetricStore;
import com.mikerusoft.kafka.clients.metrics.stores.timegroup.TimegroupMetricStore;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Named;
import com.timgroup.statsd.NoOpStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;

import static com.google.inject.matcher.Matchers.any;

@Slf4j
public class MetricModule extends AbstractModule {

    public static final String METER_STORE = "METER_STORE";
    private static final String DEF_STATSD_HOST = "localhost";
    static final Integer DEF_STATSD_PORT = 8125;

    private Config config;
    private MetricStore metrics;

    public MetricModule(Config config) {
        this.config = config;
    }

    @Provides
    @Singleton
    @Named(METER_STORE)
    public MetricStore getMeterRegistry() {
        return metrics;
    }

    @Override
    protected void configure() {
        if (getOrDefault("monitoring.statsd.enabled", false)) {
            String machineName = MetricStore.getDashesHostname();
            log.info("Starting with metrics on machine '{}' with prefix '{}', host '{}' and port '{}'",
                machineName, getOrDefault("monitoring.prefix", ""),
                getOrDefault("monitoring.statsd.host", DEF_STATSD_HOST),
                getOrDefault("monitoring.statsd.port", DEF_STATSD_PORT)
            );
            instance = metrics = new TimegroupMetricStore(
                new NonBlockingStatsDClient(
                    "offline-" + machineName + "." + getOrDefault("monitoring.prefix", ""),
                    getOrDefault("monitoring.statsd.host", DEF_STATSD_HOST),
                    getOrDefault("monitoring.statsd.port", DEF_STATSD_PORT)
                )
            );
        } else {
            instance = metrics = new TimegroupMetricStore(new NoOpStatsDClient());
        }
        bindInterceptor(any(), Matchers.annotatedWith(AddMetric.class), new GuiceMetricMethodIntercepter(metrics));
    }

    private <T> T getOrDefault(String name, T def) {
        if (config.hasPathOrNull(name)) {
            return (T) config.getAnyRef(name);
        }
        return def;
    }

    // we need this for use-case when we want to use metrics in class which hasn't been
    // bound via Guice for example, classes that created with "new"
    //(note: this module SHOULD exists in application in any case and registered in Guice, since it initialized during Guice configure state)
    public static MetricStore getMetricStore() {
        return instance;
    }
    private static MetricStore instance = new MetricStore() {};

}
