package com.mikerusoft.kafka.clients.metrics.guice;

import com.mikerusoft.kafka.clients.metrics.MetricStore;

public class MetricFactory {
    static MetricStore instance = new MetricStore() {};

    // we need this for use-case when we want to use metrics in class which hasn't been
    // bound via Guice for example, classes that created with "new"
    //(note: this module SHOULD exists in application in any case and registered in Guice, since it initialized during Guice configure state)
    public static MetricStore getMetricStore() {
        return instance;
    }
}
