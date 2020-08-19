package com.mikerusoft.kafka.clients;

import com.mikerusoft.kafka.clients.metrics.MetricStore;
import com.mikerusoft.kafka.clients.metrics.guice.MetricFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Slf4j
public class KafkaMetricReporter implements MetricsReporter {

    public static final String METRIC_PREFIX_CONFIG = "METRIC_PREFIX";
    private static final Pattern sanitizeNamePattern = Pattern.compile("[^a-zA-Z0-9_]");

    private final Object lock = new Object();
    // until MetricStore is Thread-safe we don't have problem
    private MetricStore metricStore;
    private String prefix = ""; // consumer or producer

    @Override
    public void init(List<KafkaMetric> metrics) {
        synchronized (lock) {
            if (metricStore == null)
                metricStore = MetricFactory.getMetricStore();
        }
        if (metrics != null)
            metrics.forEach(this::metricChange);
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        if (metricStore == null)
            return;
        String metricName = "";
        try {
            metricName = metric.metricName().name();
            // using old KafkaMetric.value() method, since we don't want to deal with metric logic
            metricStore.gauge(this.prefix + sanitizeName(metric.metricName().name()), metric::value, null);
        } catch (Exception e) {
            log.warn("Failed to report metric {}", metricName);
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        if (metricStore == null)
            return;
        String metricName = "";
        try {
            metricName = metric.metricName().name();
            metricStore.removeGauge(this.prefix + sanitizeName(metric.metricName().name()), null);
        } catch (Exception e) {
            log.warn("Failed to report metric {}", metricName);
        }
    }

    @Override
    public void close() {
        // default implementation required by 3rd party we use
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.prefix = configs.containsKey(METRIC_PREFIX_CONFIG) ? String.valueOf(configs.get(METRIC_PREFIX_CONFIG)) : "";
    }

    private static String sanitizeName(String name) {
        return sanitizeNamePattern.matcher(name).replaceAll("_");
    }
}
