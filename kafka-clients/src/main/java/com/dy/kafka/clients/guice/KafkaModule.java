package com.dy.kafka.clients.guice;

import com.dy.distributed.mario.utils.ReflectionUtils;
import com.dy.kafka.clients.KafkaMetricReporter;
import com.dy.kafka.clients.KafkaProperties;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public class KafkaModule {

    private static final Long DEF_MAX_POLL_INTERVAL_MS = TimeUnit.MINUTES.toMillis(30);
    private static final int DEF_MAX_POLL_RECORDS = 50;

    private static final String PREFIX = "offline.kafka.";
    private String prefix;
    private Config config;

    public KafkaModule(Config config) {
        this(config, PREFIX);
    }

    private KafkaModule(Config config, String prefix) {
        this.config = config;
        this.prefix = Optional.ofNullable(prefix).orElse("");
    }

    public static <T> T getOrDefault(Config config, String name, T def) {
        if (config == null) {
            return def;
        }
        if (config.hasPathOrNull(name)) {
            Object val = config.getAnyRef(name);
            if (def != null) {
                if (val instanceof String) {
                    val = ReflectionUtils.parseStringValue((String)val, def.getClass());
                }
                if (def instanceof Long) {
                    val = ((Number) val).longValue();
                }
            }
            return (T) val;
        }
        return def;
    }

    public KafkaProperties consumerProperties() {
        KafkaProperties properties = KafkaProperties.builder()
                .timeout(getOrDefault(this.prefix + "consumer.poll.timeout.ms", 0))
                .properties(consumerDetails())
                .topic(getOrDefault(prefix + "consumer.topic", null))
            .build();
        log.info("Starting consumer with following properties from configuration {}", properties);
        return properties;
    }

    private Properties consumerDetails() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(prefix + "bootstrap.servers"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString(prefix + "consumer.properties.group.id"));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, getOrDefault(prefix + "consumer.properties.enable.auto.commit",true));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, getOrDefault(prefix + "consumer.properties.auto.offset.reset", "earliest")); // latest, earliest
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, getOrDefault(prefix + "consumer.properties.max.poll.records", DEF_MAX_POLL_RECORDS));
        int maxPollInterval = getOrDefault(prefix + "consumer.properties.max.poll.interval.ms", DEF_MAX_POLL_INTERVAL_MS.intValue());
        if (maxPollInterval > 0)
            props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval);
        Integer requestInterval = config.hasPathOrNull(prefix + "consumer.properties.request.timeout.ms") ? config.getInt(prefix + "consumer.properties.request.timeout.ms") : null;
        // request.timeout.ms should be always greater (or at least equals) then max.poll.interval.ms
        if (maxPollInterval > 0 && requestInterval != null && maxPollInterval < requestInterval) {
            requestInterval = maxPollInterval;
        }
        if (requestInterval != null)
            props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestInterval);
        props.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, KafkaMetricReporter.class.getName());
        props.put("METRIC_PREFIX", "consumer_");
        return props;
    }

    public KafkaProperties producerProperties() {
        KafkaProperties properties = KafkaProperties.builder()
                .properties(producerDetails())
                .topic(getOrDefault(prefix + "producer.topic", null))
            .build();
        log.info("Starting producer with following properties from configuration {}", properties);
        return properties;
    }

    private Properties producerDetails() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(prefix + "bootstrap.servers"));
        props.put(ProducerConfig.ACKS_CONFIG, getOrDefault(prefix + "producer.properties.acks", "all"));
        props.put(ProducerConfig.RETRIES_CONFIG, getOrDefault(prefix + "producer.properties.retries", 1));
        props.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, KafkaMetricReporter.class.getName());
        props.put(KafkaMetricReporter.METRIC_PREFIX_CONFIG, "producer_");
        return props;
    }

    private <T> T getOrDefault(String name, T def) {
        return getOrDefault(config, name, def);
    }
}
