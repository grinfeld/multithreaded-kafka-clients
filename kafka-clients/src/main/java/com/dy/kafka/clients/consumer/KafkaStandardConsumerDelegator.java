package com.dy.kafka.clients.consumer;

import com.dy.kafka.clients.KafkaProperties;
import com.dy.kafka.clients.consumer.model.*;
import com.dy.kafka.clients.serializers.KeyValueDeserializer;
import com.dy.metrics.guice.MetricModule;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
public class KafkaStandardConsumerDelegator<K, T> implements KafkaConsumerDelegator<K, T> {

    private static LifecycleConsumerElements DEF_LIFECYCLE_ELEMENTS = LifecycleConsumerElements.builder().build();

    @Getter final private String uid;
    private Properties consumerProperties;
    private String consumerTopic;
    private long consumerTimeout;
    private AtomicBoolean running = new AtomicBoolean(false);
    private Consumer<K, T> kafkaConsumer;
    private Deserializer<K> keyDeserializer;
    private Deserializer<T> valueDeserializer;
    private OnConsumerStop onStop;
    private ConsumerRebalanceListener rebalanceListener;
    private FlowErrorHandler flowErrorHandler;
    private boolean enableAutoCommit = true;
    private Commander commander;

    private Map<Integer, AtomicBoolean> pausePartitions = new ConcurrentHashMap<>();
    private Map<Integer, AtomicBoolean> resumePartitions = new ConcurrentHashMap<>();

    // putting shutDown executor as instance variable and initiating it during startConsume, ensures that it will be called only once during close/stopConsume process
    private ExecutorService shutDown;

    public KafkaStandardConsumerDelegator(String uid, KafkaProperties properties, KeyValueDeserializer<K, T> keyValueDeserializer,
                                          LifecycleConsumerElements lifecycleConsumerElements) {
        // if it's single consumer - it could be null or empty string
        this.uid = Optional.ofNullable(uid).orElse("");
        this.consumerProperties = new Properties();
        if (properties.getProperties() != null)
            this.consumerProperties.putAll(properties.getProperties());

        String groupId = (String)this.consumerProperties.get(ConsumerConfig.GROUP_ID_CONFIG);
        if (groupId == null || groupId.trim().isEmpty()) {
            throw new IllegalArgumentException("group.id shouldn't be empty");
        }

        // the default (latest version) ENABLE_AUTO_COMMIT_CONFIG is true, we want to ensure that this value set - not depend on kafka-clients version
        // later we shouldn't commit manually in case of enableAutoCommit property set false (thw default is true)
        this.enableAutoCommit = !"false".equalsIgnoreCase((String)this.consumerProperties.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        this.consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(enableAutoCommit));

        this.consumerTopic = properties.getTopic();
        lifecycleConsumerElements = Optional.ofNullable(lifecycleConsumerElements).orElse(DEF_LIFECYCLE_ELEMENTS);
        this.consumerTimeout = properties.getTimeout() > 0 ? properties.getTimeout() : Integer.MAX_VALUE;
        this.keyDeserializer = keyValueDeserializer.keyDeSerializer();
        this.onStop = Optional.ofNullable(lifecycleConsumerElements.onStop()).orElse(LifecycleConsumerElements.ON_CONSUMER_STOP_DEF);
        this.valueDeserializer = keyValueDeserializer.valueDeSerializer();
        this.rebalanceListener = Optional.ofNullable(lifecycleConsumerElements.rebalanceListener()).orElse(LifecycleConsumerElements.DEF_NOOP_REBALANCE_LISTENER);
        this.flowErrorHandler = Optional.ofNullable(lifecycleConsumerElements.flowErrorHandler()).orElse(new FlowErrorHandler() {});
        this.commander = new Commander() {
            @Override
            public void resume(MetaData metaData) {
                if (metaData instanceof KafkaMetaData)
                    KafkaStandardConsumerDelegator.this.resume(((KafkaMetaData)metaData).getPartition());
            }

            @Override
            public void pause(MetaData metaData, Duration duration) {
                if (metaData instanceof KafkaMetaData) {
                    scheduleResume((KafkaMetaData) metaData, duration);
                    KafkaStandardConsumerDelegator.this.pause(((KafkaMetaData) metaData).getPartition());
                }

            }

            private void scheduleResume(KafkaMetaData metaData, Duration duration) {
                if (duration != null) {
                    new Timer().schedule(new TimerTask() {
                        @Override
                        public void run() {
                            KafkaStandardConsumerDelegator.this.resume(metaData.getPartition());
                        }
                    }, duration.toMillis());
                }
            }
        };
    }

    @Override
    public void startConsume(Worker<K, T> consumer) {
        // if it's already running, let's don't run it again
        boolean currentRunning = running.getAndSet(true);
        if (currentRunning)
            return;
        initConsumer();
        shutDown = Executors.newSingleThreadExecutor();
        //kafkaConsumer.subscribe(new ArrayList<>(Collections.singletonList(consumerTopic)), rebalanceListener);
        MetricModule.getMetricStore().increaseCounter("subscribed." + consumerTopic,
            () -> kafkaConsumer.subscribe(new ArrayList<>(Collections.singletonList(consumerTopic)), rebalanceListener)
        );
        initPauseResumeState();
        runPoll(consumer);
    }

    private void initPauseResumeState() {
        pausePartitions = kafkaConsumer.assignment().stream().collect(Collectors.toMap(TopicPartition::partition,
                t -> new AtomicBoolean(false), (k1, k2) -> k1, ConcurrentHashMap::new));
        resumePartitions = kafkaConsumer.assignment().stream().collect(Collectors.toMap(TopicPartition::partition,
                t -> new AtomicBoolean(false), (k1, k2) -> k1, ConcurrentHashMap::new));
    }

    private void runPoll(Worker<K, T> consumer) {
        try {
            while (running.get()) {
                try {
                    getRecords(kafkaConsumer).forEach(record -> processRecord(consumer, kafkaConsumer, record));
                } catch (Exception e) {
                    handleRecordException(e);
                    // in older kafka-clients versions we could not catch Serialization errors
                    flowErrorHandler.doOnError(e);
                }
            }
        } finally {
            running.set(false);
            silentClose();
        }
    }

    void initConsumer() {
        kafkaConsumer = new KafkaConsumer<>(consumerProperties, keyDeserializer, valueDeserializer);
    }

    private void processRecord(Worker<K, T> consumer, Consumer<K, T> kafkaConsumer, ConsumerRecord<K, T> record) {
        try {
            // todo: put some metadata (partition, offset in threadcontext ???
            T value = record.value();
            K key = record.key();
            if (value == null) {
                log.warn("Failed to deserialize object from Kafka with key '{}'", key);
                MetricModule.getMetricStore().increaseCounter("consumer_deserialization.error");
            } else {
                // todo: if I want to propagate metadata -> headers and so on
                doConsumerAction(consumer, value, key, wrapMetaData(record));
            }
            if (!enableAutoCommit)
                commitOffset(record, kafkaConsumer);
        } catch (Exception e) {
            if (running.get() && !enableAutoCommit)
                kafkaConsumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
            throw e;
        }
    }

    private MetaData wrapMetaData(ConsumerRecord<K, T> record) {
        List<Header> headers = StreamSupport.stream(record.headers().spliterator(), false).map(HeaderForKafkaHeader::new).collect(Collectors.toList());
        return new KafkaMetaData(headers, record.partition(), record.topic());
    }

    private void doConsumerAction(Worker<K, T> consumer, T value, K key, MetaData metadata) {
        consumer.accept(key, value, metadata, commander);
    }

    private void handleRecordException(Exception e) {
        log.error("We won't commit record, so we should get record again", e);
        MetricModule.getMetricStore().increaseCounter("consumer." + e.getClass().getSimpleName());
    }

    private ConsumerRecords<K, T> getRecords(Consumer<K, T> kafkaConsumer) {
        pausePartitions(kafkaConsumer);
        resumePartitions(kafkaConsumer);
        return kafkaConsumer.poll(Duration.ofMillis(consumerTimeout));
    }

    void resumePartitions(Consumer<?, ?> kafkaConsumer) {
        List<TopicPartition> partitions = findPartitionsToWorkOn(kafkaConsumer.assignment(), resumePartitions);
        if (!partitions.isEmpty())
            kafkaConsumer.resume(partitions);
    }

    void pausePartitions(Consumer<?, ?> kafkaConsumer) {
        List<TopicPartition> partitions = findPartitionsToWorkOn(kafkaConsumer.assignment(), pausePartitions);
        if (!partitions.isEmpty())
            kafkaConsumer.pause(partitions);
    }

    private List<TopicPartition> findPartitionsToWorkOn(Set<TopicPartition> partitions, Map<Integer, AtomicBoolean> stateMap) {
        return partitions.stream().filter(tp -> stateMap.containsKey(tp.partition()))
                // .getAndSet(false) -> functional with side affect. Do we have another option?
                .filter(tp -> stateMap.get(tp.partition()).getAndSet(false))
                .collect(Collectors.toList());
    }

    void commitOffset(ConsumerRecord<K, T> record, Consumer<?, ?> consumer) {
        Map<TopicPartition, OffsetAndMetadata> metadata = getCommitMetadata(record);
        try {
            commitSync(consumer, metadata);
        } catch (Exception e) {
            if (running.get() && !enableAutoCommit) {
                commitOffsetAsync(consumer, metadata);
            } else
                throw e;
        }
    }

    void commitSync(Consumer<?, ?> consumer, Map<TopicPartition, OffsetAndMetadata> metadata) {
        // todo: duration should be from properties ???
        consumer.commitSync(metadata, Duration.ofMillis(500));
    }

    void commitOffsetAsync(Consumer<?, ?> consumer, Map<TopicPartition, OffsetAndMetadata> metadata) {
        consumer.commitAsync(metadata, (offsets, exception) ->
                Optional.ofNullable(exception)
                        .ifPresent(ex -> log.error("Failed to commit, both async and sync with {}", ex.getMessage()))
        );
    }

    private Map<TopicPartition, OffsetAndMetadata> getCommitMetadata(ConsumerRecord<K, T> record) {
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset() + 1);
        Map<TopicPartition, OffsetAndMetadata> recordData = new HashMap<>();
        recordData.put(topicPartition, offsetAndMetadata);
        return recordData;
    }

    private void silentClose() {
        try {
            if (kafkaConsumer != null) {
                // let's give him 20 seconds to finish else kill
                kafkaConsumer.close(Duration.ofSeconds(20));
            }
        } catch (Exception e) {
            log.trace("Failed to close consumer: " + e.getMessage());
        }
    }

    public void resume(int partition) {
        if (!isRunning()) {
            return;
        }
        AtomicBoolean pausePartition = pausePartitions.get(partition);
        AtomicBoolean resumePartition = resumePartitions.get(partition);
        if (pausePartition != null && resumePartition != null) {
            resumePartition.set(true);
            pausePartition.set(false);
            MetricModule.getMetricStore().increaseCounter("consumer.resumed." + partition);
        }
    }

    public void pause(int partition) {
        if (!isRunning()) {
            return;
        }
        AtomicBoolean pausePartition = pausePartitions.get(partition);
        AtomicBoolean resumePartition = resumePartitions.get(partition);
        if (pausePartition != null && resumePartition != null) {
            // todo: order????
            pausePartition.set(true);
            resumePartition.set(false);
            MetricModule.getMetricStore().increaseCounter("consumer.paused." + partition);
        }
    }

    private boolean isRunning() {
        return running.get() && kafkaConsumer != null;
    }

    public void stopConsume() {
        running.set(false);
        silentClose();
        try {
            shutDown.submit(onStop::onStop);
            shutDown.shutdown();
            // todo: should be replaced with some configurable (and much less) value
            shutDown.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.warn("Exception during stopping process record " + e.getMessage());
        } finally {
            MetricModule.getMetricStore().increaseCounter("consumer.stopped." + this.uid);
        }
        // we still can be stuck inside processRecord method if no onStopConsumer implemented or onStopConsumer takes a lot of time, since it could take a lot of time
    }

    @AllArgsConstructor
    @Data
    @Value
    private static class HeaderForKafkaHeader implements Header {

        private org.apache.kafka.common.header.Header header;

        @Override
        public String key() {
            return header.key();
        }

        @Override
        public <T> T value(CustomDeserializer<byte[], T> deserializer) {
            return header.value() == null ? null : deserializer.deserialize(header.value());
        }
    }

    @AllArgsConstructor
    @Data
    @Value
    private static class KafkaMetaData implements MetaData {
        private Iterable<Header> headers;
        private int partition;
        private String topic;
    }

}
