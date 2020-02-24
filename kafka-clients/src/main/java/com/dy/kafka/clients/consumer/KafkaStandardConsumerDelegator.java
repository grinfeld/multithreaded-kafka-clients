package com.dy.kafka.clients.consumer;

import com.dy.kafka.clients.KafkaProperties;
import com.dy.kafka.clients.serializers.KeyValueDeserializer;
import com.dy.metrics.guice.MetricModule;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

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

    // putting shutDown executor as instance variable and initiating it during startConsume, ensures that it will be called only once during close/stopConsume process
    private ExecutorService shutDown;

    public KafkaStandardConsumerDelegator(String uid, KafkaProperties properties, KeyValueDeserializer<K, T> keyValueDeserializer,
                                          LifecycleConsumerElements lifecycleConsumerElements) {
        // if it's single consumer - it could be null or empty string
        this.uid = Optional.ofNullable(uid).orElse("");
        this.consumerProperties = properties.getProperties();
        this.consumerTopic = properties.getTopic();
        lifecycleConsumerElements = Optional.ofNullable(lifecycleConsumerElements).orElse(DEF_LIFECYCLE_ELEMENTS);
        this.consumerTimeout = properties.getTimeout() > 0 ? properties.getTimeout() : Integer.MAX_VALUE;
        this.keyDeserializer = keyValueDeserializer.keyDeSerializer();
        this.onStop = Optional.ofNullable(lifecycleConsumerElements.onStop()).orElse(LifecycleConsumerElements.ON_CONSUMER_STOP_DEF);
        this.valueDeserializer = keyValueDeserializer.valueDeSerializer();
        this.rebalanceListener = Optional.ofNullable(lifecycleConsumerElements.rebalanceListener()).orElse(LifecycleConsumerElements.DEF_NOOP_REBALANCE_LISTENER);
        this.flowErrorHandler = Optional.ofNullable(lifecycleConsumerElements.flowErrorHandler()).orElse(new FlowErrorHandler() {});
    }

    @Override
    public void startConsume(BiConsumer<K, T> consumer) {
        // if it's already running, let's don't run it again
        boolean currentRunning = running.getAndSet(true);
        if (currentRunning)
            return;
        initConsumer();
        shutDown = Executors.newSingleThreadExecutor();
        kafkaConsumer.subscribe(new ArrayList<>(Collections.singletonList(consumerTopic)), rebalanceListener);
        try {
            while (running.get()) {
                try {
                    getRecords(kafkaConsumer).forEach(record -> processRecord(consumer, kafkaConsumer, record));
                } catch (Exception e) {
                    handleRecordException(e);
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

    public boolean responsibleOfSomePartitions(Collection<TopicPartition> partitions) {
        return new HashSet<>(kafkaConsumer.assignment()).removeAll(partitions);
    }

    private void processRecord(BiConsumer<K, T> consumer, Consumer<K, T> kafkaConsumer, ConsumerRecord<K, T> record) {
        try {
            T value = record.value();
            K key = record.key();
            if (value == null) {
                log.warn("Failed to deserialize object from Kafka with key '{}'", key);
                MetricModule.getMetricStore().increaseCounter("consumer_deserialization.error");
            } else {
                processConsumerAction(consumer, value, key);
            }
            commitOffset(record, kafkaConsumer);
        } catch (Exception e) {
            if (running.get())
                kafkaConsumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
            throw e;
        }
    }

    private void processConsumerAction(BiConsumer<K, T> consumer, T value, K key) {
        try {
            consumer.accept(key, value);
        } catch (Exception e) {
            flowErrorHandler.doOnError(e);
            throw e;
        }
    }

    void handleRecordException(Exception e) {
        log.error("We won't commit record, so we should get record again", e);
        MetricModule.getMetricStore().increaseCounter("consumer." + e.getClass().getSimpleName());
    }

    ConsumerRecords<K, T> getRecords(Consumer<K, T> kafkaConsumer) {
        return kafkaConsumer.poll(Duration.ofMillis(consumerTimeout));
    }

    void commitOffset(ConsumerRecord<K, T> record, Consumer<?, ?> consumer) {
        Map<TopicPartition, OffsetAndMetadata> metadata = getCommitMetadata(record);
        try {
            consumer.commitSync(metadata, Duration.ofMillis(500));
        } catch (Exception e) {
            if (running.get())
                commitOffsetAsync(consumer, metadata);
            else
                throw e;
        }
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

    public void pause(Collection<TopicPartition> partitions) {
        if (running.get() && kafkaConsumer != null)
            kafkaConsumer.pause(partitions);
    }

    public void resume(Collection<TopicPartition> partitions) {
        if (running.get() && kafkaConsumer != null)
            kafkaConsumer.resume(partitions);
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
        }
        // we still can be stuck inside processRecord method if no onStopConsumer implemented, since it could take a lot of time
    }

}
