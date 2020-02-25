package com.dy.kafka.clients.consumer;

import com.dy.kafka.clients.KafkaProperties;
import com.dy.kafka.clients.serializers.KeyValueDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class KafkaConsumerManager<K, T> implements KafkaConsumerDelegator<K, T> {

    private KafkaProperties properties;
    private KeyValueDeserializer<K, T> keyValueDeserializer;
    private Map<String, KafkaStandardConsumerDelegator<K, T>> consumers;
    private int numOfThreads;
    private LifecycleConsumerElements lifecycleConsumerElements;
    private CountDownLatch latch;

    public KafkaConsumerManager(int numOfThreads, KafkaProperties properties,
                KeyValueDeserializer<K, T> keyValueDeserializer, LifecycleConsumerElements lifecycleConsumerElements) {
        this.properties = properties;
        this.numOfThreads = numOfThreads;
        this.keyValueDeserializer = keyValueDeserializer;
        this.consumers = new ConcurrentHashMap<>();
        this.lifecycleConsumerElements = lifecycleConsumerElements;
    }

    @Override
    public void startConsume(BiConsumer<K, T> consumer) {
        if (!consumers.isEmpty()) {
            throw new RuntimeException("Can't start new consumers, until all of previously started consumers not stopped");
        }
        consume(consumer);
    }

    private KafkaStandardConsumerDelegator<K, T> initConsumer(BiConsumer<K, T> consumer, String uid) {
        LifecycleConsumerElements lifecycleConsumerElementsLocal = normalizeLifeCycleElements(consumer);
        return createKafkaConsumer(uid, lifecycleConsumerElementsLocal);
    }

    KafkaStandardConsumerDelegator<K, T> createKafkaConsumer(String uid, LifecycleConsumerElements lifecycleConsumerElements) {
        return new KafkaStandardConsumerDelegator<>(uid, properties, keyValueDeserializer, lifecycleConsumerElements);
    }

    private LifecycleConsumerElements normalizeLifeCycleElements(BiConsumer<K, T> consumer) {
        ConsumerRebalanceListener rebalanceListener = new MultiThreadedRebalanceListener<>(consumer, this);
        LifecycleConsumerElements lifecycleConsumerElementsLocal = lifecycleConsumerElements;
        if (lifecycleConsumerElementsLocal == null) {
            lifecycleConsumerElementsLocal = LifecycleConsumerElements.builder().rebalanceListener(rebalanceListener).build();
        }
        if (lifecycleConsumerElementsLocal.rebalanceListener() == null) {
            lifecycleConsumerElementsLocal = lifecycleConsumerElementsLocal.toBuilder().rebalanceListener(rebalanceListener).build();
        }
        return lifecycleConsumerElementsLocal;
    }

    private void consume(BiConsumer<K, T> consumer) {
        ExecutorService executors = Executors.newFixedThreadPool(numOfThreads);
        latch = new CountDownLatch(1);
        try {
            this.consumers = IntStream.range(0, numOfThreads).mapToObj(i -> UUID.randomUUID().toString())
                    .map(uid -> initConsumer(consumer, uid))
                    .peek(c -> executors.submit(() -> c.startConsume(consumer)))
                    .collect(Collectors.toMap(KafkaStandardConsumerDelegator::getUid, Function.identity()));
            try {
                latch.await();
            } catch (InterruptedException ignore) {
                // ignore
            }
        } catch (Exception e) {
            latch.countDown();
            throw e;
        } finally {
            //this.consumers = new ConcurrentHashMap<>();
            executors.shutdown();
        }
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        this.consumers.values().stream().filter(c -> c.responsibleOfSomePartitions(partitions))
                .forEach(c -> c.pause(partitions));
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        this.consumers.values().stream().filter(c -> c.responsibleOfSomePartitions(partitions))
                .forEach(c -> c.resume(partitions));
    }

    @Override
    public void stopConsume() {
        consumers.forEach((id, consumer) -> consumer.stopConsume());
        latch.countDown();
        this.consumers = new ConcurrentHashMap<>();
    }

    // todo: ??????
    private static class MultiThreadedRebalanceListener<K, T> implements ConsumerRebalanceListener {
        private BiConsumer<K, T> consumer;
        private KafkaConsumerManager<K, T> manager;

        public MultiThreadedRebalanceListener(BiConsumer<K, T> consumer, KafkaConsumerManager<K, T> manager) {
            this.consumer = consumer;
            this.manager = manager;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            manager.stopConsume();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            // todo: check partitions, which only not already assigned
            manager.consume(consumer);
        }
    }

}
