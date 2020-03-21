package com.mikerusoft.kafka.clients.consumer;

import com.mikerusoft.kafka.clients.KafkaProperties;
import com.mikerusoft.kafka.clients.consumer.model.LifecycleConsumerElements;
import com.mikerusoft.kafka.clients.consumer.model.Worker;
import com.mikerusoft.kafka.clients.serializers.KeyValueDeserializer;
import com.mikerusoft.kafka.clients.metrics.utils.Utils;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private AtomicBoolean stop = new AtomicBoolean(false);
    private AtomicBoolean running = new AtomicBoolean(false);
    private CountDownLatch latch;
    private FutureTask<Boolean> closeFuture;

    public KafkaConsumerManager(int numOfThreads, KafkaProperties properties,
                KeyValueDeserializer<K, T> keyValueDeserializer, LifecycleConsumerElements lifecycleConsumerElements) {
        this.properties = properties;
        this.numOfThreads = numOfThreads;
        this.keyValueDeserializer = keyValueDeserializer;
        this.consumers = new ConcurrentHashMap<>();
        this.lifecycleConsumerElements = lifecycleConsumerElements;
        this.closeFuture = new FutureTask<Boolean>(() -> true);
    }

    @Override
    public void startConsume(Worker<K, T> consumer) {
        if (!consumers.isEmpty() && !stop.get()) {
            throw new RuntimeException("Can't start new consumers, until all of previously started consumers not stopped");
        }
        consume(consumer);
    }

    private KafkaStandardConsumerDelegator<K, T> initConsumer(Worker<K, T> consumer, String uid) {
        LifecycleConsumerElements lifecycleConsumerElementsLocal = normalizeLifeCycleElements(consumer);
        return createKafkaConsumer(uid, lifecycleConsumerElementsLocal);
    }

    KafkaStandardConsumerDelegator<K, T> createKafkaConsumer(String uid, LifecycleConsumerElements lifecycleConsumerElements) {
        return new KafkaStandardConsumerDelegator<>(uid, properties, keyValueDeserializer, lifecycleConsumerElements);
    }

    private LifecycleConsumerElements normalizeLifeCycleElements(Worker<K, T> consumer) {
        LifecycleConsumerElements lifecycleConsumerElementsLocal = lifecycleConsumerElements;
        if (lifecycleConsumerElementsLocal == null) {
            lifecycleConsumerElementsLocal = LifecycleConsumerElements.builder().build();
        }
        return lifecycleConsumerElementsLocal;
    }

    private void consume(Worker<K, T> consumer) {
        if (running.getAndSet(true))
            return;
        try {
            while (!stop.get()) {
                // todo: we initialize too much objects - could be replaced with different, more efficient way?
                ExecutorService executors = Executors.newFixedThreadPool(numOfThreads);
                latch = new CountDownLatch(1);
                try {
                    this.consumers = IntStream.range(0, numOfThreads).mapToObj(i -> UUID.randomUUID().toString())
                            .map(uid -> initConsumer(consumer, uid))
                            .peek(c -> executors.submit(() -> c.startConsume(consumer)))
                            .collect(Collectors.toMap(KafkaStandardConsumerDelegator::getUid, Function.identity()));
                    try {
                        latch.await();
                    } catch (Exception ignore) {
                        // ignore
                    }
                } catch (Exception e) {
                    Utils.rethrowRuntime(e);
                } finally {
                    executors.shutdown();
                }
            }
        } finally {
            stopConsume();
            closeFuture.run();
        }
    }

    private void stopConsume() {
        try {
            consumers.forEach((id, consumer) -> stopSingleConsumer(consumer));
        } finally {
            resetConsumers();
        }
    }

    private void stopSingleConsumer(KafkaStandardConsumerDelegator<K, T> consumer) {
        try {
            consumer.close();
        } catch (Exception e) {
            log.warn("Failed to stop consume with " + e.getMessage());
        }
    }

    private void resetConsumers() {
        this.consumers = new ConcurrentHashMap<>();
    }

    @Override
    public void close() throws IOException {
        if (stop.getAndSet(true))
            return; // already stopped
        try {
            this.latch.countDown();
        } catch (Exception ignore) {
            // ignoring
        }
        running.set(false);
    }

    Future<Boolean> closeWait() throws Exception {
        close();
        return this.closeFuture;
    }

    Collection<KafkaStandardConsumerDelegator<K,T>> getConsumers() {
        return consumers.values();
    }
}
