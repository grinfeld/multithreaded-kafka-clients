package com.dy.kafka.clients.consumer;

import com.dy.kafka.clients.KafkaMetricReporter;
import com.dy.kafka.clients.KafkaProperties;
import com.dy.kafka.clients.consumer.model.LifecycleConsumerElements;
import com.dy.kafka.clients.consumer.model.MetaData;
import com.dy.kafka.clients.consumer.model.Worker;
import com.dy.kafka.clients.producer.KafkaProducerDelegator;
import com.dy.kafka.clients.serializers.KeyValueDeserializer;
import com.dy.kafka.clients.serializers.gson.JsonDeserializer;
import com.dy.kafka.clients.serializers.gson.JsonSerializer;
import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.manub.embeddedkafka.EmbeddedK;
import net.manub.embeddedkafka.EmbeddedKafka;
import net.manub.embeddedkafka.EmbeddedKafkaConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import scala.collection.immutable.HashMap;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doNothing;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
// actually it tests both Consumer Manager and Delegator
class KafkaConsumerManagerTest {

    private static Random random = new Random();
    
    private EmbeddedK kafka = null;
    private KafkaStandardConsumerDelegator<String, TestObj> consumer = null;
    private KafkaConsumerManager<String, TestObj> manager = null;
    private KafkaProducerDelegator<String, TestObj> producer = null;
    private String topicName;
    private LifecycleConsumerElements lifecycleMocks = LifecycleConsumerElements.builder()
            .flowErrorHandler(mock(FlowErrorHandler.class))
            .onConsumerStop(mock(OnConsumerStop.class))
        .build();

    private static final Properties defProps = defaultProperties();

    static Properties defaultProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:6001");
        props.put("group.id", "test");
        props.put("auto.offset.reset", "earliest");
        return props;
    }

    private static Properties createAutocommitProperty(boolean autoCommit) {
        Properties props = new Properties();
        props.put("enable.auto.commit", String.valueOf(autoCommit));
        return props;
    }

    @BeforeAll
    void startKafka() {
        Properties producerProps = new Properties();
        producerProps.putAll(defProps);
        producerProps.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, KafkaMetricReporter.class.getName());
        producerProps.put(KafkaMetricReporter.METRIC_PREFIX_CONFIG, "producer_");
        KafkaProperties kafkaProperties = KafkaProperties.builder().timeout(5L).properties(producerProps).build();

        kafka = EmbeddedKafka.start(EmbeddedKafkaConfig.apply(6001, 6000, new HashMap<>(), new HashMap<>(), new HashMap<>()));
        // kafka.broker().adminManager().createTopics(10, true, );
        producer = new KafkaProducerDelegator<>(kafkaProperties, new StringSerializer(), new JsonSerializer<>(GSON));
    }

    void initConsumer(LifecycleConsumerElements lifecycleConsumerElements, Properties additionalProperties) {
        resetAllMocks();

        Properties consumerProps = new Properties();
        consumerProps.putAll(defProps);
        if (additionalProperties != null) {
            consumerProps.putAll(additionalProperties);
        }
        consumerProps.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, KafkaMetricReporter.class.getName());
        consumerProps.put(KafkaMetricReporter.METRIC_PREFIX_CONFIG, "consumer_");

        KafkaProperties props = KafkaProperties.builder().topic(topicName).timeout(5L).properties(consumerProps).build();

        consumer = spy(new KafkaStandardConsumerDelegator<>(null, props, deSerializer, lifecycleConsumerElements));
        manager = spy(new KafkaConsumerManager<>(1, props, deSerializer, lifecycleConsumerElements));
    }
    
    @BeforeEach
    void generateTopicName() {
        topicName = "test" + System.currentTimeMillis() + "_" + random.nextInt(100);
    }

    @Test
    @Timeout(value = 5L, unit = TimeUnit.SECONDS)
    @DisplayName("when no defined 'group.id' in properties, expected IllegalArgumentException thrown")
    void whenNoGroupId_expectedIllegalArgumentException() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:6001");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        KafkaProperties kafkaProperties = KafkaProperties.builder().timeout(5L).topic("test").properties(props).build();
        KafkaConsumerManager<String, TestObj> manager = new KafkaConsumerManager<>(1, kafkaProperties, deSerializer, LifecycleConsumerElements.builder().build());
        assertThrows(IllegalArgumentException.class, () -> manager.startConsume((key, value, header, commander) -> {}));
    }

    @Test
    @Timeout(value = 10L, unit = TimeUnit.SECONDS)
    @DisplayName("with enable.auto.commit=false, when consuming message performed successfully, expected commit kafka offset synchronously")
    void withAutoCommitFalse_whenConsumeDataPerformedOk_expectedCommitOffsetSucceeded() throws Exception {
        initConsumer(null, createAutocommitProperty(false));
        FutureTask<TestObj> future = new FutureTask<>(() -> new TestObj("testme"));
        doReturn(consumer).when(manager).createKafkaConsumer(anyString(), any(LifecycleConsumerElements.class));

        Worker<String, TestObj> biConsumer = (s, testObj, hs, commander) -> future.run();
        Executors.newSingleThreadExecutor().execute(() -> manager.startConsume(biConsumer));
        producer.send(topicName, "Stam", new TestObj("testme"), null, null);
        future.get();

        verify(consumer, times(1)).commitOffset(any(ConsumerRecord.class), any(Consumer.class));
        verify(consumer, never()).commitOffsetAsync(any(Consumer.class), any(Map.class));
    }

    @Test
    @Timeout(value = 10L, unit = TimeUnit.SECONDS)
    @DisplayName("with enable.auto.commit=true, when consuming message performed successfully, expected commit kafka offset synchronously")
    void withAutoCommitTrue_whenConsumeDataPerformedOk_expectedCommitOffsetSucceeded() throws Exception {
        initConsumer(null, createAutocommitProperty(true));
        FutureTask<TestObj> future = new FutureTask<>(() -> new TestObj("testme"));
        doReturn(consumer).when(manager).createKafkaConsumer(anyString(), any(LifecycleConsumerElements.class));
        doNothing().when(consumer).commitSync(any(Consumer.class), anyMap());
        doNothing().when(consumer).commitOffsetAsync(any(Consumer.class), anyMap());

        Worker<String, TestObj> biConsumer = (s, testObj, hs, commander) -> future.run();
        Executors.newSingleThreadExecutor().execute(() -> manager.startConsume(biConsumer));
        producer.send(topicName, "Stam", new TestObj("testme"), null, null);
        future.get();

        verify(consumer, never()).commitSync(any(Consumer.class), anyMap());
        verify(consumer, never()).commitOffsetAsync(any(Consumer.class), anyMap());
    }

    @Test
    @Timeout(value = 10L, unit = TimeUnit.SECONDS)
    @DisplayName("with enable.auto.commit=false, when consuming message performed successfully and commitSync fails, expected commit kafka offset synchronously")
    void withAutoCommitFalse_whenConsumeDataPerformedOkCommitSyncFails_expectedCommitOffsetSucceeded() throws Exception {
        initConsumer(null, createAutocommitProperty(false));
        FutureTask<TestObj> future = new FutureTask<>(() -> new TestObj("testme"));
        doReturn(consumer).when(manager).createKafkaConsumer(anyString(), any(LifecycleConsumerElements.class));
        doThrow(new RuntimeException()).when(consumer).commitSync(any(Consumer.class), anyMap());
        doNothing().when(consumer).commitOffsetAsync(any(Consumer.class), anyMap());

        Worker<String, TestObj> biConsumer = (s, testObj, hs, commander) -> future.run();

        Executors.newSingleThreadExecutor().execute(() -> manager.startConsume(biConsumer));
        producer.send(topicName, "Stam", new TestObj("testme"), null, null);
        future.get();

        verify(consumer, times(1)).commitSync(any(Consumer.class), anyMap());
        verify(consumer, times(1)).commitOffsetAsync(any(Consumer.class), anyMap());
    }

    @Test
    @Timeout(value = 10L, unit = TimeUnit.SECONDS)
    @DisplayName("with sending header, when consuming message performed successfully, expected received same header")
    void withSendingHeaders_whenConsumeDataPerformedOk_expectedHeadersReceived() throws Exception {
        initConsumer(null, null);
        FutureTask<TestObj> future = new FutureTask<>(() -> new TestObj("testme"));
        doReturn(consumer).when(manager).createKafkaConsumer(anyString(), any(LifecycleConsumerElements.class));
        doNothing().when(consumer).commitSync(any(Consumer.class), anyMap());
        doNothing().when(consumer).commitOffsetAsync(any(Consumer.class), anyMap());

        Worker<String, TestObj> biConsumer = (s, testObj, hs, commander) -> {
            assertThat(hs).isNotNull().hasSize(1);
            MetaData header = hs.iterator().next();
            assertThat(header.key()).isEqualTo("header");
            assertThat(header.stringValue()).isEqualTo("myheader");
            future.run();
        };

        Executors.newSingleThreadExecutor().execute(() -> manager.startConsume(biConsumer));
        Headers headers = new RecordHeaders();
        headers.add("header", "myheader".getBytes(StandardCharsets.UTF_8));
        producer.send(topicName, "Stam", new TestObj("testme"), null, headers);
        future.get();
    }

    @Test
    @Timeout(value = 10L, unit = TimeUnit.SECONDS)
    @DisplayName("when consuming message performed successfully and called paused, expected consumer paused")
    void whenConsumeDataPerformedOkAndCallingPause_expectedPauseCalled() throws Exception {
        initConsumer(null, null);
        FutureTask<TestObj> future = new FutureTask<>(() -> new TestObj("testme"));
        doReturn(consumer).when(manager).createKafkaConsumer(anyString(), any(LifecycleConsumerElements.class));

        Worker<String, TestObj> biConsumer = (s, testObj, hs, commander) -> { commander.pause(); future.run(); };

        Executors.newSingleThreadExecutor().execute(() -> manager.startConsume(biConsumer));
        Headers headers = new RecordHeaders();
        headers.add("header", "myheader".getBytes(StandardCharsets.UTF_8));
        producer.send(topicName, "Stam", new TestObj("testme"), null, headers);
        future.get();

        verify(consumer, times(1)).pauseConsumer();
    }

    @Test
    @Timeout(value = 10L, unit = TimeUnit.SECONDS)
    // it means, that we have possible duplications
    @DisplayName("when consuming message performed successfully, but commit synchronously failed, expected commit asynchronously")
    void whenConsumeThrowsException_expectedNoCommitOffsetPerformed() throws Exception {
        LifecycleConsumerElements lifecycle = LifecycleConsumerElements.builder().flowErrorHandler(lifecycleMocks.flowErrorHandler()).build();
        doNothing().when(lifecycle.flowErrorHandler()).doOnError(any(Throwable.class));
        initConsumer(lifecycle, null);

        FutureTask<TestObj> future = new FutureTask<>(() -> new TestObj("testme"));
        doReturn(consumer).when(manager).createKafkaConsumer(anyString(), any(LifecycleConsumerElements.class));

        Worker<String, TestObj> biConsumer = (s, testObj, hs, commander) -> { future.run(); throw new RuntimeException(); };

        Executors.newSingleThreadExecutor().execute(() -> manager.startConsume(biConsumer));
        producer.send(topicName, "Stam", new TestObj("testme"), null, null);
        future.get();

        verify(consumer, never()).commitOffset(any(ConsumerRecord.class), any(Consumer.class));
        verify(consumer, never()).commitOffsetAsync(any(Consumer.class), any(Map.class));
        verify(lifecycle.flowErrorHandler(), times(1)).doOnError(any(Throwable.class));
    }

    @Test
    @Timeout(value = 10L, unit = TimeUnit.SECONDS)
    @DisplayName("when stopping to consume gracefully, expected calling stop consume on consumer and onStop")
    void whenManagerCloses_expectedConsumerCloseAndOnStopInvoked() throws Exception {
        LifecycleConsumerElements lifecycle = lifecycleMocks.toBuilder().build();
        initConsumer(lifecycle, null);
        FutureTask<TestObj> future = new FutureTask<>(() -> new TestObj("testme"));
        doReturn(consumer).when(manager).createKafkaConsumer(anyString(), any(LifecycleConsumerElements.class));

        Worker<String, TestObj> biConsumer = (s, testObj, hs, commander) -> future.run();
        Executors.newSingleThreadExecutor().execute(() -> manager.startConsume(biConsumer));
        producer.send(topicName, "Stam", new TestObj("testme"), null, null);
        future.get();
        manager.stopConsume();

        verify(consumer, times(1)).stopConsume();
        verify(lifecycle.onStop(), times(1)).onStop();
    }

    @Test
    @Timeout(value = 10L, unit = TimeUnit.SECONDS)
    @DisplayName("when trying to startConsume, when not all previously started consumers stopped, expected RuntimeException to be thrown, and not calling onStop") // since we didn't start to consume again
    void whenManagerStartsConsumeAndNotAllConsumersClosed_expectedRuntimeException() throws Exception {
        LifecycleConsumerElements lifecycle = LifecycleConsumerElements.builder().onConsumerStop(lifecycleMocks.onStop()).build();
        initConsumer(lifecycle, null);
        FutureTask<TestObj> future = new FutureTask<>(() -> new TestObj("testme"));
        doReturn(consumer).when(manager).createKafkaConsumer(anyString(), any(LifecycleConsumerElements.class));

        Worker<String, TestObj> biConsumer = (s, testObj, hs, commander) -> future.run();
        Executors.newSingleThreadExecutor().execute(() -> manager.startConsume(biConsumer));
        producer.send(topicName, "Stam", new TestObj("testme"), null);
        future.get();
        assertThrows(RuntimeException.class, () -> manager.startConsume(biConsumer));
        verify(lifecycle.onStop(), never()).onStop();
    }

    @Test
    @Timeout(value = 10L, unit = TimeUnit.SECONDS)
    @DisplayName("when consumer already working and trying to start it again before it closed, expected the second start attempt is ignored")
    void whenTryingToStartConsumeAgainOnWorkingConsumer_expectedIgnoredTheSecondTime() throws Exception {
        initConsumer(null, null);
        FutureTask<TestObj> future = new FutureTask<>(() -> new TestObj("testme"));
        doReturn(consumer).when(manager).createKafkaConsumer(anyString(), any(LifecycleConsumerElements.class));

        Worker<String, TestObj> biConsumer = (s, testObj, hs, commander) -> future.run();
        Executors.newSingleThreadExecutor().execute(() -> manager.startConsume(biConsumer));
        producer.send(topicName, "Stam", new TestObj("testme"), null, null);
        future.get();
        verify(consumer, times(1)).initConsumer();
        consumer.startConsume(biConsumer);
        verify(consumer, times(1)).initConsumer();
    }

    @AfterEach
    void closeConsumer() throws Exception {
        if (consumer != null)
            consumer.stopConsume();
        if (manager != null)
            manager.stopConsume();
        resetAllMocks();
        Thread.sleep(5L);
    }

    @AfterAll
    void closeEmbeddedKafka() {
        try {
            if (kafka != null)
                kafka.stop(true);
        } catch (Exception ignore) {}
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class TestObj {
        private String str;
    }

    private static Gson GSON = new Gson();
    private static KeyValueDeserializer<String, TestObj> deSerializer = new KeyValueDeserializer<String, TestObj>() {
        @Override
        public Deserializer<String> keyDeSerializer() {
            return new StringDeserializer();
        }

        @Override
        public Deserializer<TestObj> valueDeSerializer() {
            return new JsonDeserializer<>(TestObj.class, GSON);
        }
    };

    private void resetAllMocks() {
        try {
            reset(consumer);
        } catch (Exception ignore){}
        try {
            reset(manager);
        } catch (Exception ignore){}
        try {
            reset(lifecycleMocks.onStop());
        } catch (Exception ignore){}
        try {
            reset(lifecycleMocks.flowErrorHandler());
        } catch (Exception ignore){}
        try {
            reset(lifecycleMocks.rebalanceListener());
        } catch (Exception ignore){}
    }

}