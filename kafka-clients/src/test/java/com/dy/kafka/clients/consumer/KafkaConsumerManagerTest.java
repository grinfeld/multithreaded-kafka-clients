package com.dy.kafka.clients.consumer;

import com.dy.kafka.clients.KafkaProperties;
import com.dy.kafka.clients.consumer.model.LifecycleConsumerElements;
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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import scala.collection.immutable.HashMap;

import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
// actually it tests both Consumer Manager and Delegator
class KafkaConsumerManagerTest {

    private static Random random = new Random();
    
    private EmbeddedK kafka = null;
    private KafkaStandardConsumerDelegator<String, TestObj> consumer = null;
    private KafkaConsumerManager<String, TestObj> manager = null;
    private KafkaProducerDelegator<String, TestObj> producer = null;
    private KafkaProperties kafkaProperties;
    private String topicName;
    private LifecycleConsumerElements lifecycleMocks = LifecycleConsumerElements.builder()
            .flowErrorHandler(mock(FlowErrorHandler.class))
            .onConsumerStop(mock(OnConsumerStop.class))
        .build();

    @BeforeAll
    void startKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:6001");
        props.put("group.id", "test");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        kafkaProperties = KafkaProperties.builder().timeout(5L).properties(props).build();

        kafka = EmbeddedKafka.start(EmbeddedKafkaConfig.apply(6001, 6000, new HashMap<>(), new HashMap<>(), new HashMap<>()));
        // kafka.broker().adminManager().createTopics(10, true, );
        producer = new KafkaProducerDelegator<>(kafkaProperties, new StringSerializer(), new JsonSerializer<>(GSON));
    }

    void initConsumer(LifecycleConsumerElements lifecycleConsumerElements, Properties additionalProperties) {
        resetAllMocks();

        Properties kafkaProps = new Properties();
        kafkaProps.putAll(kafkaProperties.getProperties());
        if (additionalProperties != null) {
            kafkaProps.putAll(additionalProperties);
        }
        KafkaProperties props = kafkaProperties.toBuilder().topic(topicName).properties(kafkaProps).build();

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
        assertThrows(IllegalArgumentException.class, () -> manager.startConsume((key, value, header) -> {}));
    }

    @Test
    @Timeout(value = 10L, unit = TimeUnit.SECONDS)
    @DisplayName("with enable.auto.commit=false, when consuming message performed successfully, expected commit kafka offset synchronously")
    void withAutoCommitFalse_whenConsumeDataPerformedOk_expectedCommitOffsetSucceeded() throws Exception {
        initConsumer(null, null);
        FutureTask<TestObj> future = new FutureTask<>(() -> new TestObj("testme"));
        doReturn(consumer).when(manager).createKafkaConsumer(anyString(), any(LifecycleConsumerElements.class));

        Worker<String, TestObj> biConsumer = (s, testObj, hs) -> future.run();
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ConsumerRecord<String, TestObj>> captor = ArgumentCaptor.forClass(ConsumerRecord.class);
        Executors.newSingleThreadExecutor().execute(() -> manager.startConsume(biConsumer));
        producer.send(topicName, "Stam", new TestObj("testme"), null);
        future.get();

        verify(consumer, times(1)).commitOffset(captor.capture(), any(Consumer.class));
        verify(consumer, never()).commitOffsetAsync(any(Consumer.class), any(Map.class));
    }

    @Test
    @Timeout(value = 10L, unit = TimeUnit.SECONDS)
    @DisplayName("with enable.auto.commit=true, when consuming message performed successfully, expected commit kafka offset synchronously")
    void withAutoCommitTrue_whenConsumeDataPerformedOk_expectedCommitOffsetSucceeded() throws Exception {
        Properties props = new Properties();
        props.put("enable.auto.commit", "true");
        initConsumer(null, props);
        FutureTask<TestObj> future = new FutureTask<>(() -> new TestObj("testme"));
        doReturn(consumer).when(manager).createKafkaConsumer(anyString(), any(LifecycleConsumerElements.class));

        Worker<String, TestObj> biConsumer = (s, testObj, hs) -> future.run();
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ConsumerRecord<String, TestObj>> captor = ArgumentCaptor.forClass(ConsumerRecord.class);
        Executors.newSingleThreadExecutor().execute(() -> manager.startConsume(biConsumer));
        producer.send(topicName, "Stam", new TestObj("testme"), null);
        future.get();

        verify(consumer, never()).commitOffset(captor.capture(), any(Consumer.class));
        verify(consumer, never()).commitOffsetAsync(any(Consumer.class), any(Map.class));
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

        Worker<String, TestObj> biConsumer = (s, testObj, hs) -> { future.run(); throw new RuntimeException(); };

        Executors.newSingleThreadExecutor().execute(() -> manager.startConsume(biConsumer));
        producer.send(topicName, "Stam", new TestObj("testme"), null);
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

        Worker<String, TestObj> biConsumer = (s, testObj, hs) -> future.run();
        Executors.newSingleThreadExecutor().execute(() -> manager.startConsume(biConsumer));
        producer.send(topicName, "Stam", new TestObj("testme"), null);
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

        Worker<String, TestObj> biConsumer = (s, testObj, hs) -> future.run();
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

        Worker<String, TestObj> biConsumer = (s, testObj, hs) -> future.run();
        Executors.newSingleThreadExecutor().execute(() -> manager.startConsume(biConsumer));
        producer.send(topicName, "Stam", new TestObj("testme"), null);
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