package com.dy.kafka.clients.producer;

import com.dy.kafka.clients.KafkaProperties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Optional;
import java.util.concurrent.Future;

public class KafkaProducerDelegator<K, T> {

    private final KafkaProducer<K, T> producer;
    private final String topic;

    public KafkaProducerDelegator(KafkaProperties properties, Serializer<K> keySerializer, Serializer<T> valueSerializer) {
        this.topic = properties.getTopic();
        this.producer = new KafkaProducer<>(properties.getProperties(), keySerializer, valueSerializer);
    }

    public Future<RecordMetadata> send(K key, T value) {
        return send(key, value, null);
    }

    public Future<RecordMetadata> send(K key, T value, Callback callback) {
        return send(topic, key, value, callback, null);
    }

    public Future<RecordMetadata> send(String topicName, K key, T value, Callback callback) {
        return send(topicName, key, value, callback, null);
    }

    public Future<RecordMetadata> send(String topicName, K key, T value, Callback callback, Headers headers) {
        return send(topicName, null, key, value, callback, headers);
    }

    public Future<RecordMetadata> send(String topicName, Integer partition, K key, T value, Callback callback, Headers headers) {
        ProducerRecord<K, T> producerRecord = new ProducerRecord<>(topicName, partition, key, value, Optional.ofNullable(headers).orElseGet(RecordHeaders::new));
        return producer.send(producerRecord, callback);
    }
}
