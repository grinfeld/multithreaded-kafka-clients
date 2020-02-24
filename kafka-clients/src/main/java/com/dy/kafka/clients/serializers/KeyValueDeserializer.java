package com.dy.kafka.clients.serializers;

import org.apache.kafka.common.serialization.Deserializer;

public interface KeyValueDeserializer<K, V> {
    Deserializer<K> keyDeSerializer();
    Deserializer<V> valueDeSerializer();
}
