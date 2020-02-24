package com.dy.kafka.clients.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.function.BiConsumer;

public interface KafkaConsumerDelegator<K, T> {

    void startConsume(BiConsumer<K, T> consumer);
    void pause(Collection<TopicPartition> partitions);
    void resume(Collection<TopicPartition> partitions);
    void stopConsume();

}
