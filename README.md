multithreaded-kafka-clients
==================

Previously, I was using spring kafka clients, but once I need kafka clients in project using Guice and didn't find something stable,
so developed something unstable by myself :) 

There are 3 packages.

1. [metrics](metrics) - contains metrics with statsd and Guice. Could be easily (almost) replaced with micrometer implementation
1. [utils](utils) - utils package with few methods (maybe useless package)
1. [kafka-clients](kafka-clients) - client itself

There 2 main classes:

1. [KafkaProducerDelegator](kafka-clients/src/main/java/com/mikerusoft/kafka/clients/producer/KafkaProducerDelegator.java) - is simple wrapper above [KafkaProducer](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java). There is nothing multi-threaded with producer  
1. [KafkaConsumerManager](kafka-clients/src/main/java/com/mikerusoft/kafka/clients/consumer/KafkaConsumerManager.java) - wrapper above set of [KafkaConsumer](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java) s.