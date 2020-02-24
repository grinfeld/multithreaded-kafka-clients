package com.dy.kafka.clients;

import lombok.*;

import java.util.List;
import java.util.Properties;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class KafkaProperties {
    private String topic;
    private Properties properties;
    private long timeout;
}
