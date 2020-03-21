package com.mikerusoft.kafka.clients;

import lombok.*;

import java.util.List;
import java.util.Properties;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class KafkaProperties {
    private String topic;
    private Properties properties;
    private long timeout;
}
