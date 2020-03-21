package com.mikerusoft.kafka.clients.serializers.gson;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.Map;

@Slf4j
public class JsonDeserializer<T> implements Deserializer<T> {
    private Class<T> clazz;
    private Gson gson;

    public JsonDeserializer(Class<T> clazz, Gson gson) {
        this.clazz = clazz;
        this.gson = gson;
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        // nothing to configure
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        T value = null;
        try {
            value = gson.fromJson(new InputStreamReader(new ByteArrayInputStream(data)), clazz);
        } catch (Exception e) {
            log.error("Failed to deserialize object from topic " + topic, e);
        }
        return value;
    }

    @Override
    public void close() {
        // do nothing - inherited
    }
}
