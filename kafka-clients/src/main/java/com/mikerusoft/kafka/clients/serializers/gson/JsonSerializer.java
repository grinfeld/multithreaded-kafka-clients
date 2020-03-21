package com.mikerusoft.kafka.clients.serializers.gson;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {
    private Gson gson;

    public JsonSerializer(Gson gson) {
        super();
        this.gson = gson;
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        // we have nothing to configure
    }

    @Override
    public byte[] serialize(String topic, T t) {
        return gson.toJson(t).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        // inherited from 3rd party
    }
}
