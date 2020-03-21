package com.mikerusoft.kafka.clients.metrics;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class MethodCacheStore {

    static final List<Method> INVALID = Collections.unmodifiableList(new LinkedList<>());

    private Map<String, List<Method>> cache;

    public MethodCacheStore() {
        cache = new ConcurrentHashMap<>();
    }

    public List<Method> calculateIfAbsent(String key, Function<String, List<Method>> func) {
        return cache.computeIfAbsent(key, func);
    }

    public List<Method> get(String key) { return cache.get(key); }
}
