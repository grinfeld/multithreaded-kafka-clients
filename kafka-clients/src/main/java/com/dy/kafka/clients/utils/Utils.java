package com.dy.kafka.clients.utils;

import com.dy.distributed.mario.utils.ReflectionUtils;
import com.typesafe.config.Config;

public class Utils {
    public static <T> T getOrDefault(Config config, String name, T def) {
        if (config == null) {
            return def;
        }
        if (config.hasPathOrNull(name)) {
            Object val = config.getAnyRef(name);
            if (def != null) {
                if (val instanceof String) {
                    val = ReflectionUtils.parseStringValue((String)val, def.getClass());
                }
                if (def instanceof Long) {
                    val = ((Number) val).longValue();
                }
            }
            return (T) val;
        }
        return def;
    }
}
