package com.dy.metrics;

import com.dy.distributed.mario.utils.ReflectionUtils;
import com.dy.metrics.annotations.AddMetric;
import com.dy.metrics.annotations.MetricParam;
import com.dy.metrics.annotations.Param;
import com.dy.metrics.aop.Proceeder;
import lombok.extern.slf4j.Slf4j;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.dy.metrics.MethodCacheStore.INVALID;

@Slf4j
public class MetricIntercepter {

    private static final String CACHE_KEY_DELIMITER = "*";
    private static final String METHOD_KEY_DELIMITER = "#";

    private MetricStore metrics;
    private MethodCacheStore cache;

    public MetricIntercepter(MetricStore metrics) {
        this.metrics = metrics;
        cache = new MethodCacheStore();
    }

    public Object invoke(Object object, Method method, Object[] arguments, Proceeder proceeder) throws Throwable {
        if (!ReflectionUtils.hasAnnotation(method, AddMetric.class))
            return proceeder.proceed();

        AddMetric annotation = ReflectionUtils.getAnnotation(method, AddMetric.class);

        String name = annotation.name().isEmpty() ? ReflectionUtils.extractMethodName(method) : annotation.name().trim();

        name = updateMetricNameFromMethodArgs(method, arguments, annotation, name);

        long now = System.currentTimeMillis();

        try {
            Object result = proceeder.proceed();
            try {
                if (annotation.countAll())
                    metrics.increaseCounter(name);
                if (annotation.countSuccess())
                    metrics.increaseCounter(name, false);
                if (annotation.recordTime())
                    metrics.recordTime(name, false, now);
            } catch (Exception ex) {
                // we don't want metrics to affect code running
                log.warn("Error in recording metrics {}", ex.getMessage());
            }
            return result;
        } catch (Exception e) {
            try {
                if (annotation.countAll())
                    metrics.increaseCounter(name);
                if (annotation.recordTime())
                    metrics.recordTime(name, true, now);
                if (annotation.countException())
                    metrics.increaseCounter(name, true);
            } catch (Exception ex) {
                // we don't want metrics to affect code running
                log.warn("Error in recording metrics {}", ex.getMessage());
            }
            throw e;
        }
    }

    private String updateMetricNameFromMethodArgs(Method method, Object[] arguments, AddMetric annotation, String name) {
        if (annotation.withParams().length > 0) {
            try {
                String resolvedParams = resolveParamsFromArguments(method.getParameterAnnotations(), arguments, annotation.withParams());
                if (resolvedParams != null && resolvedParams.length() > 0) {
                    name = name + "." + resolvedParams;
                }
            } catch (Exception ex) {
                log.warn("Error in resolving metric params {}", ex.getMessage());
            }
        }
        return name;
    }

    private String resolveParamsFromArguments(Annotation[][] paramAnnotations, Object[] arguments, Param[] params) {
        Map<String, String> resolved = new HashMap<>();
        if (arguments == null)
            return null;

        Map<String, String> paramsMap = Stream.of(params).collect(Collectors.toMap(Param::name, Param::expr));

        for (int i=0; i<arguments.length; i++) {
            Object arg = arguments[i];

            if (arg == null || paramAnnotations.length <= i)
                continue;

            MetricParam metricParam = ReflectionUtils.getAnnotation(paramAnnotations[i], MetricParam.class);
            if (metricParam == null)
                continue;

            String name = metricParam.value();

            if ("".equals(name) || !paramsMap.containsKey(name))
                continue;

            String expr = paramsMap.get(metricParam.value()).trim();

            if (expr.isEmpty()) {
                resolved.put(name, String.valueOf(arg));
                continue;
            }

            String key = arg.getClass().getName() + CACHE_KEY_DELIMITER + expr;

            List<Method> methods = cache.calculateIfAbsent(key, s -> resolveMethodsToGetData(expr, arg.getClass()));
            if (methods != null && methods != INVALID) {
                Object result = ReflectionUtils.invokeIterative(methods, arg);
                if (result != null) {
                    resolved.put(name, String.valueOf(result));
                }
            }
        }

        // since, we want to return suffix according to order received in AddMetric annotation
        return Stream.of(params).map(p -> resolved.get(p.name())).filter(Objects::nonNull).collect(Collectors.joining("."));
    }

    List<Method> resolveMethodsToGetData(String expr, Class<?> initialClass) {
        List<Method> methods = new ArrayList<>();
        int prevInd = 0;
        Class<?> searchClass = initialClass;
        do {
            int ind = expr.indexOf(METHOD_KEY_DELIMITER, prevInd);
            String searchExpression = ind > -1 ? expr.substring(ind + 1) : expr.substring(prevInd);

            String keyMethod = searchClass.getName() + CACHE_KEY_DELIMITER + searchExpression;
            List<Method> cacheMethods = cache.get(keyMethod);
            if (cacheMethods != null) {
                if (cacheMethods == INVALID)
                    return INVALID;
                methods.addAll(cacheMethods);
                return methods;
            }

            String methodName = expr.substring(prevInd, ind > -1 ? ind : expr.length()); // edge case -> prevInd == ind
            Method lastMethod = ReflectionUtils.findMethodNoParamsWithInheritance(searchClass, methodName);
            if (lastMethod != null) {
                methods.add(lastMethod);
                searchClass = lastMethod.getReturnType();
            } else {
                return INVALID;
            }
            prevInd = ind + 1;
        } while(prevInd > 0);

        return methods;
    }
}
