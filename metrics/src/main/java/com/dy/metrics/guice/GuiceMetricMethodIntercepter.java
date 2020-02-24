package com.dy.metrics.guice;

import com.dy.metrics.MetricIntercepter;
import com.dy.metrics.MetricStore;
import lombok.extern.slf4j.Slf4j;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

@Slf4j
public class GuiceMetricMethodIntercepter implements MethodInterceptor {

    private MetricIntercepter metrics;

    public GuiceMetricMethodIntercepter(MetricStore metrics) {
        this.metrics = new MetricIntercepter(metrics);
    }

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        return this.metrics.invoke(invocation.getThis(), invocation.getMethod(), invocation.getArguments(), invocation::proceed);
    }

}
