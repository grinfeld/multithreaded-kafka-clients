package com.dy.metrics.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME) @Target(ElementType.METHOD)
public @interface AddMetric {
    String name();
    boolean countAll() default false;
    boolean recordTime() default false;
    boolean countException() default false;
    boolean countSuccess() default false;
    Param[] withParams() default {};
}
