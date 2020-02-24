Metrics
================

This package contains metrics (statsd) dependency and Guice annotation to use with Guice DI

### Why

1. Because we can
1. Where is possible - move metrics code outside of method business logic
1. Make all metrics to be in the same form (same prefix, same suffixes - error, succ) and so on
1. Make metrics to work with lambdas and streams
1. Failures in metrics should never cause failure in running business logic code, so we can enforce it when everybody uses the same package and such behavior enforced inside package implementation
1. easy (almost) to change metric implementation without affecting code 
1. easy (almost) to change DI framework 

* **Note:** - easy to extract it into standalone package

### How to

The default implementation (and the only one, currently) is using **statsd**

```
monitoring {
  statsd {
    enabled = true
    host = localhost
    port = 8125
  }
}
```

By default, it's **NOT** ``enabled``

Works only by using Guice DI.

You should add dependency to pom and ``MetricModule`` to your ``Guice.createInjector(....)`` (or other Guice ways to add modules)

The main prefix for metrics is defined in configuration file

Example:

    monitoring {
        .....
        .....
        prefix = "offline.purchases"
        .....
        .....
    }

There are 2 main scenarios when we want to record metrics:

1. Recording metrics for some method (from the beginning to the end of method) - the preferable way is to use annotation ``@AddMetric``  
1. Recording metrics for specific line (or few) of code or for objects created NOT by Guice (directly or 3rd party)

* Framework, automatically, adds **prefix** and **type** of metric (currently, only **timer** and **counter** are supported) to metrics path

#### Annotations

* Annotations works only on ``public`` or ``package protected`` methods
* Annotations works only with methods in objects created via Guice (if you used ``new`` keyword in the middle of your code for creating object - it won't work)

There are 5 parameters:

1. name (_String_ - **Required**) - name of metrics to appear in statsd/graphite/grafana
1. recordTime (_boolean_ - **Optional**) - defines if to record time for method annotated with ``@AddMetric``
1. countSuccess (_boolean_ - **Optional**) - defines if to count success executions for method annotated with ``@AddMetric``
1. countException (_boolean_ - **Optional**) - defines if to count failure (exception is thrown in method) executions for method annotated with ``@AddMetric``
1. withParams (_array_ of ``@Param``) - array of parameters to be resolved in order to extract some additional suffix (see explanation later in README)

* **Note:** if method has **catch** block without rethrowing exception, current AOP mechanism (based on Guice) won't add **error** metrics

```java
public class SomeClass {

    //.......
    //.......

    @AddMetric(name="mymetric", recordTime=true, countException=true)
    public void foo() {
        //.......
        //.......
        //.......
    }
}
```
    
* All success timers and counters are sent with suffix "succ"  
* In case of timers, we are not able to define different timers with errors, so in case of success we'll see ``...timers.someName.succ`` and if the same method failed with exception it will be ``...timers.someName.err``

##### Resolving method parameters

In some case we want to add suffix, by some parameter or even to resolve parameter (usually, POJO) by calling getter on it

**1.** adding method parameter value for adding to metric path 

```java
public class SomeClass {

    //.......
    //.......

    @AddMetric(name="mymetric", countSuccess=true, withParams = @Param(name="sectionId"))
    public void foo(@MetricParam(name="sectionId") String sectionId) { // let's assume sectionId=12345678
        //.......
        //.......
        //.......
    }
}
``` 

metrics name will be as following: ``...count.mymetric.12345678.succ``

**2.** resolving method parameter value for adding to metric path

```java
public class UploadEvent {
    private Integer sectionId;

    //........
    //........
    //........

    public Integer getSectionId() {
        return sectionId;
    }

    //........
    //........
    //........
}

public class SomeClass {

    //.......
    //.......
    
    @AddMetric(name="mymetric", countSuccess=true, withParams = @Param(name="sectionId", expr="getSectionId"))
    public void foo(@MetricParam(name="sectionId") UploadEvent uploadEvent) { // let's assume uploadEvent.getSectionId() returns 12345678
        //.......
        //.......
        //.......
    }
}
```

metrics name will be as following: ``...count.mymetric.12345678.succ``

**3.** resolving method parameter value for adding to metric path by calling method on result parameter

```java
public class UploadEvent {
    private Section section;

    //........
    //........
    //........

    public Section getSection() {
        return section;
    }

    //........
    //........
    //........
    
    public class Section {
        private Integer id;
        public Integer getId() {
            return id;
        }
    }
}
public class SomeClass {

    //.......
    //.......
    
    @AddMetric(name="mymetric", countSuccess=true, withParams = @Param(name="sectionId", expr="getSection#getId"))
    public void foo(@MetricParam(name="sectionId") UploadEvent uploadEvent) { // let's assume uploadEvent.getSectionId() returns 12345678
        //.......
        //.......
        //.......
    }
}
```

metrics name will be as following: ``...count.mymetric.12345678.succ``

_Notes:_
* **expr** should always be method without any argument parameters (a.k.a simple getter)
* **Never!!!!!**, but **Never!!!!** use/call methods for resolving parameters for metrics with time consumed operations (going to DB, another service, disk and etc). Use only simple POJOs getter calls, since it could affect method execution time !!!!!

#### Calling to metrics directly

* Reminder: it's still works only for projects with use of Guice

Use ``MetricModule.getMetricStore()`` to get MetricStore reference (it's singleton) and created during MetricModule initialization

You get the ``MetricStore`` interface with following signature:

```java
public interface MetricStore {
    void increaseCounter(String name, Execution runnable) ;
    void increaseCounter(String name, Boolean error) ;
    void increaseCounter(String name) ;
    <V> V increaseCounter(String name, CallExecution<V> callable);
    void recordTime(String name, Execution runnable);
    void recordTime(String name, Boolean error, Long start);
    <V> V recordTime(String name, CallExecution<V> callable);
    void gauge(String name, Supplier<Double> supplier, Boolean error);
    void removeGauge(String name, Boolean error);
}
```

Usage: 

``increaseCounter`` or ``recordTime`` could be called directly or wrap lambda by metric (counter or recordTime) 

```java
public class Some {

    public void foo1() {
        MetricModule.getMetricStore().increaseCounter("myCounter", false);
    }

    public void foo2() {
        MetricModule.getMetricStore().recordTime("singleLine.write", 
            () -> System.out.println("Doing some work") 
        );
    }
}
```