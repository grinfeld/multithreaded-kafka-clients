package com.mikerusoft.kafka.clients.metrics.stores.micrometer;

import com.mikerusoft.kafka.clients.metrics.functions.Execution;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class MicrometerMetricStoreTest {

    @Nested
    class CounterTest {

        @Test
        void whenExceptionThrownInIncreaseCounter_expectedNothing() {
            MeterRegistry mock = spy(new SimpleMeterRegistry());
            Counter counter = spy(Counter.builder("abc").register(mock));
            doReturn(counter).when(mock).counter(anyString());
            doThrow(new NullPointerException()).when(counter).increment();
            MicrometerMetricStore micrometerMetricStore = new MicrometerMetricStore(mock);
            Assertions.assertDoesNotThrow(() -> micrometerMetricStore.increaseCounter("abc", true));
        }

        @Test
        void whenIncreaseCounterReturnsResultValueOfCallable_expectedValueOfCallable() {
            MeterRegistry mock = spy(new SimpleMeterRegistry());
            Counter counter = spy(Counter.builder("abc").register(mock));
            doReturn(counter).when(mock).counter(anyString());

            doNothing().when(counter).increment();
            MicrometerMetricStore micrometerMetricStore = new MicrometerMetricStore(mock);
            String result = micrometerMetricStore.increaseCounter("abc", () -> "blablabla");

            Assertions.assertEquals("blablabla", result );
        }
        
        @Test
        void whenCallableThrowsExceptionInIncreaseCounter_expectedToThrowSameException() {
            MeterRegistry mock = spy(new SimpleMeterRegistry());
            Counter counter = spy(Counter.builder("abc").register(mock));
            doReturn(counter).when(mock).counter(anyString());

            doNothing().when(counter).increment();
            MicrometerMetricStore micrometerMetricStore = new MicrometerMetricStore(mock);

            assertThrows(IllegalArgumentException.class, () -> micrometerMetricStore.increaseCounter("abc", new Execution() {
                @Override
                public void execute() {
                    throw new IllegalArgumentException();
                }
            }));
        }
    }

    @Nested
    class TimerTest {
        @Test
        void whenExceptionThrownInIncreaseCounter_expectedNothing() {
            MeterRegistry mock = spy(new SimpleMeterRegistry());
            Timer timer = mock(Timer.class);
            doReturn(timer).when(mock).timer(anyString());

            doThrow(NullPointerException.class).when(timer).record(anyLong(), nullable(TimeUnit.class));
            MicrometerMetricStore micrometerMetricStore = new MicrometerMetricStore(mock);
            Assertions.assertDoesNotThrow(() -> micrometerMetricStore.recordTime("abc", true, System.currentTimeMillis()));
        }

        @Test
        void whenIncreaseCounterReturnsResultValueOfCallable_expectedValueOfCallable() {
            MeterRegistry mock = spy(new SimpleMeterRegistry());
            Timer timer = mock(Timer.class);
            doReturn(timer).when(mock).timer(anyString());

            doNothing().when(timer).record(anyLong(), nullable(TimeUnit.class));
            MicrometerMetricStore micrometerMetricStore = new MicrometerMetricStore(mock);
            String result = micrometerMetricStore.recordTime("abc", () -> "blablabla");

            Assertions.assertEquals("blablabla", result );
        }

        @Test
        void whenCallableThrowsExceptionInIncreaseCounter_expectedToThrowSameException() {
            MeterRegistry mock = spy(new SimpleMeterRegistry());
            Timer timer = mock(Timer.class);
            doReturn(timer).when(mock).timer(anyString());

            doNothing().when(timer).record(anyLong(), nullable(TimeUnit.class));
            MicrometerMetricStore micrometerMetricStore = new MicrometerMetricStore(mock);

            assertThrows(IllegalArgumentException.class, () -> micrometerMetricStore.recordTime("abc", new Execution() {
                @Override
                public void execute() {
                    throw new IllegalArgumentException();
                }
            }));
        }

    }

    @Nested
    class GaugeTest {

        @Test
        void whenExceptionThrownInIncreaseCounter_expectedNothing() {
            MeterRegistry mock = spy(new SimpleMeterRegistry());


            doThrow(new NullPointerException()).when(mock).gauge(anyString(),anyLong());
            MicrometerMetricStore micrometerMetricStore = new MicrometerMetricStore(mock);

            Supplier<Double> doubleSupplier = () -> Double.valueOf(1);

            Assertions.assertDoesNotThrow(() -> micrometerMetricStore.gauge("abc", doubleSupplier,true));
        }

    }

}
