package com.dy.metrics;

import com.dy.metrics.annotations.AddMetric;
import com.dy.metrics.annotations.MetricParam;
import com.dy.metrics.annotations.Param;
import com.dy.metrics.aop.Proceeder;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static com.dy.metrics.MethodCacheStore.INVALID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MetricIntercepterTest {

    @Nested
    class ResolveMethodsToGetDataTest {

        @Test
        @DisplayName("When resolving existed name of direct method -> expected list with 1 method")
        void whenResolvingDirectMethodOnObject_expectedOnly1GetterMethodForSuchValue() throws Exception {
            MetricIntercepter mc = new MetricIntercepter(null);
            List<Method> methods = mc.resolveMethodsToGetData("getNum", MyObject.class);
            assertThat(methods).isNotNull().isNotEmpty().hasSize(1).containsExactly(MyObject.class.getMethod("getNum"));
        }

        @Test
        @DisplayName("When resolving chain of methods (recursive) and both methods exists in methods respectively -> expected list of 2 methods")
        void whenResolvingMethodOnInnerParameter_expected2MethodsForBothDirectAndInnerObjects() throws Exception {
            MetricIntercepter mc = new MetricIntercepter(null);
            List<Method> methods = mc.resolveMethodsToGetData("getInner#getStr", MyObject.class);
            assertThat(methods).isNotNull().isNotEmpty().hasSize(2).containsExactly(MyObject.class.getMethod("getInner"), InnerObject.class.getMethod("getStr"));
        }

        @Test
        @DisplayName("When resolving chain of methods (recursive) and both methods in hierarchy don't exist -> expected INVALID list reference")
        void whenResolvingInnerMethodAndBothMethodsDoesNotExists_expectedInvalid() {
            MetricIntercepter mc = new MetricIntercepter(null);
            List<Method> methods = mc.resolveMethodsToGetData("getBla#geetBla", MyObject.class);
            assertThat(methods).isNotNull().isSameAs(INVALID);
        }

        @Test
        @DisplayName("When resolving chain of methods (recursive) and the last method doesn't exist -> expected INVALID list reference")
        void whenResolvingInnerMethodAndTheLastOneDoesNotExists_expectedInvalid() {
            MetricIntercepter mc = new MetricIntercepter(null);
            List<Method> methods = mc.resolveMethodsToGetData("getInner#geetBla", MyObject.class);
            assertThat(methods).isNotNull().isSameAs(INVALID);
        }

        @Test
        @DisplayName("When resolving chain of methods (recursive) and the first method doesn't exist -> expected INVALID list reference")
        void whenResolvingInnerMethodAndTheFirstOneDoesNotExists_expectedInvalid() {
            MetricIntercepter mc = new MetricIntercepter(null);
            List<Method> methods = mc.resolveMethodsToGetData("getFdsdsd#getNum", MyObject.class);
            assertThat(methods).isNotNull().isSameAs(INVALID);
        }
    }

    @Nested
    class InvokeNoAddMetricTest {

        @Test
        @DisplayName("Method doesn't have AddMetric annotation, expected method executed, but metrics methods are not called")
        void withNoAddMetricAnnotation_whenCallingMethod_expectedMetricsNotRecordedAndMethodIsExecuted() throws Throwable {
            MetricStore ms = mock(MetricStore.class);
            MetricIntercepter mi = new MetricIntercepter(ms);
            doNothing().when(ms).increaseCounter(anyString(), any(Boolean.class));

            Proceeder proceeder = mock(Proceeder.class);
            doReturn(null).when(proceeder).proceed();

            // don't change!!! this to lambda, since mockito is unable tp mock/spy lambdas
            Proceeder proceed = spy(new Proceeder() {
                @Override
                public Object proceed() throws Throwable {
                    return 2;
                }
            });

            Object getNum = mi.invoke(MyObject.class, MyObject.class.getMethod("getNum"), null, proceed);

            verify(proceed, times(1)).proceed();
            verify(ms, never()).increaseCounter(anyString(), any(Boolean.class));
            verify(ms, never()).recordTime(anyString(), any(Boolean.class), any(Long.class));
            assertEquals(2, getNum);
        }
    }

    @Nested
    class InvokeWithAddMetricTest {


        @Test
        @DisplayName("Method without arguments, executing counter method throws exception, expected metrics don't stop original method from executing")
        void withAddMetricAnnotation_whenSuccessCounterAndExceptionThrownInMetricStore_expectedMetricsNotRecordedButMethodIsExecuted() throws Throwable {
            MetricStore ms = mock(MetricStore.class);
            MetricIntercepter mi = new MetricIntercepter(ms);
            doThrow(RuntimeException.class).when(ms).increaseCounter(anyString(), any(Boolean.class));

            Proceeder proceeder = mock(Proceeder.class);
            doReturn(null).when(proceeder).proceed();

            Proceeder proceed = spy(new Proceeder() {
                @Override
                public Object proceed() throws Throwable {
                    return 2;
                }
            });

            Object getNum = mi.invoke(MyObject.class, MyObject.class.getMethod("getNum"), null, proceed);

            verify(proceed, times(1)).proceed();
            verify(ms, never()).increaseCounter(anyString(), any(Boolean.class));
            verify(ms, never()).recordTime(anyString(), any(Boolean.class), any(Long.class));
            assertEquals(2, getNum);
        }

        @Test
        @DisplayName("Method without arguments and throws exception, and executing counter throws exception, too, expected original method's exception propagated up and not counter's exception")
        void withAddMetricAnnotation_whenExceptionInMethodToBeExcecutedAndExceptionThrownInMetricStoreForError_expectedMetricsNotRecordedBAndOriginalExceptionIsPropagatedUp() throws Throwable {
            MetricStore ms = mock(MetricStore.class);
            MetricIntercepter mi = new MetricIntercepter(ms);
            doThrow(RuntimeException.class).when(ms).increaseCounter(anyString(), any(Boolean.class));

            Proceeder proceeder = mock(Proceeder.class);
            doReturn(null).when(proceeder).proceed();

            Proceeder proceed = spy(new Proceeder() {
                @Override
                public Object proceed() throws Throwable {
                    throw new IllegalArgumentException();
                }
            });

            assertThrows(
                    IllegalArgumentException.class,
                    () -> mi.invoke(MyObject.class, MyObject.class.getMethod("doSomething"), null, proceed)
            );

            verify(proceed, times(1)).proceed();
            verify(ms, times(1)).increaseCounter(anyString(), eq(true));
            verify(ms, never()).increaseCounter(anyString(), eq(false));
        }

        @Test
        @DisplayName("Method with 2 parameters: first annotated and second not, expected method executed and increasing counter performed with annotation value concatenated to name")
        void withAddMetricAnnotationAndFirstParam_whenRecordCounter_expectedRecordedMetricWithParamValue() throws Throwable {
            MetricStore ms = mock(MetricStore.class);
            MetricIntercepter mi = new MetricIntercepter(ms);
            doNothing().when(ms).increaseCounter(anyString(), any(Boolean.class));

            Proceeder proceeder = mock(Proceeder.class);
            doReturn(null).when(proceeder).proceed();

            // don't change!!! this to lambda, since mockito is unable tp mock/spy lambdas
            Proceeder proceed = spy(new Proceeder() {
                @Override
                public Object proceed() throws Throwable {
                    return "First";
                }
            });

            Object result = mi.invoke(MyObject.class, MyObject.class.getMethod("doSomethingElse", String.class, String.class), new Object[]{"suffix", "other"}, proceed);

            verify(proceed, times(1)).proceed();
            verify(ms, times(1)).increaseCounter(eq("stam.suffix"), eq(false));
            verify(ms, never()).recordTime(anyString(), any(Boolean.class), any(Long.class));
            assertEquals("First", result);
        }

        @Test
        @DisplayName("Method with 2 parameters: first is not annotated, but second is, expected method executed and increasing counter performed with annotation value concatenated to name")
        void withAddMetricAnnotationAndSecondParam_whenRecordCounter_expectedRecordedMetricWithParamValue() throws Throwable {
            MetricStore ms = mock(MetricStore.class);
            MetricIntercepter mi = new MetricIntercepter(ms);
            doNothing().when(ms).increaseCounter(anyString(), any(Boolean.class));

            Proceeder proceeder = mock(Proceeder.class);
            doReturn(null).when(proceeder).proceed();

            // don't change!!! this to lambda, since mockito is unable tp mock/spy lambdas
            Proceeder proceed = spy(new Proceeder() {
                @Override
                public Object proceed() throws Throwable {
                    return "Second";
                }
            });

            Object result = mi.invoke(MyObject.class, MyObject.class.getMethod("doSomethingElseAgain", String.class, String.class), new Object[]{"other", "suffix"}, proceed);

            verify(proceed, times(1)).proceed();
            verify(ms, times(1)).increaseCounter(eq("stam.suffix"), eq(false));
            verify(ms, never()).recordTime(anyString(), any(Boolean.class), any(Long.class));
            assertEquals("Second", result);
        }

        @Test
        @DisplayName("Method with 3 parameters: 2 are annotated, but second is not, expected method executed and increasing counter performed with value from 2 annotation values concatenated to name")
        void withAddMetricAnnotationAndFirstAndThirdParam_whenRecordCounter_expectedRecordedMetricWith2ParamsValue() throws Throwable {
            MetricStore ms = mock(MetricStore.class);
            MetricIntercepter mi = new MetricIntercepter(ms);
            doNothing().when(ms).increaseCounter(anyString(), any(Boolean.class));

            Proceeder proceeder = mock(Proceeder.class);
            doReturn(null).when(proceeder).proceed();

            // don't change!!! this to lambda, since mockito is unable tp mock/spy lambdas
            Proceeder proceed = spy(new Proceeder() {
                @Override
                public Object proceed() throws Throwable {
                    return "Third";
                }
            });

            Object result = mi.invoke(MyObject.class, MyObject.class.getMethod("threeParams", String.class, String.class, String.class), new Object[]{"suffix1", "other", "suffix3"}, proceed);

            verify(proceed, times(1)).proceed();
            verify(ms, times(1)).increaseCounter(eq("stam.suffix1.suffix3"), eq(false));
            verify(ms, never()).recordTime(anyString(), any(Boolean.class), any(Long.class));
            assertEquals("Third", result);
        }

        @Test
        @DisplayName("Method with 3 parameters: 2 are annotated, but one (the last) has wrong name, and second is not annotated, expected method executed and increasing counter performed with first annotation value concatenated to name")
        void withAddMetricAnnotationAndFirstButThirdParamNameDoesnotFitAnyParam_whenRecordCounter_expectedRecordedMetricWith1ParamsValue() throws Throwable {
            MetricStore ms = mock(MetricStore.class);
            MetricIntercepter mi = new MetricIntercepter(ms);
            doNothing().when(ms).increaseCounter(anyString(), any(Boolean.class));

            Proceeder proceeder = mock(Proceeder.class);
            doReturn(null).when(proceeder).proceed();

            // don't change!!! this to lambda, since mockito is unable tp mock/spy lambdas
            Proceeder proceed = spy(new Proceeder() {
                @Override
                public Object proceed() throws Throwable {
                    return "Fourth";
                }
            });

            Object result = mi.invoke(MyObject.class, MyObject.class.getMethod("threeParamsOneNameFit", String.class, String.class, String.class), new Object[]{"suffix1", "other", "suffix3"}, proceed);

            verify(proceed, times(1)).proceed();
            verify(ms, times(1)).increaseCounter(eq("stam.suffix1"), eq(false));
            verify(ms, never()).recordTime(anyString(), any(Boolean.class), any(Long.class));
            assertEquals("Fourth", result);
        }


        @Test
        @DisplayName("Method with 2 params where only the first one annotated with expression, but it's non primitive or String Object and expression fetching data via method of inner object, expected metric's suffix resolved and added to metric counter")
        void withAddMetricAnnotation_whenParamIsNonPrimitiveObjectAndExpressionFetchingDataFromInnerObjectMethod_expectedRecordedMetricWith1ParamsValue() throws Throwable {
            MetricStore ms = mock(MetricStore.class);
            MetricIntercepter mi = new MetricIntercepter(ms);
            doNothing().when(ms).increaseCounter(anyString(), any(Boolean.class));

            Proceeder proceeder = mock(Proceeder.class);
            doReturn(null).when(proceeder).proceed();

            // don't change!!! this to lambda, since mockito is unable tp mock/spy lambdas
            Proceeder proceed = spy(new Proceeder() {
                @Override
                public Object proceed() throws Throwable {
                    return "inner";
                }
            });

            Object result = mi.invoke(MyObject.class, MyObject.class.getMethod("doInner", MyObject.class, String.class), new Object[]{new MyObject(new InnerObject("inner"), 5), "other"}, proceed);

            verify(proceed, times(1)).proceed();
            verify(ms, times(1)).increaseCounter(eq("stam.inner"), eq(false));
            verify(ms, never()).recordTime(anyString(), any(Boolean.class), any(Long.class));
            assertEquals("inner", result);
        }

    }

    @Data
    @AllArgsConstructor
    static class MyObject {
        private InnerObject inner;
        private Integer num;

        @AddMetric(name = "stam", countSuccess = true, countException = true)
        public String doSomething() {
            return "Success";
        }

        @AddMetric(name = "stam", countSuccess = true, countException = true, withParams = @Param(name = "data"))
        public String doSomethingElse(@MetricParam("data") String data, String other) {
            return "First";
        }

        @AddMetric(name = "stam", countSuccess = true, countException = true, withParams = @Param(name = "data"))
        public String doSomethingElseAgain(String other, @MetricParam("data") String data) {
            return "Second";
        }

        @AddMetric(name = "stam", countSuccess = true, countException = true, withParams = {@Param(name = "first"), @Param(name = "third")})
        public String threeParams(@MetricParam("first") String first, String second, @MetricParam("third") String data) {
            return "Third";
        }

        @AddMetric(name = "stam", countSuccess = true, countException = true, withParams = {@Param(name = "first"), @Param(name = "typo")})
        public String threeParamsOneNameFit(@MetricParam("first") String first, String second, @MetricParam("third") String data) {
            return "Fourth";
        }

        @AddMetric(name = "stam", countSuccess = true, countException = true, withParams = @Param(name = "data", expr = "getInner#getStr"))
        public String doInner(@MetricParam("data") MyObject data, String other) {
            return "Inner";
        }
    }

    @Data
    @AllArgsConstructor
    static class InnerObject {
        private String str;
    }

}