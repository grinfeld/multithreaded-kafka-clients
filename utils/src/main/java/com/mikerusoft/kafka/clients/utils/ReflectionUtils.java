package com.mikerusoft.kafka.clients.utils;

import lombok.extern.slf4j.Slf4j;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

@Slf4j
public class ReflectionUtils {
    public static String extractMethodName(Method method) {
        try {
            return method == null ? "" : method.getName();
        } catch (Exception e) {
            // we don't want metrics to affect code running
            log.warn("Error in metrics AOP {}", e.getMessage());
        }
        return "";
    }

    public static <T extends Annotation> boolean hasAnnotation(Method method, Class<T> annotationClass) {
        return method != null && method.isAnnotationPresent(annotationClass);
    }

    public static <T extends Annotation> boolean hasAnnotation(Object object, Class<T> annotationClass) {
        return object != null && object.getClass().isAnnotationPresent(annotationClass);
    }

    public static  <T extends Annotation> T getAnnotation(Method method, Class<T> annotationClass) {
        return method != null ? method.getAnnotation(annotationClass) : null;
    }

    public static <T extends Annotation> T getAnnotation(Object object, Class<T> annotationClass) {
        return object != null ? object.getClass().getAnnotation(annotationClass) : null;
    }

    public static Method findMethodNoParamsWithInheritance(Class<?> clazz, String methodName) {
        // in most cases all classes will be already loaded in ClassLoader
        Class<?> inheritedClass = clazz;
        Method method = null;
        do {
            // could be problem with default methods in interfaces
            try {
                method = clazz.getMethod(methodName);
            } catch (NoSuchMethodException ex) {
                try {
                    method = clazz.getMethod(methodName);
                } catch (NoSuchMethodException ignore) {
                    // do nothing
                }
            }
            if (method == null) {
                inheritedClass = clazz.getSuperclass();
            }
        } while (!isLastInInheritance(inheritedClass) && method == null);

        return method;
    }

    public static boolean isLastInInheritance(Class<?> clazz) {
        return clazz == null || clazz.equals(Object.class);
    }

    public static Object invokeIterative(List<Method> methods, Object obj) {
        if (obj == null)
            return null;
        Object result = obj;
        for (Method m : methods) {
            try {
                m.setAccessible(true);
                result = m.invoke(result);
            } catch (IllegalAccessException | InvocationTargetException ex) {
                result = null;
            }
            if (result == null)
                break;
        }
        return result;
    }

    public static <T extends Annotation> T getAnnotation(Annotation[] ans, Class<T> clazz) {
        try {
            Annotation annotation = Stream.of(
                Optional.ofNullable(ans).orElseGet(() -> new Annotation[0])
            ).filter(clazz::isInstance).findFirst().orElse(null);
            return clazz.cast(annotation);
        } catch (Exception ignore) {
            // do nothing
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public static <T> T parseStringValue(String str, Class<T> clazz) {
        Object val = str;

        BasicTypes basicTypes = BasicTypes.get(clazz);
        if (basicTypes != null) {
            return (T) basicTypes.func.apply(str);
        }
        else if (clazz.isAssignableFrom(Boolean.class) || clazz.isAssignableFrom(Boolean.TYPE)) {
            val = str.equalsIgnoreCase("true");
        }
        return (T)val;
    }

    private enum BasicTypes {

        int_type(Integer.TYPE, Integer::valueOf),
        intW_type(Integer.class, Integer::valueOf),
        long_type(Long.TYPE, Long::valueOf),
        longW_type(Long.class, Long::valueOf),
        byte_type(Byte.TYPE, Byte::valueOf),
        byteW_type(Byte.class, Byte::valueOf),
        short_type(Short.TYPE, Short::valueOf),
        shortW_type(Short.class, Short::valueOf),
        double_type(Double.TYPE, Double::valueOf),
        doubleW_type(Double.class, Double::valueOf),
        float_type(Float.TYPE, Float::valueOf),
        floatW_type(Float.class, Float::valueOf),
        char_type(Character.TYPE, BasicTypes::extractChar),
        charW_type(Character.class, BasicTypes::extractChar),
        string_type(String.class, s -> s)
        ;

        private static Object extractChar(String s) {
            if (s.length() > 1)
                throw new ClassCastException("");
            return s.charAt(0);
        }


        private Class<?> clazz;
        private Function<String, Object> func;

        BasicTypes(Class<?> clazz, Function<String, Object> func) {
            this.clazz = clazz;
            this.func = func;
        }

        static BasicTypes get(Class<?> clazz) {
            return Stream.of(BasicTypes.values()).filter(t -> t.clazz.isAssignableFrom(clazz)).findFirst().orElse(null);
        }

    }

}
