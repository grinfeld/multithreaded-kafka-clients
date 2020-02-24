package com.dy.distributed.mario.utils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReflectionUtilsTest {

    @Test
    void whenNeedToCastSystemValueFromStringToInt_expectedIntValue() {
        assertThat(ReflectionUtils.parseStringValue( "1000", Integer.TYPE)).isEqualTo(1000);
    }

    @Test
    void whenNeedToCastSystemValueFromStringToInteger_expectedIntValue() {
        assertThat(ReflectionUtils.parseStringValue( "1000", Integer.class)).isEqualTo(new Integer(1000));
    }

    @Test
    void whenNeedToCastSystemValueFromStringToLong_expectedLongValue() {
        assertThat(ReflectionUtils.parseStringValue( "1000", Long.class)).isEqualTo(1000l);
    }

    @Test
    void whenNeedToCastSystemValueFromStringToBool_expectedBoolTrueValue() {
        assertThat(ReflectionUtils.parseStringValue("true",Boolean.class)).isEqualTo(true);
    }

    @Test
    void whenNeedToCastSystemValueFromStringToBool_expectedBoolFalseValue() {
        assertThat(ReflectionUtils.parseStringValue("false",Boolean.class)).isEqualTo(false);
    }

    @Test
    void whenNeedToCastSystemValueFromStringToBoolWhenValueIsUpperCase_expectedBoolTrueValue() {
        assertThat(ReflectionUtils.parseStringValue("TRUE",Boolean.class)).isEqualTo(true);
    }

    @Test
    void whenNeedToCastSystemValueFromStringToBoolAndBoolExpressionIsDifferentFromTrueOrFalse_expectedBoolFalseValue() {
        assertThat(ReflectionUtils.parseStringValue("blabla",Boolean.class)).isEqualTo(false);
    }

    @Test
    void whenNeedToCastSystemValueFromStringToString_expectedBoolFalseValue() {
        assertThat(ReflectionUtils.parseStringValue("string", String.class)).isEqualTo("string");
    }

    @Test
    void whenNeedToCastSystemValueFromStringToChar_expectedCharValue() {
        assertThat(ReflectionUtils.parseStringValue("s", Character.class)).isEqualTo('s');
    }



}