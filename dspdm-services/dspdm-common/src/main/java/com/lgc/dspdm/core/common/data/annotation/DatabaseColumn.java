package com.lgc.dspdm.core.common.data.annotation;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(value = {ElementType.FIELD})
public @interface DatabaseColumn {
    String columnName();
    Class columnType();
    String boAttributeName() default "";
    boolean nullable() default true;
    int length();
    String remarks() default "";
}
