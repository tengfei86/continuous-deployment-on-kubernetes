package com.lgc.dspdm.core.common.data.annotation;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(value = {ElementType.TYPE})
public @interface DatabaseTable {
    String tableName();
    String boName() default "";
    String schemaName() default "";
    String remarks() default "";
}
