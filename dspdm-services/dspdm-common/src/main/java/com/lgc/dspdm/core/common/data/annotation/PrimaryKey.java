package com.lgc.dspdm.core.common.data.annotation;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(value = {ElementType.TYPE})
public @interface PrimaryKey {
    /**
     * Java class property name for primary keys. It is not a business object attribute and not a physical database column name
     * @return
     */
    String[] javaPropertyNames();
    
    /**
     * Business Object Attribute name to be used in the dynamic dto
     * @return
     */
    String[] boAttrNames();
    
    String[] physicalColumnNames();
}
