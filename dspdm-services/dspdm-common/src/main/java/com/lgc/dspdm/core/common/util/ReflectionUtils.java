package com.lgc.dspdm.core.common.util;

import com.lgc.dspdm.core.common.exception.DSPDMException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

public class ReflectionUtils {

    public static Method getSetterForField(Field field, ExecutionContext executionContext) {
        Method method = null;
        String methodName = field.getName();
        try {
            if (methodName.startsWith("is")) {
                methodName = methodName.substring(2);
            }
            methodName = StringUtils.makeFirstLetterCapital(methodName);
            method = field.getDeclaringClass().getDeclaredMethod("set" + methodName, field.getType());
        } catch (NoSuchMethodException e) {
            throw new DSPDMException("Setter method '{}' not found for class '{}'", executionContext.getExecutorLocale(), e, "set" + methodName, field.getDeclaringClass().getName());
        }
        return method;
    }

    public static Method getGetterForField(Field field, ExecutionContext executionContext) {
        Method method = null;
        String methodName = field.getName();
        try {
            if (methodName.startsWith("is")) {
                methodName = methodName.substring(2);
            }
            methodName = StringUtils.makeFirstLetterCapital(methodName);
            // include super class methods by calling getMethods. getDeclaredMethod does not include super class methods
            method = field.getDeclaringClass().getMethod("get" + methodName);
        } catch (NoSuchMethodException e) {
            try {
                // include super class methods by calling getMethods. getDeclaredMethod does not include super class methods
                method = field.getDeclaringClass().getMethod("is" + methodName);
            } catch (NoSuchMethodException ex) {
                throw new DSPDMException("Getter methods '{}' and '{}' not found for class '{}'", e, executionContext.getExecutorLocale(),
                        "get" + methodName + "()", "is" + methodName + "()", field.getDeclaringClass().getName());
            }
        }
        return method;
    }

    public static Map<String, Object> getFieldValueMap(Object model) {
        Map<String, Object> valueMap = new HashMap<String, Object>();
        Field[] modelFields = model.getClass().getDeclaredFields();
        for (Field field : modelFields) {
            if (field.isSynthetic()) {
                continue;
            }
            Object fieldValue = getFieldValueByName(model, field.getName());
            valueMap.put(field.getName(), fieldValue);
        }
        return valueMap;
    }

    public static List<String> getAnnotationFields(Object model, Class annotationClass) {
        List<String> annotationFields = new ArrayList();
        Field[] modelFields = model.getClass().getDeclaredFields();
        for (Field field : modelFields) {
            if (field.getAnnotation(annotationClass) != null) {
                annotationFields.add(field.getName());
            }
        }
        return annotationFields;
    }

    public static Object getFieldValueByName(Object model, String fieldName) {
        Object resultValue = null;
        try {
            String firtsChar = fieldName.substring(0, 1);
            String setFieldMethodName = "get"
                    + fieldName.replaceFirst(firtsChar, firtsChar.toUpperCase(Locale.ENGLISH));
            Method[] methodArray = model.getClass().getDeclaredMethods();
            for (Method method : methodArray) {
                if (method.getName().equals(setFieldMethodName)) {
                    try {
                        resultValue = method.invoke(model);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultValue;
    }

    public static Field getFieldByName(Class model, String fieldName) {
        Field[] declaredFields = model.getDeclaredFields();
        for (int i = 0; i < declaredFields.length; i++) {
            Field field = declaredFields[i];
            if (field.getName().equals(fieldName)) {
                return field;
            }
        }

        Field[] parentFields = model.getSuperclass().getDeclaredFields();
        for (int i = 0; i < parentFields.length; i++) {
            Field field = parentFields[i];
            if (field.getName().equals(fieldName)) {
                return field;
            }
        }
        return null;
    }

    public static void setFieldValue(Object entity, Map<String, Object> filedValueMap) {
        for (Map.Entry<String, Object> filedEntry : filedValueMap.entrySet()) {
            String fieldName = filedEntry.getKey();
            String firtsChar = fieldName.substring(0, 1);
            String setFieldMethodName = "set"
                    + fieldName.replaceFirst(firtsChar, firtsChar.toUpperCase(Locale.ENGLISH));
            Method[] methodArray = entity.getClass().getDeclaredMethods();
            for (Method method : methodArray) {
                if (method.getName().equals(setFieldMethodName)) {
                    try {
                        method.invoke(entity, filedEntry.getValue());
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
    }

    public static void setFieldValue(Object model, Field field, Object value) {
        if (value != null) {
            try {
                if (value != null) {
                    if (field.getType() != value.getClass()) {
                        if (field.getType() == Double.class) {
                            if (!value.toString().equals("")) {
                                value = Double.parseDouble(value.toString());
                            } else {
                                value = null;
                            }
                        } else if (field.getType() == Integer.class) {
                            if (!value.toString().equals("")) {
                                value = Integer.parseInt(value.toString());
                            } else {
                                value = null;
                            }
                        } else if (field.getType() == String.class) {
                            value = value.toString();
                        }
                    }
                    String filedName = field.getName();
                    String firtsChar = filedName.substring(0, 1);
                    String setFieldMethodName = "set"
                            + filedName.replaceFirst(firtsChar, firtsChar.toUpperCase(Locale.ENGLISH));
                    Method[] methodArray = model.getClass().getDeclaredMethods();
                    for (Method method : methodArray) {
                        if (method.getName().equals(setFieldMethodName)) {
                            try {
                                method.invoke(model, value);
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
