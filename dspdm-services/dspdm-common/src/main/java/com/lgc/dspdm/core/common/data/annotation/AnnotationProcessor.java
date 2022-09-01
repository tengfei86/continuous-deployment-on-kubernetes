package com.lgc.dspdm.core.common.data.annotation;

import com.lgc.dspdm.core.common.data.dto.IBaseDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

public class AnnotationProcessor {
    /**
     * throws runtime exception if the database table name annotation is not found on this class
     *
     * @param c
     * @param <T>
     * @return
     */
    public static <T extends IBaseDTO> String getDatabaseTableName(Class<T> c, ExecutionContext executionContext) {
        String tableName = null;
        DatabaseTable databaseTableAnnotation = (DatabaseTable) c.getAnnotation(DatabaseTable.class);
        if (databaseTableAnnotation != null) {
            tableName = databaseTableAnnotation.tableName();
            if ((databaseTableAnnotation.schemaName() != null) && (databaseTableAnnotation.schemaName().trim().length() > 0)) {
                tableName = databaseTableAnnotation.schemaName() + "." + tableName;
            }
        } else {
            throw new DSPDMException("No database table annotation found on class {}", executionContext.getExecutorLocale(), c.getName());
        }
        return tableName;
    }
    
    public static <T extends IBaseDTO> String getBOName(Class<T> c, ExecutionContext executionContext) {
        String boName = null;
        DatabaseTable databaseTableAnnotation = (DatabaseTable) c.getAnnotation(DatabaseTable.class);
        if (databaseTableAnnotation != null) {
            boName = databaseTableAnnotation.boName();
        } else {
            throw new DSPDMException("No database table annotation found on class {}", executionContext.getExecutorLocale(), c.getName());
        }
        return boName;
    }
    
    public static String getDatabaseColumnName(Field field) {
        String columnName = null;
        DatabaseColumn databaseColumnAnnotation = (DatabaseColumn) field.getAnnotation(DatabaseColumn.class);
        if (databaseColumnAnnotation != null) {
            columnName = databaseColumnAnnotation.columnName();
        }
        return columnName;
    }
    
    public static String getBOAttributeName(Field field) {
        String boAttributeName = null;
        DatabaseColumn databaseColumnAnnotation = (DatabaseColumn) field.getAnnotation(DatabaseColumn.class);
        if (databaseColumnAnnotation != null) {
            boAttributeName = databaseColumnAnnotation.boAttributeName();
        }
        return boAttributeName;
    }
    
    public static Class getDatabaseColumnType(Field field) {
        Class columnType = null;
        DatabaseColumn databaseColumnAnnotation = (DatabaseColumn) field.getAnnotation(DatabaseColumn.class);
        if (databaseColumnAnnotation != null) {
            columnType = databaseColumnAnnotation.columnType();
        }
        return columnType;
    }
    
    public static <T extends IBaseDTO> List<String> getPrimaryKeyColumnNames(Class<T> c, ExecutionContext executionContext) {
        List<String> primaryKeyColumnNames = null;
        PrimaryKey primaryKeyAnnotation = (PrimaryKey) c.getAnnotation(PrimaryKey.class);
        if (primaryKeyAnnotation != null) {
            primaryKeyColumnNames = Arrays.asList(primaryKeyAnnotation.boAttrNames());
        } else {
            throw new DSPDMException("No primary key annotation found on class {}", executionContext.getExecutorLocale(), c.getName());
        }
        return primaryKeyColumnNames;
    }
}
