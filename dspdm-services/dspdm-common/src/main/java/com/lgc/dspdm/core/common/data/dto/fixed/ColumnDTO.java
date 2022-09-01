package com.lgc.dspdm.core.common.data.dto.fixed;

import com.lgc.dspdm.core.common.data.annotation.DatabaseColumn;
import com.lgc.dspdm.core.common.util.DSPDMConstants;

public class ColumnDTO {
    private String schema = null;
    private String tableName = null;
    private String type = null;
    private String columnName = null;
    private String dataType = null;
    private Integer columnSize = 0;
    private Integer decimalDigits = 0;
    private boolean isNullAble = false;
    private boolean isPrimarykey = false;
    private String defaultValue = null;
    private Integer columnIndex = 0;
    private String boName = null;
    private String referenceBoName = null;
    private String referenceBoAttrValue = null;
    private String referenceBoAttrLabel = null;

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public Integer getColumnSize() {
        return columnSize;
    }

    public void setColumnSize(Integer columnSize) {
        this.columnSize = columnSize;
    }

    public Integer getDecimalDigits() {
        return decimalDigits;
    }

    public void setDecimalDigits(Integer decimalDigits) {
        this.decimalDigits = decimalDigits;
    }

    public boolean isNullAble() {
        return isNullAble;
    }

    public void setNullAble(boolean nullAble) {
        isNullAble = nullAble;
    }

    public boolean isPrimarykey() {
        return isPrimarykey;
    }

    public void setPrimarykey(boolean primarykey) {
        isPrimarykey = primarykey;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public Integer getColumnIndex() {
        return columnIndex;
    }

    public void setColumnIndex(Integer columnIndex) {
        this.columnIndex = columnIndex;
    }

    public String getBoName() { return boName; }

    public void setBoName(String boName) { this.boName = boName; }

    public String getReferenceBoName() { return referenceBoName; }

    public void setReferenceBoName(String referenceBoName) { this.referenceBoName = referenceBoName; }

    public String getReferenceBoAttrValue() {
        return referenceBoAttrValue;
    }

    public void setReferenceBoAttrValue(String referenceBoAttrValue) {
        this.referenceBoAttrValue = referenceBoAttrValue;
    }

    public String getReferenceBoAttrLabel() {
        return referenceBoAttrLabel;
    }

    public void setReferenceBoAttrLabel(String referenceBoAttrLabel) {
        this.referenceBoAttrLabel = referenceBoAttrLabel;
    }
}
