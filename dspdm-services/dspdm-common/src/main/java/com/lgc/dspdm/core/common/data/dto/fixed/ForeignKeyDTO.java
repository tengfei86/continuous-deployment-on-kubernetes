package com.lgc.dspdm.core.common.data.dto.fixed;

import java.util.Objects;

public class ForeignKeyDTO {
    private String schema = null;
    private String tableName = null;
    private String columnName = null;
    private String foreignkeyName = null;
    private Integer columnIndex = 0;
    private String foreignkeyTable = null;
    private String foreignkeyColumn = null;
    private String type = null;
    private String foreignKeyBoName = null;

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

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getForeignkeyName() {
        return foreignkeyName;
    }

    public void setForeignkeyName(String foreignkeyName) {
        this.foreignkeyName = foreignkeyName;
    }

    public Integer getColumnIndex() {
        return columnIndex;
    }

    public void setColumnIndex(Integer columnIndex) {
        this.columnIndex = columnIndex;
    }

    public String getForeignkeyTable() {
        return foreignkeyTable;
    }

    public void setForeignkeyTable(String foreignkeyTable) {
        this.foreignkeyTable = foreignkeyTable;
    }

    public String getForeignkeyColumn() {
        return foreignkeyColumn;
    }

    public void setForeignkeyColumn(String foreignkeyColumn) {
        this.foreignkeyColumn = foreignkeyColumn;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getForeignKeyBoName() { return foreignKeyBoName; }

    public void setForeignKeyBoName(String foreignKeyBoName) {
        this.foreignKeyBoName = foreignKeyBoName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ForeignKeyDTO that = (ForeignKeyDTO) o;
        return Objects.equals(schema, that.schema) && Objects.equals(tableName, that.tableName) && Objects.equals(columnName, that.columnName) && Objects.equals(foreignkeyName, that.foreignkeyName) && Objects.equals(columnIndex, that.columnIndex) && Objects.equals(foreignkeyTable, that.foreignkeyTable) && Objects.equals(foreignkeyColumn, that.foreignkeyColumn) && Objects.equals(type, that.type) && Objects.equals(foreignKeyBoName, that.foreignKeyBoName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, tableName, columnName, foreignkeyName, columnIndex, foreignkeyTable, foreignkeyColumn, type, foreignkeyName);
    }
}
