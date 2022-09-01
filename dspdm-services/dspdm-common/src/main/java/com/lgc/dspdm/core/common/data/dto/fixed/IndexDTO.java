package com.lgc.dspdm.core.common.data.dto.fixed;

import com.lgc.dspdm.core.common.data.annotation.DatabaseColumn;
import com.lgc.dspdm.core.common.util.DSPDMConstants;

public class IndexDTO {
    private String schema = null;
    private String tableName = null;
    private String type = null;
    private String columnName = null;
    private String indexName = null;
    private String indexType = null;
    private Integer columnIndex = 0;
    private Boolean nonUnique;

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

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public Integer getColumnIndex() {
        return columnIndex;
    }

    public void setColumnIndex(Integer columnIndex) {
        this.columnIndex = columnIndex;
    }

    public Boolean getNonUnique() { return nonUnique; }

    public void setNonUnique(Boolean nonUnique) { this.nonUnique = nonUnique; }
}
