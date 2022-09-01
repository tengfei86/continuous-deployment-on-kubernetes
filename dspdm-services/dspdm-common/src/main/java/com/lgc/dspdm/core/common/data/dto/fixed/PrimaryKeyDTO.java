package com.lgc.dspdm.core.common.data.dto.fixed;

import com.lgc.dspdm.core.common.data.annotation.DatabaseColumn;
import com.lgc.dspdm.core.common.util.DSPDMConstants;

public class PrimaryKeyDTO {
    private String schema = null;
    private String tableName = null;
    private String columnName = null;
    private String primarykeyName = null;
    private Integer columnIndex = 0;
    private String type = null;

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

    public String getPrimarykeyName() {
        return primarykeyName;
    }

    public void setPrimarykeyName(String primarykeyName) {
        this.primarykeyName = primarykeyName;
    }

    public Integer getColumnIndex() {
        return columnIndex;
    }

    public void setColumnIndex(Integer columnIndex) {
        this.columnIndex = columnIndex;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
