package com.lgc.dspdm.core.common.data.dto.fixed;

import java.util.List;

public class TableDTO {
    private String entity = null;
    private String schema = null;
    private String tableName = null;
    private String type = null;
    private String remarks = null;
    private List<ColumnDTO> columnDTOList = null;
    private List<PrimaryKeyDTO> primaryKeyDTOList = null;
    private List<IndexDTO> uniqueIndexDTOList = null;
    private List<IndexDTO> searchIndexDTOList = null;
    private List<ForeignKeyDTO> foreignKeyDTOList = null;

    public String getEntity() { return entity; }

    public void setEntity(String entity) { this.entity = entity; }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRemarks() {
        return remarks;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }

    public List<ColumnDTO> getColumnDTOList() {
        return columnDTOList;
    }

    public void setColumnDTOList(List<ColumnDTO> columnDTOList) {
        this.columnDTOList = columnDTOList;
    }

    public List<PrimaryKeyDTO> getPrimaryKeyDTOList() {
        return primaryKeyDTOList;
    }

    public void setPrimaryKeyDTOList(List<PrimaryKeyDTO> primaryKeyDTOList) {
        this.primaryKeyDTOList = primaryKeyDTOList;
    }

    public List<IndexDTO> getUniqueIndexDTOList() {
        return uniqueIndexDTOList;
    }

    public void setUniqueIndexDTOList(List<IndexDTO> uniqueIndexDTOList) {
        this.uniqueIndexDTOList = uniqueIndexDTOList;
    }

    public List<ForeignKeyDTO> getForeignKeyDTOList() {
        return foreignKeyDTOList;
    }

    public void setForeignKeyDTOList(List<ForeignKeyDTO> foreignKeyDTOList) {
        this.foreignKeyDTOList = foreignKeyDTOList;
    }

    public List<IndexDTO> getSearchIndexDTOList() { return searchIndexDTOList; }

    public void setSearchIndexDTOList(List<IndexDTO> searchIndexDTOList) { this.searchIndexDTOList = searchIndexDTOList; }
}
