package com.lgc.dspdm.core.common.data.dto.fixed;

import com.lgc.dspdm.core.common.data.annotation.DatabaseColumn;
import com.lgc.dspdm.core.common.data.annotation.DatabaseTable;
import com.lgc.dspdm.core.common.data.annotation.PrimaryKey;
import com.lgc.dspdm.core.common.data.dto.IBaseDTO;
import com.lgc.dspdm.core.common.util.DSPDMConstants;

import java.sql.Timestamp;
import java.util.Objects;

@PrimaryKey(javaPropertyNames = {"id"}, boAttrNames = {"BUSINESS_OBJECT_ID"}, physicalColumnNames = {"BUSINESS_OBJECT_ID"})
@DatabaseTable(tableName = "BUSINESS_OBJECT", boName = "BUSINESS OBJECT", schemaName = "", remarks = "Metadata table Business Object to contain simple header information of all other tables")
public class BusinessObjectDTO implements IBaseDTO<Integer> {

    public static enum properties {
        /**
         * constants to be used in code
         */
        id,
        boName,
        boDisplayName,
        entityName,
        boDesc,
        isActive,
        isMasterData,
        isMetadataTable,
        isOperationalTable,
        isReferenceTable,
        isResultTable,
        schemaName,
        sequenceName,
        rowCreatedBy,
        rowCreatedDate,
        rowChangedDate,
        rowChangedBy
    }

    ;

    /*
   do not change the definition order of the field. BO_NAME AND BO_ATTR_NAME must be defined earlier
    */
    // BUSINESS OBJECT ATTRIBUTE ID
    @DatabaseColumn(columnName = "BUSINESS_OBJECT_ID", boAttributeName = DSPDMConstants.BoAttrName.BUSINESS_OBJECT_ID, columnType = Integer.class, nullable = false, length = 10)
    private Integer id = null;
    // BO
    @DatabaseColumn(columnName = "BO_NAME", boAttributeName = DSPDMConstants.BoAttrName.BO_NAME, columnType = String.class, nullable = false, length = 50)
    private String boName = null;
    @DatabaseColumn(columnName = "BO_DISPLAY_NAME", boAttributeName = DSPDMConstants.BoAttrName.BO_DISPLAY_NAME, columnType = String.class, nullable = false, length = 500)
    private String boDisplayName = null;
    @DatabaseColumn(columnName = "ENTITY", boAttributeName = DSPDMConstants.BoAttrName.ENTITY, columnType = String.class, nullable = false, length = 50)
    private String entityName = null;
    @DatabaseColumn(columnName = "BO_DESC", boAttributeName = DSPDMConstants.BoAttrName.BO_DESC, columnType = String.class, nullable = false, length = 500)
    private String boDesc = null;
    // boolean variables in database they are character 'y' or 'n'    
    @DatabaseColumn(columnName = "IS_ACTIVE", boAttributeName = DSPDMConstants.BoAttrName.IS_ACTIVE, columnType = Boolean.class, nullable = false, length = 1)
    private Boolean isActive = null;
    @DatabaseColumn(columnName = "IS_MASTER_DATA", boAttributeName = DSPDMConstants.BoAttrName.IS_MASTER_DATA, columnType = Boolean.class, nullable = false, length = 1)
    private Boolean isMasterData = null;
    @DatabaseColumn(columnName = "IS_METADATA_TABLE", boAttributeName = DSPDMConstants.BoAttrName.IS_METADATA_TABLE, columnType = Boolean.class, nullable = false, length = 1)
    private Boolean isMetadataTable = null;
    @DatabaseColumn(columnName = "IS_OPERATIONAL_TABLE", boAttributeName = DSPDMConstants.BoAttrName.IS_OPERATIONAL_TABLE, columnType = Boolean.class, nullable = false, length = 1)
    private Boolean isOperationalTable = null;
    @DatabaseColumn(columnName = "IS_REFERENCE_TABLE", boAttributeName = DSPDMConstants.BoAttrName.IS_REFERENCE_TABLE, columnType = Boolean.class, nullable = false, length = 1)
    private Boolean isReferenceTable = null;
    @DatabaseColumn(columnName = "IS_RESULT_TABLE", boAttributeName = DSPDMConstants.BoAttrName.IS_RESULT_TABLE, columnType = Boolean.class, nullable = false, length = 1)
    private Boolean isResultTable = null;
    @DatabaseColumn(columnName = "SCHEMA_NAME", boAttributeName = DSPDMConstants.BoAttrName.SCHEMA_NAME, columnType = String.class, nullable = true, length = 50)
    private String schemaName = null;
    @DatabaseColumn(columnName = "KEY_SEQ_NAME", boAttributeName = DSPDMConstants.BoAttrName.KEY_SEQ_NAME, columnType = String.class, nullable = true, length = 50)
    private String sequenceName = null;
    @DatabaseColumn(columnName = "ROW_CREATED_BY", boAttributeName = DSPDMConstants.BoAttrName.ROW_CREATED_BY, columnType = String.class, nullable = true, length = 30)
    private String rowCreatedBy = null;
    @DatabaseColumn(columnName = "ROW_CREATED_DATE", boAttributeName = DSPDMConstants.BoAttrName.ROW_CREATED_DATE, columnType = Timestamp.class, nullable = false, length = 12)
    private Timestamp rowCreatedDate = null;
    @DatabaseColumn(columnName = "ROW_CHANGED_BY", boAttributeName = DSPDMConstants.BoAttrName.ROW_CHANGED_BY, columnType = String.class, nullable = true, length = 30)
    private String rowChangedBy = null;
    @DatabaseColumn(columnName = "ROW_CHANGED_DATE", boAttributeName = DSPDMConstants.BoAttrName.ROW_CHANGED_DATE, columnType = Timestamp.class, nullable = false, length = 12)
    private Timestamp rowChangedDate = null;

    /* **************************************************** */
    /* ************* Getters & Setters ******************** */
    /* **************************************************** */

    @Override
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getBoName() {
        return boName;
    }

    public void setBoName(String boName) {
        this.boName = boName;
    }

    public String getBoDisplayName() {
        return boDisplayName;
    }

    public void setBoDisplayName(String boDisplayName) {
        this.boDisplayName = boDisplayName;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public String getBoDesc() {
        return boDesc;
    }

    public void setBoDesc(String boDesc) {
        this.boDesc = boDesc;
    }

    public Boolean getActive() {
        return isActive;
    }

    public void setActive(Boolean active) {
        isActive = active;
    }

    public Boolean getMasterData() {
        return isMasterData;
    }

    public void setMasterData(Boolean masterData) {
        isMasterData = masterData;
    }

    public Boolean getMetadataTable() {
        return isMetadataTable;
    }

    public void setMetadataTable(Boolean metadataTable) {
        isMetadataTable = metadataTable;
    }

    public Boolean getOperationalTable() {
        return isOperationalTable;
    }

    public void setOperationalTable(Boolean operationalTable) {
        isOperationalTable = operationalTable;
    }

    public Boolean getReferenceTable() {
        return isReferenceTable;
    }

    public void setReferenceTable(Boolean referenceTable) {
        isReferenceTable = referenceTable;
    }

    public Boolean getResultTable() {
        return isResultTable;
    }

    public void setResultTable(Boolean resultTable) {
        isResultTable = resultTable;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSequenceName() {
        return sequenceName;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    public String getRowCreatedBy() {
        return rowCreatedBy;
    }

    public void setRowCreatedBy(String rowCreatedBy) {
        this.rowCreatedBy = rowCreatedBy;
    }

    public Timestamp getRowCreatedDate() {
        return rowCreatedDate;
    }

    public void setRowCreatedDate(Timestamp rowCreatedDate) {
        this.rowCreatedDate = rowCreatedDate;
    }

    public String getRowChangedBy() {
        return rowChangedBy;
    }

    public void setRowChangedBy(String rowChangedBy) {
        this.rowChangedBy = rowChangedBy;
    }

    public Timestamp getRowChangedDate() {
        return rowChangedDate;
    }

    public void setRowChangedDate(Timestamp rowChangedDate) {
        this.rowChangedDate = rowChangedDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BusinessObjectDTO that = (BusinessObjectDTO) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(boName, that.boName) &&
                Objects.equals(boDisplayName, that.boDisplayName) &&
                Objects.equals(entityName, that.entityName) &&
                Objects.equals(boDesc, that.boDesc) &&
                Objects.equals(isActive, that.isActive) &&
                Objects.equals(isMasterData, that.isMasterData) &&
                Objects.equals(isMetadataTable, that.isMetadataTable) &&
                Objects.equals(isOperationalTable, that.isOperationalTable) &&
                Objects.equals(isReferenceTable, that.isReferenceTable) &&
                Objects.equals(isResultTable, that.isResultTable) &&
                Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(sequenceName, that.sequenceName) &&
                Objects.equals(rowCreatedBy, that.rowCreatedBy) &&
                Objects.equals(rowCreatedDate, that.rowCreatedDate) &&
                Objects.equals(rowChangedBy, that.rowChangedBy) &&
                Objects.equals(rowChangedDate, that.rowChangedDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, boName, boDisplayName, entityName, boDesc, isActive, isMasterData, isMetadataTable, isOperationalTable, isReferenceTable, isResultTable, schemaName, sequenceName, rowCreatedBy, rowCreatedDate, rowChangedBy, rowChangedDate);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BusinessObjectDTO{");
        sb.append("id=").append(id);
        sb.append(", boName='").append(boName).append('\'');
        sb.append(", boDisplayName='").append(boDisplayName).append('\'');
        sb.append(", entityName='").append(entityName).append('\'');
        sb.append(", boDesc='").append(boDesc).append('\'');
        sb.append(", isActive=").append(isActive);
        sb.append(", isMasterData=").append(isMasterData);
        sb.append(", isMetadataTable=").append(isMetadataTable);
        sb.append(", isOperationalTable=").append(isOperationalTable);
        sb.append(", isReferenceTable=").append(isReferenceTable);
        sb.append(", isResultTable=").append(isResultTable);
        sb.append(", schemaName='").append(schemaName).append('\'');
        sb.append(", sequenceName='").append(sequenceName).append('\'');
        sb.append(", rowCreatedBy='").append(rowCreatedBy).append('\'');
        sb.append(", rowCreatedDate=").append(rowCreatedDate);
        sb.append(", rowChangedBy='").append(rowChangedBy).append('\'');
        sb.append(", rowChangedDate=").append(rowChangedDate);
        sb.append('}');
        return sb.toString();
    }
}
