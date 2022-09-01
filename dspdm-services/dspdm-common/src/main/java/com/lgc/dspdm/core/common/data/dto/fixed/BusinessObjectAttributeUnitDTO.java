package com.lgc.dspdm.core.common.data.dto.fixed;

import com.lgc.dspdm.core.common.data.annotation.DatabaseColumn;
import com.lgc.dspdm.core.common.data.annotation.DatabaseTable;
import com.lgc.dspdm.core.common.data.annotation.PrimaryKey;
import com.lgc.dspdm.core.common.data.dto.IBaseDTO;
import com.lgc.dspdm.core.common.util.DSPDMConstants;

import java.sql.Timestamp;
import java.util.Objects;

@PrimaryKey(javaPropertyNames = {"id"}, boAttrNames = {"BUSINESS_OBJECT_ATTR_UNIT_ID"}, physicalColumnNames = {"BUSINESS_OBJECT_ATTR_UNIT_ID"})
@DatabaseTable(tableName = "BUSINESS_OBJECT_ATTR_UNIT", boName = DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR_UNIT, schemaName = "", remarks = "Metadata table to contain unit attribute relationship with its owner attribute")
public class BusinessObjectAttributeUnitDTO implements IBaseDTO<Integer> {
    /**
     * constants to be used in code
     */
    public static enum properties {

        id,
        boName,
        boAttrName,
        boAttrUnitAttrName,
        isActive,
        rowCreatedBy,
        rowCreatedDate,
        rowChangedBy,
        rowChangedDate
    }

    ;

    @DatabaseColumn(columnName = "BUSINESS_OBJECT_ATTR_UNIT_ID", boAttributeName = DSPDMConstants.BoAttrName.BUSINESS_OBJECT_ATTR_UNIT_ID, columnType = Integer.class, nullable = false, length = 10)
    private Integer id = null;
    @DatabaseColumn(columnName = "BO_NAME", boAttributeName = DSPDMConstants.BoAttrName.BO_NAME, columnType = String.class, nullable = true, length = 50)
    private String boName = null;
    @DatabaseColumn(columnName = "BO_ATTR_NAME", boAttributeName = DSPDMConstants.BoAttrName.BO_ATTR_NAME, columnType = String.class, nullable = true, length = 50)
    private String boAttrName = null;
    @DatabaseColumn(columnName = "BO_ATTR_UNIT_ATTR_NAME", boAttributeName = DSPDMConstants.BoAttrName.BO_ATTR_UNIT_ATTR_NAME, columnType = String.class, nullable = true, length = 50)
    private String boAttrUnitAttrName = null;
    @DatabaseColumn(columnName = "IS_ACTIVE", boAttributeName = DSPDMConstants.BoAttrName.IS_ACTIVE, columnType = Boolean.class, nullable = false, length = 1)
    private Boolean isActive = null;
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

    public String getBoAttrName() {
        return boAttrName;
    }

    public void setBoAttrName(String boAttrName) {
        this.boAttrName = boAttrName;
    }

    public String getBoAttrUnitAttrName() {
        return boAttrUnitAttrName;
    }

    public void setBoAttrUnitAttrName(String boAttrUnitAttrName) {
        this.boAttrUnitAttrName = boAttrUnitAttrName;
    }

    public Boolean getActive() {
        return isActive;
    }

    public void setActive(Boolean active) {
        isActive = active;
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
        BusinessObjectAttributeUnitDTO that = (BusinessObjectAttributeUnitDTO) o;
        return Objects.equals(boName, that.boName) &&
                Objects.equals(boAttrName, that.boAttrName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(boName, boAttrName);
    }
}
