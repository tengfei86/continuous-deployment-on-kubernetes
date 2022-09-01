package com.lgc.dspdm.core.common.data.dto.fixed;

import com.lgc.dspdm.core.common.data.annotation.DatabaseColumn;
import com.lgc.dspdm.core.common.data.annotation.DatabaseTable;
import com.lgc.dspdm.core.common.data.annotation.PrimaryKey;
import com.lgc.dspdm.core.common.data.dto.IBaseDTO;
import com.lgc.dspdm.core.common.util.DSPDMConstants;

import java.util.Objects;

@PrimaryKey(javaPropertyNames = {"id"}, boAttrNames = {"BUS_OBJ_ATTR_UNIQ_CONS_ID"}, physicalColumnNames = {"BUS_OBJ_ATTR_UNIQ_CONS_ID"})
@DatabaseTable(tableName = "BUS_OBJ_ATTR_UNIQ_CONSTRAINTS", boName = "BUS OBJ ATTR UNIQ CONSTRAINTS", schemaName = "", remarks = "Metadata table to contain unique constraints inside a business object")
public class BusinessObjectAttributeConstraintsDTO implements IBaseDTO<Integer> {
    /**
     * constants to be used in code
     */
    public static enum properties {
        id,
        attributeName,
        entityName,
        constraintName,
        isActive,
        boAttrName,
        boName,
        verify
    }
    
    ;
    
    @DatabaseColumn(columnName = "BUS_OBJ_ATTR_UNIQ_CONS_ID", boAttributeName = DSPDMConstants.BoAttrName.BUS_OBJ_ATTR_UNIQ_CONS_ID, columnType = Integer.class, nullable = false, length = 10)
    private Integer id = null;
    @DatabaseColumn(columnName = "ENTITY", boAttributeName = DSPDMConstants.BoAttrName.ENTITY, columnType = String.class, nullable = true, length = 30)
    private String entityName = null;
    @DatabaseColumn(columnName = "ATTRIBUTE", boAttributeName = DSPDMConstants.BoAttrName.ATTRIBUTE, columnType = String.class, nullable = true, length = 30)
    private String attributeName = null;
    @DatabaseColumn(columnName = "CONSTRAINT_NAME", boAttributeName = DSPDMConstants.BoAttrName.CONSTRAINT_NAME, columnType = String.class, nullable = true, length = 200)
    private String constraintName = null;
    @DatabaseColumn(columnName = "IS_ACTIVE", boAttributeName = DSPDMConstants.BoAttrName.IS_ACTIVE, columnType = Boolean.class, nullable = false, length = 1)
    private Boolean isActive = null;
    @DatabaseColumn(columnName = "BO_ATTR_NAME", boAttributeName = DSPDMConstants.BoAttrName.BO_ATTR_NAME, columnType = String.class, nullable = true, length = 50)
    private String boAttrName = null;
    @DatabaseColumn(columnName = "BO_NAME", boAttributeName = DSPDMConstants.BoAttrName.BO_NAME, columnType = String.class, nullable = true, length = 50)
    private String boName = null;
    @DatabaseColumn(columnName = "VERIFY", boAttributeName = DSPDMConstants.BoAttrName.VERIFY, columnType = Boolean.class, nullable = false, length = 1)
    private Boolean verify = null;
    
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
    
    public String getEntityName() {
        return entityName;
    }
    
    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }
    
    public String getAttributeName() {
        return attributeName;
    }
    
    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }
    
    public String getConstraintName() {
        return constraintName;
    }
    
    public void setConstraintName(String constraintName) {
        this.constraintName = constraintName;
    }
    
    public Boolean getActive() {
        return isActive;
    }
    
    public void setActive(Boolean active) {
        isActive = active;
    }
    
    public String getBoAttrName() {
        return boAttrName;
    }
    
    public void setBoAttrName(String boAttrName) {
        this.boAttrName = boAttrName;
    }
    
    public String getBoName() {
        return boName;
    }
    
    public void setBoName(String boName) {
        this.boName = boName;
    }

    public Boolean getVerify() {
        return verify;
    }

    public void setVerify(Boolean verify) {
        this.verify = verify;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BusinessObjectAttributeConstraintsDTO that = (BusinessObjectAttributeConstraintsDTO) o;
        return Objects.equals(constraintName, that.constraintName) &&
                Objects.equals(boAttrName, that.boAttrName) &&
                Objects.equals(boName, that.boName);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(constraintName, boAttrName, boName);
    }
}
