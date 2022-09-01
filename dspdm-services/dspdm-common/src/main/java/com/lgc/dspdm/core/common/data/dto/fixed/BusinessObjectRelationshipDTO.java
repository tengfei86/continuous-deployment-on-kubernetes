package com.lgc.dspdm.core.common.data.dto.fixed;

import com.lgc.dspdm.core.common.data.annotation.DatabaseColumn;
import com.lgc.dspdm.core.common.data.annotation.DatabaseTable;
import com.lgc.dspdm.core.common.data.annotation.PrimaryKey;
import com.lgc.dspdm.core.common.data.dto.IBaseDTO;
import com.lgc.dspdm.core.common.util.DSPDMConstants;

import java.util.Objects;

@PrimaryKey(javaPropertyNames = {"id"}, boAttrNames = {"BUS_OBJ_RELATIONSHIP_ID"}, physicalColumnNames = {"BUS_OBJ_RELATIONSHIP_ID"})
@DatabaseTable(tableName = "BUS_OBJ_RELATIONSHIP", boName = "BUS OBJ RELATIONSHIP", schemaName = "", remarks = "Metadata table to contain relationships between business object")
public class BusinessObjectRelationshipDTO implements IBaseDTO<Integer> {
    /**
     * constants to be used in code
     */
    public static enum properties {
        id,
        name,
        constraintName,
        childBoName,
        childBoAttrName,
        parentBoName,
        parentBoAttrName,
        isPrimaryKeyRelationship,
        childEntityName,
        parentEntityName,
        childOrder
    }
    
    ;
    
    @DatabaseColumn(columnName = "BUS_OBJ_RELATIONSHIP_ID", boAttributeName = DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_ID, columnType = Integer.class, nullable = false, length = 10)
    private Integer id = null;
    @DatabaseColumn(columnName = "BUS_OBJ_RELATIONSHIP_NAME", boAttributeName = DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME, columnType = String.class, nullable = true, length = 200)
    private String name = null;
    @DatabaseColumn(columnName = "CONSTRAINT_NAME", boAttributeName = DSPDMConstants.BoAttrName.CONSTRAINT_NAME, columnType = String.class, nullable = true, length = 200)
    private String constraintName = null;
    @DatabaseColumn(columnName = "CHILD_BO_NAME", boAttributeName = DSPDMConstants.BoAttrName.CHILD_BO_NAME, columnType = String.class, nullable = true, length = 50)
    private String childBoName = null;
    @DatabaseColumn(columnName = "CHILD_BO_ATTR_NAME", boAttributeName = DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME, columnType = String.class, nullable = true, length = 50)
    private String childBoAttrName = null;
    @DatabaseColumn(columnName = "PARENT_BO_NAME", boAttributeName = DSPDMConstants.BoAttrName.PARENT_BO_NAME, columnType = String.class, nullable = true, length = 50)
    private String parentBoName = null;
    @DatabaseColumn(columnName = "PARENT_BO_ATTR_NAME", boAttributeName = DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME, columnType = String.class, nullable = true, length = 50)
    private String parentBoAttrName = null;
    @DatabaseColumn(columnName = "IS_PRIMARY_KEY_RELATIONSHIP", boAttributeName = DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, columnType = Boolean.class, nullable = false, length = 1)
    private Boolean isPrimaryKeyRelationship = null;
    @DatabaseColumn(columnName = "CHILD_ENTITY_NAME", boAttributeName = DSPDMConstants.BoAttrName.CHILD_ENTITY_NAME, columnType = String.class, nullable = false, length = 30)
    private String childEntityName = null;
    @DatabaseColumn(columnName = "PARENT_ENTITY_NAME", boAttributeName = DSPDMConstants.BoAttrName.PARENT_ENTITY_NAME, columnType = String.class, nullable = false, length = 30)
    private String parentEntityName = null;
    @DatabaseColumn(columnName = "CHILD_ORDER", boAttributeName = DSPDMConstants.BoAttrName.CHILD_ORDER, columnType = Integer.class, nullable = false, length = 2)
    private Integer childOrder = null;

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
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }

    public String getConstraintName() {
        return constraintName;
    }

    public void setConstraintName(String constraintName) {
        this.constraintName = constraintName;
    }

    public String getChildBoName() {
        return childBoName;
    }
    
    public void setChildBoName(String childBoName) {
        this.childBoName = childBoName;
    }
    
    public String getChildBoAttrName() {
        return childBoAttrName;
    }
    
    public void setChildBoAttrName(String childBoAttrName) {
        this.childBoAttrName = childBoAttrName;
    }
    
    public String getParentBoName() {
        return parentBoName;
    }
    
    public void setParentBoName(String parentBoName) {
        this.parentBoName = parentBoName;
    }
    
    public String getParentBoAttrName() {
        return parentBoAttrName;
    }
    
    public void setParentBoAttrName(String parentBoAttrName) {
        this.parentBoAttrName = parentBoAttrName;
    }
    
    public Boolean getPrimaryKeyRelationship() {
        return isPrimaryKeyRelationship;
    }
    
    public void setPrimaryKeyRelationship(Boolean primaryKeyRelationship) {
        isPrimaryKeyRelationship = primaryKeyRelationship;
    }

    public String getChildEntityName() {
        return childEntityName;
    }

    public void setChildEntityName(String childEntityName) {
        this.childEntityName = childEntityName;
    }

    public String getParentEntityName() {
        return parentEntityName;
    }

    public void setParentEntityName(String parentEntityName) {
        this.parentEntityName = parentEntityName;
    }

    public Integer getChildOrder() { return childOrder; }

    public void setChildOrder(Integer childOrder) { this.childOrder = childOrder; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BusinessObjectRelationshipDTO that = (BusinessObjectRelationshipDTO) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(name, that.name) &&
                Objects.equals(constraintName, that.constraintName) &&
                Objects.equals(childBoName, that.childBoName) &&
                Objects.equals(childBoAttrName, that.childBoAttrName) &&
                Objects.equals(parentBoName, that.parentBoName) &&
                Objects.equals(parentBoAttrName, that.parentBoAttrName) &&
                Objects.equals(isPrimaryKeyRelationship, that.isPrimaryKeyRelationship) &&
                Objects.equals(childEntityName, that.childEntityName) &&
                Objects.equals(parentEntityName, that.parentEntityName)&&
                Objects.equals(childOrder, that.childOrder);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id, name, constraintName, childBoName, childBoAttrName, parentBoName, parentBoAttrName, isPrimaryKeyRelationship, childEntityName, parentEntityName, childOrder);
    }
}
