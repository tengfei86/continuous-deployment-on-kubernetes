package com.lgc.dspdm.core.common.data.dto.fixed;

import com.lgc.dspdm.core.common.data.annotation.DatabaseColumn;
import com.lgc.dspdm.core.common.data.annotation.DatabaseTable;
import com.lgc.dspdm.core.common.data.annotation.PrimaryKey;
import com.lgc.dspdm.core.common.data.dto.IBaseDTO;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.metadata.MetadataUtils;
import com.lgc.dspdm.core.common.util.StringUtils;

import java.sql.Types;
import java.util.Objects;

@PrimaryKey(javaPropertyNames = { "id" }, boAttrNames = { "BUSINESS_OBJECT_ATTR_ID" }, physicalColumnNames = {
		"BUSINESS_OBJECT_ATTR_ID" })
@DatabaseTable(tableName = "BUSINESS_OBJECT_ATTR", boName = "BUSINESS OBJECT ATTR", schemaName = "", remarks = "Metadata table Business Object Attribute to contain structure information of all other tables along with their attributes")
public class BusinessObjectAttributeDTO implements IBaseDTO<Integer> {

	public static enum properties {
		/**
		 * constants to be used in code
		 */
        id,
        boAttrName,
        boName,
        boDisplayName,
        attributeName,
        attributeDatatype,
        attributeDefault,
        attributeDesc,
        attributeDisplayName,
        controlType,
        entityName,
        historyAttribute,
        historyTable,
        isActive,
        isHidden,
        isMandatory,
        isPrimaryKey,
        isReferenceInd,
        isSortable,
        isUploadNeeded,
        isInternal,
        isReadOnly,
		relatedBoAttrName,
        referenceBOName,
        referenceBOAttrNameForValue,
        referenceBOAttrNameForLabel,
        schemaName,
        sequenceNumber,
        isCustomAttribute,
		unit
	}

	;

	/*
	 * do not change the definition order of the field. BO_NAME AND BO_ATTR_NAME
	 * must be defined earlier
	 */
	// BUSINESS OBJECT ATTRIBUTE ID
	@DatabaseColumn(columnName = "BUSINESS_OBJECT_ATTR_ID", boAttributeName = DSPDMConstants.BoAttrName.BUSINESS_OBJECT_ATTR_ID, columnType = Integer.class, nullable = false, length = 10)
	private Integer id = null;
	// BO
	@DatabaseColumn(columnName = "BO_ATTR_NAME", boAttributeName = DSPDMConstants.BoAttrName.BO_ATTR_NAME, columnType = String.class, nullable = false, length = 50)
	private String boAttrName = null;
	@DatabaseColumn(columnName = "BO_NAME", boAttributeName = DSPDMConstants.BoAttrName.BO_NAME, columnType = String.class, nullable = false, length = 50)
	private String boName = null;
	@DatabaseColumn(columnName = "BO_DISPLAY_NAME", boAttributeName = DSPDMConstants.BoAttrName.BO_DISPLAY_NAME, columnType = String.class, nullable = false, length = 500)
	private String boDisplayName = null;
	// ATTRIBUTE
	@DatabaseColumn(columnName = "ATTRIBUTE", boAttributeName = DSPDMConstants.BoAttrName.ATTRIBUTE, columnType = String.class, nullable = true, length = 30)
	private String attributeName = null;
	@DatabaseColumn(columnName = "ATTRIBUTE_DATATYPE", boAttributeName = DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE, columnType = String.class, nullable = true, length = 150)
	private String attributeDatatype = null;
	@DatabaseColumn(columnName = "ATTRIBUTE_DEFAULT", boAttributeName = DSPDMConstants.BoAttrName.ATTRIBUTE_DEFAULT, columnType = String.class, nullable = true, length = 100)
	private String attributeDefault = null;
	@DatabaseColumn(columnName = "ATTRIBUTE_DESC", boAttributeName = DSPDMConstants.BoAttrName.ATTRIBUTE_DESC, columnType = String.class, nullable = true, length = 500)
	private String attributeDesc = null;
	@DatabaseColumn(columnName = "ATTRIBUTE_DISPLAYNAME", boAttributeName = DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME, columnType = String.class, nullable = true, length = 30)
	private String attributeDisplayName = null;
	// CONTROL TYPE
	@DatabaseColumn(columnName = "CONTROL_TYPE", boAttributeName = DSPDMConstants.BoAttrName.CONTROL_TYPE, columnType = String.class, nullable = true, length = 50)
	private String controlType = null;
	// ENTITY
	@DatabaseColumn(columnName = "ENTITY", boAttributeName = DSPDMConstants.BoAttrName.ENTITY, columnType = String.class, nullable = true, length = 50)
	private String entityName = null;
	// HISTORY
	@DatabaseColumn(columnName = "HISTORY_ATTRIBUTE", boAttributeName = DSPDMConstants.BoAttrName.HISTORY_ATTRIBUTE, columnType = String.class, nullable = true, length = 50)
	private String historyAttribute = null;
	@DatabaseColumn(columnName = "HISTORY_TABLE", boAttributeName = DSPDMConstants.BoAttrName.HISTORY_TABLE, columnType = String.class, nullable = true, length = 50)
	private String historyTable = null;
	// boolean variables in database they are character 'y' or 'n'
	@DatabaseColumn(columnName = "IS_ACTIVE", boAttributeName = DSPDMConstants.BoAttrName.IS_ACTIVE, columnType = Boolean.class, nullable = false, length = 1)
	private Boolean isActive = null;
	@DatabaseColumn(columnName = "IS_HIDDEN", boAttributeName = DSPDMConstants.BoAttrName.IS_HIDDEN, columnType = Boolean.class, nullable = false, length = 1)
	private Boolean isHidden = null;
	@DatabaseColumn(columnName = "IS_MANDATORY", boAttributeName = DSPDMConstants.BoAttrName.IS_MANDATORY, columnType = Boolean.class, nullable = false, length = 1)
	private Boolean isMandatory = null;
	@DatabaseColumn(columnName = "IS_PRIMARY_KEY", boAttributeName = DSPDMConstants.BoAttrName.IS_PRIMARY_KEY, columnType = Boolean.class, nullable = false, length = 1)
	private Boolean isPrimaryKey = null;
	@DatabaseColumn(columnName = "IS_REFERENCE_IND", boAttributeName = DSPDMConstants.BoAttrName.IS_REFERENCE_IND, columnType = Boolean.class, nullable = false, length = 1)
	private Boolean isReferenceInd = null;
	@DatabaseColumn(columnName = "IS_SORTABLE", boAttributeName = DSPDMConstants.BoAttrName.IS_SORTABLE, columnType = Boolean.class, nullable = false, length = 1)
	private Boolean isSortable = null;
	@DatabaseColumn(columnName = "IS_UPLOAD_NEEDED", boAttributeName = DSPDMConstants.BoAttrName.IS_UPLOAD_NEEDED, columnType = Boolean.class, nullable = false, length = 1)
	private Boolean isUploadNeeded = null;
	@DatabaseColumn(columnName = "IS_INTERNAL", boAttributeName = DSPDMConstants.BoAttrName.IS_INTERNAL, columnType = Boolean.class, nullable = false, length = 1)
	private Boolean isInternal = null;
	@DatabaseColumn(columnName = "IS_READ_ONLY", boAttributeName = DSPDMConstants.BoAttrName.IS_READ_ONLY, columnType = Boolean.class, nullable = false, length = 1)
	private Boolean isReadOnly = null;
	//related boAttrName in bo
	@DatabaseColumn(columnName = "RELATED_BO_ATTR_NAME", boAttributeName = DSPDMConstants.BoAttrName.RELATED_BO_ATTR_NAME, columnType = String.class, nullable = true, length = 50)
	private String relatedBoAttrName = null;
	// REFERENCEBoAttrName
	@DatabaseColumn(columnName = "REFERENCE_BO_NAME", boAttributeName = DSPDMConstants.BoAttrName.REFERENCE_BO_NAME, columnType = String.class, nullable = true, length = 50)
	private String referenceBOName = null;
	@DatabaseColumn(columnName = "REFERENCE_BO_ATTR_VALUE", boAttributeName = DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_VALUE, columnType = String.class, nullable = true, length = 50)
	private String referenceBOAttrNameForValue = null;
	@DatabaseColumn(columnName = "REFERENCE_BO_ATTR_LABEL", boAttributeName = DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_LABEL, columnType = String.class, nullable = true, length = 50)
	private String referenceBOAttrNameForLabel = null;
	@DatabaseColumn(columnName = "SCHEMA_NAME", boAttributeName = DSPDMConstants.BoAttrName.SCHEMA_NAME, columnType = String.class, nullable = true, length = 50)
	private String schemaName = null;
	@DatabaseColumn(columnName = "SEQUENCE_NUM", boAttributeName = DSPDMConstants.BoAttrName.SEQUENCE_NUM, columnType = Integer.class, nullable = false, length = 10)
	private Integer sequenceNumber = null;
	// UNIT
	@DatabaseColumn(columnName = "IS_CUSTOM_ATTRIBUTE", boAttributeName = DSPDMConstants.BoAttrName.IS_CUSTOM_ATTRIBUTE, columnType = Boolean.class, nullable = false, length = 1)
	private Boolean isCustomAttribute = null;
	@DatabaseColumn(columnName = "UNIT", boAttributeName = DSPDMConstants.BoAttrName.UNIT, columnType = String.class, nullable = true, length = DSPDMConstants.Units.UNIT_MAX_LENGTH)
	private String unit = null;
	/* **************************************************** */
	/* ***************** Non-DB Fields ******************** */
	/* **************************************************** */
	private transient Class javaDataType = null;
	private transient int sqlDataType = Types.NULL;
	private transient int maxAllowedLength = 0;
	private transient int maxAllowedDecimalPlaces = 0;
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

	public String getBoDisplayName() {
		return boDisplayName;
	}

	public void setBoDisplayName(String boDisplayName) {
		this.boDisplayName = boDisplayName;
	}

	public String getAttributeName() {
		return attributeName;
	}

	public void setAttributeName(String attributeName) {
		this.attributeName = attributeName;
	}

	public String getAttributeDatatype() {
		return attributeDatatype;
	}

	public void setAttributeDatatype(String attributeDatatype) {
		this.attributeDatatype = attributeDatatype;
		if (StringUtils.hasValue(this.attributeDatatype)) {
			// bo_attr_name must be initialized before this call because field
			// initialization takes place in order they are defined
			javaDataType = MetadataUtils.getJavaDataTypeFromString(attributeDatatype, boAttrName, boName,
					ExecutionContext.getSystemUserExecutionContext());
			sqlDataType = MetadataUtils.getSQLDataTypeFromString(attributeDatatype, boAttrName, boName,
					ExecutionContext.getSystemUserExecutionContext());
			maxAllowedLength = MetadataUtils.getDataTypeLengthFromString(attributeDatatype, boAttrName, boName,
					ExecutionContext.getSystemUserExecutionContext());
			maxAllowedDecimalPlaces = MetadataUtils.getDataTypeDecimalLengthFromString(attributeDatatype, boAttrName,
					boName, ExecutionContext.getSystemUserExecutionContext());
		}
	}

	public String getAttributeDefault() {
		return attributeDefault;
	}

	public void setAttributeDefault(String attributeDefault) {
		this.attributeDefault = attributeDefault;
	}

	public String getAttributeDesc() {
		return attributeDesc;
	}

	public void setAttributeDesc(String attributeDesc) {
		this.attributeDesc = attributeDesc;
	}

	public String getAttributeDisplayName() {
		return attributeDisplayName;
	}

	public void setAttributeDisplayName(String attributeDisplayName) {
		this.attributeDisplayName = attributeDisplayName;
	}

	public String getControlType() {
		return controlType;
	}

	public void setControlType(String controlType) {
		this.controlType = controlType;
	}

	public String getEntityName() {
		return entityName;
	}

	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}

	public String getHistoryAttribute() {
		return historyAttribute;
	}

	public void setHistoryAttribute(String historyAttribute) {
		this.historyAttribute = historyAttribute;
	}

	public String getHistoryTable() {
		return historyTable;
	}

	public void setHistoryTable(String historyTable) {
		this.historyTable = historyTable;
	}

	public Boolean getActive() {
		return isActive;
	}

	public void setActive(Boolean active) {
		isActive = active;
	}

	public Boolean getHidden() {
		return isHidden;
	}

	public void setHidden(Boolean hidden) {
		isHidden = hidden;
	}

	public Boolean getMandatory() {
		return isMandatory;
	}

	public void setMandatory(Boolean mandatory) {
		isMandatory = mandatory;
	}

	public Boolean getPrimaryKey() {
		return isPrimaryKey;
	}

	public void setPrimaryKey(Boolean primaryKey) {
		isPrimaryKey = primaryKey;
	}

	public Boolean getReferenceInd() {
		return isReferenceInd;
	}

	public void setReferenceInd(Boolean referenceInd) {
		isReferenceInd = referenceInd;
	}

	public Boolean getSortable() {
		return isSortable;
	}

	public void setSortable(Boolean sortable) {
		isSortable = sortable;
	}

	public Boolean getUploadNeeded() {
		return isUploadNeeded;
	}

	public void setUploadNeeded(Boolean uploadNeeded) {
		isUploadNeeded = uploadNeeded;
	}

	public Boolean getInternal() {
		return isInternal;
	}

	public void setInternal(Boolean internal) {
		isInternal = internal;
	}

	public Boolean getReadOnly() {
		return isReadOnly;
	}

	public void setReadOnly(Boolean readOnly) {
		isReadOnly = readOnly;
	}

	public String getRelatedBoAttrName() {
		return relatedBoAttrName;
	}

	public void setRelatedBoAttrName(String relatedBoAttrName) {
		this.relatedBoAttrName = relatedBoAttrName;
	}

	public String getReferenceBOName() {
		return referenceBOName;
	}

	public void setReferenceBOName(String referenceBOName) {
		this.referenceBOName = referenceBOName;
	}

	public String getReferenceBOAttrNameForValue() {
		return referenceBOAttrNameForValue;
	}

	public void setReferenceBOAttrNameForValue(String referenceBOAttrNameForValue) {
		this.referenceBOAttrNameForValue = referenceBOAttrNameForValue;
	}

	public String getReferenceBOAttrNameForLabel() {
		return referenceBOAttrNameForLabel;
	}

	public void setReferenceBOAttrNameForLabel(String referenceBOAttrNameForLabel) {
		this.referenceBOAttrNameForLabel = referenceBOAttrNameForLabel;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public String getUnit() {
		return unit;
	}

	public void setUnit(String unit) {
		this.unit = unit;
	}

	public Integer getSequenceNumber() {
		return sequenceNumber;
	}

	public void setSequenceNumber(Integer sequenceNumber) {
		this.sequenceNumber = sequenceNumber;
	}

	public Boolean getCustomAttribute() {
		return isCustomAttribute;
	}

	public void setCustomAttribute(Boolean customAttribute) {
		isCustomAttribute = customAttribute;
	}

	/* **************************************************** */
	/* ********** Non-DB Fields Getters Only ************** */
	/* **************************************************** */

	public Class getJavaDataType() {
		return javaDataType;
	}

	public int getSqlDataType() {
		return sqlDataType;
	}

	public int getMaxAllowedLength() {
		return maxAllowedLength;
	}

	public int getMaxAllowedDecimalPlaces() {
		return maxAllowedDecimalPlaces;
	}

	public boolean isUpdateAuditField() {
		return (DSPDMConstants.BoAttrName.ROW_CHANGED_BY.equalsIgnoreCase(boAttrName)
				|| DSPDMConstants.BoAttrName.ROW_CHANGED_DATE.equalsIgnoreCase(boAttrName));
	}

	public boolean isCreateAuditField() {
		return (DSPDMConstants.BoAttrName.ROW_CREATED_BY.equalsIgnoreCase(boAttrName)
				|| DSPDMConstants.BoAttrName.ROW_CREATED_DATE.equalsIgnoreCase(boAttrName));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		BusinessObjectAttributeDTO that = (BusinessObjectAttributeDTO) o;
        return sqlDataType == that.sqlDataType &&
                maxAllowedLength == that.maxAllowedLength &&
                maxAllowedDecimalPlaces == that.maxAllowedDecimalPlaces &&
                Objects.equals(id, that.id) &&
                Objects.equals(boAttrName, that.boAttrName) &&
                Objects.equals(boName, that.boName) &&
                Objects.equals(boDisplayName, that.boDisplayName) &&
                Objects.equals(attributeName, that.attributeName) &&
                Objects.equals(attributeDatatype, that.attributeDatatype) &&
                Objects.equals(attributeDefault, that.attributeDefault) &&
                Objects.equals(attributeDesc, that.attributeDesc) &&
                Objects.equals(attributeDisplayName, that.attributeDisplayName) &&
                Objects.equals(controlType, that.controlType) &&
                Objects.equals(entityName, that.entityName) &&
                Objects.equals(historyAttribute, that.historyAttribute) &&
                Objects.equals(historyTable, that.historyTable) &&
                Objects.equals(isActive, that.isActive) &&
                Objects.equals(isHidden, that.isHidden) &&
                Objects.equals(isMandatory, that.isMandatory) &&
                Objects.equals(isPrimaryKey, that.isPrimaryKey) &&
                Objects.equals(isReferenceInd, that.isReferenceInd) &&
                Objects.equals(isSortable, that.isSortable) &&
                Objects.equals(isUploadNeeded, that.isUploadNeeded) &&
                Objects.equals(isInternal, that.isInternal) &&
                Objects.equals(isReadOnly, that.isReadOnly) &&
				Objects.equals(relatedBoAttrName, that.relatedBoAttrName) &&
                Objects.equals(referenceBOName, that.referenceBOName) &&
                Objects.equals(referenceBOAttrNameForValue, that.referenceBOAttrNameForValue) &&
                Objects.equals(referenceBOAttrNameForLabel, that.referenceBOAttrNameForLabel) &&
                Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(sequenceNumber, that.sequenceNumber) &&
                Objects.equals(isCustomAttribute, that.isCustomAttribute) &&
				Objects.equals(unit, that.unit) &&
                Objects.equals(javaDataType, that.javaDataType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, boAttrName, boName, boDisplayName, attributeName, attributeDatatype, attributeDefault,
				attributeDesc, attributeDisplayName, controlType,
				entityName, historyAttribute, historyTable, isActive, isHidden, isMandatory,
				isPrimaryKey, isReferenceInd, isSortable, isUploadNeeded, isInternal, isReadOnly, relatedBoAttrName,
				referenceBOName, referenceBOAttrNameForValue, referenceBOAttrNameForLabel, schemaName, sequenceNumber,
				isCustomAttribute, unit, javaDataType, sqlDataType, maxAllowedLength,
				maxAllowedDecimalPlaces);
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("BusinessObjectAttributeDTO{");
		sb.append("id=").append(id);
		sb.append(", boAttrName=").append(boAttrName);
		sb.append(", boName=").append(boName);
		sb.append(", boDisplayName=").append(boDisplayName);
		sb.append(", attributeName=").append(attributeName);
		sb.append(", attributeDatatype=").append(attributeDatatype);
		sb.append(", attributeDefault=").append(attributeDefault);
		sb.append(", attributeDesc=").append(attributeDesc);
		sb.append(", attributeDisplayName=").append(attributeDisplayName);
		sb.append(", controlType=").append(controlType);
		sb.append(", entityName=").append(entityName);
		sb.append(", historyAttribute=").append(historyAttribute);
		sb.append(", historyTable=").append(historyTable);
		sb.append(", isActive=").append(isActive);
		sb.append(", isHidden=").append(isHidden);
		sb.append(", isMandatory=").append(isMandatory);
		sb.append(", isPrimaryKey=").append(isPrimaryKey);
		sb.append(", isReferenceInd=").append(isReferenceInd);
		sb.append(", isSortable=").append(isSortable);
		sb.append(", isUploadNeeded=").append(isUploadNeeded);
		sb.append(", isInternal=").append(isInternal);
		sb.append(", isReadOnly=").append(isReadOnly);
		sb.append(", relatedBoAttrName=").append(relatedBoAttrName);
		sb.append(", referenceBOName=").append(referenceBOName);
		sb.append(", referenceBOAttrNameForValue=").append(referenceBOAttrNameForValue);
		sb.append(", referenceBOAttrNameForLabel=").append(referenceBOAttrNameForLabel);
		sb.append(", schemaName=").append(schemaName);
		sb.append(", sequenceNumber=").append(sequenceNumber);
		sb.append(", isCustomAttribute=").append(isCustomAttribute);
		sb.append(", unit=").append(unit);
		sb.append(", javaDataType=").append(javaDataType);
		sb.append(", sqlDataType=").append(sqlDataType);
		sb.append(", maxAllowedLength=").append(maxAllowedLength);
		sb.append(", maxAllowedDecimalPlaces=").append(maxAllowedDecimalPlaces);
		sb.append('}');
		return sb.toString();
	}
}
