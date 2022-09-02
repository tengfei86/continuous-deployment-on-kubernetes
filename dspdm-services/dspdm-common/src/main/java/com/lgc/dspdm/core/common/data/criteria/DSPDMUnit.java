package com.lgc.dspdm.core.common.data.criteria;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class DSPDMUnit implements Serializable  {
	private String boName;
	private String boAttrId;
	private String boAttrName;
	private String sourceUnit;
	private String targetUnit;
	private String boAttrAliasName;
	private int maxDecimalLengthFromDataType;
	private List<Unit> canConversionUnits;
	
	public String getBoName() {
		return boName;
	}

	public void setBoName(String boName) {
		this.boName = boName;
	}

	public String getBoAttrId() {
		return boAttrId;
	}

	public void setBoAttrId(String boAttrId) {
		this.boAttrId = boAttrId;
	}

	public String getBoAttrName() {
		return boAttrName;
	}

	public void setBoAttrName(String boAttrName) {
		this.boAttrName = boAttrName;
	}

	public String getSourceUnit() {
		return sourceUnit;
	}

	public void setSourceUnit(String sourceUnit) {
		this.sourceUnit = sourceUnit;
	}

	public String getTargetUnit() {
		return targetUnit;
	}

	public void setTargetUnit(String targetUnit) {
		this.targetUnit = targetUnit;
	}

	public String getBoAttrAliasName() {
		return boAttrAliasName;
	}

	public void setBoAttrAliasName(String boAttrAliasName) {
		this.boAttrAliasName = boAttrAliasName;
	}

	public int getMaxDecimalLengthFromDataType() {
		return maxDecimalLengthFromDataType;
	}

	public void setMaxDecimalLengthFromDataType(int maxDecimalLengthFromDataType) {
		this.maxDecimalLengthFromDataType = maxDecimalLengthFromDataType;
	}

	public List<Unit> getCanConversionUnits() {
		return canConversionUnits;
	}

	public void setCanConversionUnits(List<Unit> canConversionUnits) {
		this.canConversionUnits = canConversionUnits;
	}

	public DSPDMUnit() {}
	
	public DSPDMUnit(String boName, String boAttrId, String boAttrName,  
			String targetUnit) {
		this.boName = boName;
		this.boAttrId = boAttrId;
		this.boAttrName = boAttrName; 
		this.targetUnit = targetUnit;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		DSPDMUnit dspdmUnit = (DSPDMUnit) o;
		return Objects.equals(boName, dspdmUnit.boName) && Objects.equals(boAttrId, dspdmUnit.boAttrId) && Objects.equals(boAttrName, dspdmUnit.boAttrName) && Objects.equals(sourceUnit, dspdmUnit.sourceUnit) && Objects.equals(targetUnit, dspdmUnit.targetUnit) && Objects.equals(boAttrAliasName, dspdmUnit.boAttrAliasName);
	}

	@Override
	public int hashCode() {
		return Objects.hash(boName, boAttrId, boAttrName, sourceUnit, targetUnit, boAttrAliasName);
	}
}
