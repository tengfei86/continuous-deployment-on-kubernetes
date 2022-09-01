package com.lgc.dspdm.msp.mainservice.model;

import com.fasterxml.jackson.annotation.JsonProperty; 

import io.swagger.v3.oas.annotations.media.Schema;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.List;

@XmlRootElement
@Schema(name = "DSPDMUnit", description = "Unitversion CustomAttributeUnit option")
public class DSPDMUnit implements Serializable {
	@JsonProperty("boName")
	@Schema(name = "boName", description = "boName")
	private String boName;

	@JsonProperty("boAttrId")
	@Schema(name = "boAttrId", description = "boAttrId")
	private String boAttrId;

	@JsonProperty("boAttrName")
	@Schema(name = "boAttrName", description = "boAttrName")
	private String boAttrName;

	@JsonProperty("targetUnit")
	@Schema(name = "targetUnit", description = "targetUnit")
	String targetUnit;
  
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


	public String getTargetUnit() {
		return targetUnit;
	}

	public void setTargetUnit(String targetUnit) {
		this.targetUnit = targetUnit;
	}
  
	public DSPDMUnit() {
	}

	public DSPDMUnit(String boName, String boAttrId, String boAttrName, String sourceUnit,
			String targetUnit ) {
		this.boName = boName;
		this.boAttrId = boAttrId;
		this.boAttrName = boAttrName;
		this.targetUnit = targetUnit; 
	}
}
