package com.lgc.dspdm.msp.mainservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

@XmlRootElement
@Schema(name = "CriteriaFilter", description = "criteria condition for boquery")
public class CriteriaFilter implements Serializable {

    @JsonProperty("boAttrName")
    @Schema(name = "boAttrName", description = "BO attribute name to be applied as a filter")
    String boAttrName;

    @JsonProperty("operator")
    @Schema(name = "operator", description = "operator to be applied to the filter condition. Supported values are EQUALS, NOT_EQUALS, GREATER_THAN, LESS_THAN, GREATER_OR_EQUALS, LESS_OR_EQUALS, BETWEEN, NOT_BETWEEN, IN, NOT_IN, LIKE, NOT_LIKE, ILIKE, NOT_ILIKE, JSONB_FIND_EXACT, JSONB_FIND_LIKE, JSONB_DOT, JSONB_DOT_FOR_TEXT")
    Operator operator;

    @JsonProperty("values")
    @Schema(name = "values", description = "Array of values to be applied to the filter")
    Object[] values;

    public CriteriaFilter() {
    }

    public String getBoAttrName() {
        return boAttrName;
    }

    public Operator getOperator() {
        return operator;
    }

    public Object[] getValues() {
        return values;
    }

    public void setBoAttrName(String boAttrName) {
		this.boAttrName = boAttrName;
	}

	public void setOperator(Operator operator) {
		this.operator = operator;
	}

	public void setValues(Object[] values) {
		this.values = values;
	}

	public CriteriaFilter(String boAttrName, Operator operator, Object[] values) {
        this.boAttrName = boAttrName;
        this.operator = operator;
        this.values = values;
    }
}
