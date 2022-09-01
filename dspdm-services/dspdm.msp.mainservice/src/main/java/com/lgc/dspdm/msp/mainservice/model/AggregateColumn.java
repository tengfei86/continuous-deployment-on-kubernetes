package com.lgc.dspdm.msp.mainservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateFunction;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Aggregate column class to be used in json post request body
 *
 * @author muhammadimran.ansari
 * @since 23-Apr-2020
 */
@XmlRootElement
@Schema(name = "AggregateColumn", description = "aggregate column for boquery")
public class AggregateColumn {

    @JsonProperty("boAttrName")
    @Schema(name = "boAttrName", description = "BO attribute name an aggregate function to be applied on it")
    private String boAttrName = null;

    @JsonProperty("aggregateFunction")
    @Schema(name = "aggregateFunction", description = "aggregate function to be applied to the boAttrName. Supported values are AVG, COUNT, MAX, MIN, SUM")
    private AggregateFunction aggregateFunction = null;

    @JsonProperty("alias")
    @Schema(name = "alias", description = "Alias name to the given aggregate column. Using this alias the values can be retrieved back")
    private String alias = null;

    public AggregateColumn() {
    }

    public AggregateColumn(String boAttrName, AggregateFunction aggregateFunction) {
        this.boAttrName = boAttrName;
        this.aggregateFunction = aggregateFunction;
    }

    public AggregateColumn(String boAttrName, AggregateFunction aggregateFunction, String alias) {
        this.boAttrName = boAttrName;
        this.aggregateFunction = aggregateFunction;
        this.alias = alias;
    }

    public String getBoAttrName() {
        return boAttrName;
    }

    public AggregateFunction getAggregateFunction() {
        return aggregateFunction;
    }

    public String getAlias() {
        return alias;
    }

	public void setBoAttrName(String boAttrName) {
		this.boAttrName = boAttrName;
	}

	public void setAggregateFunction(AggregateFunction aggregateFunction) {
		this.aggregateFunction = aggregateFunction;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}
	
}
