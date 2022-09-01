package com.lgc.dspdm.msp.mainservice.model;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Having filter class to be used in json post request body
 * Having filter can only be applied to the aggregate columns.
 * For normal columns please use criteria filter
 *
 * @author muhammadimran.ansari
 * @since 23-Apr-2020
 */
@XmlRootElement
@Schema(name = "HavingFilter", description = "having condition for boquery only to be applied on aggregate columns")
public class HavingFilter {

    @JsonProperty("aggregateColumn")
    @Schema(name = "aggregateColumn", description = "aggregate column to be applied as a having filter")
    private AggregateColumn aggregateColumn = null;

    @JsonProperty("operator")
    @Schema(name = "operator", description = "operator to be applied to the filter condition. Supported values are EQUALS, NOT_EQUALS, GREATER_THAN, LESS_THAN, GREATER_OR_EQUALS, LESS_OR_EQUALS, BETWEEN, NOT_BETWEEN, IN, NOT_IN, LIKE, NOT_LIKE, ILIKE, NOT_ILIKE, JSONB_FIND_EXACT, JSONB_FIND_LIKE, JSONB_DOT, JSONB_DOT_FOR_TEXT")
    Operator operator;

    @JsonProperty("values")
    @Schema(name = "values", description = "Array of values to be applied to the filter")
    Object[] values;

    public HavingFilter() {
    }

    public HavingFilter(AggregateColumn aggregateColumn, Operator operator, Object[] values) {
        this.aggregateColumn = aggregateColumn;
        this.operator = operator;
        this.values = values;
    }

    public AggregateColumn getAggregateColumn() {
        return aggregateColumn;
    }

    public Operator getOperator() {
        return operator;
    }

    public Object[] getValues() {
        return values;
    }

	public void setAggregateColumn(AggregateColumn aggregateColumn) {
		this.aggregateColumn = aggregateColumn;
	}

	public void setOperator(Operator operator) {
		this.operator = operator;
	}

	public void setValues(Object[] values) {
		this.values = values;
	}
}
