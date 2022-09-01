package com.lgc.dspdm.msp.mainservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

@XmlRootElement
@Schema(name = "FilterGroup", description = "filter group  for boquery")
public class FilterGroup implements Serializable {

    @JsonProperty("groupOperator")
    @Schema(name = "groupOperator", description = "operator as a filter group")
    Operator groupOperator;

    @JsonProperty("combineWith")
    @Schema(name = "combineWith", description = "combine operator in a filter group criteria")
    Operator combineWith;

    @JsonProperty("criteriaFilters")
    @Schema(name = "criteriaFilters", description = "criteria filters in a filter group")
    CriteriaFilter[] criteriaFilters;

    @JsonProperty("filterGroups")
    @Schema(name = "filterGroups", description = "filter groups list in a filter group")
    FilterGroup[] filterGroups;
    
    public FilterGroup() {
    }

    public Operator getGroupOperator() {
		return groupOperator;
	}

	public Operator getCombineWith() {
		return combineWith;
	}

	public CriteriaFilter[] getCriteriaFilters() {
		return criteriaFilters;
	}

	public FilterGroup[] getFilterGroups() {
		return filterGroups;
	}

    public void setGroupOperator(Operator groupOperator) {
		this.groupOperator = groupOperator;
	}

	public void setCombineWith(Operator combineWith) {
		this.combineWith = combineWith;
	}

	public void setCriteriaFilters(CriteriaFilter[] criteriaFilters) {
		this.criteriaFilters = criteriaFilters;
	}

	public void setFilterGroups(FilterGroup[] filterGroups) {
		this.filterGroups = filterGroups;
	}

	public FilterGroup(Operator groupOperator, Operator combineWith, CriteriaFilter[] criteriaFilters) {
        this.groupOperator = groupOperator;
        this.combineWith = combineWith;
        this.criteriaFilters = criteriaFilters;
    }
    
    public FilterGroup(Operator groupOperator, Operator combineWith, CriteriaFilter[] criteriaFilters, FilterGroup[] filterGroups) {
        this.groupOperator = groupOperator;
        this.combineWith = combineWith;
        this.criteriaFilters = criteriaFilters;
        this.filterGroups = filterGroups;
    }
}
