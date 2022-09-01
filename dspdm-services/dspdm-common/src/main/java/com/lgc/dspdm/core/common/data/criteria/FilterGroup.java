package com.lgc.dspdm.core.common.data.criteria;

import java.io.Serializable;
import java.sql.Types;

/**
 * Class to be used for SQL where filter groups
 */
public class FilterGroup implements Serializable {
    private Operator groupOperator;
    private Operator combineWith;
    private CriteriaFilter[] criteriaFilters = null;
    private FilterGroup[] filterGroups = null;

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

	public Operator getGroupOperator() {
		return groupOperator;
	}

	public void setGroupOperator(Operator groupOperator) {
		this.groupOperator = groupOperator;
	}

	public Operator getCombineWith() {
		return combineWith;
	}

	public void setCombineWith(Operator combineWith) {
		this.combineWith = combineWith;
	}

	public CriteriaFilter[] getCriteriaFilters() {
		return criteriaFilters;
	}

	public void setCriteriaFilters(CriteriaFilter[] criteriaFilters) {
		this.criteriaFilters = criteriaFilters;
	}

	public FilterGroup[] getFilterGroups() {
		return filterGroups;
	}

	public void setFilterGroups(FilterGroup[] filterGroups) {
		this.filterGroups = filterGroups;
	}

    
}
