package com.lgc.dspdm.core.common.data.criteria;

import com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateColumn;
import com.lgc.dspdm.core.common.data.criteria.aggregate.HavingFilter;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FilterList implements Serializable, Cloneable {
    private List<CriteriaFilter> filters = null;
    private List<FilterGroup> filterGroups = null;
    private List<HavingFilter> havingFilters = null;

    public FilterList clone(ExecutionContext executionContext) {
        FilterList filterList = new FilterList();
        if (this.filters != null) {
            filterList.filters = new ArrayList<>(this.filters);
        }
        if (this.filterGroups != null) {
            filterList.filterGroups = new ArrayList<>(this.filterGroups);
        }
        if (this.havingFilters != null) {
            filterList.havingFilters = new ArrayList<>(this.havingFilters);
        }
        return filterList;
    }

    public List<CriteriaFilter> getFilters() {
        return filters;
    }

    public List<FilterGroup> getFilterGroups() {
        return filterGroups;
    }

    public List<HavingFilter> getHavingFilters() {
        return havingFilters;
    }

    public FilterList addFilter(String columnName, Operator operator, Object... values) {
        return addFilter(new CriteriaFilter(columnName, operator, values));
    }

    public FilterList addFilter(CriteriaFilter criteriaFilter) {
        if (filters == null) {
            filters = new ArrayList<>();
        }
        filters.add(criteriaFilter);
        return this;
    }

    public FilterList addFilterGroup(Operator groupOperator, Operator combineWith, CriteriaFilter[] criteriaFilters, FilterGroup[] filterGroups) {
        return addFilterGroup(new FilterGroup(groupOperator, combineWith, criteriaFilters, filterGroups));
    }

    public FilterList addFilterGroup(FilterGroup filterGroup) {
        if (filterGroups == null) {
        	filterGroups = new ArrayList<>();
        }
        filterGroups.add(filterGroup);
        return this;
    }

    public FilterList addFilterGroups(Collection<FilterGroup> filterGroup) {
        if (filterGroups == null) {
            filterGroups = new ArrayList<>();
        }
        filterGroups.addAll(filterGroup);
        return this;
    }
    
    public FilterList addHavingFilter(AggregateColumn aggregateColumn, Operator operator, Object... value) {
        return addHavingFilter(new HavingFilter(aggregateColumn, operator, value));
    }

    public FilterList addHavingFilter(HavingFilter havingFilter) {
        if (havingFilters == null) {
            havingFilters = new ArrayList<>();
        }
        havingFilters.add(havingFilter);
        return this;
    }

    public Object[] getValuesWithOperator(Operator operator) {
        Object[] values = null;
        if (filters != null) {
            for (CriteriaFilter criteriaFilter : filters) {
                if (operator == criteriaFilter.getOperator()) {
                    values = criteriaFilter.getValues();
                    break;
                }
            }
        }
        return values;
    }

    public void removeFiltersWithOperator(Operator operator) {
        if (filters != null) {
            CriteriaFilter criteriaFilter = null;
            for (int i = 0; i < filters.size(); i++) {
                criteriaFilter = filters.get(i);
                if (operator == criteriaFilter.getOperator()) {
                    filters.remove(i);
                    i--;
                }
            }
        }
    }

    public CriteriaFilter getFilterForLongRangeINOrNotINSQL() {
        CriteriaFilter requiredFilter = null;
        if (filters != null) {
            for (CriteriaFilter criteriaFilter : filters) {
                if ((Operator.IN == criteriaFilter.getOperator()) || (Operator.NOT_IN == criteriaFilter.getOperator())) {
                    if ((criteriaFilter.getValues() != null) && (criteriaFilter.getValues().length > DSPDMConstants.SQL.MAX_SQL_IN_ARGS)) {
                        requiredFilter = criteriaFilter;
                        break;
                    }
                }
            }
        }
        return requiredFilter;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FilterList{");
        if (CollectionUtils.hasValue(filters)) {
            sb.append("filters=").append(CollectionUtils.getCommaSeparated(filters));
        }
        sb.append('}');
        return sb.toString();
    }
}
