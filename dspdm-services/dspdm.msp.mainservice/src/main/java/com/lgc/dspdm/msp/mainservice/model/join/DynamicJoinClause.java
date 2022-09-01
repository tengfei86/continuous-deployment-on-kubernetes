package com.lgc.dspdm.msp.mainservice.model.join;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.lgc.dspdm.msp.mainservice.model.AggregateColumn;
import com.lgc.dspdm.msp.mainservice.model.CriteriaFilter;
import com.lgc.dspdm.msp.mainservice.model.FilterGroup;
import com.lgc.dspdm.msp.mainservice.model.HavingFilter;
import com.lgc.dspdm.msp.mainservice.model.OrderBy;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

/**
 * Class is to be used for creating a read request for reading from multiple non-existing tables using sql join clause
 *
 * @author muhammadimran.ansari
 * @since 30-Apr-2020
 */
public class DynamicJoinClause {

    @JsonProperty("selectList")
    @Schema(name = "selectList", description = "fields to show")
    private List<String> selectList;

    @JsonProperty("aggregateSelectList")
    @Schema(name = "aggregateSelectList", description = "aggregate fields to show")
    private List<AggregateColumn> aggregateSelectList;

    @JsonProperty("joinAlias")
    @Schema(name = "joinAlias", description = "Alias to be used to joins the result of this dynamic table with other existing business objects")
    private String joinAlias;

    @JsonProperty("joinType")
    @Schema(name = "joinType", description = "Join type. Possible values are INNER, LEFT, RIGHT, FULL", required = true)
    private String joinType;

    @JsonProperty("joinOrder")
    @Schema(name = "joinOrder", description = "Join order to specify which join to apply first and which one after that and so on.", minLength = 1, maxLength = 4, minimum = "-127", maximum = "127")
    private Byte joinOrder;

    @JsonProperty("dynamicTables")
    @Schema(name = "dynamicTables", description = "Dynamic table to select. Can have more than one tables joined using simple joins")
    private List<DynamicTable> dynamicTables;

    @JsonProperty("joiningConditions")
    @Schema(name = "joiningConditions", description = "At least one or more joining conditions on the basis table is going to be joined ")
    private List<JoiningCondition> joiningConditions;

    @JsonProperty("criteriaFilters")
    @Schema(name = "criteriaFilters", description = "where condition")
    private List<CriteriaFilter> criteriaFilters;
    
    @JsonProperty("filterGroups")
    @Schema(name = "filterGroups", description = "where condition groups")
    private List<FilterGroup> filterGroups;

    @JsonProperty("havingFilters")
    @Schema(name = "havingFilters", description = "having condition")
    private List<HavingFilter> havingFilters;

    @JsonProperty("orderBy")
    @Schema(name = "orderBy", description = "result order")
    private List<OrderBy> orderBy;

    public List<String> getSelectList() {
        return selectList;
    }

    public void setSelectList(List<String> selectList) {
        this.selectList = selectList;
    }

    public List<AggregateColumn> getAggregateSelectList() {
        return aggregateSelectList;
    }

    public void setAggregateSelectList(List<AggregateColumn> aggregateSelectList) {
        this.aggregateSelectList = aggregateSelectList;
    }

    public String getJoinAlias() {
        return joinAlias;
    }

    public void setJoinAlias(String joinAlias) {
        this.joinAlias = joinAlias;
    }

    public String getJoinType() {
        return joinType;
    }

    public void setJoinType(String joinType) {
        this.joinType = joinType;
    }

    public Byte getJoinOrder() {
        return joinOrder;
    }

    public void setJoinOrder(Byte joinOrder) {
        this.joinOrder = joinOrder;
    }

    public List<DynamicTable> getDynamicTables() {
        return dynamicTables;
    }

    public void setDynamicTables(List<DynamicTable> dynamicTables) {
        this.dynamicTables = dynamicTables;
    }

    public List<JoiningCondition> getJoiningConditions() {
        return joiningConditions;
    }

    public void setJoiningConditions(List<JoiningCondition> joiningConditions) {
        this.joiningConditions = joiningConditions;
    }

    public List<CriteriaFilter> getCriteriaFilters() {
        return criteriaFilters;
    }

    public void setCriteriaFilters(List<CriteriaFilter> criteriaFilters) {
        this.criteriaFilters = criteriaFilters;
    }

    public List<FilterGroup> getFilterGroups() {
		return filterGroups;
	}

	public void setFilterGroups(List<FilterGroup> filterGroups) {
		this.filterGroups = filterGroups;
	}

    public List<HavingFilter> getHavingFilters() {
        return havingFilters;
    }

    public void setHavingFilters(List<HavingFilter> havingFilters) {
        this.havingFilters = havingFilters;
    }

    public List<OrderBy> getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(List<OrderBy> orderBy) {
        this.orderBy = orderBy;
    }
}
