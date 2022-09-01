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
 * Class is to be used for creating a read request for reading from multiple tables using sql join clause
 *
 * @author muhammadimran.ansari
 * @since 30-Apr-2020
 */
public class SimpleJoinClause {
    private static final long serialVersionUID = 1L;

    @JsonProperty("boName")
    @Schema(name = "boName", description = "WELL", required = true)
    private String boName;

    @JsonProperty("joinAlias")
    @Schema(name = "joinAlias", description = "Alias to be used in joins with other business objects")
    private String joinAlias;

    @JsonProperty("joinType")
    @Schema(name = "joinType", description = "Join type. Possible values are INNER, LEFT, RIGHT, FULL", required = true)
    private String joinType;

    @JsonProperty("joinOrder")
    @Schema(name = "joinOrder", description = "Join order to specify which join to apply first and which one after that and so on.", minLength = 1, maxLength = 4, minimum = "-127", maximum = "127")
    private Byte joinOrder;

    @JsonProperty("selectList")
    @Schema(name = "selectList", description = "fields to show")
    private List<String> selectList;

    @JsonProperty("aggregateSelectList")
    @Schema(name = "aggregateSelectList", description = "aggregate fields to show")
    private List<AggregateColumn> aggregateSelectList;

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

    @JsonProperty("joiningConditions")
    @Schema(name = "joiningConditions", description = "Zero or more joining conditions. It is optional and not required for business object for which relationships are already defined in metadata")
    private List<JoiningCondition> joiningConditions;

    public String getBoName() {
        return boName;
    }

    public SimpleJoinClause setBoName(String boName) {
        this.boName = boName;
        return this;
    }

    public String getJoinAlias() {
        return joinAlias;
    }

    public SimpleJoinClause setJoinAlias(String joinAlias) {
        this.joinAlias = joinAlias;
        return this;
    }

    public String getJoinType() {
        return joinType;
    }

    public SimpleJoinClause setJoinType(String joinType) {
        this.joinType = joinType;
        return this;
    }

    public Byte getJoinOrder() {
        return joinOrder;
    }

    public SimpleJoinClause setJoinOrder(Byte joinOrder) {
        this.joinOrder = joinOrder;
        return this;
    }

    public List<String> getSelectList() {
        return selectList;
    }

    public SimpleJoinClause setSelectList(List<String> selectList) {
        this.selectList = selectList;
        return this;
    }

    public List<AggregateColumn> getAggregateSelectList() {
        return aggregateSelectList;
    }

    public SimpleJoinClause setAggregateSelectList(List<AggregateColumn> aggregateSelectList) {
        this.aggregateSelectList = aggregateSelectList;
        return this;
    }

    public List<CriteriaFilter> getCriteriaFilters() {
        return criteriaFilters;
    }

    public SimpleJoinClause setCriteriaFilters(List<CriteriaFilter> criteriaFilters) {
        this.criteriaFilters = criteriaFilters;
        return this;
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

    public SimpleJoinClause setHavingFilters(List<HavingFilter> havingFilters) {
        this.havingFilters = havingFilters;
        return this;
    }

    public List<OrderBy> getOrderBy() {
        return orderBy;
    }

    public SimpleJoinClause setOrderBy(List<OrderBy> orderBy) {
        this.orderBy = orderBy;
        return this;
    }

    public List<JoiningCondition> getJoiningConditions() {
        return joiningConditions;
    }

    public SimpleJoinClause setJoiningConditions(List<JoiningCondition> joiningConditions) {
        this.joiningConditions = joiningConditions;
        return this;
    }
}
