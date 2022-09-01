package com.lgc.dspdm.core.common.data.criteria.join;

import com.lgc.dspdm.core.common.data.criteria.*;
import com.lgc.dspdm.core.common.data.criteria.aggregate.HavingFilter;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public abstract class BaseJoinClause implements Serializable, Cloneable{
    private String joinAlias;
    private JoinType joinType;
    private Byte joinOrder;
    private SelectList selectList;
    private List<JoiningCondition> joiningConditions;
    private FilterList filterList = null;
    private OrderBy orderBy = null;

    /**
     * constructor will be invoked from simple join nd dynamic join clause
     *
     * @param joinAlias
     * @param joinType
     */
    public BaseJoinClause(String joinAlias, JoinType joinType) {
        this.joinAlias = joinAlias;
        this.joinType = joinType;
    }

    /**
     * This constructor will be invoked from dynamic table clause
     *
     * @param selectList
     */
    public BaseJoinClause(SelectList selectList) {
        this.selectList = selectList;
    }

    public String getJoinAlias() {
        return joinAlias;
    }

    public void setJoinAlias(String joinAlias) {
        this.joinAlias = joinAlias;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public void setJoinType(JoinType joinType) {
        this.joinType = joinType;
    }

    public Byte getJoinOrder() {
        return joinOrder;
    }

    public void setJoinOrder(Byte joinOrder) {
        this.joinOrder = joinOrder;
    }

    public SelectList getSelectList() {
        return selectList;
    }

    public void setSelectList(SelectList selectList) {
        this.selectList = selectList;
    }

    public List<JoiningCondition> getJoiningConditions() {
        return joiningConditions;
    }

    public void setJoiningConditions(List<JoiningCondition> joiningConditions) {
        this.joiningConditions = joiningConditions;
    }

    public FilterList getFilterList() {
        return filterList;
    }

    public void setFilterList(FilterList filterList) {
        this.filterList = filterList;
    }

    public OrderBy getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(OrderBy orderBy) {
        this.orderBy = orderBy;
    }

    public boolean hasSimpleColumnToSelect() {
        return ((selectList != null) && (CollectionUtils.hasValue(selectList.getColumnsToSelect())));
    }

    public boolean hasAggregateColumnToSelect() {
        return ((selectList != null) && (CollectionUtils.hasValue(selectList.getAggregateColumnsToSelect())));
    }

    private void addFilter(BaseFilter filter) {
        if (filterList == null) {
            filterList = new FilterList();
        }
        Object[] values = filter.getValues();
        if ((values != null) && (values.length == 1) && (values[0] != null)) {
            if (values[0].getClass().isArray()) {
                List<Object> list = Arrays.asList(values[0]);
                filter.replaceValues(list.toArray());
            } else if (values[0] instanceof Collection) {
                filter.replaceValues(((Collection) values[0]).toArray());
            }
        }
        if (filter instanceof CriteriaFilter) {
            filterList.addFilter((CriteriaFilter) filter);
        } else {
            filterList.addHavingFilter((HavingFilter) filter);
        }
    }

    public void addCriteriaFilter(List<CriteriaFilter> criteriaFilters) {
        for (CriteriaFilter filter : criteriaFilters) {
            addCriteriaFilter(filter);
        }
    }

    public void addCriteriaFilter(CriteriaFilter criteriaFilter) {
        addFilter(criteriaFilter);
    }

    public void addFilterGroup(List<FilterGroup> filterGroups) {
    	for (FilterGroup filterGroup : filterGroups) {
    		addFilterGroup(filterGroup);
        }
    }

	public void addFilterGroup(FilterGroup filterGroup) {
		if (filterList == null) {
			filterList = new FilterList();
		}
		filterList.addFilterGroup(filterGroup);
	}
    
    public void addHavingFilter(List<HavingFilter> havingFilters) {
        for (HavingFilter filter : havingFilters) {
            addHavingFilter(filter);
        }
    }

    public void addHavingFilter(HavingFilter havingFilter) {
        addFilter(havingFilter);
    }

    public void addOrderByAsc(String attributeName) {
        if (orderBy == null) {
            orderBy = new OrderBy();
        }
        orderBy.addOrderByAsc(attributeName);
    }

    public void addOrderByDesc(String attributeName) {
        if (orderBy == null) {
            orderBy = new OrderBy();
        }
        orderBy.addOrderByDesc(attributeName);
    }

    /**
     * static and stateless method to be used in derived classes
     *
     * @param joinsAlreadyAdded
     * @param joinsToAdd
     */
    public static void addJoinsToListAsPerTheirOrder(List<BaseJoinClause> joinsAlreadyAdded, List<? extends BaseJoinClause> joinsToAdd) {
        for (BaseJoinClause joinToAdd : joinsToAdd) {
            if (joinToAdd.getJoinOrder() == null) {
                joinsAlreadyAdded.add(joinToAdd);
            } else {
                int currentIndex = 0;
                boolean added = false;
                for (BaseJoinClause joinAlreadyAdded : joinsAlreadyAdded) {
                    if ((joinAlreadyAdded.getJoinOrder() != null) && (joinToAdd.getJoinOrder() < joinAlreadyAdded.getJoinOrder())) {
                        // add the new one on the current index and move all by one index to the right
                        joinsAlreadyAdded.add(currentIndex, joinToAdd);
                        added = true;
                        break;
                    }
                    currentIndex++;
                }
                if (!added) {
                    // add the new one at the end of ordered joins list because
                    // it has the highest order among those which are already existing in the list
                    joinsAlreadyAdded.add(joinToAdd);
                }
            }
        }
    }

    public BaseJoinClause clone(ExecutionContext executionContext) {
        BaseJoinClause baseJoinClause = null;
        try {
            baseJoinClause = (BaseJoinClause) this.clone();
            // select list
            if (this.selectList != null) {
                baseJoinClause.selectList = this.selectList.clone(executionContext);
            }
            // filter list
            if (this.joiningConditions != null) {
                baseJoinClause.joiningConditions = new ArrayList<>(this.joiningConditions);
            }
            // filter list
            if (this.filterList != null) {
                baseJoinClause.filterList = this.filterList.clone(executionContext);
            }
            // order by
            if (this.orderBy != null) {
                baseJoinClause.orderBy = this.orderBy.clone(executionContext);
            }
        } catch (CloneNotSupportedException e) {
            DSPDMException.throwException(e, executionContext);
        }
        return baseJoinClause;
    }
}
