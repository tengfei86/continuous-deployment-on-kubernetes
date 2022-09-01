package com.lgc.dspdm.core.common.data.criteria;

import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateColumn;
import com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateFunction;
import com.lgc.dspdm.core.common.data.criteria.aggregate.HavingFilter;
import com.lgc.dspdm.core.common.data.criteria.join.BaseJoinClause;
import com.lgc.dspdm.core.common.data.criteria.join.DynamicJoinClause;
import com.lgc.dspdm.core.common.data.criteria.join.DynamicTable;
import com.lgc.dspdm.core.common.data.criteria.join.SimpleJoinClause;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;

import java.io.Serializable;
import java.util.*;

public class BusinessObjectInfo implements Serializable, Cloneable {
    private String businessObjectType = null;
    private String alias = null;
    private SelectList selectList = null;
    private FilterList filterList = null;
    private List<SimpleJoinClause> simpleJoins;
    private List<DynamicJoinClause> dynamicJoins;
    private OrderBy orderBy = null;
    private Pagination pagination = null;
    private List<String> parentBONamesToRead = null;
    private List<BusinessObjectInfo> childBONamesToRead = null;
    private boolean readBack = false;
    private boolean readUnique = false;
    private boolean readFirst = false;
    private boolean readMetadata = false;
    private boolean readMetadataConstraints = false;
    private boolean readReferenceData = false;
    private boolean readReferenceDataForFilters = false;
    private boolean readReferenceDataConstraints = false;
    private boolean readAllRecords = false;
    private boolean readRecordsCount = false;
    private boolean readWithDistinct = false;
    private boolean prependAlias = false;
    private Boolean isUploadNeeded = null;
    private String unitPolicy;
    private List<DSPDMUnit> dspdmUnits;
    private boolean selectAll = true;
    // fields map for unit conversion 
    private HashMap<String, HashMap<String, String>> unitFieldMap ;
    // filter fields map for unit conversion
    private HashMap<String, HashMap<String, String>> unitFieldFilterMap ;


    /* **************************************************** */
    /* ******************* CONSTRUCTOR ******************** */
    /* **************************************************** */
    public BusinessObjectInfo(String businessObjectType, ExecutionContext executionContext) {
        this.businessObjectType = businessObjectType;
        this.pagination = new Pagination();
    }

    public BusinessObjectInfo clone(ExecutionContext executionContext) {
        BusinessObjectInfo businessObjectInfo = null;
        try {
            businessObjectInfo = (BusinessObjectInfo) this.clone();
        } catch (CloneNotSupportedException e) {
            DSPDMException.throwException(e, executionContext);
        }
		if (businessObjectInfo != null) {
			// select list
			if (this.selectList != null) {
				businessObjectInfo.selectList = this.selectList.clone(executionContext);
			}
			// filter list
			if (this.filterList != null) {
				businessObjectInfo.filterList = this.filterList.clone(executionContext);
			}
			// order by
			if (this.orderBy != null) {
				businessObjectInfo.orderBy = this.orderBy.clone(executionContext);
			}
			// pagination
			if (this.pagination != null) {
				businessObjectInfo.pagination = this.pagination.clone(executionContext);
			}
			// simple join list
            if (this.simpleJoins != null) {
                businessObjectInfo.simpleJoins = new ArrayList<>(this.simpleJoins.size());
                for (SimpleJoinClause simpleJoinClause : this.simpleJoins) {
                    businessObjectInfo.simpleJoins.add(simpleJoinClause.clone(executionContext));
                }
            }
            // dynamic join list
            if (this.dynamicJoins != null) {
                businessObjectInfo.dynamicJoins = new ArrayList<>(this.dynamicJoins.size());
                for (DynamicJoinClause dynamicJoinClause : this.dynamicJoins) {
                    businessObjectInfo.dynamicJoins.add(dynamicJoinClause.clone(executionContext));
                }
            }
			// parent bo names to read
			if (this.parentBONamesToRead != null) {
				businessObjectInfo.parentBONamesToRead = new ArrayList<>(this.parentBONamesToRead);
			}
			// child bo names to read
			if (this.childBONamesToRead != null) {
				businessObjectInfo.childBONamesToRead = new ArrayList<>(this.childBONamesToRead);
			}
		}
        return businessObjectInfo;
    }

    /* **************************************************** */
    /* ********************* GETTERS ********************** */
    /* **************************************************** */

    public String getBusinessObjectType() {
        return businessObjectType;
    }

    public OrderBy getOrderBy() {
        return orderBy;
    }

    public Pagination getPagination() {
        return pagination;
    }

    public SelectList getSelectList() {
        return selectList;
    }

    public List<String> getParentBONamesToRead() {
        return parentBONamesToRead;
    }

    public List<BusinessObjectInfo> getChildBONamesToRead() {
        return childBONamesToRead;
    }

    public FilterList getFilterList() {
        return filterList;
    }

    public List<SimpleJoinClause> getSimpleJoins() {
        return simpleJoins;
    }

    public List<DynamicJoinClause> getDynamicJoins() {
        return dynamicJoins;
    }

    public List<DSPDMUnit> getDSPDMUnits() {
        return dspdmUnits;
    }

    public HashMap<String, HashMap<String, String>> getUnitFieldMap() {
        return unitFieldMap;
    }

    public void setUnitFieldMap(HashMap<String, HashMap<String, String>> unitFieldMap) {
        this.unitFieldMap = unitFieldMap;
    }
    
    public HashMap<String, HashMap<String, String>> getUnitFieldFilterMap() {
		return unitFieldFilterMap;
	}

	public void setUnitFieldFilterMap(HashMap<String, HashMap<String, String>> unitFieldFilterMap) {
		this.unitFieldFilterMap = unitFieldFilterMap;
	}
	
    /* **************************************************** */
    /* ************** SQL RELATED FUNCTIONS *************** */
    /* **************************************************** */

	public BusinessObjectInfo addOrderByAsc(String attributeName) {
        if (orderBy == null) {
            orderBy = new OrderBy();
        }
        orderBy.addOrderByAsc(attributeName);
        return this;
    }

    public BusinessObjectInfo addOrderByDesc(String attributeName) {
        if (orderBy == null) {
            orderBy = new OrderBy();
        }
        orderBy.addOrderByDesc(attributeName);
        return this;
    }

    public BusinessObjectInfo addPagesToRead(ExecutionContext executionContext, Integer recordsPerPage, Integer... pageNumbersToRead) {
        if (CollectionUtils.isNullOrEmpty(pageNumbersToRead)) {
            pageNumbersToRead = new Integer[]{PagedList.FIRST_PAGE_NUMBER};
        }
        pagination.setRecordsPerPage(recordsPerPage, executionContext).addPagesToRead(executionContext, pageNumbersToRead);
        return this;
    }

    public BusinessObjectInfo setRecordsAndPages(ExecutionContext executionContext, Integer recordsPerPage, Integer... pageNumbersToRead) {
        if (CollectionUtils.isNullOrEmpty(pageNumbersToRead)) {
            pageNumbersToRead = new Integer[]{PagedList.FIRST_PAGE_NUMBER};
        }
        pagination.setRecordsPerPage(recordsPerPage, executionContext).setPagesToRead(executionContext, pageNumbersToRead);
        return this;
    }
    
    public BusinessObjectInfo setRecordsPerPage(Integer recordsPerPage, ExecutionContext executionContext) {
        pagination.setRecordsPerPage(recordsPerPage, executionContext);
        if(CollectionUtils.isNullOrEmpty(pagination.getPagesToRead())) {
            pagination.setPagesToRead(executionContext, PagedList.FIRST_PAGE_NUMBER);
        }
        return this;
    }

    public BusinessObjectInfo addParentBONamesToRead(List<String> parentBONamesToRead) {
        if (CollectionUtils.hasValue(parentBONamesToRead)) {
            if (this.parentBONamesToRead == null) {
                this.parentBONamesToRead = new ArrayList<>();
            }
            this.parentBONamesToRead.addAll(parentBONamesToRead);
        }
        return this;
    }

    public BusinessObjectInfo addChildBONamesToRead(List<BusinessObjectInfo> childBONamesToRead) {
        if (CollectionUtils.hasValue(childBONamesToRead)) {
            if (this.childBONamesToRead == null) {
                this.childBONamesToRead = new ArrayList<>();
            }
            this.childBONamesToRead.addAll(childBONamesToRead);
        }
        return this;
    }

    public void clearSelectList() {
        if (selectList != null) {
            selectList = new SelectList();
        }
    }

    public BusinessObjectInfo addColumnsToSelect(String... columnsToSelect) {
        if (selectList == null) {
            selectList = new SelectList();
        }
        selectList.addColumnsToSelect(columnsToSelect);
        return this;
    }

    public BusinessObjectInfo addColumnsToSelect(List<String> columnsToSelect) {
        if (selectList == null) {
            selectList = new SelectList();
        }
        selectList.addColumnsToSelect(columnsToSelect);
        return this;
    }

    public BusinessObjectInfo addAggregateColumnToSelect(String boAttrName, AggregateFunction aggregateFunction, String columnAlias) {
        if (selectList == null) {
            selectList = new SelectList();
        }
        selectList.addAggregateColumnsToSelect(new AggregateColumn(aggregateFunction, boAttrName, columnAlias));
        return this;
    }

    public BusinessObjectInfo addDSPDMUnits(DSPDMUnit dspdmUnit) {
        if (dspdmUnits == null) {
            dspdmUnits = new ArrayList<DSPDMUnit>();
        }
        dspdmUnits.add(dspdmUnit);
        return this;
    }

    public BusinessObjectInfo addAggregateColumnToSelect(AggregateColumn aggregateColumns) {
        if (selectList == null) {
            selectList = new SelectList();
        }
        selectList.addAggregateColumnsToSelect(aggregateColumns);
        return this;
    }

    public BusinessObjectInfo addAggregateColumnsToSelect(AggregateColumn... aggregateColumns) {
        if (selectList == null) {
            selectList = new SelectList();
        }
        selectList.addAggregateColumnsToSelect(aggregateColumns);
        return this;
    }

    public BusinessObjectInfo addAggregateColumnsToSelect(List<AggregateColumn> aggregateColumns) {
        if (selectList == null) {
            selectList = new SelectList();
        }
        selectList.addAggregateColumnsToSelect(aggregateColumns);
        return this;
    }

    public boolean hasSimpleColumnToSelect() {
        return ((selectList != null) && (CollectionUtils.hasValue(selectList.getColumnsToSelect())));
    }

    public boolean hasAggregateColumnToSelect() {
        return ((selectList != null) && (CollectionUtils.hasValue(selectList.getAggregateColumnsToSelect())));
    }

    public BusinessObjectInfo addFilter(String columnName, Object... values) {
        if (filterList == null) {
            filterList = new FilterList();
        }
        if ((values != null) && (values.length == 1) && (values[0] != null)) {
            if (values[0].getClass().isArray()) {
                List<Object> list = Arrays.asList(values[0]);
                values = list.toArray();
            } else if (values[0] instanceof Collection) {
                values = ((Collection) values[0]).toArray();
            }
        }
        if (values.length == 1) {
            filterList.addFilter(columnName, Operator.EQUALS, values);
        } else {
            filterList.addFilter(columnName, Operator.IN, values);
        }
        return this;
    }

    public BusinessObjectInfo addFilter(String columnName, Operator operator, Object... values) {
        return addFilter(new CriteriaFilter(columnName, operator, values));
    }

    public BusinessObjectInfo addFilter(CriteriaFilter criteriaFilter) {
        if (filterList == null) {
            filterList = new FilterList();
        }
        Object[] values = criteriaFilter.getValues();
        if ((values != null) && (values.length == 1) && (values[0] != null)) {
            if (values[0].getClass().isArray()) {
                List<Object> list = Arrays.asList(values[0]);
                values = list.toArray();
                criteriaFilter.replaceValues(values);
            } else if (values[0] instanceof Collection) {
                values = ((Collection) values[0]).toArray();
                criteriaFilter.replaceValues(values);
            }
        }
        filterList.addFilter(criteriaFilter);
        return this;
    }

    public BusinessObjectInfo addFilterGroup(FilterGroup filterGroup) {
        if (filterList == null) {
            filterList = new FilterList();
        }
        filterList.addFilterGroup(filterGroup);
        return this;
    }

    public BusinessObjectInfo addFilterGroups(Collection<FilterGroup> filterGroups) {
        if (filterList == null) {
            filterList = new FilterList();
        }
        filterList.addFilterGroups(filterGroups);
        return this;
    }

    public BusinessObjectInfo addHavingFilter(AggregateColumn aggregateColumn, Operator operator, Object... value) {
        return addHavingFilter(new HavingFilter(aggregateColumn, operator, value));
    }

    public BusinessObjectInfo addHavingFilter(HavingFilter havingFilter) {
        if (filterList == null) {
            filterList = new FilterList();
        }
        Object[] values = havingFilter.getValues();
        if ((values != null) && (values.length == 1) && (values[0] != null)) {
            if (values[0].getClass().isArray()) {
                List<Object> list = Arrays.asList(values[0]);
                values = list.toArray();
                havingFilter.replaceValues(values);
            } else if (values[0] instanceof Collection) {
                values = ((Collection) values[0]).toArray();
                havingFilter.replaceValues(values);
            }
        }
        filterList.addHavingFilter(havingFilter);
        return this;
    }

    public BusinessObjectInfo addSimpleJoin(SimpleJoinClause simpleJoinClause) {
        if (simpleJoins == null) {
            simpleJoins = new ArrayList<>();
        }
        simpleJoins.add(simpleJoinClause);
        return this;
    }

    public BusinessObjectInfo addDynamicJoin(DynamicJoinClause dynamicJoinClause) {
        if (dynamicJoins == null) {
            dynamicJoins = new ArrayList<>();
        }
        dynamicJoins.add(dynamicJoinClause);
        return this;
    }

    public String getAlias() {
        return alias;
    }

    public BusinessObjectInfo setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public String getUnitPolicy() {
        return unitPolicy;
    }

    public BusinessObjectInfo setUnitPolicy(String unitPolicy) {
        this.unitPolicy = unitPolicy;
        return this;
    }

    public boolean isReadBack() {
        return readBack;
    }

    public BusinessObjectInfo setReadBack(boolean readBack) {
        this.readBack = readBack;
        return this;
    }

    public boolean isReadUnique() {
        return readUnique;
    }

    public BusinessObjectInfo setReadUnique(boolean readUnique) {
        this.readUnique = readUnique;
        return this;
    }

    public boolean isReadFirst() {
        return readFirst;
    }

    public BusinessObjectInfo setReadFirst(boolean readFirst) {
        this.readFirst = readFirst;
        return this;
    }

    public boolean isReadMetadata() {
        return readMetadata;
    }

    public BusinessObjectInfo setReadMetadata(boolean readMetadata) {
        this.readMetadata = readMetadata;
        return this;
    }

    public boolean isReadReferenceData() {
        return readReferenceData;
    }

    public BusinessObjectInfo setReadReferenceData(boolean readReferenceData) {
        this.readReferenceData = readReferenceData;
        return this;
    }

    public boolean isReadReferenceDataForFilters() {
        return readReferenceDataForFilters;
    }

    public BusinessObjectInfo setReadReferenceDataForFilters(boolean readReferenceDataForFilters) {
        this.readReferenceDataForFilters = readReferenceDataForFilters;
        return this;
    }

    public boolean isReadReferenceDataConstraints() {
        return readReferenceDataConstraints;
    }

    public BusinessObjectInfo setReadReferenceDataConstraints(boolean readReferenceDataConstraints) {
        this.readReferenceDataConstraints = readReferenceDataConstraints;
        return this;
    }

    public boolean isReadAllRecords() {
        return readAllRecords;
    }

    public BusinessObjectInfo setReadAllRecords(boolean readAllRecords) {
        this.readAllRecords = readAllRecords;
        return this;
    }

    public boolean isReadMetadataConstraints() {
        return readMetadataConstraints;
    }

    public BusinessObjectInfo setReadMetadataConstraints(boolean readMetadataConstraints) {
        this.readMetadataConstraints = readMetadataConstraints;
        return this;
    }

    public boolean isReadRecordsCount() {
        return readRecordsCount;
    }

    public BusinessObjectInfo setReadRecordsCount(boolean readRecordsCount) {
        this.readRecordsCount = readRecordsCount;
        return this;
    }

    public boolean isReadWithDistinct() {
        return readWithDistinct;
    }

    public BusinessObjectInfo setReadWithDistinct(boolean readWithDistinct) {
        this.readWithDistinct = readWithDistinct;
        return this;
    }

    public boolean isPrependAlias() {
        return prependAlias;
    }

    public BusinessObjectInfo setPrependAlias(boolean prependAlias) {
        this.prependAlias = prependAlias;
        return this;
    }

    //it is false when selectList has values or simplejoins selectList has values or dynamicJoins selectList has values
    //or it is true
    public boolean isSelectAll() {
        return selectAll;
    }

    public BusinessObjectInfo setSelectAll(boolean selectAll) {
        this.selectAll = selectAll;
        return this;
    }

    public Boolean isUploadNeeded() {
        return isUploadNeeded;
    }

    public BusinessObjectInfo setUploadNeeded(Boolean uploadNeeded) {
        isUploadNeeded = uploadNeeded;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BusinessObjectInfo{");
        sb.append("businessObjectType=").append(businessObjectType);
        sb.append(", orderBy=").append(orderBy);
        sb.append(", pagination=").append(pagination);
        sb.append(", selectList=").append(selectList);
        sb.append(", parentBONamesToRead=").append((parentBONamesToRead == null) ? null : CollectionUtils.getCommaSeparated(parentBONamesToRead));
        sb.append(", childBONamesToRead=").append((childBONamesToRead == null) ? null : childBONamesToRead.size());
        sb.append(", filterList=").append(filterList);
        sb.append(", readBack=").append(readBack);
        sb.append(", readUnique=").append(readUnique);
        sb.append(", readFirst=").append(readFirst);
        sb.append(", readMetadata=").append(readMetadata);
        sb.append(", readMetadataConstraints=").append(readMetadataConstraints);
        sb.append(", readReferenceData=").append(readReferenceData);
        sb.append(", readReferenceDataForFilters=").append(readReferenceDataForFilters);
        sb.append(", readReferenceDataConstraints=").append(readReferenceDataConstraints);
        sb.append(", readAllRecords=").append(readAllRecords);
        sb.append(", readRecordsCount=").append(readRecordsCount);
        sb.append(", readWithDistinct=").append(readWithDistinct);
        sb.append(", prependAlias=").append(prependAlias);
        sb.append(", isUploadNeeded=").append(isUploadNeeded);
        sb.append('}');
        return sb.toString();
    }

    public boolean isJoiningRead() {
        return (StringUtils.hasValue(alias)) && ((CollectionUtils.hasValue(simpleJoins)) || (CollectionUtils.hasValue(dynamicJoins)));
    }

    public boolean hasSimpleJoinsToRead() {
        return (StringUtils.hasValue(alias)) && (CollectionUtils.hasValue(simpleJoins));
    }

    public boolean hasDynamicJoinsToRead() {
        return (StringUtils.hasValue(alias)) && (CollectionUtils.hasValue(dynamicJoins));
    }

	public BusinessObjectInfo addUnitField(String boName, HashMap<String, String> boAttNameList) {
		if (unitFieldMap == null) {
			unitFieldMap = new HashMap<String, HashMap<String, String>>();
		}
		if (unitFieldMap.containsKey(boName)) {
			HashMap<String, String> boAttList = unitFieldMap.get(boName);
			boAttList.putAll(boAttNameList);
			// for (Map.Entry<String, String> boAttName : boAttNameList.entrySet()) {
			// if (!boAttList.containsKey(boAttName.getKey())) {
			// boAttList.put(boAttName.getKey(), boAttName.getValue());
			// }
			// }
		} else {
			unitFieldMap.put(boName, boAttNameList);
		}
		return this;
	}
    
    public BusinessObjectInfo addUnitFieldFilter(String boName, HashMap<String, String> boAttNameList) {
    	if (unitFieldFilterMap == null) {
    		unitFieldFilterMap = new HashMap<String, HashMap<String, String>>();
		}
        if (unitFieldFilterMap.containsKey(boName)) {
            HashMap<String, String> boAttList = unitFieldFilterMap.get(boName);
            boAttList.putAll(boAttNameList);
        } else {
            unitFieldFilterMap.put(boName, boAttNameList);
        }
        return this;
    }
    
    /**
     * Returns a new list of containing all the simple and dynamic joins according to their order. If no order is specified then simple joins will come first
     *
     * @return
     * @author Muhammad Imran Ansari
     * @since 30-Jul-2020
     */
    public List<BaseJoinClause> getAllJoinsInTheirJoinOrder() {
        List<BaseJoinClause> orderedJoins = new LinkedList<>();
        if (CollectionUtils.hasValue(simpleJoins)) {
            BaseJoinClause.addJoinsToListAsPerTheirOrder(orderedJoins, simpleJoins);
        }
        if (CollectionUtils.hasValue(dynamicJoins)) {
            BaseJoinClause.addJoinsToListAsPerTheirOrder(orderedJoins, dynamicJoins);
        }
        return orderedJoins;
    }

    /**
     * Find the first join clause matching the given bo name
     *
     * @param boName
     * @return
     * @author Muhammad Imran Ansari
     * @since 16-Dec-2021
     */
    public BaseJoinClause findFirstJoinClauseForBoName(String boName) {
        BaseJoinClause matchedBaseJoinClause = null;
        if (isJoiningRead()) {
            List<BaseJoinClause> orderedJoins = getAllJoinsInTheirJoinOrder();
            for (BaseJoinClause baseJoinClause : orderedJoins) {
                if (baseJoinClause instanceof SimpleJoinClause) {
                    SimpleJoinClause simpleJoinClause = (SimpleJoinClause) baseJoinClause;
                    if (simpleJoinClause.getBoName().equalsIgnoreCase(boName)) {
                        matchedBaseJoinClause = simpleJoinClause;
                        break;
                    }
                } else if (baseJoinClause instanceof DynamicJoinClause) {
                    DynamicJoinClause dynamicJoinClause = (DynamicJoinClause) baseJoinClause;
                    for (DynamicTable dynamicTable : dynamicJoinClause.getDynamicTablesInTheirJoinOrder()) {
                        if (dynamicTable.getBoName().equalsIgnoreCase(boName)) {
                            matchedBaseJoinClause = dynamicTable;
                            break;
                        }
                    }
                }
            }
        }
        return matchedBaseJoinClause;
    }

    /**
     * Find the first join clause matching the given bo name
     *
     * @param joinAlias
     * @return
     * @author Muhammad Imran Ansari
     * @since 16-Dec-2021
     */
    public BaseJoinClause findFirstJoinClauseForJoinAlias(String joinAlias) {
        BaseJoinClause matchedBaseJoinClause = null;
        if (isJoiningRead()) {
            List<BaseJoinClause> orderedJoins = getAllJoinsInTheirJoinOrder();
            for (BaseJoinClause baseJoinClause : orderedJoins) {
                if (baseJoinClause instanceof SimpleJoinClause) {
                    SimpleJoinClause simpleJoinClause = (SimpleJoinClause) baseJoinClause;
                    if (simpleJoinClause.getJoinAlias().equalsIgnoreCase(joinAlias)) {
                        matchedBaseJoinClause = simpleJoinClause;
                        break;
                    }
                } else if (baseJoinClause instanceof DynamicJoinClause) {
                    DynamicJoinClause dynamicJoinClause = (DynamicJoinClause) baseJoinClause;
                    if (dynamicJoinClause.getJoinAlias().equalsIgnoreCase(joinAlias)) {
                        matchedBaseJoinClause = dynamicJoinClause;
                        break;
                    } else {
                        for (DynamicTable dynamicTable : dynamicJoinClause.getDynamicTablesInTheirJoinOrder()) {
                            if (dynamicTable.getBoName().equalsIgnoreCase(joinAlias)) {
                                matchedBaseJoinClause = dynamicTable;
                                break;
                            }
                        }
                    }
                }
            }
        }
        return matchedBaseJoinClause;
    }
}
