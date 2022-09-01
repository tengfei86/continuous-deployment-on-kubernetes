package com.lgc.dspdm.msp.mainservice.utils;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.config.WafByPassRulesProperties;
import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.SelectList;
import com.lgc.dspdm.core.common.data.criteria.join.JoinType;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.*;
import com.lgc.dspdm.msp.mainservice.model.*;
import com.lgc.dspdm.msp.mainservice.model.join.DynamicJoinClause;
import com.lgc.dspdm.msp.mainservice.model.join.DynamicTable;
import com.lgc.dspdm.msp.mainservice.model.join.JoiningCondition;
import com.lgc.dspdm.msp.mainservice.model.join.SimpleJoinClause;

import java.util.*;

/**
 * @author Yuan Tian
 */
public class BusinessObjectBuilder {

    public static BusinessObjectInfo build(BOQuery entity, ExecutionContext executionContext) {

        if (StringUtils.isNullOrEmpty(entity.getBoName())) {
            throw new DSPDMException("Parameter boName (case sensitive) is required", executionContext.getExecutorLocale());
        }
        // Bypass WAF Rules
        WAFRulesByPasser.byPassWafRules(entity);
        // SET LOCALE/LANGUAGE
        Locale locale = BusinessObjectValidator.validateLanguage(entity.getLanguage(), executionContext);
        // change locale in execution context
        executionContext.setExecutorLocale(locale);

        // SET TIMEZONE
        TimeZone timezone = entity.getTimezoneOffset() == null
                ? BusinessObjectValidator.validateTimezone(entity.getTimezone(), executionContext)
                : BusinessObjectValidator.validateTimezone(entity.getTimezoneOffset(), executionContext);


        // change timezone in execution context too
        executionContext.setExecutorTimeZone(timezone.toZoneId());

        // SET SHOW SQL STATS
        if (ConfigProperties.getInstance().allow_sql_stats_collection.getBooleanValue()) {
            Try.apply(() -> executionContext.setCollectSQLStats(entity.getShowSQLStats().booleanValue()));
            // SET COLLECT SQL SCRIPT
            CollectSQLScriptOptions option = CollectSQLScriptOptions.getByValue(entity.getCollectSQLScript());
            if (option != null) {
                executionContext.setCollectSQLScriptOptions(option);
            } else {
                throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                        "Invalid value '{}' given for param '{}'. {}", executionContext.getExecutorLocale(), entity.getCollectSQLScript(),
                        DSPDMConstants.DSPDM_REQUEST.COLLECT_SQL_SCRIPT_KEY, CollectSQLScriptOptions.description);
            }
        }

        // SET WRITE NULL VALUES
        Try.apply(() -> executionContext.setWriteNullValues(entity.getWriteNullValues().booleanValue()));

        // BO NAME
        BusinessObjectInfo bo = new BusinessObjectInfo(entity.getBoName(), executionContext);

        // Join Alias
        if (StringUtils.hasValue(entity.getJoinAlias())) {
            bo.setAlias(BusinessObjectValidator.validateTableAlias(entity.getJoinAlias(), executionContext));
        }

        // PAGINATION
        if (entity.getPagination() != null) {
            if (entity.getPagination().getRecordsPerPage() != null) {
                // make sure that given page size is not less than 1
                if (entity.getPagination().getRecordsPerPage() < 1) {
                    throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                            "Pagination value for 'recordsPerPage' cannot be less than 1", executionContext.getExecutorLocale());
                }
                // make sure that given page size does not exceed the max page size
                if (entity.getPagination().getRecordsPerPage() > ConfigProperties.getInstance().max_page_size.getIntegerValue()) {
                    throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                            "Pagination value for 'recordsPerPage' cannot be greater than '{}'",
                            executionContext.getExecutorLocale(), ConfigProperties.getInstance().max_page_size.getIntegerValue());
                }
            }
            if (CollectionUtils.hasValue(entity.getPagination().getPages())) {
                // make sure that given page size is not null
                if (entity.getPagination().getRecordsPerPage() == null) {
                    throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                            "Pagination value for 'pages' should be accompanied with 'recordsPerPage'", executionContext.getExecutorLocale());
                }
                // make sure that given pages are consecutive and in ascending order
                Integer prevPageNumber = null;
                for (Integer currPageNumber : entity.getPagination().getPages()) {
                    if ((currPageNumber == null) || (currPageNumber < PagedList.FIRST_PAGE_NUMBER)) {
                        throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                                "Pagination value for 'pages' cannot contain null or a page number less than {}", executionContext.getExecutorLocale(), PagedList.FIRST_PAGE_NUMBER);
                    }
                    if ((prevPageNumber != null) && ((currPageNumber - prevPageNumber) != 1)) {
                        throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                                "Pagination value for 'pages' if provided multiple pages then it should be consecutive and in ascending order",
                                executionContext.getExecutorLocale());
                    }
                    // assign to previous page number
                    prevPageNumber = currPageNumber;
                }
                // make sure that total number of records to read does not exceed the max number of records to read
                if ((entity.getPagination().getPages().size() * entity.getPagination().getRecordsPerPage()) >
                        ConfigProperties.getInstance().max_page_size.getIntegerValue()) {
                    throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                            "Total number of records to read cannot be greater than '{}'",
                            executionContext.getExecutorLocale(), ConfigProperties.getInstance().max_records_to_read.getIntegerValue());
                }
                bo.addPagesToRead(executionContext, entity.getPagination().getRecordsPerPage(),
                        entity.getPagination().getPages().toArray(new Integer[0]));
            } else {
                // read first page by default
                bo.addPagesToRead(executionContext, entity.getPagination().getRecordsPerPage(), PagedList.FIRST_PAGE_NUMBER);
            }
        }

        // ORDER BY
        if (CollectionUtils.hasValue(entity.getOrderBy())) {
            for (OrderBy orderBy : entity.getOrderBy()) {
                // validate
                BusinessObjectValidator.validateOrderBy(orderBy, entity.getBoName(), executionContext);
                // add to business object info
                if (orderBy.getOrder() == null) {
                    bo.addOrderByAsc(orderBy.getBoAttrName());
                } else if (Order.ASC == orderBy.getOrder()) {
                    bo.addOrderByAsc(orderBy.getBoAttrName());
                } else {
                    bo.addOrderByDesc(orderBy.getBoAttrName());
                }
            }
        }

        // UnitPolicy
        if (entity.getUnitPolicy() != null && !entity.getUnitPolicy().isEmpty()) {
            bo.setUnitPolicy(entity.getUnitPolicy());
        }

        // DSPDMUnits
        if (CollectionUtils.hasValue(entity.getDSPDMUnits())) {
            for (DSPDMUnit customAttributeUnit : entity.getDSPDMUnits()) {
                // Validate
                BusinessObjectValidator.validateCustomAttributeUnit(customAttributeUnit, executionContext);
                // add to business object info
                bo.addDSPDMUnits(BusinessObjectConverter.convert(customAttributeUnit));
            }
        }

        // SELECT LIST
        if (entity.getSelectList() != null && entity.getSelectList().size() > 0) {
            bo.addColumnsToSelect(entity.getSelectList());
            bo.setSelectAll(false);
        }

        // add field to unit conversion list
        HashMap<String, String> mapBoAttrNames = new HashMap<String, String>();
        if (CollectionUtils.hasValue(entity.getSelectList())) {
            for (String boAttrName : entity.getSelectList()) {
                mapBoAttrNames.put(boAttrName, boAttrName);
            }
        }
        if (CollectionUtils.hasValue(entity.getAggregateSelectList())) {
            for (AggregateColumn aggregateColumn : entity.getAggregateSelectList()) {
                mapBoAttrNames.put(aggregateColumn.getBoAttrName(), aggregateColumn.getAlias());
            }
        }
        if (mapBoAttrNames.size() > 0) {
            bo.addUnitField(entity.getBoName(), mapBoAttrNames);
        }

        // AGGREGATE SELECT LIST
        if (CollectionUtils.hasValue(entity.getAggregateSelectList())) {
            for (AggregateColumn aggregateColumn : entity.getAggregateSelectList()) {
                // Validate
                BusinessObjectValidator.validateAggregateColumn(aggregateColumn, entity.getBoName(), executionContext);
                // add to business object info
                bo.addAggregateColumnToSelect(BusinessObjectConverter.convert(aggregateColumn));
            }
        }

        // FILL RELATED DETAILS
        if (CollectionUtils.hasValue(entity.getReadParentBO())) {
            bo.addParentBONamesToRead(entity.getReadParentBO());
        }

        // Fill Children related details
        if (CollectionUtils.hasValue(entity.getReadChildBO())) {
            for (BOQuery boQuery : entity.getReadChildBO()) {
                bo.addChildBONamesToRead(Arrays.asList(build(boQuery, executionContext)));
            }
        }

        // CRITERIA FILTERS
        if (CollectionUtils.hasValue(entity.getCriteriaFilters())) {
            for (CriteriaFilter criteriaFilter : entity.getCriteriaFilters()) {
                // validate
                BusinessObjectValidator.validateCriteriaFilter(criteriaFilter, entity.getBoName(), executionContext);
                // add to business object info
                bo.addFilter(BusinessObjectConverter.convert(criteriaFilter));
                // add criteria fields to filter unit conversion list
                bo.addUnitFieldFilter(entity.getBoName(), new HashMap<String, String>() {
                    {
                        put(criteriaFilter.getBoAttrName(), criteriaFilter.getBoAttrName());
                    }
                });
            }
        }
        
        // FILTERS Groups
        if (CollectionUtils.hasValue(entity.getFilterGroups())) {
            for (FilterGroup filterGroup : entity.getFilterGroups()) {
                // validate
                BusinessObjectValidator.validateFilterGroup(filterGroup, entity.getBoName(), executionContext);
                // add to business object info
                bo.addFilterGroup(BusinessObjectConverter.convert(filterGroup));
                // add criteria fields to filter unit conversion list
                addUnitFieldFilterGroup(bo, entity.getBoName(), filterGroup);
            }
        }
        
        // HAVING FILTERS
        if (CollectionUtils.hasValue(entity.getHavingFilters())) {
            for (HavingFilter havingFilter : entity.getHavingFilters()) {
                // validate
                BusinessObjectValidator.validateHavingFilter(havingFilter, entity.getBoName(), executionContext);
                // add to business object info
                bo.addHavingFilter(BusinessObjectConverter.convert(havingFilter));
                // add having fields to filter unit conversion list
                bo.addUnitFieldFilter(entity.getBoName(), new HashMap<String, String>() {
                    {
                        put(havingFilter.getAggregateColumn().getBoAttrName(),
                                havingFilter.getAggregateColumn().getBoAttrName());
                    }
                });
            }
        }

        // Simple Joins
        if (CollectionUtils.hasValue(entity.getSimpleJoins())) {
            if (StringUtils.isNullOrEmpty(entity.getJoinAlias())) {
                throw new DSPDMException("Please define joinAlias (case sensitive) at root level business object '{}' to use simple join feature.", executionContext.getExecutorLocale(), bo.getBusinessObjectType());
            }
            for (SimpleJoinClause simpleJoinClause : entity.getSimpleJoins()) {
                // validate
                BusinessObjectValidator.validateSimpleJoinClause(simpleJoinClause, executionContext);
                // add to business object info
                bo.addSimpleJoin(BusinessObjectConverter.convert(simpleJoinClause));

                // add field to unit conversion list
                HashMap<String, String> simpleJoinMapBoAttrNames = new HashMap<String, String>();
                if (CollectionUtils.hasValue(simpleJoinClause.getSelectList())) {
                    for (String boAttrName : simpleJoinClause.getSelectList()) {
                        simpleJoinMapBoAttrNames.put(boAttrName, boAttrName);
                    }
                    bo.setSelectAll(false);
                }
                if (CollectionUtils.hasValue(simpleJoinClause.getAggregateSelectList())) {
                    for (AggregateColumn aggregateColumn : simpleJoinClause.getAggregateSelectList()) {
                        simpleJoinMapBoAttrNames.put(aggregateColumn.getBoAttrName(), aggregateColumn.getAlias());
                    }
                }
                if (simpleJoinMapBoAttrNames.size() > 0) {
                    bo.addUnitField(simpleJoinClause.getBoName(), simpleJoinMapBoAttrNames);
                }
                // add simple join criteria fields to filter unit conversion list
                if (CollectionUtils.hasValue(simpleJoinClause.getCriteriaFilters())) {
                    // CRITERIA FILTERS
                    for (CriteriaFilter criteriaFilter : simpleJoinClause.getCriteriaFilters()) {
                        bo.addUnitFieldFilter(simpleJoinClause.getBoName(), new HashMap<String, String>() {
                            {
                                put(criteriaFilter.getBoAttrName(), criteriaFilter.getBoAttrName());
                            }
                        });
                    }
                }
                // add Filter Groups
                if (CollectionUtils.hasValue(simpleJoinClause.getFilterGroups())) {
                    for (FilterGroup filterGroup : simpleJoinClause.getFilterGroups()) {
                        // validate
                        BusinessObjectValidator.validateFilterGroup(filterGroup, simpleJoinClause.getBoName(), executionContext);                         
                        // add criteria fields to filter unit conversion list
                        addUnitFieldFilterGroup(bo, simpleJoinClause.getBoName(), filterGroup);
                    }
                }
                // add simple join having filters fields to filter unit conversion list
                if (CollectionUtils.hasValue(simpleJoinClause.getHavingFilters())) {
                    for (HavingFilter havingFilter : simpleJoinClause.getHavingFilters()) {
                        // HAVING FILTERS
                        bo.addUnitFieldFilter(simpleJoinClause.getBoName(), new HashMap<String, String>() {
                            {
                                put(havingFilter.getAggregateColumn().getBoAttrName(),
                                        havingFilter.getAggregateColumn().getBoAttrName());
                            }
                        });
                    }
                }
            }
        }

        // Dynamic Joins
        if (CollectionUtils.hasValue(entity.getDynamicJoins())) {
            if (StringUtils.isNullOrEmpty(entity.getJoinAlias())) {
                throw new DSPDMException("Please define joinAlias (case sensitive) at root level business object '{}' to use dynamic join feature.", executionContext.getExecutorLocale(), bo.getBusinessObjectType());
            }
            for (DynamicJoinClause dynamicJoinClause : entity.getDynamicJoins()) {
                // validate
                BusinessObjectValidator.validateDynamicJoinClause(dynamicJoinClause, executionContext);
                // add to business object info
                bo.addDynamicJoin(BusinessObjectConverter.convert(dynamicJoinClause));
                // add field to unit conversion list
                List<DynamicTable> dynamicTables = dynamicJoinClause.getDynamicTables();
                for (DynamicTable dynamicTable : dynamicTables) {
                    if (dynamicTable != null) {
                        HashMap<String, String> dynamicJoinMapBoAttrNames = new HashMap<String, String>();
                        if (CollectionUtils.hasValue(dynamicTable.getSelectList())) {
                            for (String boAttrName : dynamicTable.getSelectList()) {
                                dynamicJoinMapBoAttrNames.put(boAttrName, boAttrName);
                            }
                            bo.setSelectAll(false);
                        }
                        if (CollectionUtils.hasValue(dynamicTable.getAggregateSelectList())) {
                            for (AggregateColumn aggregateColumn : dynamicTable.getAggregateSelectList()) {
                                dynamicJoinMapBoAttrNames.put(aggregateColumn.getBoAttrName(), aggregateColumn.getAlias());
                            }
                        }
                        if (dynamicJoinMapBoAttrNames.size() > 0) {
                            bo.addUnitField(dynamicTable.getBoName(), dynamicJoinMapBoAttrNames);
                        }
                        // add dynamic join criteria fields to filter unit conversion list
                        if (CollectionUtils.hasValue(dynamicTable.getCriteriaFilters())) {
                            // CRITERIA FILTERS
                            for (CriteriaFilter criteriaFilter : dynamicTable.getCriteriaFilters()) {
                                bo.addUnitFieldFilter(dynamicTable.getBoName(), new HashMap<String, String>() {
                                    {
                                        put(criteriaFilter.getBoAttrName(), criteriaFilter.getBoAttrName());
                                    }
                                });
                            }
                        }
                        // add Filter Groups
                        if (CollectionUtils.hasValue(dynamicTable.getFilterGroups())) {
                            for (FilterGroup filterGroup : dynamicTable.getFilterGroups()) {
                                // validate
                                BusinessObjectValidator.validateFilterGroup(filterGroup, dynamicTable.getBoName(), executionContext);
                                 
                                // add criteria fields to filter unit conversion list
                                addUnitFieldFilterGroup(bo, dynamicTable.getBoName(), filterGroup);
                            }
                        }
                        // add dynamic join having filters fields to filter unit conversion list
                        if (CollectionUtils.hasValue(dynamicTable.getHavingFilters())) {
                            for (HavingFilter havingFilter : dynamicTable.getHavingFilters()) {
                                // HAVING FILTERS
                                bo.addUnitFieldFilter(dynamicTable.getBoName(), new HashMap<String, String>() {
                                    {
                                        put(havingFilter.getAggregateColumn().getBoAttrName(),
                                                havingFilter.getAggregateColumn().getBoAttrName());
                                    }
                                });
                            }
                        }
                    }
                }
            }
        }

        // SET READBACK
        Try.apply(() -> bo.setReadBack(entity.getReadBack().booleanValue()));

        // SET READUNIQUE
        Try.apply(() -> bo.setReadUnique(entity.getReadUnique().booleanValue()));

        // SET READFIRST
        Try.apply(() -> bo.setReadFirst(entity.getReadFirst().booleanValue()));

        // SET READ METADATA
        Try.apply(() -> bo.setReadMetadata(entity.getReadMetadata().booleanValue()));

        // SET READ METADATA CONSTRAINTS
        Try.apply(() -> bo.setReadMetadataConstraints(entity.getReadMetadataConstraints().booleanValue()));

        // SET READ REFERENCE DATA
        Try.apply(() -> bo.setReadReferenceData(entity.getReadReferenceData().booleanValue()));

        // SET READ REFERENCE DATA FOR FILTERS
        Try.apply(() -> bo.setReadReferenceDataForFilters(entity.getReadReferenceDataForFilters().booleanValue()));

        // SET READ REFERENCE DATA CONSTRAINTS
        Try.apply(() -> bo.setReadReferenceDataConstraints(entity.getReadReferenceDataConstraints().booleanValue()));

        // SET READ RECORDS COUNT
        Try.apply(() -> bo.setReadRecordsCount(entity.getReadRecordsCount().booleanValue()));

        // SET READ WITH DISTINCT
        Try.apply(() -> bo.setReadWithDistinct(entity.getReadWithDistinct().booleanValue()));

        // SET READ ALL RECORDS
        Try.apply(() -> bo.setReadAllRecords(entity.getReadAllRecords().booleanValue()));

        // SET PREPEND ALIAS
        Try.apply(() -> bo.setPrependAlias(entity.getPrependAlias().booleanValue()));

        // SET PREPEND ALIAS
        Try.apply(() -> bo.setUploadNeeded(entity.getUploadNeeded()));

        return bo;
    }

	// Recursive adds the criteriaFilters fields of field group to unit conversion list
	private static void addUnitFieldFilterGroup(BusinessObjectInfo bo, String boName, FilterGroup filterGroup) {
		for (CriteriaFilter criteriaFilter : filterGroup.getCriteriaFilters()) {
			bo.addUnitFieldFilter(boName, new HashMap<String, String>() {
				{
					put(criteriaFilter.getBoAttrName(), criteriaFilter.getBoAttrName());
				}
			});
		}
		if (CollectionUtils.hasValue(filterGroup.getFilterGroups())) {
			for (FilterGroup childFilterGroup : filterGroup.getFilterGroups()) {
				addUnitFieldFilterGroup(bo, boName, childFilterGroup);
			}
		}
	}
	
    private static class BusinessObjectConverter {
        private static com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateColumn convert(AggregateColumn aggregateColumn) {
            return new com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateColumn(aggregateColumn.getAggregateFunction(),
                    aggregateColumn.getBoAttrName(), aggregateColumn.getAlias());
        }
        
        private static com.lgc.dspdm.core.common.data.criteria.DSPDMUnit convert(
                DSPDMUnit customAttributeUnit) {
            return new com.lgc.dspdm.core.common.data.criteria.DSPDMUnit(customAttributeUnit.getBoName(),
                    customAttributeUnit.getBoAttrId(), customAttributeUnit.getBoAttrName(),
                    customAttributeUnit.getTargetUnit());
        }

        private static List<com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateColumn> convert(AggregateColumn[] aggregateColumns) {
            List<com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateColumn> list = new ArrayList<>(aggregateColumns.length);
            for (AggregateColumn aggregateColumn : aggregateColumns) {
                list.add(convert(aggregateColumn));
            }
            return list;
        }

        private static SelectList convert(List<String> selectColumns, List<AggregateColumn> aggregateColumns) {
            SelectList selectList = new SelectList();
            if (CollectionUtils.hasValue(selectColumns)) {
                selectList.addColumnsToSelect(selectColumns);
            }
            if (CollectionUtils.hasValue(aggregateColumns)) {
                selectList.addAggregateColumnsToSelect(convert(aggregateColumns.toArray(new AggregateColumn[]{})));
            }
            return selectList;
        }

        private static List<com.lgc.dspdm.core.common.data.criteria.CriteriaFilter> convert(CriteriaFilter[] criteriaFilters) {
            List<com.lgc.dspdm.core.common.data.criteria.CriteriaFilter> list = new ArrayList<>(criteriaFilters.length);
            for (CriteriaFilter criteriaFilter : criteriaFilters) {
                list.add(convert(criteriaFilter));
            }
            return list;
        }

        private static com.lgc.dspdm.core.common.data.criteria.CriteriaFilter convert(CriteriaFilter criteriaFilter) {
            com.lgc.dspdm.core.common.data.criteria.CriteriaFilter filter = null;
            if (criteriaFilter.getOperator() == Operator.JSONB_FIND_LIKE || criteriaFilter.getOperator() == Operator.JSONB_FIND_EXACT) {
                // convert criteria filter values to lower case for search_jsonb
                Object[] values = new Object[criteriaFilter.getValues().length];
                for (int i = 0; i < criteriaFilter.getValues().length; i++) {
                    values[i] = String.valueOf(criteriaFilter.getValues()[i]).toLowerCase();
                }
                filter = new com.lgc.dspdm.core.common.data.criteria.CriteriaFilter(criteriaFilter.getBoAttrName(),
                        criteriaFilter.getOperator(), values);
            } else {
                filter = new com.lgc.dspdm.core.common.data.criteria.CriteriaFilter(criteriaFilter.getBoAttrName(),
                        criteriaFilter.getOperator(), criteriaFilter.getValues());
            }
            return filter;
        }

        private static List<com.lgc.dspdm.core.common.data.criteria.FilterGroup> convert(FilterGroup[] filterGroups) {
        	List<com.lgc.dspdm.core.common.data.criteria.FilterGroup> list =  new ArrayList<>(filterGroups.length);;
        	for (FilterGroup filterGroup : filterGroups) {
                list.add(convert(filterGroup));
            }
            return list;
        }
        private static com.lgc.dspdm.core.common.data.criteria.FilterGroup convert(FilterGroup filterGroup) {
            com.lgc.dspdm.core.common.data.criteria.FilterGroup filter = null;
            if (CollectionUtils.hasValue(filterGroup.getFilterGroups())) {
				filter = new com.lgc.dspdm.core.common.data.criteria.FilterGroup(filterGroup.getGroupOperator(), filterGroup.getCombineWith(),
						convert(filterGroup.getCriteriaFilters()).toArray(new com.lgc.dspdm.core.common.data.criteria.CriteriaFilter[0]),
						convert(filterGroup.getFilterGroups()).toArray(new com.lgc.dspdm.core.common.data.criteria.FilterGroup[0]));
            } else {
				filter = new com.lgc.dspdm.core.common.data.criteria.FilterGroup(filterGroup.getGroupOperator(), filterGroup.getCombineWith(), 
						convert(filterGroup.getCriteriaFilters()).toArray(new com.lgc.dspdm.core.common.data.criteria.CriteriaFilter[0]));
            }
            return filter;
        }
        
        private static List<com.lgc.dspdm.core.common.data.criteria.aggregate.HavingFilter> convert(HavingFilter[] havingFilters) {
            List<com.lgc.dspdm.core.common.data.criteria.aggregate.HavingFilter> list = new ArrayList<>(havingFilters.length);
            for (HavingFilter havingFilter : havingFilters) {
                list.add(convert(havingFilter));
            }
            return list;
        }

        private static com.lgc.dspdm.core.common.data.criteria.aggregate.HavingFilter convert(HavingFilter havingFilter) {
            return new com.lgc.dspdm.core.common.data.criteria.aggregate.HavingFilter(convert(
                    havingFilter.getAggregateColumn()), havingFilter.getOperator(), havingFilter.getValues());
        }

        private static com.lgc.dspdm.core.common.data.criteria.OrderBy convert(OrderBy[] orderByArray) {
            com.lgc.dspdm.core.common.data.criteria.OrderBy orderBy = new com.lgc.dspdm.core.common.data.criteria.OrderBy();
            for (OrderBy order : orderByArray) {
                if (order.getOrder() == null) {
                    orderBy.addOrderByAsc(order.getBoAttrName());
                } else if (Order.ASC == order.getOrder()) {
                    orderBy.addOrderByAsc(order.getBoAttrName());
                } else {
                    orderBy.addOrderByDesc(order.getBoAttrName());
                }
            }
            return orderBy;
        }

        private static com.lgc.dspdm.core.common.data.criteria.join.JoiningCondition.JoiningConditionOperand convert(JoiningCondition.JoiningConditionOperand joiningConditionOperand) {
            return new com.lgc.dspdm.core.common.data.criteria.join.JoiningCondition.JoiningConditionOperand(joiningConditionOperand.getJoinAlias(), joiningConditionOperand.getBoAttrName());
        }

        private static com.lgc.dspdm.core.common.data.criteria.join.JoiningCondition convert(JoiningCondition joiningCondition) {
            return new com.lgc.dspdm.core.common.data.criteria.join.JoiningCondition(
                    convert(joiningCondition.getLeftSide()),
                    joiningCondition.getOperator(),
                    convert(joiningCondition.getRightSide()));
        }

        private static List<com.lgc.dspdm.core.common.data.criteria.join.JoiningCondition> convert(JoiningCondition[] joiningConditions) {
            List<com.lgc.dspdm.core.common.data.criteria.join.JoiningCondition> list = new ArrayList<>(joiningConditions.length);
            for (JoiningCondition joiningCondition : joiningConditions) {
                list.add(convert(joiningCondition));
            }
            return list;
        }

        private static com.lgc.dspdm.core.common.data.criteria.join.SimpleJoinClause convert(SimpleJoinClause simpleJoinClause) {
            com.lgc.dspdm.core.common.data.criteria.join.SimpleJoinClause joinClause = new com.lgc.dspdm.core.common.data.criteria.join.SimpleJoinClause(
                    simpleJoinClause.getBoName(), simpleJoinClause.getJoinAlias(), JoinType.valueOf(simpleJoinClause.getJoinType().trim().toUpperCase()));
            // join order
            if (simpleJoinClause.getJoinOrder() != null) {
                joinClause.setJoinOrder(simpleJoinClause.getJoinOrder());
            }
            // joining conditions
            if (CollectionUtils.hasValue(simpleJoinClause.getJoiningConditions())) {
                joinClause.setJoiningConditions(convert(simpleJoinClause.getJoiningConditions().toArray(new JoiningCondition[]{})));
            }
            // select list
            if ((CollectionUtils.hasValue(simpleJoinClause.getSelectList())) || (CollectionUtils.hasValue(simpleJoinClause.getAggregateSelectList()))) {
                joinClause.setSelectList(convert(simpleJoinClause.getSelectList(), simpleJoinClause.getAggregateSelectList()));
            }
            // criteria filters
            if (CollectionUtils.hasValue(simpleJoinClause.getCriteriaFilters())) {
                joinClause.addCriteriaFilter(convert(simpleJoinClause.getCriteriaFilters().toArray(new CriteriaFilter[]{})));
            }
            // filter groups
            if (CollectionUtils.hasValue(simpleJoinClause.getFilterGroups())) {
                joinClause.addFilterGroup(convert(simpleJoinClause.getFilterGroups().toArray(new FilterGroup[]{})));
            }
            // having filters
            if (CollectionUtils.hasValue(simpleJoinClause.getHavingFilters())) {
                joinClause.addHavingFilter(convert(simpleJoinClause.getHavingFilters().toArray(new HavingFilter[]{})));
            }
            // order by
            if (CollectionUtils.hasValue(simpleJoinClause.getOrderBy())) {
                joinClause.setOrderBy(convert(simpleJoinClause.getOrderBy().toArray(new OrderBy[]{})));
            }
            return joinClause;
        }

        private static List<com.lgc.dspdm.core.common.data.criteria.join.DynamicTable> convert(DynamicTable[] dynamicTables) {
            List<com.lgc.dspdm.core.common.data.criteria.join.DynamicTable> list = new ArrayList<>(dynamicTables.length);
            for (DynamicTable dynamicTable : dynamicTables) {
                list.add(convert(dynamicTable));
            }
            return list;
        }

        private static com.lgc.dspdm.core.common.data.criteria.join.DynamicTable convert(DynamicTable dynamicTable) {
            com.lgc.dspdm.core.common.data.criteria.join.DynamicTable table = new com.lgc.dspdm.core.common.data.criteria.join.DynamicTable(
                    dynamicTable.getBoName(), convert(dynamicTable.getSelectList(), dynamicTable.getAggregateSelectList()));
            // join order
            if (dynamicTable.getJoinOrder() != null) {
                table.setJoinOrder(dynamicTable.getJoinOrder());
            }
            // criteria filters
            if (CollectionUtils.hasValue(dynamicTable.getCriteriaFilters())) {
                table.addCriteriaFilter(convert(dynamicTable.getCriteriaFilters().toArray(new CriteriaFilter[]{})));
            }
            // having filters
            if (CollectionUtils.hasValue(dynamicTable.getHavingFilters())) {
                table.addHavingFilter(convert(dynamicTable.getHavingFilters().toArray(new HavingFilter[]{})));
            }
            // join alias
            table.setJoinAlias(dynamicTable.getJoinAlias());
            // join type
            if (StringUtils.hasValue(dynamicTable.getJoinType())) {
                table.setJoinType(JoinType.valueOf(dynamicTable.getJoinType().trim().toUpperCase()));
            }
            // joining conditions
            if (CollectionUtils.hasValue(dynamicTable.getJoiningConditions())) {
                table.setJoiningConditions(convert(dynamicTable.getJoiningConditions().toArray(new JoiningCondition[]{})));
            }
            // order by
            if (CollectionUtils.hasValue(dynamicTable.getOrderBy())) {
                table.setOrderBy(convert(dynamicTable.getOrderBy().toArray(new OrderBy[]{})));
            }
            return table;
        }

        private static com.lgc.dspdm.core.common.data.criteria.join.DynamicJoinClause convert(DynamicJoinClause dynamicJoinClause) {
            com.lgc.dspdm.core.common.data.criteria.join.DynamicJoinClause joinClause = new com.lgc.dspdm.core.common.data.criteria.join.DynamicJoinClause(
                    dynamicJoinClause.getJoinAlias(), JoinType.valueOf(dynamicJoinClause.getJoinType().trim().toUpperCase()));
            // join order
            if (dynamicJoinClause.getJoinOrder() != null) {
                joinClause.setJoinOrder(dynamicJoinClause.getJoinOrder());
            }
            // joining conditions
            joinClause.setJoiningConditions(convert(dynamicJoinClause.getJoiningConditions().toArray(new JoiningCondition[]{})));
            // select list
            joinClause.setSelectList(convert(dynamicJoinClause.getSelectList(), dynamicJoinClause.getAggregateSelectList()));
            // dynamic tables
            joinClause.setDynamicTables(convert(dynamicJoinClause.getDynamicTables().toArray(new DynamicTable[]{})));
            // criteria filters
            if (CollectionUtils.hasValue(dynamicJoinClause.getCriteriaFilters())) {
                joinClause.addCriteriaFilter(convert(dynamicJoinClause.getCriteriaFilters().toArray(new CriteriaFilter[]{})));
            }
            // filter groups
            if (CollectionUtils.hasValue(dynamicJoinClause.getFilterGroups())) {
                joinClause.addFilterGroup(convert(dynamicJoinClause.getFilterGroups().toArray(new FilterGroup[]{})));
            }
            // having filters
            if (CollectionUtils.hasValue(dynamicJoinClause.getHavingFilters())) {
                joinClause.addHavingFilter(convert(dynamicJoinClause.getHavingFilters().toArray(new HavingFilter[]{})));
            }
            // order by
            if (CollectionUtils.hasValue(dynamicJoinClause.getOrderBy())) {
                joinClause.setOrderBy(convert(dynamicJoinClause.getOrderBy().toArray(new OrderBy[]{})));
            }
            return joinClause;
        }
    }

    private static class BusinessObjectValidator {

        private static Locale validateLanguage(final String language, ExecutionContext executionContext) {
            Locale locale = null;
            if (StringUtils.hasValue(language)) {
                locale = LocaleUtils.getLocale(language.trim().toLowerCase());
                if (locale == null) {
                    throw new DSPDMException("Value for parameter language is not recognized : {}", executionContext.getExecutorLocale(), language);
                }
            } else {
                throw new DSPDMException("Parameter language (case sensitive) is required", executionContext.getExecutorLocale());
            }
            return locale;
        }

        private static TimeZone validateTimezone(final String timezoneStr, ExecutionContext executionContext) {
            TimeZone timezone = null;
            if (StringUtils.hasValue(timezoneStr)) {
                timezone = LocaleUtils.getTimezone(timezoneStr.trim());
                if (timezone == null) {
                    throw new DSPDMException("Value for parameter timezone is not recognized : {}", executionContext.getExecutorLocale(), timezoneStr);
                }
            } else {
                throw new DSPDMException("Parameter timezone (case sensitive) is required", executionContext.getExecutorLocale());
            }
            return timezone;
        }

        private static TimeZone validateTimezone(final Float timezoneOffset, ExecutionContext executionContext) {
            TimeZone timezone = null;
            if (timezoneOffset != null) {
                timezone = LocaleUtils.getTimezone(timezoneOffset);
                if (timezone == null) {
                    throw new DSPDMException("Value for parameter timezone is not recognized : {}", executionContext.getExecutorLocale(), timezoneOffset.toString());
                }
            } else {
                throw new DSPDMException("Parameter timezone (case sensitive) is required", executionContext.getExecutorLocale());
            }
            return timezone;
        }

        private static String validateTableAlias(String alias, ExecutionContext executionContext) {
            if (StringUtils.isAlphaNumeric(alias)) {
                return alias;
            } else {
                throw new DSPDMException("Invalid value found for parameter joinAlias. JoinAlias must be alpha numeric or underscore", executionContext.getExecutorLocale());
            }
        }

        private static void validateOrderBy(OrderBy orderBy, String boName, ExecutionContext executionContext) {
            if (StringUtils.isNullOrEmpty(orderBy.getBoAttrName())) {
                throw new DSPDMException("Order by attribute name cannot be null or empty for business object '{}'.", executionContext.getExecutorLocale(), boName);
            }
        }

        private static void validateAggregateColumn(AggregateColumn aggregateColumn, String boName, ExecutionContext executionContext) {
            if (StringUtils.isNullOrEmpty(aggregateColumn.getBoAttrName())) {
                throw new DSPDMException("aggregate column attribute name cannot be null or empty for business object '{}'.", executionContext.getExecutorLocale(), boName);
            }
            if (aggregateColumn.getAggregateFunction() == null) {
                throw new DSPDMException("aggregate function name cannot be null or empty for business object '{}'.", executionContext.getExecutorLocale(), boName);
            }
            if (StringUtils.isNullOrEmpty(aggregateColumn.getAlias())) {
                throw new DSPDMException("aggregate column alias cannot be null or empty for business object '{}'.", executionContext.getExecutorLocale(), boName);
            } else if (!(StringUtils.isAlphaNumericOrWhitespace(aggregateColumn.getAlias()))) {
                throw new DSPDMException("Invalid value found for aggregate column alias. Column alias must have alpha numeric or underscore or hyphen or whitespace", executionContext.getExecutorLocale());
            }
        }

        private static void validateCustomAttributeUnit(DSPDMUnit dspdmUnit,
                                                        ExecutionContext executionContext) {
            if (StringUtils.isNullOrEmpty(dspdmUnit.getBoAttrName())) {
                throw new DSPDMException("dspdmUnit attrName attribute cannot be null or empty .",
                        executionContext.getExecutorLocale());
            }
            if (StringUtils.isNullOrEmpty(dspdmUnit.getTargetUnit())) {
                throw new DSPDMException("dspdmUnit targetUnit attribute cannot be null or empty .",
                        executionContext.getExecutorLocale());
            }
        }

        private static void validateCriteriaFilter(CriteriaFilter criteriaFilter, String boName, ExecutionContext executionContext) {
            if (StringUtils.isNullOrEmpty(criteriaFilter.getBoAttrName())) {
                throw new DSPDMException("Criteria filter name cannot be null or empty for business object '{}'.", executionContext.getExecutorLocale(), boName);
            }
            if (criteriaFilter.getOperator() == null) {
                throw new DSPDMException("Criteria filter operator cannot be null for business object '{}'.", executionContext.getExecutorLocale(), boName);
            }
            if (CollectionUtils.isNullOrEmpty(criteriaFilter.getValues())) {
                throw new DSPDMException("Criteria filter values cannot be null or empty for business object '{}'.", executionContext.getExecutorLocale(), boName);
            }
        }        
        // Recursive validation of filter group
        private static void validateFilterGroup(FilterGroup filterGroup, String boName, ExecutionContext executionContext) {
            if (filterGroup.getGroupOperator()== null) {
                throw new DSPDMException("Filter group operator cannot be null or empty for business object '{}'.", executionContext.getExecutorLocale(), boName);
            }
            if (filterGroup.getCombineWith() == null) {
                throw new DSPDMException("Filter group combine operator cannot be null for business object '{}'.", executionContext.getExecutorLocale(), boName);
            }
            if (CollectionUtils.isNullOrEmpty(filterGroup.getCriteriaFilters())) {
                throw new DSPDMException("Criteria filter values cannot be null or empty for business object '{}'.", executionContext.getExecutorLocale(), boName);
            }else {
            	for (CriteriaFilter criteriaFilter : filterGroup.getCriteriaFilters()) {
                    BusinessObjectValidator.validateCriteriaFilter(criteriaFilter, boName, executionContext);
                }
            }
			if (CollectionUtils.hasValue(filterGroup.getFilterGroups())) {
				for (FilterGroup childFilterGroup : filterGroup.getFilterGroups()) {
					validateFilterGroup(childFilterGroup, boName, executionContext);
				}
			}
        }

		
        private static void validateHavingFilter(HavingFilter havingFilter, String boName, ExecutionContext executionContext) {
            if (havingFilter.getAggregateColumn() == null) {
                throw new DSPDMException("Having filter aggregate column cannot be null or empty for business object '{}'.", executionContext.getExecutorLocale(), boName);
            } else {
                if (StringUtils.isNullOrEmpty(havingFilter.getAggregateColumn().getBoAttrName())) {
                    throw new DSPDMException("Having filter aggregate attribute name cannot be null or empty for business object '{}'.", executionContext.getExecutorLocale(), boName);
                }
                if (havingFilter.getAggregateColumn().getAggregateFunction() == null) {
                    throw new DSPDMException("Having filter aggregate function cannot be null or empty for business object '{}'.", executionContext.getExecutorLocale(), boName);
                }
            }
            if (havingFilter.getOperator() == null) {
                throw new DSPDMException("Having filter operator cannot be null for business object '{}'.", executionContext.getExecutorLocale(), boName);
            }
            if (CollectionUtils.isNullOrEmpty(havingFilter.getValues())) {
                throw new DSPDMException("Having filter values cannot be null or empty for business object '{}'.", executionContext.getExecutorLocale(), boName);
            }
        }

        private static void validateJoinOnConditionOperand(JoiningCondition.JoiningConditionOperand joiningConditionOperand, String boName, ExecutionContext executionContext) {
            if (StringUtils.isNullOrEmpty(joiningConditionOperand.getBoAttrName())) {
                throw new DSPDMException("Join on condition boAttrName cannot be null or empty for business object '{}'.", executionContext.getExecutorLocale(), boName);
            }
            if (StringUtils.isNullOrEmpty(joiningConditionOperand.getJoinAlias())) {
                throw new DSPDMException("Join on condition joinAlias cannot be null or empty for business object '{}'.", executionContext.getExecutorLocale(), boName);
            }
        }

        private static void validateJoinOnCondition(JoiningCondition joiningCondition, String boName, ExecutionContext executionContext) {
            // validate left side
            if (joiningCondition.getLeftSide() == null) {
                throw new DSPDMException("Left side of the Join on condition cannot be null or empty for business object '{}'.", executionContext.getExecutorLocale(), boName);
            } else {
                validateJoinOnConditionOperand(joiningCondition.getLeftSide(), boName, executionContext);
            }
            // validate operator
            if (joiningCondition.getOperator() == null) {
                throw new DSPDMException("Join on condition operator cannot be null for business object '{}'.", executionContext.getExecutorLocale(), boName);
            }
            // validate right side
            if (joiningCondition.getRightSide() == null) {
                throw new DSPDMException("Right side of the Join on condition cannot be null or empty for business object '{}'.", executionContext.getExecutorLocale(), boName);
            } else {
                validateJoinOnConditionOperand(joiningCondition.getRightSide(), boName, executionContext);
            }
        }

        private static void validateSimpleJoinClause(SimpleJoinClause simpleJoinClause, ExecutionContext executionContext) {
            // bo name to join
            if (StringUtils.isNullOrEmpty(simpleJoinClause.getBoName())) {
                throw new DSPDMException("Parameter boName (case sensitive) is required for join", executionContext.getExecutorLocale());
            }
            // join alias
            if (StringUtils.isNullOrEmpty(simpleJoinClause.getJoinAlias())) {
                throw new DSPDMException("Parameter joinAlias (case sensitive) is required for simple join", executionContext.getExecutorLocale());
            } else {
                validateTableAlias(simpleJoinClause.getJoinAlias(), executionContext);
            }

            // join type
            if (StringUtils.isNullOrEmpty(simpleJoinClause.getJoinType())) {
                throw new DSPDMException("Parameter joinType (case sensitive) is required for simple join", executionContext.getExecutorLocale());
            } else {
                JoinType joinType = null;
                try {
                    joinType = JoinType.valueOf(simpleJoinClause.getJoinType().trim().toUpperCase());
                } catch (Exception e) {
                    throw new DSPDMException("Unable to recognize the value '{}' for joinType. Possible values are '{}'",
                            executionContext.getExecutorLocale(), simpleJoinClause.getJoinType(),
                            CollectionUtils.getCommaSeparated(CollectionUtils.getListFromArray(JoinType.values())));
                }
            }

            // join order
            if (simpleJoinClause.getJoinOrder() != null) {
                if (simpleJoinClause.getJoinOrder() < 1) {
                    throw new DSPDMException("Parameter joinOrder (case sensitive) value should be between 1 and 127", executionContext.getExecutorLocale());
                }
            }

            // Join ON Conditions
            if (CollectionUtils.hasValue(simpleJoinClause.getJoiningConditions())) {
                for (JoiningCondition joiningCondition : simpleJoinClause.getJoiningConditions()) {
                    // validate
                    validateJoinOnCondition(joiningCondition, simpleJoinClause.getBoName(), executionContext);
                }
            }

            // AGGREGATE SELECT LIST
            if (CollectionUtils.hasValue(simpleJoinClause.getAggregateSelectList())) {
                for (AggregateColumn aggregateColumn : simpleJoinClause.getAggregateSelectList()) {
                    // Validate
                    validateAggregateColumn(aggregateColumn, simpleJoinClause.getBoName(), executionContext);
                }
            }

            // CRITERIA FILTERS
            if (CollectionUtils.hasValue(simpleJoinClause.getCriteriaFilters())) {
                for (CriteriaFilter criteriaFilter : simpleJoinClause.getCriteriaFilters()) {
                    // validate
                    validateCriteriaFilter(criteriaFilter, simpleJoinClause.getBoName(), executionContext);
                }
            }

            // HAVING FILTERS
            if (CollectionUtils.hasValue(simpleJoinClause.getHavingFilters())) {
                for (HavingFilter havingFilter : simpleJoinClause.getHavingFilters()) {
                    // validate
                    validateHavingFilter(havingFilter, simpleJoinClause.getBoName(), executionContext);
                }
            }

            // ORDER BY
            if (CollectionUtils.hasValue(simpleJoinClause.getOrderBy())) {
                for (OrderBy orderBy : simpleJoinClause.getOrderBy()) {
                    // validate
                    BusinessObjectValidator.validateOrderBy(orderBy, simpleJoinClause.getBoName(), executionContext);
                }
            }
        }

        private static void validateDynamicTable(DynamicTable dynamicTable, ExecutionContext executionContext) {
            if (StringUtils.isNullOrEmpty(dynamicTable.getBoName())) {
                throw new DSPDMException("Parameter boName (case sensitive) is required for dynamicTable", executionContext.getExecutorLocale());
            }
            // validate that there must be something selected from dynamic table otherwise no need dynamic table
            if ((CollectionUtils.isNullOrEmpty(dynamicTable.getSelectList())) && (CollectionUtils.isNullOrEmpty(dynamicTable.getAggregateSelectList()))) {
                throw new DSPDMException("SelectList and aggregateSelectList both cannot be null at a time for dynamic join", executionContext.getExecutorLocale());
            }

            // AGGREGATE SELECT LIST
            if (CollectionUtils.hasValue(dynamicTable.getAggregateSelectList())) {
                for (AggregateColumn aggregateColumn : dynamicTable.getAggregateSelectList()) {
                    // Validate
                    validateAggregateColumn(aggregateColumn, dynamicTable.getBoName(), executionContext);
                }
            }

            // CRITERIA FILTERS
            if (CollectionUtils.hasValue(dynamicTable.getCriteriaFilters())) {
                for (CriteriaFilter criteriaFilter : dynamicTable.getCriteriaFilters()) {
                    // validate
                    validateCriteriaFilter(criteriaFilter, dynamicTable.getBoName(), executionContext);
                }
            }

            // HAVING FILTERS
            if (CollectionUtils.hasValue(dynamicTable.getHavingFilters())) {
                for (HavingFilter havingFilter : dynamicTable.getHavingFilters()) {
                    // validate
                    validateHavingFilter(havingFilter, dynamicTable.getBoName(), executionContext);
                }
            }

            // validate join alias
            if (StringUtils.hasValue(dynamicTable.getJoinAlias())) {
                validateTableAlias(dynamicTable.getJoinAlias(), executionContext);
            }
            // validate join type
            if (StringUtils.hasValue(dynamicTable.getJoinType())) {
                JoinType joinType = JoinType.valueOf(dynamicTable.getJoinType().trim().toUpperCase());
                if (joinType == null) {
                    throw new DSPDMException("Unable to recognize the value '{}' for joinType. Possible values are '{}'",
                            executionContext.getExecutorLocale(), dynamicTable.getJoinType(),
                            CollectionUtils.getCommaSeparated(CollectionUtils.getListFromArray(JoinType.values())));
                }
            }

            // join order
            if (dynamicTable.getJoinOrder() != null) {
                if (dynamicTable.getJoinOrder() < 1) {
                    throw new DSPDMException("Parameter joinOrder (case sensitive) value should be between 1 and 127", executionContext.getExecutorLocale());
                }
            }
        }

        private static void validateDynamicJoinClause(DynamicJoinClause dynamicJoinClause, ExecutionContext executionContext) {
            // validate join alias
            if (StringUtils.isNullOrEmpty(dynamicJoinClause.getJoinAlias())) {
                throw new DSPDMException("Parameter joinAlias (case sensitive) is required for simple join", executionContext.getExecutorLocale());
            } else {
                validateTableAlias(dynamicJoinClause.getJoinAlias(), executionContext);
            }
            // validate join type
            if (StringUtils.isNullOrEmpty(dynamicJoinClause.getJoinType())) {
                throw new DSPDMException("Parameter joinType (case sensitive) is required for simple join", executionContext.getExecutorLocale());
            } else {
                JoinType joinType = JoinType.valueOf(dynamicJoinClause.getJoinType().trim().toUpperCase());
                if (joinType == null) {
                    throw new DSPDMException("Unable to recognize the value '{}' for joinType. Possible values are '{}'",
                            executionContext.getExecutorLocale(), dynamicJoinClause.getJoinType(),
                            CollectionUtils.getCommaSeparated(CollectionUtils.getListFromArray(JoinType.values())));
                }
            }

            // join order
            if (dynamicJoinClause.getJoinOrder() != null) {
                if (dynamicJoinClause.getJoinOrder() < 1) {
                    throw new DSPDMException("Parameter joinOrder (case sensitive) value should be between 1 and 127", executionContext.getExecutorLocale());
                }
            }
            // validate that there must be something selected rom dynamic table otherwise no need to join
            if ((CollectionUtils.isNullOrEmpty(dynamicJoinClause.getSelectList())) && (CollectionUtils.isNullOrEmpty(dynamicJoinClause.getAggregateSelectList()))) {
                throw new DSPDMException("SelectList and aggregateSelectList both cannot be null at a time for dynamic join", executionContext.getExecutorLocale());
            }
            // validate aggregate select list if provided
            if (CollectionUtils.hasValue(dynamicJoinClause.getAggregateSelectList())) {
                for (AggregateColumn aggregateColumn : dynamicJoinClause.getAggregateSelectList()) {
                    validateAggregateColumn(aggregateColumn, dynamicJoinClause.getJoinAlias(), executionContext);
                }
            }
            // validate joining conditions
            if (CollectionUtils.isNullOrEmpty(dynamicJoinClause.getJoiningConditions())) {
                throw new DSPDMException("There must be at least one entry in joiningConditions for dynamic join", executionContext.getExecutorLocale());
            } else {
                for (JoiningCondition joiningCondition : dynamicJoinClause.getJoiningConditions()) {
                    // validate
                    validateJoinOnCondition(joiningCondition, dynamicJoinClause.getJoinAlias(), executionContext);
                }
            }

            // validate joining tables
            if (CollectionUtils.isNullOrEmpty(dynamicJoinClause.getDynamicTables())) {
                throw new DSPDMException("There must be at least one entry in dynamicTables for dynamic join", executionContext.getExecutorLocale());
            } else {
                for (DynamicTable dynamicTable : dynamicJoinClause.getDynamicTables()) {
                    // validate dynamic table
                    validateDynamicTable(dynamicTable, executionContext);
                }
            }
        }
    }

    private static class WAFRulesByPasser{
        private static void byPassWafRules(BOQuery entity){
            for(WafByPassRulesProperties.WAFByPassRuleProperty wafByPassRulesProperty: WafByPassRulesProperties.getInstance().getWafByPassProperties()){
                // BO NAME
                entity.setBoName(wafByPassRulesProperty.applyWafRule(entity.getBoName()));
                // LANGUAGE
                if(!StringUtils.isNullOrEmpty(entity.getLanguage())){
                    entity.setLanguage(wafByPassRulesProperty.applyWafRule(entity.getLanguage()));
                }
                // TIMEZONE
                if(!StringUtils.isNullOrEmpty(entity.getTimezone())){
                    entity.setTimezone(wafByPassRulesProperty.applyWafRule(entity.getTimezone()));
                }
                // JOIN ALIAS
                if(!StringUtils.isNullOrEmpty(entity.getJoinAlias())){
                    entity.setJoinAlias(wafByPassRulesProperty.applyWafRule(entity.getJoinAlias()));
                }
                //UNIT POLICY
                if(!StringUtils.isNullOrEmpty(entity.getUnitPolicy())){
                    entity.setUnitPolicy(wafByPassRulesProperty.applyWafRule(entity.getUnitPolicy()));
                }
                // DSPDM UNITS
                if(CollectionUtils.hasValue(entity.getDSPDMUnits())){
                    for(DSPDMUnit dspdmUnit: entity.getDSPDMUnits()){
                        dspdmUnit.setBoAttrId(wafByPassRulesProperty.applyWafRule(dspdmUnit.getBoAttrId()));
                        dspdmUnit.setBoAttrName(wafByPassRulesProperty.applyWafRule(dspdmUnit.getBoAttrName()));
                        dspdmUnit.setBoName(wafByPassRulesProperty.applyWafRule(dspdmUnit.getBoName()));
                        dspdmUnit.setTargetUnit(wafByPassRulesProperty.applyWafRule(dspdmUnit.getTargetUnit()));
                    }
                }
                // SELECT LIST
                if(CollectionUtils.hasValue(entity.getSelectList())){
                    byPassWafRulesForSelectList(entity.getSelectList(), wafByPassRulesProperty);
                }
                // AGGREGATE SELECT LIST
                if(CollectionUtils.hasValue(entity.getAggregateSelectList())){
                    byPassWafRulesForAggregateSelect(entity.getAggregateSelectList(), wafByPassRulesProperty);
                }
                // CRITERIA FILTER
                if(CollectionUtils.hasValue(entity.getCriteriaFilters())){
                    byPassWafRulesForCriteriaFilter( entity.getCriteriaFilters(), wafByPassRulesProperty);
                }
                // FILTER GROUP
                if (CollectionUtils.hasValue(entity.getFilterGroups())) {
                    byPassWafRulesForFilterGroup(entity.getFilterGroups(), wafByPassRulesProperty);
                }
                // HAVING FILTER
                if(CollectionUtils.hasValue(entity.getHavingFilters())){
                    byPassWafRulesForHavingFilters(entity.getHavingFilters(), wafByPassRulesProperty);
                }
                // ORDER BY
                if(CollectionUtils.hasValue(entity.getOrderBy())){
                    byPassWafRulesForOrderBy(entity.getOrderBy(), wafByPassRulesProperty);
                }
                // SIMPLE JOIN
                if(CollectionUtils.hasValue(entity.getSimpleJoins())){
                    byPassWafRulesForSimpleJoins(entity, wafByPassRulesProperty);
                }
                // DYNAMIC JOIN
                if(CollectionUtils.hasValue(entity.getDynamicJoins())){
                    byPassWafRulesForDynamicJoins(entity, wafByPassRulesProperty);
                }
                // READ PARENT BO
                if(CollectionUtils.hasValue(entity.getReadParentBO())){
                    byPassWafRulesForReadParentBo(entity.getReadParentBO(), wafByPassRulesProperty);
                }
                // READ CHILD BO
                if(CollectionUtils.hasValue(entity.getReadChildBO())){
                    for(BOQuery childBoQuery: entity.getReadChildBO()){
                        byPassWafRules(childBoQuery);
                    }
                }
                // SHEET NAME
                entity.setSheetName(wafByPassRulesProperty.applyWafRule(entity.getSheetName()));
            }
        }

        private static void byPassWafRulesForCriteriaFilter(List<CriteriaFilter> criteriaFilterList, WafByPassRulesProperties.WAFByPassRuleProperty wafByPassRuleProperty){
            for(CriteriaFilter criteriaFilter : criteriaFilterList){

                criteriaFilter.setBoAttrName(wafByPassRuleProperty.applyWafRule(criteriaFilter.getBoAttrName()));
                // for Operator, we're not going to byPass, because to apply bypass rule, we need to define dedicated enum
                // just like we did for havingFilters (introducing new BOQuery variable hav_ingFilters to bypass having 403 on WAF)
                for(int i=0; i<criteriaFilter.getValues().length; i++){
                    if(criteriaFilter.getValues()[i] instanceof java.lang.String){
                        criteriaFilter.getValues()[i] = wafByPassRuleProperty.applyWafRule((String)criteriaFilter.getValues()[i]);
                    }
                }
            }
        }

        private static void byPassWafRulesForFilterGroupRecursive(WafByPassRulesProperties.WAFByPassRuleProperty wafByPassRuleProperty, FilterGroup filterGroup){
            byPassWafRulesForCriteriaFilter(Arrays.asList(filterGroup.getCriteriaFilters()), wafByPassRuleProperty);
            if (CollectionUtils.hasValue(filterGroup.getFilterGroups())) {
                for (FilterGroup childFilterGroup : filterGroup.getFilterGroups()) {
                    byPassWafRulesForFilterGroupRecursive(wafByPassRuleProperty, childFilterGroup);
                }
            }
        }

        private static void byPassWafRulesForSelectList( List<String> selectList, WafByPassRulesProperties.WAFByPassRuleProperty wafByPassRuleProperty){
            for(int i=0; i<selectList.size(); i++){
                selectList.set(i,wafByPassRuleProperty.applyWafRule(selectList.get(i)));
            }
        }

        private static void byPassWafRulesForAggregateSelect( List<AggregateColumn> aggregateSelectList, WafByPassRulesProperties.WAFByPassRuleProperty wafByPassRuleProperty){
            for(AggregateColumn aggregateColumn: aggregateSelectList){
                aggregateColumn.setAlias(wafByPassRuleProperty.applyWafRule(aggregateColumn.getAlias()));
                // for AggregateFunction, we're not going to byPass, because to apply bypass rule, we need to define dedicated enum
                // just like we did for havingFilters (introducing new BOQuery variable hav_ingFilters to bypass having 403 on WAF)
                aggregateColumn.setBoAttrName(wafByPassRuleProperty.applyWafRule(aggregateColumn.getBoAttrName()));
            }
        }

        private static void byPassWafRulesForFilterGroup(List<FilterGroup> filterGroupList, WafByPassRulesProperties.WAFByPassRuleProperty wafByPassRuleProperty){
            for (FilterGroup childFilterGroup : filterGroupList) {
                byPassWafRulesForFilterGroupRecursive(wafByPassRuleProperty, childFilterGroup);
            }
        }

        private static void byPassWafRulesForHavingFilters(List<HavingFilter> havingFilterList, WafByPassRulesProperties.WAFByPassRuleProperty wafByPassRuleProperty){
            for(HavingFilter havingFilter: havingFilterList){
                if(havingFilter.getAggregateColumn() != null){
                    if(StringUtils.hasValue(havingFilter.getAggregateColumn().getAlias())){
                        havingFilter.getAggregateColumn().setAlias(wafByPassRuleProperty.applyWafRule(havingFilter.getAggregateColumn().getAlias()));
                    }
                }
                if(CollectionUtils.hasValue(havingFilter.getValues())){
                    for(int i=0; i<havingFilter.getValues().length; i++){
                        if(havingFilter.getValues()[i] instanceof java.lang.String){
                            havingFilter.getValues()[i] = wafByPassRuleProperty.applyWafRule((String)havingFilter.getValues()[i]);
                        }
                    }
                }
            }
        }

        private static void byPassWafRulesForOrderBy(List<OrderBy> orderByList, WafByPassRulesProperties.WAFByPassRuleProperty wafByPassRuleProperty){
            for(OrderBy orderBy: orderByList){
                orderBy.setBoAttrName(wafByPassRuleProperty.applyWafRule(orderBy.getBoAttrName()));
            }
        }

        private static void byPassWafRulesForJoiningCondition(List<JoiningCondition> joiningConditionList, WafByPassRulesProperties.WAFByPassRuleProperty wafByPassRuleProperty){
            for(JoiningCondition joiningCondition: joiningConditionList){
                if(joiningCondition.getLeftSide() != null ){
                    joiningCondition.getLeftSide().setJoinAlias(wafByPassRuleProperty.applyWafRule(joiningCondition.getLeftSide().getJoinAlias()));
                    joiningCondition.getLeftSide().setBoAttrName(wafByPassRuleProperty.applyWafRule(joiningCondition.getLeftSide().getBoAttrName()));
                }
                if(joiningCondition.getRightSide() != null  ){
                    joiningCondition.getRightSide().setJoinAlias(wafByPassRuleProperty.applyWafRule(joiningCondition.getRightSide().getJoinAlias()));
                    joiningCondition.getRightSide().setBoAttrName(wafByPassRuleProperty.applyWafRule(joiningCondition.getRightSide().getBoAttrName()));
                }
            }
        }

        private static void byPassWafRulesForSimpleJoins(BOQuery entity, WafByPassRulesProperties.WAFByPassRuleProperty wafByPassRuleProperty){
            for(SimpleJoinClause simpleJoinClause: entity.getSimpleJoins()){
                simpleJoinClause.setJoinAlias(wafByPassRuleProperty.applyWafRule(simpleJoinClause.getJoinAlias()));
                simpleJoinClause.setJoinType((wafByPassRuleProperty.applyWafRule(simpleJoinClause.getJoinType())));
                simpleJoinClause.setBoName((wafByPassRuleProperty.applyWafRule(simpleJoinClause.getBoName())));

                if(CollectionUtils.hasValue(simpleJoinClause.getAggregateSelectList())){
                    byPassWafRulesForAggregateSelect(simpleJoinClause.getAggregateSelectList(), wafByPassRuleProperty);
                }
                if(CollectionUtils.hasValue(simpleJoinClause.getCriteriaFilters())){
                    byPassWafRulesForCriteriaFilter(simpleJoinClause.getCriteriaFilters(), wafByPassRuleProperty);
                }
                if(CollectionUtils.hasValue(simpleJoinClause.getFilterGroups())){
                    byPassWafRulesForFilterGroup(simpleJoinClause.getFilterGroups(), wafByPassRuleProperty);
                }
                if(CollectionUtils.hasValue(simpleJoinClause.getOrderBy())){
                    byPassWafRulesForOrderBy(simpleJoinClause.getOrderBy(), wafByPassRuleProperty);
                }
                if(CollectionUtils.hasValue(simpleJoinClause.getSelectList())){
                    byPassWafRulesForSelectList(simpleJoinClause.getSelectList(), wafByPassRuleProperty);
                }
                if(CollectionUtils.hasValue(simpleJoinClause.getJoiningConditions())){
                    byPassWafRulesForJoiningCondition(simpleJoinClause.getJoiningConditions(), wafByPassRuleProperty);
                }
                if(CollectionUtils.hasValue(simpleJoinClause.getHavingFilters())){
                    byPassWafRulesForHavingFilters(simpleJoinClause.getHavingFilters(), wafByPassRuleProperty);
                }
            }
        }

        private static void byPassWafRulesForDynamicJoins(BOQuery entity, WafByPassRulesProperties.WAFByPassRuleProperty wafByPassRuleProperty){
            for(DynamicJoinClause dynamicJoinClause: entity.getDynamicJoins()){
                dynamicJoinClause.setJoinAlias(wafByPassRuleProperty.applyWafRule(dynamicJoinClause.getJoinAlias()));
                dynamicJoinClause.setJoinType((wafByPassRuleProperty.applyWafRule(dynamicJoinClause.getJoinType())));

                if(CollectionUtils.hasValue(dynamicJoinClause.getAggregateSelectList())){
                    byPassWafRulesForAggregateSelect(dynamicJoinClause.getAggregateSelectList(), wafByPassRuleProperty);
                }
                if(CollectionUtils.hasValue(dynamicJoinClause.getCriteriaFilters())){
                    byPassWafRulesForCriteriaFilter(dynamicJoinClause.getCriteriaFilters(), wafByPassRuleProperty);
                }
                if(CollectionUtils.hasValue(dynamicJoinClause.getFilterGroups())){
                    byPassWafRulesForFilterGroup(dynamicJoinClause.getFilterGroups(), wafByPassRuleProperty);
                }
                if(CollectionUtils.hasValue(dynamicJoinClause.getOrderBy())){
                    byPassWafRulesForOrderBy(dynamicJoinClause.getOrderBy(), wafByPassRuleProperty);
                }
                if(CollectionUtils.hasValue(dynamicJoinClause.getSelectList())){
                    byPassWafRulesForSelectList(dynamicJoinClause.getSelectList(), wafByPassRuleProperty);
                }
                if(CollectionUtils.hasValue(dynamicJoinClause.getHavingFilters())){
                    byPassWafRulesForHavingFilters(dynamicJoinClause.getHavingFilters(), wafByPassRuleProperty);
                }
                if(CollectionUtils.hasValue(dynamicJoinClause.getJoiningConditions())){
                    byPassWafRulesForJoiningCondition(dynamicJoinClause.getJoiningConditions(), wafByPassRuleProperty);
                }
                if(CollectionUtils.hasValue(dynamicJoinClause.getDynamicTables())){
                    byPassWafRulesForDynamicTables(dynamicJoinClause.getDynamicTables(), wafByPassRuleProperty);
                }
            }
        }

        private static void byPassWafRulesForDynamicTables( List<DynamicTable> dynamicTableList, WafByPassRulesProperties.WAFByPassRuleProperty wafByPassRuleProperty){
            for(DynamicTable dynamicTable: dynamicTableList){
                dynamicTable.setJoinAlias(wafByPassRuleProperty.applyWafRule(dynamicTable.getJoinAlias()));
                dynamicTable.setJoinType((wafByPassRuleProperty.applyWafRule(dynamicTable.getJoinType())));
                dynamicTable.setBoName((wafByPassRuleProperty.applyWafRule(dynamicTable.getBoName())));

                if(CollectionUtils.hasValue(dynamicTable.getAggregateSelectList())){
                    byPassWafRulesForAggregateSelect(dynamicTable.getAggregateSelectList(), wafByPassRuleProperty);
                }
                if(CollectionUtils.hasValue(dynamicTable.getCriteriaFilters())){
                    byPassWafRulesForCriteriaFilter(dynamicTable.getCriteriaFilters(), wafByPassRuleProperty);
                }
                if(CollectionUtils.hasValue(dynamicTable.getFilterGroups())){
                    byPassWafRulesForFilterGroup(dynamicTable.getFilterGroups(), wafByPassRuleProperty);
                }
                if(CollectionUtils.hasValue(dynamicTable.getOrderBy())){
                    byPassWafRulesForOrderBy(dynamicTable.getOrderBy(), wafByPassRuleProperty);
                }
                if(CollectionUtils.hasValue(dynamicTable.getSelectList())){
                    byPassWafRulesForSelectList(dynamicTable.getSelectList(), wafByPassRuleProperty);
                }
                if(CollectionUtils.hasValue(dynamicTable.getHavingFilters())){
                    byPassWafRulesForHavingFilters(dynamicTable.getHavingFilters(), wafByPassRuleProperty);
                }
            }
        }

        private static void byPassWafRulesForReadParentBo( List<String> readParentBoList, WafByPassRulesProperties.WAFByPassRuleProperty wafByPassRuleProperty){
            for(int i=0; i<readParentBoList.size(); i++){
                readParentBoList.set(i,wafByPassRuleProperty.applyWafRule(readParentBoList.get(i)));
            }
        }

    }
}
