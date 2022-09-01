package com.lgc.dspdm.core.dao.dynamic.businessobject.impl;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.data.criteria.*;
import com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateColumn;
import com.lgc.dspdm.core.common.data.criteria.aggregate.HavingFilter;
import com.lgc.dspdm.core.common.data.criteria.join.*;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectAttributeDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectRelationshipDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.core.common.util.metadata.MetadataUtils;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAOImpl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.*;

public abstract class AbstractDynamicDAOImplForJoiningRead extends AbstractDynamicDAOImplForRead implements IDynamicDAOImpl {
    private static DSPDMLogger logger = new DSPDMLogger(AbstractDynamicDAOImplForInsert.class);
    private static final String SQL_PARAM_PLACEHOLDER = String.valueOf(DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER);

    protected AbstractDynamicDAOImplForJoiningRead(BusinessObjectDTO businessObjectDTO, ExecutionContext executionContext) {
        super(businessObjectDTO, executionContext);
    }

    /* ************************************************************************** */
    /* ****************** Public/Protected Business Methods ********************* */
    /* ************************************************************************** */

    @Override
    public int count(BusinessObjectInfo businessObjectInfo, boolean hasSQLExpression, Connection connection, ExecutionContext executionContext) {
        if (!(businessObjectInfo.isJoiningRead())) {
            return super.count(businessObjectInfo, hasSQLExpression, connection, executionContext);
        } else {
            int count = 0;
            PreparedStatement pstmt = null;
            ResultSet resultSet = null;
            StringBuilder sqlForCount = null;
            String finalSQL = null;
            long startTime = 0;
            // create a map to hold all the dao classes which are already joined to be passed on to the child methods
            // create a case insensitive map because the data is coming from request not from data store
            Map<String, AbstractDynamicDAOImplForRead> joinAliasToAlreadyJoinedDAOMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            try {
                sqlForCount = getSQLForCount(businessObjectInfo, this, joinAliasToAlreadyJoinedDAOMap, executionContext);
                pstmt = prepareStatementForCount(sqlForCount.toString(), businessObjectInfo, executionContext, connection);
                bindSQLParametersForCount(pstmt, businessObjectInfo, hasSQLExpression, executionContext);
                finalSQL = logSQLForCount(sqlForCount, SQL_PARAM_PLACEHOLDER, businessObjectInfo, this, executionContext);
                startTime = System.currentTimeMillis();
                resultSet = pstmt.executeQuery();
                logSQLTimeTaken(finalSQL, 1, (System.currentTimeMillis() - startTime), executionContext);
                while (resultSet.next()) {
                    count = count + resultSet.getInt(1);
                }
            } catch (Exception e) {
                if (finalSQL != null) {
                    logSQLTimeTaken(finalSQL, 1, (System.currentTimeMillis() - startTime), executionContext);
                } else {
                    // add to sql stats
                    logSQLTimeTaken((sqlForCount == null) ? null : sqlForCount.toString(), 0, 0, executionContext);
                    // log in error logs console
                    logSQL((sqlForCount == null) ? null : sqlForCount.toString(), executionContext);
                }
                DSPDMException.throwException(e, executionContext);
            } finally {
                closeResultSet(resultSet, executionContext);
                closeStatement(pstmt, executionContext);
            }
            return count;
        }
    }

    @Override
    public List<DynamicDTO> read(BusinessObjectInfo businessObjectInfo, boolean hasSQLExpression, Connection connection, ExecutionContext executionContext) {
        if (!(businessObjectInfo.isJoiningRead())) {
            return super.read(businessObjectInfo, hasSQLExpression, connection, executionContext);
        } else {
            List<DynamicDTO> list = null;
            PreparedStatement pstmt = null;
            ResultSet resultSet = null;
            StringBuilder sqlForRead = null;
            String finalSQL = null;
            long startTime = 0;
            // create a map to hold all the dao classes which are already joined to be passed on to the child methods
            // create a case insensitive map because the data is coming from request not from data store
            Map<String, AbstractDynamicDAOImplForRead> joinAliasToAlreadyJoinedDAOMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            try {
                sqlForRead = getSQLForRead(businessObjectInfo, this, joinAliasToAlreadyJoinedDAOMap, executionContext);
                pstmt = prepareStatementForRead(sqlForRead.toString(), businessObjectInfo, executionContext, connection);
                bindSQLParametersForRead(pstmt, businessObjectInfo, hasSQLExpression, executionContext);
                finalSQL = logSQLForRead(sqlForRead, SQL_PARAM_PLACEHOLDER, businessObjectInfo, this, executionContext);
                startTime = System.currentTimeMillis();
                resultSet = pstmt.executeQuery();
                logSQLTimeTaken(finalSQL, 1, (System.currentTimeMillis() - startTime), executionContext);
                list = extractDynamicDTOListFromResultSet(businessObjectInfo, resultSet, joinAliasToAlreadyJoinedDAOMap, executionContext);
                if ((businessObjectInfo.isReadUnique()) && (list.size() > 1)) {
                    throw new DSPDMException("Unique data was not found.  Found {} records that met the filter criteria.", executionContext.getExecutorLocale(), list.size());
                }
                logger.info("{} records of type {} read successfully.", list.size(), getType());
            } catch (Exception e) {
                if (finalSQL != null) {
                    logSQLTimeTaken(finalSQL, 1, (System.currentTimeMillis() - startTime), executionContext);
                } else {
                    // add to sql stats
                    logSQLTimeTaken((sqlForRead == null) ? null : sqlForRead.toString(), 0, 0, executionContext);
                    // log in error logs console
                    logSQL((sqlForRead == null) ? null : sqlForRead.toString(), executionContext);
                }
                DSPDMException.throwException(e, executionContext);
            } finally {
                closeResultSet(resultSet, executionContext);
                closeStatement(pstmt, executionContext);
            }
            return list;
        }
    }

    /* ******************************************************** */
    /* ****************** Utility Methods ********************* */
    /* ******************************************************** */

    private static StringBuilder getSQLForCount(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForJoiningRead dynamicDAO, Map<String, AbstractDynamicDAOImplForRead> joinAliasToAlreadyJoinedDAOMap, ExecutionContext executionContext) {
        StringBuilder sql = new StringBuilder();
        // SELECT CLAUSE
        sql.append(getSelectClauseForCount(businessObjectInfo, dynamicDAO, executionContext));
        // FROM CLAUSE
        sql.append(getFromClause(businessObjectInfo, dynamicDAO, joinAliasToAlreadyJoinedDAOMap, executionContext));
        // WHERE CLAUSE
        sql.append(getWhereClause(businessObjectInfo, dynamicDAO, executionContext));
        // GROUP BY CLAUSE
        sql.append(getGroupByClause(businessObjectInfo, dynamicDAO, executionContext));
        // HAVING CLAUSE
        sql.append(getHavingClause(businessObjectInfo, dynamicDAO, executionContext));
        return sql;
    }

    private static StringBuilder getSQLForRead(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForJoiningRead dynamicDAO, Map<String, AbstractDynamicDAOImplForRead> joinAliasToAlreadyJoinedDAOMap, ExecutionContext executionContext) {
        StringBuilder sql = new StringBuilder();
        // SELECT CLAUSE
        sql.append(getSelectClauseForRead(businessObjectInfo, dynamicDAO, executionContext));
        // FROM CLAUSE
        sql.append(getFromClause(businessObjectInfo, dynamicDAO, joinAliasToAlreadyJoinedDAOMap, executionContext));
        // WHERE CLAUSE
        sql.append(getWhereClause(businessObjectInfo, dynamicDAO, executionContext));
        // GROUP BY CLAUSE
        sql.append(getGroupByClause(businessObjectInfo, dynamicDAO, executionContext));
        // HAVING CLAUSE
        sql.append(getHavingClause(businessObjectInfo, dynamicDAO, executionContext));
        // ORDER BY CLAUSE
        sql.append(getOrderByClause(businessObjectInfo, dynamicDAO, executionContext));
        // PAGINATION CLAUSE
        sql.append(getPaginationClause(businessObjectInfo, dynamicDAO, executionContext));
        return sql;
    }

    private static String getSelectClauseForCount(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForJoiningRead dynamicDAO, ExecutionContext executionContext) {
        String selectClause = null;

        List<String> physicalColumnsToSelect = getPhysicalAllColumnsToSelectForCount(businessObjectInfo, dynamicDAO, executionContext);
        if (CollectionUtils.hasValue(physicalColumnsToSelect) && (physicalColumnsToSelect.size() == 1)) {
            selectClause = physicalColumnsToSelect.get(0);
            // ADD DISTINCT IF REQUIRED
            if (businessObjectInfo.isReadWithDistinct()) {
                selectClause = "DISTINCT " + selectClause;
            }
        } else {
            selectClause = "*";
        }
        selectClause = "SELECT COUNT(" + selectClause + ")";

        return selectClause;
    }

    private static String getSelectClauseForRead(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForJoiningRead dynamicDAO, ExecutionContext executionContext) {
        StringBuilder selectClause = new StringBuilder();
        selectClause.append("SELECT");
        if (businessObjectInfo.isReadWithDistinct()) {
            selectClause.append(" DISTINCT");
        }
        List<String> physicalColumnsToSelect = getPhysicalAllColumnsToSelectForRead(businessObjectInfo, dynamicDAO, executionContext);
        if (CollectionUtils.hasValue(physicalColumnsToSelect)) {
            String commaSeparatedSelectList = CollectionUtils.getCommaSeparated(physicalColumnsToSelect);
            if (StringUtils.hasValue(commaSeparatedSelectList)) {
                selectClause.append(DSPDMConstants.SPACE).append(commaSeparatedSelectList);
            } else {
                selectClause.append(" *");
            }
        } else {
            selectClause.append(" *");
        }
        return selectClause.toString();
    }

    private static String getFromClause(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForJoiningRead dynamicDAO, Map<String, AbstractDynamicDAOImplForRead> joinAliasToAlreadyJoinedDAOMap, ExecutionContext executionContext) {
        // start creating from clause
        StringBuilder fromClause = new StringBuilder();
        // add from clause for main/parent business object
        fromClause.append(" FROM ").append(dynamicDAO.getDatabaseTableName());
        if (StringUtils.hasValue(businessObjectInfo.getAlias())) {
            fromClause.append(" AS ").append(businessObjectInfo.getAlias());
        }

        // add DAO to the already joined dao instances alias map
        if (StringUtils.hasValue(businessObjectInfo.getAlias())) {
            joinAliasToAlreadyJoinedDAOMap.put(businessObjectInfo.getAlias(), dynamicDAO);
        } else {
            joinAliasToAlreadyJoinedDAOMap.put(businessObjectInfo.getBusinessObjectType(), dynamicDAO);
        }

        // add all join clauses now to the main sql as per their orders
        for (BaseJoinClause baseJoinClause : businessObjectInfo.getAllJoinsInTheirJoinOrder()) {
            if (baseJoinClause instanceof SimpleJoinClause) {
                SimpleJoinClause simpleJoinClause = (SimpleJoinClause) baseJoinClause;
                AbstractDynamicDAOImplForJoiningRead currentJoinDynamicDAO = (AbstractDynamicDAOImplForJoiningRead) getDynamicDAO(simpleJoinClause.getBoName(), executionContext);
                // append join sql clause to the from clause
                fromClause.append(" ").append(getSimpleJoinClause(businessObjectInfo, joinAliasToAlreadyJoinedDAOMap, simpleJoinClause, currentJoinDynamicDAO, executionContext));
                // add DAO to the already joined dao instances alias map
                if (StringUtils.hasValue(simpleJoinClause.getJoinAlias())) {
                    joinAliasToAlreadyJoinedDAOMap.put(simpleJoinClause.getJoinAlias(), currentJoinDynamicDAO);
                } else {
                    // do not put with bo name. Alias will always be there
                    // joinAliasToAlreadyJoinedDAOMap.put(simpleJoinClause.getBoName(), currentJoinDynamicDAO);
                }
            } else if (baseJoinClause instanceof DynamicJoinClause) {
                DynamicJoinClause dynamicJoinClause = (DynamicJoinClause) baseJoinClause;
                // append join sql clause to the from clause
                fromClause.append(" ").append(getDynamicJoinClause(businessObjectInfo, joinAliasToAlreadyJoinedDAOMap, dynamicJoinClause, executionContext));
            }
        }
        return fromClause.toString();
    }

    private static String getWhereClause(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForJoiningRead dynamicDAO, ExecutionContext executionContext) {
    	String whereClause = getPhysicalColumnsCriteriaFiltersWithPlaceHolders(businessObjectInfo, String.valueOf(DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER), dynamicDAO, executionContext);
		if (StringUtils.hasValue(whereClause)) {
			whereClause = " WHERE " + whereClause;
		}
		return whereClause;
    }

    private static String getGroupByClause(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForJoiningRead dynamicDAO, ExecutionContext executionContext) {
        String groupByClause = DSPDMConstants.EMPTY_STRING;
        List<String> physicalSimpleColumnsToSelect = new ArrayList<>();
        boolean hasSimpleColumnToSelect = false;
        boolean hasAggregateColumnToSelect = false;
        // first check group by columns for main business object
        if (businessObjectInfo.hasSimpleColumnToSelect()) {
            // pre-pend alias must be false becuase we are getting these columns to be added to the group by clause
            physicalSimpleColumnsToSelect.addAll(getPhysicalSimpleColumnsToSelect(businessObjectInfo.getAlias(), false, businessObjectInfo.getSelectList().getColumnsToSelect(), dynamicDAO, executionContext));
            hasSimpleColumnToSelect = true;
        }
        if (businessObjectInfo.hasAggregateColumnToSelect()) {
            hasAggregateColumnToSelect = true;
        }
        // now check group by columns for simple joined business objects
        if (businessObjectInfo.hasSimpleJoinsToRead()) {
            for (SimpleJoinClause simpleJoinClause : businessObjectInfo.getSimpleJoins()) {
                if (simpleJoinClause.hasSimpleColumnToSelect()) {
                    AbstractDynamicDAOImplForJoiningRead joiningDynamicDAO = (AbstractDynamicDAOImplForJoiningRead) getDynamicDAO(simpleJoinClause.getBoName(), executionContext);
                    // pre-pend alias must be false becuase we are getting these columns to be added to the group by clause
                    physicalSimpleColumnsToSelect.addAll(getPhysicalSimpleColumnsToSelect(simpleJoinClause.getJoinAlias(), false, simpleJoinClause.getSelectList().getColumnsToSelect(), joiningDynamicDAO, executionContext));
                    hasSimpleColumnToSelect = true;
                }
                if (simpleJoinClause.hasAggregateColumnToSelect()) {
                    hasAggregateColumnToSelect = true;
                }
            }
        }
        // now check group by columns for dynamic joined business objects
        if (businessObjectInfo.hasDynamicJoinsToRead()) {
            for (DynamicJoinClause dynamicJoinClause : businessObjectInfo.getDynamicJoins()) {
                if (dynamicJoinClause.hasSimpleColumnToSelect()) {
                    // get simple columns select columns list for dynamic join
                    AbstractDynamicDAOImplForJoiningRead joiningDynamicDAO = null;
                    // pre-pend alias must be false becuase we are getting these columns to be added to the group by clause
                    physicalSimpleColumnsToSelect.addAll(getPhysicalSimpleColumnsToSelect(dynamicJoinClause.getJoinAlias(), false, dynamicJoinClause.getSelectList().getColumnsToSelect(), null, executionContext));
                    hasSimpleColumnToSelect = true;
                }
                if (dynamicJoinClause.hasAggregateColumnToSelect()) {
                    hasAggregateColumnToSelect = true;
                }
            }
        }
        // Grouping clause is required if and only if both simple and aggregate kind of columns rae in select list
        if (hasSimpleColumnToSelect && hasAggregateColumnToSelect) {
            // if there are some group by columns then add them to group by sql clause
            if (CollectionUtils.hasValue(physicalSimpleColumnsToSelect)) {
                String joinedWithComma = CollectionUtils.getCommaSeparated(physicalSimpleColumnsToSelect);
                if (StringUtils.hasValue(joinedWithComma)) {
                    groupByClause = " GROUP BY " + joinedWithComma;
                }
            }
        }
        return groupByClause;
    }

    private static String getHavingClause(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForJoiningRead dynamicDAO, ExecutionContext executionContext) {
        String havingClause = DSPDMConstants.EMPTY_STRING;
        List<String> filtersWithPlaceHolders = getPhysicalColumnsHavingFiltersWithPlaceHolders(businessObjectInfo, SQL_PARAM_PLACEHOLDER, dynamicDAO, executionContext);
        if (CollectionUtils.hasValue(filtersWithPlaceHolders)) {
            String joinedWithAnd = CollectionUtils.joinWithAnd(filtersWithPlaceHolders);
            if (StringUtils.hasValue(joinedWithAnd)) {
                havingClause = " HAVING " + joinedWithAnd;
            }
        }
        return havingClause;
    }

    private static String getOrderByClause(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForJoiningRead dynamicDAO, ExecutionContext executionContext) {
        String orderByClause = DSPDMConstants.EMPTY_STRING;
        Map<String, String> physicalColumnsForOrder = getPhysicalColumnsForOrder(businessObjectInfo, dynamicDAO, executionContext);
        if (CollectionUtils.hasValue(physicalColumnsForOrder)) {
            String joinedWithComma = CollectionUtils.getCommaSeparated(physicalColumnsForOrder);
            if (StringUtils.hasValue(joinedWithComma)) {
                orderByClause = " ORDER BY " + joinedWithComma;
            }
        } else {
            // apply default order only in case of pagination only.
            // if pagination is not specified then do not apply any default order
            if (!(businessObjectInfo.isReadAllRecords())) {
                if ((businessObjectInfo.isReadWithDistinct()) || (businessObjectInfo.hasAggregateColumnToSelect())) {
                    orderByClause = " ORDER BY 1";
                } else {
                    orderByClause = dynamicDAO.getDefaultOrderByClause(businessObjectInfo);
                }
            }
        }
        return orderByClause;
    }

    /**
     * It will return the join clause for the simple join. This clause will be appended to the from clause of the main SQL.
     * Simple join are those whose tables are not generated in sql at runtime. Their tables already exists.
     *
     * @param businessObjectInfo
     * @param joinAliasToAlreadyJoinedDAOMap
     * @param simpleJoinClause
     * @param currentJoinDynamicDAO
     * @param executionContext
     * @return
     * @author muhammadimran.ansari
     * @since 08-Jun-2020
     */
    private static String getSimpleJoinClause(BusinessObjectInfo businessObjectInfo, Map<String, AbstractDynamicDAOImplForRead> joinAliasToAlreadyJoinedDAOMap, SimpleJoinClause simpleJoinClause, AbstractDynamicDAOImplForJoiningRead currentJoinDynamicDAO, ExecutionContext executionContext) {
        StringBuilder joinClause = new StringBuilder();
        // create join sql clause
        // 1. append join type : inner, left, right, full
        joinClause.append(" ").append(simpleJoinClause.getJoinType().getClause());
        // 2. append joining table name
        joinClause.append(" ").append(currentJoinDynamicDAO.getDatabaseTableName()).append(" AS ").append(simpleJoinClause.getJoinAlias());
        // 3. append on conditions. combine with and if more than one
        if (CollectionUtils.isNullOrEmpty(simpleJoinClause.getJoiningConditions())) {
            // joining conditions are not provided in the request therefore bring joining conditions from relationships
            simpleJoinClause.setJoiningConditions(makeJoiningConditionsFromRelationships(businessObjectInfo, simpleJoinClause, currentJoinDynamicDAO, executionContext));
        }
        // now convert to physical conditions and then append join conditions to avoid cross product
        List<String> sqlJoinOnClauses = getPhysicalSQLSimpleJoinOnClauses(simpleJoinClause, joinAliasToAlreadyJoinedDAOMap, currentJoinDynamicDAO, executionContext);
        if (CollectionUtils.isNullOrEmpty(sqlJoinOnClauses)) {
            throw new DSPDMException("Unable to join business object '{}' and '{}'. No joining condition found and cross product is not allowed.",
                    executionContext.getExecutorLocale(), businessObjectInfo.getBusinessObjectType(), simpleJoinClause.getBoName());
        }
        // now combine all with and and append to on
        joinClause.append(" ON ").append(CollectionUtils.joinWithAnd(sqlJoinOnClauses));
        return joinClause.toString();
    }

    /**
     * It will return the join clause for the dynamic join. This clause will be appended to the from clause of the main SQL.
     * Dynamic join are those whose tables are generated in sql at runtime. Their tables do not exists.
     *
     * @param businessObjectInfo
     * @param joinAliasToAlreadyJoinedDAOMap
     * @param dynamicJoinClause
     * @param executionContext
     * @return
     * @author muhammadimran.ansari
     * @since 08-Jun-2020
     */
    private static String getDynamicJoinClause(BusinessObjectInfo businessObjectInfo, Map<String, AbstractDynamicDAOImplForRead> joinAliasToAlreadyJoinedDAOMap, DynamicJoinClause dynamicJoinClause, ExecutionContext executionContext) {
        StringBuilder joinClause = new StringBuilder();
        // create join sql clause
        // 1. append join type : inner, left, right, full
        joinClause.append(" ").append(dynamicJoinClause.getJoinType().getClause());
        // 2. append Dynamic table clause
        // create dynamic table select clause as per their join order
        String dynamicTableClause = getDynamicTableClause(dynamicJoinClause.getDynamicTablesInTheirJoinOrder(), executionContext);
        // append dynamic table to the from table clause as a join
        joinClause.append(" (").append(dynamicTableClause).append(") AS ").append(dynamicJoinClause.getJoinAlias());
        // 3. append on conditions. combine with and if more than one
        if (CollectionUtils.isNullOrEmpty(dynamicJoinClause.getJoiningConditions())) {
            throw new DSPDMException("Unable to join business object '{}' and '{}' dynamically. No joining condition found and cross product is not allowed.",
                    executionContext.getExecutorLocale(), businessObjectInfo.getBusinessObjectType(), dynamicJoinClause.getJoinAlias());
        }
        // now convert to physical conditions and then append join conditions to avoid cross product
        List<String> sqlJoinOnClauses = getPhysicalSQLDynamicJoinOnClauses(dynamicJoinClause.getJoiningConditions(), joinAliasToAlreadyJoinedDAOMap, executionContext);
        if (CollectionUtils.isNullOrEmpty(sqlJoinOnClauses)) {
            throw new DSPDMException("Unable to join business object '{}' and '{}' dynamically. No joining condition found and cross product is not allowed.",
                    executionContext.getExecutorLocale(), businessObjectInfo.getBusinessObjectType(), dynamicJoinClause.getJoinAlias());
        }
        // now combine all with and and append to on
        joinClause.append(" ON ").append(CollectionUtils.joinWithAnd(sqlJoinOnClauses));
        return joinClause.toString();
    }

    /**
     * It will return the SQL of the dynamic table only. This small SQL will be appended to the main SQL from clause and it will be considered as a table to be joined physically.
     *
     * @param dynamicTables
     * @param executionContext
     * @return
     * @author muhammadimran.ansari
     * @since 08-Jun-2020
     */
    private static String getDynamicTableClause(List<DynamicTable> dynamicTables, ExecutionContext executionContext) {
        DynamicTable firstDynamicTable = dynamicTables.get(0);
        // bo name
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(firstDynamicTable.getBoName(), executionContext);
        // alias
        businessObjectInfo.setAlias(firstDynamicTable.getJoinAlias());
        // no pagination
        businessObjectInfo.setReadAllRecords(true);
        // select list
        if (firstDynamicTable.getSelectList() != null) {
            // simple columns
            if (CollectionUtils.hasValue(firstDynamicTable.getSelectList().getColumnsToSelect())) {
                businessObjectInfo.addColumnsToSelect(firstDynamicTable.getSelectList().getColumnsToSelect());
            }
            // aggregate columns
            if (CollectionUtils.hasValue(firstDynamicTable.getSelectList().getAggregateColumnsToSelect())) {
                businessObjectInfo.addAggregateColumnsToSelect(firstDynamicTable.getSelectList().getAggregateColumnsToSelect());
            }
        }
        // criteria filters
        if ((firstDynamicTable.getFilterList() != null) && (CollectionUtils.hasValue(firstDynamicTable.getFilterList().getFilters()))) {
            for (CriteriaFilter filter : firstDynamicTable.getFilterList().getFilters()) {
                businessObjectInfo.addFilter(filter);
            }
        }
        // having filters
        if ((firstDynamicTable.getFilterList() != null) && (CollectionUtils.hasValue(firstDynamicTable.getFilterList().getHavingFilters()))) {
            for (HavingFilter filter : firstDynamicTable.getFilterList().getHavingFilters()) {
                businessObjectInfo.addHavingFilter(filter);
            }
        }
        //order by
        if (firstDynamicTable.getOrderBy() != null) {
            if (CollectionUtils.hasValue(firstDynamicTable.getOrderBy().getOrderMap())) {
                Map<String, Boolean> orderMap = firstDynamicTable.getOrderBy().getOrderMap();
                for (String boAttrName : orderMap.keySet()) {
                    if (orderMap.get(boAttrName)) {
                        businessObjectInfo.addOrderByAsc(boAttrName);
                    } else {
                        businessObjectInfo.addOrderByDesc(boAttrName);
                    }
                }
            }
        }
        // if we have more dynamic tables then join them and add them to business object info
        if (dynamicTables.size() > 1) {
            DynamicTable joinedDynamicTable = null;
            for (int i = 1; i < dynamicTables.size(); i++) {
                joinedDynamicTable = dynamicTables.get(i);
                SimpleJoinClause simpleJoinClause = new SimpleJoinClause(joinedDynamicTable.getBoName(), joinedDynamicTable.getJoinAlias(), joinedDynamicTable.getJoinType());
                // select list
                simpleJoinClause.setSelectList(joinedDynamicTable.getSelectList());
                // criteria filters
                if ((joinedDynamicTable.getFilterList() != null) && (CollectionUtils.hasValue(joinedDynamicTable.getFilterList().getFilters()))) {
                    simpleJoinClause.addCriteriaFilter(joinedDynamicTable.getFilterList().getFilters());
                }
                // having filters
                if ((joinedDynamicTable.getFilterList() != null) && (CollectionUtils.hasValue(joinedDynamicTable.getFilterList().getHavingFilters()))) {
                    simpleJoinClause.addHavingFilter(joinedDynamicTable.getFilterList().getHavingFilters());
                }
                // order by
                simpleJoinClause.setOrderBy(joinedDynamicTable.getOrderBy());
                // joining conditions
                simpleJoinClause.setJoiningConditions(joinedDynamicTable.getJoiningConditions());
                // add simple join to the main business object info
                businessObjectInfo.addSimpleJoin(simpleJoinClause);
            }
        }
        // create a map to hold all the dao classes which are already joined to be passed on to the child methods
        // create a case insensitive map because the data is coming from request not from data store
        Map<String, AbstractDynamicDAOImplForRead> joinAliasToAlreadyJoinedDAOMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        AbstractDynamicDAOImplForJoiningRead dynamicTableDynamicDAO = (AbstractDynamicDAOImplForJoiningRead) getDynamicDAO(firstDynamicTable.getBoName(), executionContext);
        StringBuilder dynamicTableSQL = getSQLForRead(businessObjectInfo, dynamicTableDynamicDAO, joinAliasToAlreadyJoinedDAOMap, executionContext);
        return dynamicTableSQL.toString();
    }

    /**
     * This method will return a list of string. Each string will have a physical sql simple (not aggregate) column to select.
     * This method will return the list of all physical simple columns to select for all including business object info, simple joins and dynamic joins
     * Because this method is for count only therefore aggregate columns will not be included
     *
     * @param businessObjectInfo
     * @param dynamicDAO
     * @param executionContext
     * @return
     * @author muhammadimran.ansari
     * @since 08-Jun-2020
     */
    private static List<String> getPhysicalAllColumnsToSelectForCount(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForJoiningRead dynamicDAO, ExecutionContext executionContext) {
        // add physical columns for main/parent business object
        List<String> physicalColumnsNames = new ArrayList<>();
        if (businessObjectInfo.getSelectList() != null) {
            //simple columns
            if (CollectionUtils.hasValue(businessObjectInfo.getSelectList().getColumnsToSelect())) {
                physicalColumnsNames.addAll(getPhysicalSimpleColumnsToSelect(businessObjectInfo.getAlias(), businessObjectInfo.isPrependAlias(), businessObjectInfo.getSelectList().getColumnsToSelect(), dynamicDAO, executionContext));
            }
            // aggregate columns
            // following code must be commented we cannot apply count over any aggregate column
            // if (CollectionUtils.hasValue(businessObjectInfo.getSelectList().getAggregateColumnsToSelect())) {
            //    physicalColumnsNames.addAll(getPhysicalAggregateColumnsToSelect(businessObjectInfo, executionContext));
            // }
        }
        // add physical columns for simple joined business object
        if (businessObjectInfo.hasSimpleJoinsToRead()) {
            for (SimpleJoinClause simpleJoinClause : businessObjectInfo.getSimpleJoins()) {
                if ((simpleJoinClause.getSelectList() != null) && (CollectionUtils.hasValue(simpleJoinClause.getSelectList().getColumnsToSelect()))) {
                    AbstractDynamicDAOImplForJoiningRead joiningDynamicDAO = (AbstractDynamicDAOImplForJoiningRead) getDynamicDAO(simpleJoinClause.getBoName(), executionContext);
                    physicalColumnsNames.addAll(getPhysicalSimpleColumnsToSelect(simpleJoinClause.getJoinAlias(), businessObjectInfo.isPrependAlias(), simpleJoinClause.getSelectList().getColumnsToSelect(), joiningDynamicDAO, executionContext));
                }
            }
        }
        // add physical columns for dynamic joined business object
        if (businessObjectInfo.hasDynamicJoinsToRead()) {
            for (DynamicJoinClause dynamicJoinClause : businessObjectInfo.getDynamicJoins()) {
                if (dynamicJoinClause.hasSimpleColumnToSelect()) {
                    // get simple columns select columns list for dynamic join
                    AbstractDynamicDAOImplForJoiningRead joiningDynamicDAO = null; // must be null
                    physicalColumnsNames.addAll(getPhysicalSimpleColumnsToSelect(dynamicJoinClause.getJoinAlias(), businessObjectInfo.isPrependAlias(), dynamicJoinClause.getSelectList().getColumnsToSelect(), null, executionContext));
                }
            }
        }
        return physicalColumnsNames;
    }

    /**
     * This method will return a list of string. Each string will have a physical sql simple and aggregate both columns to select.
     * This method will return the list of all physical columns to select for all including business object info, simple joins and dynamic joins
     *
     * @param businessObjectInfo
     * @param dynamicDAO
     * @param executionContext
     * @return
     * @author muhammadimran.ansari
     * @since 08-Jun-2020
     */
    private static List<String> getPhysicalAllColumnsToSelectForRead(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForJoiningRead dynamicDAO, ExecutionContext executionContext) {
        // add physical columns for main/parent business object
        List<String> physicalColumnsNames = getPhysicalAllColumnsToSelectForRead(businessObjectInfo.getAlias(), businessObjectInfo.isPrependAlias(), businessObjectInfo.getSelectList(), dynamicDAO, executionContext);
        // add physical columns for joined business object
        if (businessObjectInfo.hasSimpleJoinsToRead()) {
            for (SimpleJoinClause simpleJoinClause : businessObjectInfo.getSimpleJoins()) {
                AbstractDynamicDAOImplForJoiningRead joiningDynamicDAO = (AbstractDynamicDAOImplForJoiningRead) getDynamicDAO(simpleJoinClause.getBoName(), executionContext);
                physicalColumnsNames.addAll(getPhysicalAllColumnsToSelectForRead(simpleJoinClause.getJoinAlias(), businessObjectInfo.isPrependAlias(), simpleJoinClause.getSelectList(), joiningDynamicDAO, executionContext));
            }
        }
        // add physical columns for dynamic joined business object
        if (businessObjectInfo.hasDynamicJoinsToRead()) {
            for (DynamicJoinClause dynamicJoinClause : businessObjectInfo.getDynamicJoins()) {
                if (dynamicJoinClause.hasSimpleColumnToSelect()) {
                    // get simple columns select columns list for dynamic join
                    AbstractDynamicDAOImplForJoiningRead joiningDynamicDAO = null; // must be null
                    physicalColumnsNames.addAll(getPhysicalAllColumnsToSelectForRead(dynamicJoinClause.getJoinAlias(), businessObjectInfo.isPrependAlias(), dynamicJoinClause.getSelectList(), null, executionContext));
                }
            }
        }
        return physicalColumnsNames;
    }

    /**
     * This method will return a list of string. Each string will have a physical sql simple and aggregate both columns to select.
     * This method will return the list of all physical columns to select for all including business object info, simple joins and dynamic joins
     *
     * @param alias
     * @param prependAlias
     * @param selectList
     * @param dynamicDAO
     * @param executionContext
     * @return
     * @author muhammadimran.ansari
     * @since 08-Jun-2020
     */
    private static List<String> getPhysicalAllColumnsToSelectForRead(String alias, boolean prependAlias, SelectList selectList, AbstractDynamicDAOImplForJoiningRead dynamicDAO, ExecutionContext executionContext) {
        ArrayList<String> physicalColumnsNames = new ArrayList<>();
        if (selectList != null) {
            physicalColumnsNames.ensureCapacity(selectList.getTotalColumnsCount());
            //simple columns
            if (CollectionUtils.hasValue(selectList.getColumnsToSelect())) {
                physicalColumnsNames.addAll(getPhysicalSimpleColumnsToSelect(alias, prependAlias, selectList.getColumnsToSelect(), dynamicDAO, executionContext));
            }
            // aggregate columns
            if (CollectionUtils.hasValue(selectList.getAggregateColumnsToSelect())) {
                physicalColumnsNames.addAll(getPhysicalAggregateColumnsToSelect(alias, prependAlias, selectList.getAggregateColumnsToSelect(), dynamicDAO, executionContext));
            }
        }
        return physicalColumnsNames;
    }

    /**
     * This method will return a list of string. Each string will have a physical sql simple (not aggregate) column to select.
     * This method will work on simple columns of business object info and simple columns of simple joins
     * because they have a specific dao class corresponding to their business object name. This method will not work for dynamic joins
     * because in dynamic joins the table is selected at run time and no fixed dao already exists for dynamic table.
     *
     * @param alias
     * @param prependAlias
     * @param columnsToSelect
     * @param dynamicDAO
     * @param executionContext
     * @return
     * @author muhammadimran.ansari
     * @since 08-Jun-2020
     */
    private static List<String> getPhysicalSimpleColumnsToSelect(String alias, boolean prependAlias, List<String> columnsToSelect, AbstractDynamicDAOImplForJoiningRead dynamicDAO, ExecutionContext executionContext) {
        List<String> physicalColumnsNames = new ArrayList<>(columnsToSelect.size());
        Map<String, BusinessObjectAttributeDTO> columnsMetadataMap = (dynamicDAO == null) ? null : dynamicDAO.getBoAttributeNamesMetadataMap();
        // simple columns
        for (String bo_attr_name : columnsToSelect) {
            if (dynamicDAO == null) {
                // dynamic join case
                if (StringUtils.isNullOrEmpty(alias)) {
                    physicalColumnsNames.add(bo_attr_name);
                } else {
                    if (prependAlias) {
                        if (StringUtils.isAlphaNumericOrUnderScore(bo_attr_name)) {
                            physicalColumnsNames.add(alias + "." + bo_attr_name + " AS " + alias + "_" + bo_attr_name);
                        } else {
                            if (bo_attr_name.lastIndexOf(DSPDMConstants.DOT) > 0) {
                                int indexDot = bo_attr_name.lastIndexOf(DSPDMConstants.DOT) + 1;
                                bo_attr_name = bo_attr_name.substring(indexDot);
                                physicalColumnsNames.add(alias + "." + bo_attr_name + " AS " + alias + "_" + bo_attr_name);
                            } else {
                                physicalColumnsNames.add(bo_attr_name + " AS " + alias + "_" + bo_attr_name);
                            }
                        }
                    } else {
                        if (StringUtils.isAlphaNumericOrUnderScore(bo_attr_name)) {
                            physicalColumnsNames.add(alias + "." + bo_attr_name);
                        } else {
                            physicalColumnsNames.add(bo_attr_name);
                        }
                    }
                }
            } else {
                BusinessObjectAttributeDTO boAttrDTO = (columnsMetadataMap == null) ? null : columnsMetadataMap.get(bo_attr_name);
                if ((boAttrDTO == null) && (columnsMetadataMap != null) && (bo_attr_name.lastIndexOf(DSPDMConstants.DOT) > 0)) {
                    int indexDot = bo_attr_name.lastIndexOf(DSPDMConstants.DOT) + 1;
                    boAttrDTO = columnsMetadataMap.get(bo_attr_name.substring(indexDot));
                }
                if (boAttrDTO != null) {
                    // column is found. Add physical name
                    if (StringUtils.isNullOrEmpty(alias)) {
                        physicalColumnsNames.add(boAttrDTO.getAttributeName());
                    } else {
                        if (prependAlias) {
                            physicalColumnsNames.add(alias + "." + boAttrDTO.getAttributeName() + " AS " + alias + "_" + boAttrDTO.getBoAttrName());
                        } else {
                            physicalColumnsNames.add(alias + "." + boAttrDTO.getAttributeName());
                        }
                    }
                } else {
                    // add logical name itself. It might be an expression
                    if ((StringUtils.hasValue(alias)) && (StringUtils.isAlphaNumericOrUnderScore(bo_attr_name))) {
                        if (prependAlias) {
                            physicalColumnsNames.add(alias + "." + bo_attr_name + " AS " + alias + "_" + bo_attr_name);
                        } else {
                            physicalColumnsNames.add(alias + "." + bo_attr_name);
                        }
                    } else {
                        physicalColumnsNames.add(bo_attr_name);
                    }
                }
            }
        }
        return physicalColumnsNames;
    }


    /**
     * This method will return a list of string. Each string will have a physical sql aggregate (not simple) column to select.
     *
     * @param alias
     * @param prependAlias
     * @param aggregateColumnsToSelect
     * @param dynamicDAO
     * @param executionContext
     * @return
     * @author muhammadimran.ansari
     * @since 08-Jun-2020
     */
    private static List<String> getPhysicalAggregateColumnsToSelect(String alias, boolean prependAlias, List<AggregateColumn> aggregateColumnsToSelect, AbstractDynamicDAOImplForJoiningRead dynamicDAO, ExecutionContext executionContext) {
        List<String> physicalColumnsNames = new ArrayList<>(aggregateColumnsToSelect.size());
        Map<String, BusinessObjectAttributeDTO> columnsMetadataMap = (dynamicDAO == null) ? null : dynamicDAO.getBoAttributeNamesMetadataMap();
        // aggregate columns
        int index = 0;
        for (AggregateColumn aggregateColumn : aggregateColumnsToSelect) {
            if (StringUtils.isNullOrEmpty(aggregateColumn.getColumnAlias())) {
                // generate an alias and assign it back to column so that can be used in future to get data back from result set
                aggregateColumn.setColumnAlias(aggregateColumn.getAggregateFunction().getFunctionName() + "_" + index);
            }
            if (dynamicDAO == null) {
                // dynamic join case
                if (StringUtils.isNullOrEmpty(alias)) {
                    physicalColumnsNames.add(aggregateColumn.getAggregateFunction().getFunctionName() + "(" + aggregateColumn.getBoAttrName() + ") AS " + aggregateColumn.getColumnAlias());
                } else {
                    String boAttrName = aggregateColumn.getBoAttrName();
                    if (StringUtils.isAlphaNumericOrUnderScore(aggregateColumn.getBoAttrName())) {
                        boAttrName = alias + "." + boAttrName;
                    }
                    if (prependAlias) {
                        physicalColumnsNames.add(aggregateColumn.getAggregateFunction().getFunctionName() + "(" + boAttrName + ") AS " + alias + "_" + aggregateColumn.getColumnAlias());
                    } else {
                        physicalColumnsNames.add(aggregateColumn.getAggregateFunction().getFunctionName() + "(" + boAttrName + ") AS " + aggregateColumn.getColumnAlias());
                    }
                }
            } else {
                BusinessObjectAttributeDTO boAttrDTO = (columnsMetadataMap == null) ? null : columnsMetadataMap.get(aggregateColumn.getBoAttrName());
                if ((boAttrDTO == null) && (columnsMetadataMap != null) && (aggregateColumn.getBoAttrName().lastIndexOf(DSPDMConstants.DOT) > 0)) {
                    int indexDot = aggregateColumn.getBoAttrName().lastIndexOf(DSPDMConstants.DOT) + 1;
                    boAttrDTO = columnsMetadataMap.get(aggregateColumn.getBoAttrName().substring(indexDot));
                }
                if (boAttrDTO != null) {
                    // column is found. Add physical name
                    if (StringUtils.isNullOrEmpty(alias)) {
                        physicalColumnsNames.add(aggregateColumn.getAggregateFunction().getFunctionName() + "(" + boAttrDTO.getAttributeName() + ") AS " + aggregateColumn.getColumnAlias());
                    } else {
                        if (prependAlias) {
                            physicalColumnsNames.add(aggregateColumn.getAggregateFunction().getFunctionName() + "(" + alias + "." + boAttrDTO.getAttributeName() + ") AS " + alias + "_" + aggregateColumn.getColumnAlias());
                        } else {
                            physicalColumnsNames.add(aggregateColumn.getAggregateFunction().getFunctionName() + "(" + alias + "." + boAttrDTO.getAttributeName() + ") AS " + aggregateColumn.getColumnAlias());
                        }
                    }
                } else {
                    // add logical name itself. It might be an expression
                    if (StringUtils.isNullOrEmpty(alias)) {
                        physicalColumnsNames.add(aggregateColumn.getAggregateFunction().getFunctionName() + "(" + aggregateColumn.getBoAttrName() + ") AS " + aggregateColumn.getColumnAlias());
                    } else {
                        String boAttrName = aggregateColumn.getBoAttrName();
                        if (StringUtils.isAlphaNumericOrUnderScore(aggregateColumn.getBoAttrName())) {
                            boAttrName = alias + "." + boAttrName;
                        }
                        if (prependAlias) {
                            physicalColumnsNames.add(aggregateColumn.getAggregateFunction().getFunctionName() + "(" + boAttrName + ") AS " + alias + "_" + aggregateColumn.getColumnAlias());
                        } else {
                            physicalColumnsNames.add(aggregateColumn.getAggregateFunction().getFunctionName() + "(" + boAttrName + ") AS " + aggregateColumn.getColumnAlias());
                        }
                    }
                }
            }
        }
        return physicalColumnsNames;
    }

    /**
     * If joining on conditions are not specified in the json request then those conditions are extracted from the relationship table.
     * If relationship metadata table does not have any data for the joining two tables then an error will be thrown that join is not possible
     *
     * @param businessObjectInfo
     * @param simpleJoinClause
     * @param childDynamicDAO
     * @param executionContext
     * @return
     */
    private static List<JoiningCondition> makeJoiningConditionsFromRelationships(BusinessObjectInfo businessObjectInfo, SimpleJoinClause simpleJoinClause,
                                                                                 AbstractDynamicDAOImplForJoiningRead childDynamicDAO, ExecutionContext executionContext) {
        List<JoiningCondition> list = new ArrayList<>();
        List<BusinessObjectRelationshipDTO> businessObjectRelationshipDTOS = childDynamicDAO.getBusinessObjectRelationshipDTOS(businessObjectInfo.getBusinessObjectType(), simpleJoinClause.getBoName(), true);
        if (CollectionUtils.hasValue(businessObjectRelationshipDTOS)) {
            // primary key relationship is found
            // now making extracting join conditions from relationships
            for (BusinessObjectRelationshipDTO relationshipDTO : businessObjectRelationshipDTOS) {
                list.add(new JoiningCondition(new JoiningCondition.JoiningConditionOperand(
                        businessObjectInfo.getAlias(), relationshipDTO.getParentBoAttrName()), Operator.EQUALS,
                        new JoiningCondition.JoiningConditionOperand(simpleJoinClause.getJoinAlias(), relationshipDTO.getChildBoAttrName())));
            }
        } else {
            businessObjectRelationshipDTOS = childDynamicDAO.getBusinessObjectRelationshipDTOS(simpleJoinClause.getBoName(), businessObjectInfo.getBusinessObjectType(), true);
            if (CollectionUtils.hasValue(businessObjectRelationshipDTOS)) {
                // primary key relationship is found
                // now making extracting join conditions from relationships
                for (BusinessObjectRelationshipDTO relationshipDTO : businessObjectRelationshipDTOS) {
                    list.add(new JoiningCondition(new JoiningCondition.JoiningConditionOperand(
                            simpleJoinClause.getJoinAlias(), relationshipDTO.getParentBoAttrName()), Operator.EQUALS,
                            new JoiningCondition.JoiningConditionOperand(businessObjectInfo.getAlias(), relationshipDTO.getChildBoAttrName())));
                }
            } else {
                throw new DSPDMException("Joining conditions are not provided in request and no primary key relationship found for business object '{}' and '{}'",
                        executionContext.getExecutorLocale(), businessObjectInfo.getBusinessObjectType(), simpleJoinClause.getBoName());
            }
        }
        return list;
    }

    /**
     * It will transform the logical join on conditions to physical conditions. Logical conditions are based on business object name and
     * boAttrName while physical conditions are based on entity and attribute name
     *
     * @param simpleJoinClause
     * @param joinAliasToAlreadyJoinedDAOMap
     * @param currentJoinDynamicDAO
     * @param executionContext
     * @return
     */
    private static List<String> getPhysicalSQLSimpleJoinOnClauses(SimpleJoinClause simpleJoinClause, Map<String, AbstractDynamicDAOImplForRead> joinAliasToAlreadyJoinedDAOMap, AbstractDynamicDAOImplForJoiningRead currentJoinDynamicDAO, ExecutionContext executionContext) {
        AbstractDynamicDAOImplForJoiningRead leftSideDynamicDAO = null;
        AbstractDynamicDAOImplForJoiningRead rightSideDynamicDAO = null;
        Map<String, BusinessObjectAttributeDTO> leftSideColumnsMetadataMap = null;
        Map<String, BusinessObjectAttributeDTO> rightSideColumnsMetadataMap = null;
        List<String> sqlJoinOnClauses = new ArrayList<>(simpleJoinClause.getJoiningConditions().size());
        BusinessObjectAttributeDTO attributeDTO = null;
        for (JoiningCondition joiningCondition : simpleJoinClause.getJoiningConditions()) {
            StringBuilder stringBuilder = new StringBuilder();
            // prepare left side and right side metadata first for the current condition
            if (joinAliasToAlreadyJoinedDAOMap.containsKey(joiningCondition.getLeftSide().getJoinAlias())) {
                leftSideDynamicDAO = (AbstractDynamicDAOImplForJoiningRead) joinAliasToAlreadyJoinedDAOMap.get(joiningCondition.getLeftSide().getJoinAlias());
                rightSideDynamicDAO = currentJoinDynamicDAO;
            } else if (joinAliasToAlreadyJoinedDAOMap.containsKey(joiningCondition.getRightSide().getJoinAlias())) {
                leftSideDynamicDAO = currentJoinDynamicDAO;
                rightSideDynamicDAO = (AbstractDynamicDAOImplForJoiningRead) joinAliasToAlreadyJoinedDAOMap.get(joiningCondition.getRightSide().getJoinAlias());
            } else {
                throw new DSPDMException("No business object found in request definition against left join alias '{}' and right join alias '{}'",
                        executionContext.getExecutorLocale(), joiningCondition.getLeftSide().getJoinAlias(), joiningCondition.getRightSide().getJoinAlias());
            }
            leftSideColumnsMetadataMap = leftSideDynamicDAO.getBoAttributeNamesMetadataMap();
            // metadata is ready now create physical condition
            attributeDTO = leftSideColumnsMetadataMap.get(joiningCondition.getLeftSide().getBoAttrName());
            if (attributeDTO != null) {
                stringBuilder.append(joiningCondition.getLeftSide().getJoinAlias()).append(".").append(attributeDTO.getAttributeName());
            } else {
                // do not append alias on sql expression
                //stringBuilder.append(joiningCondition.getLeftSide().getBoAttrName());
                throw new DSPDMException("Unable to evaluate join condition. Business object attribute '{}' does not exist for business object '{}'", executionContext.getExecutorLocale(), joiningCondition.getLeftSide().getBoAttrName(), leftSideDynamicDAO.getType());
            }
            // operator
            stringBuilder.append(joiningCondition.getOperator().getOperator());
            // right side
            // read attribute metadata
            rightSideColumnsMetadataMap = rightSideDynamicDAO.getBoAttributeNamesMetadataMap();
            attributeDTO = rightSideColumnsMetadataMap.get(joiningCondition.getRightSide().getBoAttrName());
            if (attributeDTO != null) {
                stringBuilder.append(joiningCondition.getRightSide().getJoinAlias()).append(".").append(attributeDTO.getAttributeName());
            } else {
                // do not append alias on sql expression
                // stringBuilder.append(joiningCondition.getRightSide().getBoAttrName());
                throw new DSPDMException("Unable to evaluate join condition. Business object attribute '{}' does not exist for business object '{}'", executionContext.getExecutorLocale(), joiningCondition.getRightSide().getBoAttrName(), currentJoinDynamicDAO.getType());
            }
            sqlJoinOnClauses.add(stringBuilder.toString());
        }
        return sqlJoinOnClauses;
    }

    /**
     * It will transform the logical join on conditions to physical conditions. Logical conditions are based on business object name and
     * boAttrName while physical conditions are based on entity and attribute name
     *
     * @param joiningConditions
     * @param joinAliasToAlreadyJoinedDAOMap
     * @param executionContext
     * @return
     */
    private static List<String> getPhysicalSQLDynamicJoinOnClauses(List<JoiningCondition> joiningConditions, Map<String, AbstractDynamicDAOImplForRead> joinAliasToAlreadyJoinedDAOMap, ExecutionContext executionContext) {
        List<String> sqlJoinOnClauses = new ArrayList<>(joiningConditions.size());
        for (JoiningCondition joiningCondition : joiningConditions) {
            StringBuilder stringBuilder = new StringBuilder();
            // check whether which side of the dynamic join is fixed and which side is attribute based
            AbstractDynamicDAOImplForJoiningRead leftSideDynamicDAO = null;
            AbstractDynamicDAOImplForJoiningRead rightSideDynamicDAO = null;
            if (joinAliasToAlreadyJoinedDAOMap.containsKey(joiningCondition.getLeftSide().getJoinAlias())) {
                leftSideDynamicDAO = (AbstractDynamicDAOImplForJoiningRead) joinAliasToAlreadyJoinedDAOMap.get(joiningCondition.getLeftSide().getJoinAlias());
            } else if (joinAliasToAlreadyJoinedDAOMap.containsKey(joiningCondition.getRightSide().getJoinAlias())) {
                rightSideDynamicDAO = (AbstractDynamicDAOImplForJoiningRead) joinAliasToAlreadyJoinedDAOMap.get(joiningCondition.getRightSide().getJoinAlias());
            }

            if (leftSideDynamicDAO != null) {
                Map<String, BusinessObjectAttributeDTO> leftSideColumnsMetadataMap = leftSideDynamicDAO.getBoAttributeNamesMetadataMap();
                BusinessObjectAttributeDTO attributeDTO = leftSideColumnsMetadataMap.get(joiningCondition.getLeftSide().getBoAttrName());
                if (attributeDTO != null) {
                    stringBuilder.append(joiningCondition.getLeftSide().getJoinAlias()).append(".").append(attributeDTO.getAttributeName());
                } else {
                    // do not append alias on sql expression
                    //stringBuilder.append(joiningCondition.getLeftSide().getBoAttrName());
                    throw new DSPDMException("Unable to evaluate join condition. Business object attribute '{}' does not exist for business object '{}'",
                            executionContext.getExecutorLocale(), joiningCondition.getLeftSide().getBoAttrName(), leftSideDynamicDAO.getType());
                }
                // operator
                stringBuilder.append(joiningCondition.getOperator().getOperator());
                // right side
                // this condition is fixed because the right side of the condition is based on dynamic table and no dao or attribute metadata exists
                stringBuilder.append(joiningCondition.getRightSide().getJoinAlias()).append(".").append(joiningCondition.getRightSide().getBoAttrName());
            } else if (rightSideDynamicDAO != null) {
                // this condition is fixed because the left side of the condition is based on dynamic table and no dao or attribute metadata exists
                stringBuilder.append(joiningCondition.getLeftSide().getJoinAlias()).append(".").append(joiningCondition.getLeftSide().getBoAttrName());
                // operator
                stringBuilder.append(joiningCondition.getOperator().getOperator());
                // right side
                Map<String, BusinessObjectAttributeDTO> rightSideColumnsMetadataMap = rightSideDynamicDAO.getBoAttributeNamesMetadataMap();
                BusinessObjectAttributeDTO attributeDTO = rightSideColumnsMetadataMap.get(joiningCondition.getRightSide().getBoAttrName());
                if (attributeDTO != null) {
                    stringBuilder.append(joiningCondition.getRightSide().getJoinAlias()).append(".").append(attributeDTO.getAttributeName());
                } else {
                    // do not append alias on sql expression
                    //stringBuilder.append(joiningCondition.getLeftSide().getBoAttrName());
                    throw new DSPDMException("Unable to evaluate join condition. Business object attribute '{}' does not exist for business object '{}'",
                            executionContext.getExecutorLocale(), joiningCondition.getRightSide().getBoAttrName(), rightSideDynamicDAO.getType());
                }
            } else {
                // both sides are fixed
                // this condition is fixed because the left side of the condition is based on dynamic table and no dao or attribute metadata exists
                stringBuilder.append(joiningCondition.getLeftSide().getJoinAlias()).append(".").append(joiningCondition.getLeftSide().getBoAttrName());
                // operator
                stringBuilder.append(joiningCondition.getOperator().getOperator());
                // right side
                // this condition is fixed because the right side of the condition is based on dynamic table and no dao or attribute metadata exists
                stringBuilder.append(joiningCondition.getRightSide().getJoinAlias()).append(".").append(joiningCondition.getRightSide().getBoAttrName());
            }
            sqlJoinOnClauses.add(stringBuilder.toString());
        }
        return sqlJoinOnClauses;
    }
    
    /**
     * This method returns where clause filters. These filters do not contain values they only contain place holders
     *
     * @param businessObjectInfo
     * @param placeHolder
     * @param dynamicDAO
     * @param executionContext
     * @return
     */
    private static String getPhysicalColumnsCriteriaFiltersWithPlaceHolders(BusinessObjectInfo businessObjectInfo, String placeHolder,
                                                                            AbstractDynamicDAOImplForJoiningRead dynamicDAO,
                                                                            ExecutionContext executionContext) {
        StringBuilder mainStringBuilder = new StringBuilder();

        // add criteria filters for main parent business object
        if (businessObjectInfo.getFilterList() != null) {
            boolean filterAdded = false;
            if (CollectionUtils.hasValue(businessObjectInfo.getFilterList().getFilters())) {
                List<String> filtersWithPlaceHolders = getPhysicalColumnsCriteriaFiltersWithPlaceHolders(businessObjectInfo.getAlias(),
                        businessObjectInfo.getFilterList().getFilters(), placeHolder, dynamicDAO, executionContext);
                mainStringBuilder.append(CollectionUtils.joinWithAnd(filtersWithPlaceHolders));
                filterAdded = true;
            }
            if (CollectionUtils.hasValue(businessObjectInfo.getFilterList().getFilterGroups())) {
                // add filter groups criteria filters for main parent business object
                String sqlForFilterGroups = getWhereClauseForFilterGroups(businessObjectInfo.getAlias(), placeHolder, businessObjectInfo.getFilterList(), dynamicDAO, executionContext);
                if (StringUtils.hasValue(sqlForFilterGroups)) {
                    if (filterAdded) {
                        // if group filter clause is not the first clause then append AND
                        mainStringBuilder.insert(0, "(");
                        mainStringBuilder.append(Operator.AND.getOperator());
                    }
                    mainStringBuilder.append(sqlForFilterGroups);
                    if (filterAdded) {
                        mainStringBuilder.append(")");
                    }
                }
            }
        }
        // add criteria filters for simple joins
        if (businessObjectInfo.hasSimpleJoinsToRead()) {
            for (SimpleJoinClause simpleJoinClause : businessObjectInfo.getSimpleJoins()) {
                if (simpleJoinClause.getFilterList() != null) {
                    AbstractDynamicDAOImplForJoiningRead joiningDynamicDAO = (AbstractDynamicDAOImplForJoiningRead) getDynamicDAO(simpleJoinClause.getBoName(), executionContext);
                    StringBuilder joinStringBuilder = new StringBuilder();
                    boolean filterAdded = false;
                    if (CollectionUtils.hasValue(simpleJoinClause.getFilterList().getFilters())) {
                        List<String> filtersWithPlaceHolders = getPhysicalColumnsCriteriaFiltersWithPlaceHolders(simpleJoinClause.getJoinAlias(),
                                simpleJoinClause.getFilterList().getFilters(), placeHolder, joiningDynamicDAO, executionContext);
                        joinStringBuilder.append(CollectionUtils.joinWithAnd(filtersWithPlaceHolders));
                        filterAdded = true;
                    }
                    if (CollectionUtils.hasValue(simpleJoinClause.getFilterList().getFilterGroups())) {
                        // add filter groups criteria filters for simple joins
                        String sqlForFilterGroups = getWhereClauseForFilterGroups(simpleJoinClause.getJoinAlias(), placeHolder, simpleJoinClause.getFilterList(), joiningDynamicDAO, executionContext);
                        if (StringUtils.hasValue(sqlForFilterGroups)) {
                            if (filterAdded) {
                                joinStringBuilder.insert(0, "(");
                                // ig group filter clause is not the first clause then append AND
                                joinStringBuilder.append(Operator.AND.getOperator());
                            }
                            joinStringBuilder.append(sqlForFilterGroups);
                            if (filterAdded) {
                                joinStringBuilder.append(")");
                            }
                        }
                    }
                    if(joinStringBuilder.length() > 0) {
                        if (mainStringBuilder.length() > 0) {
                            mainStringBuilder.append(Operator.AND.getOperator());
                        }
                        mainStringBuilder.append(joinStringBuilder.toString());
                    }
                }
            }
        }
        // add criteria filters for dynamic joins
        if (businessObjectInfo.hasDynamicJoinsToRead()) {
            for (DynamicJoinClause dynamicJoinClause : businessObjectInfo.getDynamicJoins()) {
                if (dynamicJoinClause.getFilterList() != null) {
                    AbstractDynamicDAOImplForJoiningRead joiningDynamicDAO = null;
                    StringBuilder joinStringBuilder = new StringBuilder();
                    boolean filterAdded = false;
                    if (CollectionUtils.hasValue(dynamicJoinClause.getFilterList().getFilters())) {
                        List<String> filtersWithPlaceHolders = getPhysicalColumnsCriteriaFiltersWithPlaceHolders(dynamicJoinClause.getJoinAlias(),
                                dynamicJoinClause.getFilterList().getFilters(), placeHolder, null, executionContext);
                        joinStringBuilder.append(CollectionUtils.joinWithAnd(filtersWithPlaceHolders));
                        filterAdded = true;
                    }
                    if (CollectionUtils.hasValue(dynamicJoinClause.getFilterList().getFilterGroups())) {
                        // add filter groups criteria filters for dynamic joins
                        String sqlForFilterGroups = getWhereClauseForFilterGroups(dynamicJoinClause.getJoinAlias(), placeHolder, dynamicJoinClause.getFilterList(), joiningDynamicDAO, executionContext);
                        if (StringUtils.hasValue(sqlForFilterGroups)) {
                            if (filterAdded) {
                                joinStringBuilder.insert(0, "(");
                                // if group filter clause is not the first clause then append AND
                                joinStringBuilder.append(Operator.AND.getOperator());
                            }
                            joinStringBuilder.append(sqlForFilterGroups);
                            if (filterAdded) {
                                joinStringBuilder.append(")");
                            }
                        }
                    }
                    if(joinStringBuilder.length() > 0) {
                        if (mainStringBuilder.length() > 0) {
                            mainStringBuilder.append(Operator.AND.getOperator());
                        }
                        mainStringBuilder.append(joinStringBuilder.toString());
                    }
                }
            }
        }
        return mainStringBuilder.toString();
    }

    /**
     * This method will return a list of string. Each string will have a physical sql where clause condition.
     * These conditions will not contain any value.
     *
     * @param alias
     * @param filters
     * @param placeHolder
     * @param dynamicDAO
     * @param executionContext
     * @return
     */
    private static List<String> getPhysicalColumnsCriteriaFiltersWithPlaceHolders(String alias, List<CriteriaFilter> filters,
                                                                                  String placeHolder,
                                                                                  AbstractDynamicDAOImplForJoiningRead dynamicDAO,
                                                                                  ExecutionContext executionContext) {
        List<String> physicalFilter = new ArrayList<>(filters.size());
        Map<String, BusinessObjectAttributeDTO> columnsMetadataMap = (dynamicDAO == null) ? null : dynamicDAO.getBoAttributeNamesMetadataMap();
        String physicalColumnName = null;
        String filterClause = null;
        for (CriteriaFilter filter : filters) {
            filterClause = getEachFilterClause(alias, placeHolder, filter, columnsMetadataMap, executionContext);
            physicalFilter.add("(" + filterClause + ")");
        }
        return physicalFilter;
    }

    /**
     * This method will return a list of string. Each string will have a physical sql having clause condition.
     * These conditions will not contain any value. This method will work on filters of business object info and simple columns of simple joins and dynamic joins
     *
     * @param businessObjectInfo
     * @param placeHolder
     * @param dynamicDAO
     * @param executionContext
     * @return
     */
    private static List<String> getPhysicalColumnsHavingFiltersWithPlaceHolders(BusinessObjectInfo businessObjectInfo, String placeHolder, AbstractDynamicDAOImplForJoiningRead dynamicDAO, ExecutionContext executionContext) {
        List<String> physicalFilter = new ArrayList<>();
        // add having filters for main parent business object
        if ((businessObjectInfo.getFilterList() != null) && (CollectionUtils.hasValue(businessObjectInfo.getFilterList().getHavingFilters()))) {
            physicalFilter.addAll(getPhysicalColumnsHavingFiltersWithPlaceHolders(businessObjectInfo.getAlias(), businessObjectInfo.getFilterList().getHavingFilters(), placeHolder, dynamicDAO, executionContext));

        }
        // add having filters for simple joins
        if (businessObjectInfo.hasSimpleJoinsToRead()) {
            for (SimpleJoinClause simpleJoinClause : businessObjectInfo.getSimpleJoins()) {
                if ((simpleJoinClause.getFilterList() != null) && (CollectionUtils.hasValue(simpleJoinClause.getFilterList().getHavingFilters()))) {
                    // add having filters for simple joined business objects
                    AbstractDynamicDAOImplForJoiningRead joiningDynamicDAO = (AbstractDynamicDAOImplForJoiningRead) getDynamicDAO(simpleJoinClause.getBoName(), executionContext);
                    physicalFilter.addAll(getPhysicalColumnsHavingFiltersWithPlaceHolders(simpleJoinClause.getJoinAlias(), simpleJoinClause.getFilterList().getHavingFilters(), placeHolder, joiningDynamicDAO, executionContext));
                }
            }
        }
        // add having filters for dynamic joins
        if (businessObjectInfo.hasDynamicJoinsToRead()) {
            for (DynamicJoinClause dynamicJoinClause : businessObjectInfo.getDynamicJoins()) {
                if ((dynamicJoinClause.getFilterList() != null) && (CollectionUtils.hasValue(dynamicJoinClause.getFilterList().getHavingFilters()))) {
                    AbstractDynamicDAOImplForJoiningRead joiningDynamicDAO = null;
                    physicalFilter.addAll(getPhysicalColumnsHavingFiltersWithPlaceHolders(dynamicJoinClause.getJoinAlias(), dynamicJoinClause.getFilterList().getHavingFilters(), placeHolder, null, executionContext));
                }
            }
        }
        return physicalFilter;
    }

    /**
     * This method will return a list of string. Each string will have a physical sql having clause condition.
     * These conditions will not contain any value.
     *
     * @param alias
     * @param havingFilters
     * @param placeHolder
     * @param dynamicDAO
     * @param executionContext
     * @return
     */
    private static List<String> getPhysicalColumnsHavingFiltersWithPlaceHolders(String alias, List<HavingFilter> havingFilters, String placeHolder, AbstractDynamicDAOImplForJoiningRead dynamicDAO, ExecutionContext executionContext) {
        List<String> physicalFilter = new ArrayList<>(havingFilters.size());
        Map<String, BusinessObjectAttributeDTO> columnsMetadataMap = (dynamicDAO == null) ? null : dynamicDAO.getBoAttributeNamesMetadataMap();
        String physicalColumnName = null;
        int sqlDataType = Types.VARCHAR;
        String filterClause = null;

        for (HavingFilter havingFilter : havingFilters) {
            if (dynamicDAO == null) {
                // dynamic join case
                physicalColumnName = havingFilter.getAggregateColumn().getAggregateFunction().getFunctionName() + "(" + alias + "." + havingFilter.getAggregateColumn().getBoAttrName() + ")";
            } else {
                BusinessObjectAttributeDTO boAttrDTO = (columnsMetadataMap == null) ? null : columnsMetadataMap.get(havingFilter.getAggregateColumn().getBoAttrName());
                if (boAttrDTO != null) {
                    // column is found. Add physical name with alias
                    physicalColumnName = havingFilter.getAggregateColumn().getAggregateFunction().getFunctionName() + "(" + alias + "." + boAttrDTO.getAttributeName() + ")";
                } else if ((columnsMetadataMap != null) && (havingFilter.getAggregateColumn().getBoAttrName().lastIndexOf(DSPDMConstants.DOT) > 0)) {
                    int indexDot = havingFilter.getAggregateColumn().getBoAttrName().lastIndexOf(DSPDMConstants.DOT) + 1;
                    boAttrDTO = columnsMetadataMap.get(havingFilter.getAggregateColumn().getBoAttrName().substring(indexDot));
                    if (boAttrDTO != null) {
                        // column is found. add name coming from request without alias
                        physicalColumnName = havingFilter.getAggregateColumn().getAggregateFunction().getFunctionName() + "("
                                + havingFilter.getAggregateColumn().getBoAttrName() + ")";
                    }
                }
                if (physicalColumnName == null) {
                    // add logical name itself. It might be an expression
                    if (StringUtils.isAlphaNumericOrUnderScore(havingFilter.getAggregateColumn().getBoAttrName())) {
                        // add alias in start if alias provided as it is just a column name
                        physicalColumnName = havingFilter.getAggregateColumn().getAggregateFunction().getFunctionName() + "("
                                + alias + "." + havingFilter.getAggregateColumn().getBoAttrName() + ")";
                    } else {
                        // do not add alias as it is not a simple column name
                        physicalColumnName = havingFilter.getAggregateColumn().getAggregateFunction().getFunctionName() + "("
                                + havingFilter.getAggregateColumn().getBoAttrName() + ")";
                    }
                }
                if (boAttrDTO != null) {
                    sqlDataType = boAttrDTO.getSqlDataType();
                    // convert values data type if not already converted in case of internal calls to the service
                    if (String.class != boAttrDTO.getJavaDataType()) {
                        Object[] values = havingFilter.getValues();
                        if ((values != null) && (values.length > 0) && (values[0] instanceof String)) {
                            // in case value is string and the metadata type is not string then covert
                            for (Object value : values) {
                                if ((value != null) && (value instanceof String)) {
                                    havingFilter.replaceValue(MetadataUtils.convertValueToJavaDataTypeFromString((String) value,
                                            boAttrDTO.getJavaDataType(), boAttrDTO.getBoName(), boAttrDTO.getBoAttrName(), executionContext));
                                }
                            }
                        }
                    }
                }
            }
            switch (havingFilter.getAggregateColumn().getAggregateFunction()) {
                case COUNT:
                case AVG:
                case SUM:
                    havingFilter.setSqlDataType(Types.DOUBLE);
                    break;
                case MIN:
                case MAX:
                    havingFilter.setSqlDataType(sqlDataType);
                    break;
                default:
                    havingFilter.setSqlDataType(Types.VARCHAR);
            }
            // column must be casted to varchar to be compared becuase in case of aggregate functions mostly we do not know the result data type.
            if (havingFilter.getSqlDataType() == Types.VARCHAR) {
                physicalColumnName = "CAST(" + physicalColumnName + " AS VARCHAR)";
            }
            filterClause = dynamicDAO.getSQLOperatorCondition(havingFilter, physicalColumnName, placeHolder, executionContext);
            physicalFilter.add("(" + filterClause + ")");
        }
        return physicalFilter;
    }

    private static Map<String, String> getPhysicalColumnsForOrder(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForJoiningRead dynamicDAO, ExecutionContext executionContext) {
        // use LinkedHashMap to maintain column order
        Map<String, String> physicalColumnsAndOrder = new LinkedHashMap<>();
        // add order for main parent business object
        if ((businessObjectInfo.getOrderBy() != null) && (CollectionUtils.hasValue(businessObjectInfo.getOrderBy().getOrderMap()))) {
            physicalColumnsAndOrder.putAll(getPhysicalColumnsForOrder(businessObjectInfo.getBusinessObjectType(), businessObjectInfo.getAlias(), businessObjectInfo.getOrderBy().getOrderMap(), dynamicDAO, executionContext));

        }
        // add column order for simple joins
        if (businessObjectInfo.hasSimpleJoinsToRead()) {
            for (SimpleJoinClause simpleJoinClause : businessObjectInfo.getSimpleJoins()) {
                if ((simpleJoinClause.getOrderBy() != null) && (CollectionUtils.hasValue(simpleJoinClause.getOrderBy().getOrderMap()))) {
                    // add having filters for simple joined business objects
                    AbstractDynamicDAOImplForJoiningRead joiningDynamicDAO = (AbstractDynamicDAOImplForJoiningRead) getDynamicDAO(simpleJoinClause.getBoName(), executionContext);
                    physicalColumnsAndOrder.putAll(getPhysicalColumnsForOrder(simpleJoinClause.getBoName(), simpleJoinClause.getJoinAlias(), simpleJoinClause.getOrderBy().getOrderMap(), joiningDynamicDAO, executionContext));
                }
            }
        }
        // add column order for dynamic joins
        if (businessObjectInfo.hasDynamicJoinsToRead()) {
            for (DynamicJoinClause dynamicJoinClause : businessObjectInfo.getDynamicJoins()) {
                if ((dynamicJoinClause.getOrderBy() != null) && (CollectionUtils.hasValue(dynamicJoinClause.getOrderBy().getOrderMap()))) {
                    // add having filters for simple joined business objects
                    AbstractDynamicDAOImplForJoiningRead joiningDynamicDAO = null;
                    physicalColumnsAndOrder.putAll(getPhysicalColumnsForOrder(null, dynamicJoinClause.getJoinAlias(), dynamicJoinClause.getOrderBy().getOrderMap(), null, executionContext));
                }
            }
        }
        return physicalColumnsAndOrder;
    }

    private static Map<String, String> getPhysicalColumnsForOrder(String boName, String alias, Map<String, Boolean> orderMap, AbstractDynamicDAOImplForJoiningRead dynamicDAO, ExecutionContext executionContext) {
        // use LinkedHashMap to maintain column order
        Map<String, String> physicalColumnsAndOrder = new LinkedHashMap<>(orderMap.size());
        Map<String, BusinessObjectAttributeDTO> columnsMetadataMap = (dynamicDAO == null) ? null : dynamicDAO.getBoAttributeNamesMetadataMap();
        for (String logicalName : orderMap.keySet()) {
            if (dynamicDAO == null) {
                // dynamic join case
                physicalColumnsAndOrder.put(alias + "." + logicalName, ((orderMap.get(logicalName)) ? "ASC" : "DESC"));
            } else {
                BusinessObjectAttributeDTO boAttrDTO =(columnsMetadataMap==null)?null: columnsMetadataMap.get(logicalName);
                if (boAttrDTO != null) {
                    // column is found. Add physical name
                    physicalColumnsAndOrder.put(alias + "." + boAttrDTO.getAttributeName(), ((orderMap.get(logicalName)) ? "ASC" : "DESC"));
                } else {
                    // Order must be handle separately for BO_SEARCH due to presence of JSONB
                    if ((boName != null) && (DSPDMConstants.BoName.BO_SEARCH.equalsIgnoreCase(boName))) {
                        physicalColumnsAndOrder.put(DSPDMConstants.BoAttrName.OBJECT_JSONB + Operator.JSONB_DOT.getOperator() + "'" + alias + "." + logicalName + "'", ((orderMap.get(logicalName)) ? "ASC" : "DESC"));
                    } else {
                        // add logical name itself. It might be an expression
                        physicalColumnsAndOrder.put(logicalName, ((orderMap.get(logicalName)) ? "ASC" : "DESC"));
                    }
                }
            }
        }
        return physicalColumnsAndOrder;
    }

    private static int bindSQLParametersForCount(PreparedStatement pstmt, BusinessObjectInfo businessObjectInfo, boolean hasSQLExpression, ExecutionContext executionContext) {
        // start from 1
        int paramIndex = bindSQLParametersForWhereClause(pstmt, businessObjectInfo, 1, hasSQLExpression, executionContext);
        paramIndex = bindSQLParametersForHavingClause(pstmt, businessObjectInfo, paramIndex, hasSQLExpression, executionContext);
        return paramIndex;
    }

    private static int bindSQLParametersForRead(PreparedStatement pstmt, BusinessObjectInfo businessObjectInfo, boolean hasSQLExpression, ExecutionContext executionContext) {
        // start from 1
        int paramIndex = bindSQLParametersForWhereClause(pstmt, businessObjectInfo, 1, hasSQLExpression, executionContext);
        paramIndex = bindSQLParametersForHavingClause(pstmt, businessObjectInfo, paramIndex, hasSQLExpression, executionContext);
        paramIndex = bindSQLParametersForPagination(pstmt, businessObjectInfo, paramIndex, executionContext);
        return paramIndex;
    }

    private static String logSQLForCount(StringBuilder sql, String sqlMarker, BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForJoiningRead dao, ExecutionContext executionContext) {
        String finalSQL = null;
        if ((logger.isInfoEnabled()) || (executionContext.isCollectSQLStats()) || (executionContext.getCollectSQLScriptOptions().isCountEnabled())) {
            injectWhereClauseParamValuesList(sql, sqlMarker, businessObjectInfo, dao, executionContext);
            injectHavingClauseParamValuesList(sql, sqlMarker, businessObjectInfo, dao, executionContext);
            finalSQL = sql.toString();
            // after this we need to release the memory occupied by string builder
            sql.setLength(0);
            sql.trimToSize();
            logSQL(finalSQL, executionContext);
            // collect sql script only if required
            if(executionContext.getCollectSQLScriptOptions().isCountEnabled()) {
                executionContext.addSqlScript(finalSQL);
            }
        }
        return finalSQL;
    }

    private static String logSQLForRead(StringBuilder sql, String sqlMarker, BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForJoiningRead dao, ExecutionContext executionContext) {
        String finalSQL = null;
        if ((logger.isInfoEnabled()) || (executionContext.isCollectSQLStats()) || (executionContext.getCollectSQLScriptOptions().isReadEnabled())) {
            injectWhereClauseParamValuesList(sql, sqlMarker, businessObjectInfo, dao, executionContext);
            injectHavingClauseParamValuesList(sql, sqlMarker, businessObjectInfo, dao, executionContext);
            injectPaginationParamValuesList(sql, sqlMarker, businessObjectInfo, dao, executionContext);
            finalSQL = sql.toString();
            // after this we need to release the memory occupied by string builder
            sql.setLength(0);
            sql.trimToSize();
            logSQL(finalSQL, executionContext);
            // collect sql script only if required
            if(executionContext.getCollectSQLScriptOptions().isReadEnabled()) {
                executionContext.addSqlScript(finalSQL);
            }
        }
        return finalSQL;
    }

    private static void injectWhereClauseParamValuesList(StringBuilder sql, String sqlMarker, BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForJoiningRead dao, ExecutionContext executionContext) {
        // replace sql markers against for dynamic tables first
        if (businessObjectInfo.hasDynamicJoinsToRead()) {
            for (DynamicJoinClause dynamicJoinClause : businessObjectInfo.getDynamicJoins()) {
                for (DynamicTable dynamicTable : dynamicJoinClause.getDynamicTables()) {
                    if(dynamicTable.getFilterList() != null) {
                        injectWhereClauseParamValuesList(sql, sqlMarker, dynamicTable.getFilterList(), dao, executionContext);
                        if(CollectionUtils.hasValue(dynamicTable.getFilterList().getHavingFilters())) {
                            injectHavingClauseParamValuesList(sql, sqlMarker, dynamicTable.getFilterList().getHavingFilters(), dao, executionContext);
                        }
                    }
                }
            }
        }
        // replace sql markers against filters for main parent business object
    	injectWhereClauseParamValuesList(sql, sqlMarker, businessObjectInfo.getFilterList(), dao, executionContext);
 
        // replace sql markers against for simple joins
        if (businessObjectInfo.hasSimpleJoinsToRead()) {
            for (SimpleJoinClause simpleJoinClause : businessObjectInfo.getSimpleJoins()) {
            	injectWhereClauseParamValuesList(sql, sqlMarker, simpleJoinClause.getFilterList(), dao, executionContext);

            }
        }
        // replace sql markers against for dynamic joins
        if (businessObjectInfo.hasDynamicJoinsToRead()) {
            for (DynamicJoinClause dynamicJoinClause : businessObjectInfo.getDynamicJoins()) {
            	injectWhereClauseParamValuesList(sql, sqlMarker, dynamicJoinClause.getFilterList(),dao, executionContext);
            }
        }
    }

    private static void injectWhereClauseParamValuesList(StringBuilder sql, String sqlMarker, FilterList filterList, AbstractDynamicDAOImplForJoiningRead dao, ExecutionContext executionContext) {
    	 if (filterList != null) {
         	if(filterList.getFilters() != null) {
 	            List<CriteriaFilter> filters = filterList.getFilters();
 	            for (CriteriaFilter filter : filters) {
 	                setParametersValue(sql, sqlMarker, executionContext, filter, dao);
 	            }
         	}
         	if(filterList.getFilterGroups() != null) {
         		RecursiveWhereClauseParamValuesFromGroups(sql, filterList.getFilterGroups(), dao, executionContext);
         	}
         }
    }

    private static void setParametersValue(StringBuilder sql, String sqlMarker,  ExecutionContext executionContext, CriteriaFilter filter, AbstractDynamicDAOImplForRead dao) {
		if (CollectionUtils.hasValue(filter.getValues())) {
		    for (Object value : filter.getValues()) {
				if ((value == null) && (filter.getOperator() != Operator.IN) && (filter.getOperator() != Operator.NOT_IN)) {
					// skip null values because they are already hard coded as is null or is not null 
					continue;
		    	}
				int firstStartIndex= sql.indexOf(sqlMarker);
				if(firstStartIndex>=0) {
					sql.replace(firstStartIndex, firstStartIndex+1, getSQLMarkerReplacementString(value,dao, executionContext));
				}							
		    }
		}
	}

	// recurse filter groups to set parameters values
    private static void RecursiveWhereClauseParamValuesFromGroups(StringBuilder sql, List<FilterGroup> filterGroups, AbstractDynamicDAOImplForJoiningRead dao, ExecutionContext executionContext) {
        if (CollectionUtils.hasValue(filterGroups)) {
            for(FilterGroup filterGroup: filterGroups) {
	            for (CriteriaFilter filter : filterGroup.getCriteriaFilters()) {
	                setParametersValue(sql, String.valueOf(DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER), executionContext, filter, dao);
				}
	            if(CollectionUtils.hasValue(filterGroup.getFilterGroups())) {
	            	RecursiveWhereClauseParamValuesFromGroups(sql,Arrays.asList(filterGroup.getFilterGroups()),dao, executionContext);
	            }
			}
		}
	}
    private static void injectHavingClauseParamValuesList(StringBuilder sql, String sqlMarker, BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForJoiningRead dao, ExecutionContext executionContext) {
        // replace sql markers against filters for main parent business object
        if ((businessObjectInfo.getFilterList() != null) && (CollectionUtils.hasValue(businessObjectInfo.getFilterList().getHavingFilters()))) {
            injectHavingClauseParamValuesList(sql, sqlMarker, businessObjectInfo.getFilterList().getHavingFilters(), dao, executionContext);

        }
        // replace sql markers against for simple joins
        if (businessObjectInfo.hasSimpleJoinsToRead()) {
            for (SimpleJoinClause simpleJoinClause : businessObjectInfo.getSimpleJoins()) {
                if ((simpleJoinClause.getFilterList() != null) && (CollectionUtils.hasValue(simpleJoinClause.getFilterList().getHavingFilters()))) {
                    injectHavingClauseParamValuesList(sql, sqlMarker, simpleJoinClause.getFilterList().getHavingFilters(), dao, executionContext);
                }
            }
        }
        // replace sql markers against for dynamic joins
        if (businessObjectInfo.hasDynamicJoinsToRead()) {
            for (DynamicJoinClause dynamicJoinClause : businessObjectInfo.getDynamicJoins()) {
                if ((dynamicJoinClause.getFilterList() != null) && (CollectionUtils.hasValue(dynamicJoinClause.getFilterList().getHavingFilters()))) {
                    injectHavingClauseParamValuesList(sql, sqlMarker, dynamicJoinClause.getFilterList().getHavingFilters(), dao, executionContext);
                }
            }
        }
    }

    private static void injectHavingClauseParamValuesList(StringBuilder sql, String sqlMarker, List<HavingFilter> filters, AbstractDynamicDAOImplForJoiningRead dao,  ExecutionContext executionContext) {
        int index = 0;
        for (HavingFilter filter : filters) {
            if (CollectionUtils.hasValue(filter.getValues())) {
                for (Object value : filter.getValues()) {
                    if ((value != null) || ((filter.getOperator() == Operator.IN) || (filter.getOperator() == Operator.NOT_IN))) {
                        index = sql.indexOf(sqlMarker);
                        if (index >= 0) {
                            sql = sql.replace(index, (index + sqlMarker.length()), getSQLMarkerReplacementString(value,dao, executionContext));
                        }
                    }
                    // skip null values because they are already hard coded as is null or is not null
                }
            }
        }
    }

    private static void injectPaginationParamValuesList(StringBuilder sql, String sqlMarker, BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForJoiningRead dao,  ExecutionContext executionContext) {
        if (!(businessObjectInfo.isReadAllRecords())) {
            int index = 0;
            if (businessObjectInfo.getPagination().getReadFromIndex() > 0) {
                // SET OFFSET to actual value
                index = sql.indexOf(sqlMarker);
                if (index >= 0) {
                    sql = sql.replace(index, (index + sqlMarker.length()), getSQLMarkerReplacementString(businessObjectInfo.getPagination().getReadFromIndex(),dao, executionContext));
                }
            } else {
                // SET OFFSET to Zero as default
                index = sql.indexOf(sqlMarker);
                if (index >= 0) {
                    sql = sql.replace(index, (index + sqlMarker.length()), getSQLMarkerReplacementString(0,dao, executionContext));
                }
            }
            // SET FETCH NEXT ROWS ONLY COUNT
            if (businessObjectInfo.isReadFirst()) {
                // SET FETCH NEXT ROWS ONLY COUNT
                index = sql.indexOf(sqlMarker);
                if (index >= 0) {
                    sql = sql.replace(index, (index + sqlMarker.length()), getSQLMarkerReplacementString(1,dao, executionContext));
                }
            } else if (businessObjectInfo.isReadUnique()) {
                // SET FETCH NEXT ROWS ONLY COUNT
                index = sql.indexOf(sqlMarker);
                if (index >= 0) {
                    sql = sql.replace(index, (index + sqlMarker.length()), getSQLMarkerReplacementString(2,dao, executionContext));
                }
            } else if (businessObjectInfo.getPagination().getRecordsToRead() != Integer.MAX_VALUE) {
                // SET FETCH NEXT ROWS ONLY COUNT
                index = sql.indexOf(sqlMarker);
                if (index >= 0) {
                    sql = sql.replace(index, (index + sqlMarker.length()), getSQLMarkerReplacementString(businessObjectInfo.getPagination().getRecordsToRead(),dao, executionContext));
                }
            }
        }
    }

    private static List<DynamicDTO> extractDynamicDTOListFromResultSet(BusinessObjectInfo businessObjectInfo, ResultSet resultSet, Map<String, AbstractDynamicDAOImplForRead> joinAliasToAlreadyJoinedDAOMap, ExecutionContext executionContext) {
        List<DynamicDTO> list = new ArrayList<>();
        try {
            LinkedHashSet<String> columnNamesOrderedSet = null;
            if ((businessObjectInfo.getSelectList() != null) && (businessObjectInfo.getSelectList().getTotalColumnsCount() > 0)) {
                // select list is provided by user so iterate over provided column list from result set  and do not iterate on attribute metadata
                columnNamesOrderedSet = new LinkedHashSet<>(getColumnNamesFromResultSetMetadata(resultSet));

            } else if (businessObjectInfo.isJoiningRead()) {
                if (businessObjectInfo.hasSimpleJoinsToRead()) {
                    for (SimpleJoinClause simpleJoinClause : businessObjectInfo.getSimpleJoins()) {
                        if ((simpleJoinClause.getSelectList() != null) && (simpleJoinClause.getSelectList().getTotalColumnsCount() > 0)) {
                            // select list is provided by user so iterate over provided column list from result set  and do not iterate on attribute metadata
                            columnNamesOrderedSet = new LinkedHashSet<>(getColumnNamesFromResultSetMetadata(resultSet));
                            break;
                        }
                    }
                } else if (businessObjectInfo.hasDynamicJoinsToRead()) {
                    for (DynamicJoinClause dynamicJoinClause : businessObjectInfo.getDynamicJoins()) {
                        if ((dynamicJoinClause.getSelectList() != null) && (dynamicJoinClause.getSelectList().getTotalColumnsCount() > 0)) {
                            // select list is provided by user so iterate over provided column list from result set  and do not iterate on attribute metadata
                            columnNamesOrderedSet = new LinkedHashSet<>(getColumnNamesFromResultSetMetadata(resultSet));
                            break;
                        }
                    }
                }
            }
            if (CollectionUtils.hasValue(columnNamesOrderedSet)) {
                AbstractDynamicDAOImplForJoiningRead dynamicDAO = (AbstractDynamicDAOImplForJoiningRead) ((StringUtils.hasValue(businessObjectInfo.getAlias())) ?
                        joinAliasToAlreadyJoinedDAOMap.get(businessObjectInfo.getAlias()) :
                        joinAliasToAlreadyJoinedDAOMap.get(businessObjectInfo.getBusinessObjectType()));
                while (resultSet.next()) {
                    // order of column is specified in the set
                    DynamicDTO dynamicDTO = extractDynamicDTOFromResultSet(resultSet, columnNamesOrderedSet, new DynamicDTO(dynamicDAO.getType(), dynamicDAO.getPrimaryKeyColumnNames(), executionContext), joinAliasToAlreadyJoinedDAOMap, executionContext);
                    list.add(dynamicDTO);
                    if (list.size() > ConfigProperties.getInstance().max_records_to_read.getIntegerValue()) {
                        throw new DSPDMException("Reading more than '{}' records in a single request for bo name '{}' is not allowed. Please use pagination feature.", executionContext.getExecutorLocale(), ConfigProperties.getInstance().max_records_to_read.getIntegerValue(), dynamicDAO.getType());
                    }
                }
            } else {
                // select list is not provided by user so iterate over business object metadata
                // so that the column order is by sequence number column
                TreeSet<String> columnNamesSortedSetFromResultSet = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
                columnNamesSortedSetFromResultSet.addAll(getColumnNamesFromResultSetMetadata(resultSet));

                AbstractDynamicDAOImplForJoiningRead dynamicDAO = (AbstractDynamicDAOImplForJoiningRead) ((StringUtils.hasValue(businessObjectInfo.getAlias())) ?
                        joinAliasToAlreadyJoinedDAOMap.get(businessObjectInfo.getAlias()) :
                        joinAliasToAlreadyJoinedDAOMap.get(businessObjectInfo.getBusinessObjectType()));
                while (resultSet.next()) {
                    // order of column is specified in the metadata sequence number
                    DynamicDTO dto = extractDynamicDTOFromResultSet(resultSet, columnNamesSortedSetFromResultSet, new DynamicDTO(dynamicDAO.getType(), dynamicDAO.getPrimaryKeyColumnNames(), executionContext), dynamicDAO, executionContext);
                    if (businessObjectInfo.hasSimpleJoinsToRead()) {
                        for (SimpleJoinClause simpleJoinClause : businessObjectInfo.getSimpleJoins()) {
                            AbstractDynamicDAOImplForJoiningRead joiningDynamicDAO = (AbstractDynamicDAOImplForJoiningRead) ((StringUtils.hasValue(simpleJoinClause.getJoinAlias())) ?
                                    joinAliasToAlreadyJoinedDAOMap.get(simpleJoinClause.getJoinAlias()) :
                                    joinAliasToAlreadyJoinedDAOMap.get(simpleJoinClause.getBoName()));
                            dto = extractDynamicDTOFromResultSet(resultSet, columnNamesSortedSetFromResultSet, dto, joiningDynamicDAO, executionContext);
                        }
                    }
                    if (businessObjectInfo.hasDynamicJoinsToRead()) {
                        for (DynamicJoinClause dynamicJoinClause : businessObjectInfo.getDynamicJoins()) {
                            AbstractDynamicDAOImplForJoiningRead joiningDynamicDAO = null;
                            dto = extractDynamicDTOFromResultSet(resultSet, columnNamesSortedSetFromResultSet, dto, dynamicJoinClause.getSelectList(), executionContext);
                        }
                    }
                    list.add(dto);
                    if (list.size() > ConfigProperties.getInstance().max_records_to_read.getIntegerValue()) {
                        throw new DSPDMException("Reading more than '{}' records in a single request for bo name '{}' is not allowed. Please use pagination feature.", executionContext.getExecutorLocale(), ConfigProperties.getInstance().max_records_to_read.getIntegerValue(), dynamicDAO.getType());
                    }
                }
            }
        } catch (Throwable e) {
            DSPDMException.throwException(e, executionContext);
        }
        return list;
    }
}
