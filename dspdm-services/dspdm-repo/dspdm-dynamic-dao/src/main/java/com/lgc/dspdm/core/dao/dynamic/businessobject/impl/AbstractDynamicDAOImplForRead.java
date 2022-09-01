package com.lgc.dspdm.core.dao.dynamic.businessobject.impl;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.criteria.*;
import com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateColumn;
import com.lgc.dspdm.core.common.data.criteria.aggregate.HavingFilter;
import com.lgc.dspdm.core.common.data.criteria.join.BaseJoinClause;
import com.lgc.dspdm.core.common.data.criteria.join.DynamicJoinClause;
import com.lgc.dspdm.core.common.data.criteria.join.DynamicTable;
import com.lgc.dspdm.core.common.data.criteria.join.SimpleJoinClause;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicPK;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectAttributeDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.core.common.util.metadata.MetadataUtils;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAOImpl;
import com.lgc.dspdm.core.dao.dynamic.businessobject.BaseDynamicDAO;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;

/**
 * Class to hold common code for read methods or base methods.
 * These methods will not be public and can only be invoked from the child classes.
 * This class will not contain any public business method
 *
 * @author Muhammad Imran Ansari
 * @date 27-August-2019
 */
public abstract class AbstractDynamicDAOImplForRead extends BaseDynamicDAO implements IDynamicDAOImpl {
    private static DSPDMLogger logger = new DSPDMLogger(AbstractDynamicDAOImplForRead.class);

    protected AbstractDynamicDAOImplForRead(BusinessObjectDTO businessObjectDTO, ExecutionContext executionContext) {
        super(businessObjectDTO, executionContext);
    }

    /* ******************************************************** */
    /* ****************** Public Business Methods Methods ********************* */
    /* ******************************************************** */

    @Override
    public int count(BusinessObjectInfo businessObjectInfo, boolean hasSQLExpression, Connection connection, ExecutionContext executionContext) {
        int count = 0;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        String sqlForCount = null;
        String finalSQL = null;
        long startTime = 0;
        try {
            if ((businessObjectInfo.isReadWithDistinct()) && (businessObjectInfo.getSelectList().getTotalColumnsCount() > 1)) {
                sqlForCount = "SELECT COUNT(*) FROM (" + getSQLForReadForDistinctCount(businessObjectInfo, this, executionContext) + ") dynamicTableForDistinctCount";
            } else {
                sqlForCount = getSQLForCount(businessObjectInfo, this, executionContext);
            }
            pstmt = prepareStatementForCount(sqlForCount, businessObjectInfo, executionContext, connection);
            bindSQLParametersForCount(pstmt, businessObjectInfo, hasSQLExpression, executionContext);
            finalSQL = logSQLForCount(sqlForCount, businessObjectInfo, this, executionContext);
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
                logSQLTimeTaken(sqlForCount, 0, 0, executionContext);
                // log in error logs console
                logSQL(sqlForCount, executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeResultSet(resultSet, executionContext);
            closeStatement(pstmt, executionContext);
        }
        return count;
    }

    @Override
    public DynamicDTO readOne(final DynamicPK dynamicPK, Connection connection, ExecutionContext executionContext) {
        DynamicDTO dynamicDTO = null;
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(getType(), executionContext).setReadFirst(true);
        List<BusinessObjectAttributeDTO> primaryKeyColumns = getPrimaryKeyColumns();
        if (CollectionUtils.hasValue(primaryKeyColumns)) {
            boolean firstCondition = true;
            int counter = 0;
            for (BusinessObjectAttributeDTO boAttrDTO : primaryKeyColumns) {
                businessObjectInfo.addFilter(boAttrDTO.getBoAttrName(), dynamicPK.getPK()[counter]);
                counter++;
            }
        }
        List<DynamicDTO> list = this.read(businessObjectInfo, false, connection, executionContext);
        if (CollectionUtils.hasValue(list)) {
            dynamicDTO = list.get(0);
        }

        return dynamicDTO;
    }

    public List<DynamicDTO> readCopyAndVerifyBoAttrNamesExists(List<DynamicDTO> boListToDelete, Connection connection, ExecutionContext executionContext, String... boAttrNamesToVerify) {
        // verify primary key exists for all the objects
        validateBoAttrNamesExist(boListToDelete, executionContext, boAttrNamesToVerify);
        // read a copy from db for all the objects
        return readCopyWithCustomBoAttrNamesFilter(boListToDelete, connection, executionContext, boAttrNamesToVerify);
    }

    public List<DynamicDTO> readCopyAndVerifyIdExists(List<DynamicDTO> boListToDelete, Connection connection, ExecutionContext executionContext) {

        // verify primary key exists for all the objects
        validatePrimaryKeyExists(boListToDelete, getPrimaryKeyColumns(), executionContext);
        // read a copy from db for all the objects
        return readCopy(boListToDelete, connection, executionContext);
    }

    private void validateBoAttrNamesExist(List<DynamicDTO> boListToRead, ExecutionContext executionContext, String... boAttrNamesToVerify) {
        for (DynamicDTO dynamicDTO : boListToRead) {
            for (String boAttrName : boAttrNamesToVerify) {
                if (dynamicDTO.get(boAttrName) == null) {
                    throw new DSPDMException("{} is mandatory to read a record copy", executionContext.getExecutorLocale(), boAttrName);
                } else if (dynamicDTO.get(boAttrName) instanceof java.lang.String) {
                    if (StringUtils.isNullOrEmpty((String) dynamicDTO.get(boAttrName))) {
                        throw new DSPDMException("{} is mandatory to read a record copy", executionContext.getExecutorLocale(), boAttrName);
                    }
                }
            }
        }
    }

    private void validatePrimaryKeyExists(List<DynamicDTO> boListToDelete, List<BusinessObjectAttributeDTO> primaryKeyColumns, ExecutionContext executionContext) {
        for (DynamicDTO dynamicDTO : boListToDelete) {
            dynamicDTO.setPrimaryKeyColumnNames(getPrimaryKeyColumnNames());
            for (BusinessObjectAttributeDTO primaryKeyDTO : primaryKeyColumns) {
                if (dynamicDTO.get(primaryKeyDTO.getBoAttrName()) == null) {
                    throw new DSPDMException("{} is mandatory to read a record copy", executionContext.getExecutorLocale(), primaryKeyDTO.getAttributeDisplayName());
                }
            }
        }
    }

    @Override
    public List<DynamicDTO> readCopyWithCustomBoAttrNamesFilter(List<DynamicDTO> businessObjectsToRead, Connection connection, ExecutionContext executionContext, String... boAttrNames) {

        List<DynamicDTO> businessObjectsFromRead = null;
        if ((boAttrNames == null) || (boAttrNames.length == 0)) {
            throw new DSPDMException("Cannot read a copy with a custom filter because filters are not provided", executionContext.getExecutorLocale());
        } else if (boAttrNames.length > 1) {
            // read one by one due to composite where clause
            businessObjectsFromRead = new ArrayList<>(businessObjectsToRead.size());
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(getType(), executionContext);
            for (DynamicDTO dto : businessObjectsToRead) {
                if ((businessObjectInfo.getFilterList() != null) && (businessObjectInfo.getFilterList().getFilters() != null)) {
                    businessObjectInfo.getFilterList().getFilters().clear();
                }
                for (String boAttrName : boAttrNames) {
                    businessObjectInfo.addFilter(boAttrName, dto.get(boAttrName));
                }
                businessObjectsFromRead.addAll(read(businessObjectInfo, false, connection, executionContext));
            }
        } else {
            // read all at once using in sql clause
            String boAttrName = boAttrNames[0];
            Set<Object> valuesFromList = CollectionUtils.getValuesSetFromList(businessObjectsToRead, boAttrName);
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(getType(), executionContext);
            businessObjectsFromRead = readLongRangeUsingInClause(businessObjectInfo, null, boAttrName,
                    valuesFromList.toArray(), Operator.IN, connection, executionContext);
        }
        return businessObjectsFromRead;
    }

    @Override
    public List<DynamicDTO> readCopy(List<DynamicDTO> businessObjectsToRead, Connection connection, ExecutionContext executionContext) {
        List<DynamicDTO> businessObjectsFromRead = null;
        if (isCompositePrimaryKey()) {
            // read one by one due to composite primary key
            businessObjectsFromRead = new ArrayList<>(businessObjectsToRead.size());
            DynamicDTO businessObjectFromRead = null;
            for (DynamicDTO dto : businessObjectsToRead) {
                // send read by primary key call to db for a single object
                businessObjectFromRead = readOne(dto.getId(), connection, executionContext);
                if (businessObjectFromRead != null) {
                    businessObjectsFromRead.add(businessObjectFromRead);
                }
            }
        } else {
            // read all at once using in sql clause
            String primaryKeyColumnName = getPrimaryKeyColumns().get(0).getBoAttrName();
            Set<Object> valuesFromList = CollectionUtils.getValuesSetFromList(businessObjectsToRead, primaryKeyColumnName);
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(getType(), executionContext);
            businessObjectsFromRead = readLongRangeUsingInClause(businessObjectInfo, null, primaryKeyColumnName, valuesFromList.toArray(), Operator.IN, connection, executionContext);
        }
        return businessObjectsFromRead;
    }

    @Override
    public List<DynamicDTO> readCopyRetainChildren(List<DynamicDTO> businessObjectsToRead, Connection connection, ExecutionContext executionContext) {
        List<DynamicDTO> businessObjectsFromRead = null;
        if (isCompositePrimaryKey()) {
            // read one by one due to composite primary key
            businessObjectsFromRead = new ArrayList<>(businessObjectsToRead.size());
            DynamicDTO businessObjectFromRead = null;
            for (DynamicDTO dto : businessObjectsToRead) {
                // send read by primary key call to db for a single object
                businessObjectFromRead = readOne(dto.getId(), connection, executionContext);
                if (businessObjectFromRead != null) {
                    if (dto.containsKey(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY)) {
                        // copy children from old dto to new dto
                        businessObjectFromRead.put(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY, dto.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY));
                    }
                    businessObjectsFromRead.add(businessObjectFromRead);
                }
            }
        } else {
            // read all at once using in sql clause
            String primaryKeyColumnName = getPrimaryKeyColumns().get(0).getBoAttrName();
            Set<Object> valuesFromList = CollectionUtils.getValuesSetFromList(businessObjectsToRead, primaryKeyColumnName);
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(getType(), executionContext);
            businessObjectsFromRead = readLongRangeUsingInClause(businessObjectInfo, null, primaryKeyColumnName, valuesFromList.toArray(), Operator.IN, connection, executionContext);
            // now copy children from old dto to new dto
            if (CollectionUtils.hasValue(businessObjectsFromRead)) {
                DynamicDTO businessObjectToRead = null;
                // preapre a map of source dto list
                Map<Object, DynamicDTO> businessObjectsToReadMap = CollectionUtils.prepareMapFromValuesOfKey(businessObjectsToRead, primaryKeyColumnName);
                // iterate over all destination objects
                for (DynamicDTO businessObjectFromRead : businessObjectsFromRead) {
                    businessObjectToRead = businessObjectsToReadMap.get(businessObjectFromRead.get(primaryKeyColumnName));
                    if (businessObjectToRead.containsKey(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY)) {
                        // copy children from old dto to new dto
                        businessObjectFromRead.put(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY, businessObjectToRead.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY));
                    }
                }
            }
        }
        return businessObjectsFromRead;
    }

    @Override
    public Map<DynamicPK, DynamicDTO> readCopyAsMap(List<DynamicDTO> businessObjectsToRead, Connection connection, ExecutionContext executionContext) {
        List<DynamicDTO> businessObjectsFromRead = readCopy(businessObjectsToRead, connection, executionContext);
        // now put all the business objects which are read from database in a hash map who key is primary key DymanicPK
        // DynamicPK equals() and hashCode() methods are properly overridden before doing this
        Map<DynamicPK, DynamicDTO> readObjectsMap = new HashMap<>(businessObjectsFromRead.size());
        for (DynamicDTO readDTO : businessObjectsFromRead) {
            readObjectsMap.put(readDTO.getId(), readDTO);
        }
        return readObjectsMap;
    }

    @Override
    public List<DynamicDTO> read(BusinessObjectInfo businessObjectInfo, boolean hasSQLExpression, Connection connection, ExecutionContext executionContext) {
        List<DynamicDTO> list = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        String sqlForRead = null;
        String finalSQL = null;
        long startTime = 0;
        try {
            sqlForRead = getSQLForRead(businessObjectInfo, this, executionContext);
            pstmt = prepareStatementForRead(sqlForRead, businessObjectInfo, executionContext, connection);
            bindSQLParametersForRead(pstmt, businessObjectInfo, hasSQLExpression, executionContext);
            finalSQL = logSQLForRead(sqlForRead, businessObjectInfo, this, executionContext);
            startTime = System.currentTimeMillis();
            resultSet = pstmt.executeQuery();
            logSQLTimeTaken(finalSQL, 1, (System.currentTimeMillis() - startTime), executionContext);
            list = extractDynamicDTOListFromResultSet(businessObjectInfo, resultSet, this, executionContext);
            if ((businessObjectInfo.isReadUnique()) && (list.size() > 1)) {
                throw new DSPDMException("Unique data was not found.  Found {} records that met the filter criteria.", executionContext.getExecutorLocale(), list.size());
            }
            logger.info("{} records of type {} read successfully.", list.size(), getType());
        } catch (Exception e) {
            if (finalSQL != null) {
                logSQLTimeTaken(finalSQL, 1, (System.currentTimeMillis() - startTime), executionContext);
            } else {
                // add to sql stats
                logSQLTimeTaken(sqlForRead, 0, 0, executionContext);
                // log in error logs console
                logSQL(sqlForRead, executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeResultSet(resultSet, executionContext);
            closeStatement(pstmt, executionContext);
        }
        return list;
    }

    @Override
    public List<DynamicDTO> readLongRangeUsingInClause(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter,
                                                       String boAttrNameForInClause, Object[] uniqueValuesForInClause,
                                                       Operator operator, Connection connection, ExecutionContext executionContext) {
        // must read read all records to true otherwise full data will not come
        List<DynamicDTO> uniqueBOList = new ArrayList<>(uniqueValuesForInClause.length);
        // SQL IN clause has a limit of 256 at max. so split
        for (int start = 0; start < uniqueValuesForInClause.length; ) {
            int max = start + DSPDMConstants.SQL.MAX_SQL_IN_ARGS;
            if (max > uniqueValuesForInClause.length) {
                max = uniqueValuesForInClause.length;
            }
            Object[] smallArray = Arrays.copyOfRange(uniqueValuesForInClause, start, max);
            if ((businessObjectInfo.isJoiningRead()) && (joinClauseToApplyFilter != null)) {
                // clear all existing filters
                if ((joinClauseToApplyFilter.getFilterList() != null)
                        && (joinClauseToApplyFilter.getFilterList().getFilters() != null)) {
                    joinClauseToApplyFilter.getFilterList().getFilters().clear();
                }
                // add new filter as new data
                joinClauseToApplyFilter.addCriteriaFilter(new CriteriaFilter(boAttrNameForInClause, operator, smallArray));
            } else {
                // clear all existing filters
                if ((businessObjectInfo.getFilterList() != null)
                        && (businessObjectInfo.getFilterList().getFilters() != null)) {
                    businessObjectInfo.getFilterList().getFilters().clear();
                }
                // add new filter as new data
                businessObjectInfo.addFilter(boAttrNameForInClause, operator, smallArray);
            }

            if (businessObjectInfo.getPagination().isDefaultPagination()) {
                businessObjectInfo.setReadAllRecords(true);
            }
            // Read reference business objects ids and names
            uniqueBOList.addAll(read(businessObjectInfo, false, connection, executionContext));
            start = start + DSPDMConstants.SQL.MAX_SQL_IN_ARGS;
        }
        return uniqueBOList;
    }

    @Override
    public List<DynamicDTO> readLongRangeUsingInClauseAndExistingFilters(BusinessObjectInfo businessObjectInfo,
                                                                         BaseJoinClause joinClauseToApplyFilter,
                                                                         String boAttrNameForInClause, Object[] uniqueValuesForInClause,
                                                                         Operator operator, Connection connection,
                                                                         ExecutionContext executionContext) {
        // must read read all records to true otherwise full data will not come
        List<DynamicDTO> uniqueBOList = new ArrayList<>(uniqueValuesForInClause.length);
        // SQL IN clause has a limit of 256 at max. so split
        BusinessObjectInfo newBusinessObjectInfo = null;
        for (int start = 0; start < uniqueValuesForInClause.length; ) {
            int max = start + DSPDMConstants.SQL.MAX_SQL_IN_ARGS;
            if (max > uniqueValuesForInClause.length) {
                max = uniqueValuesForInClause.length;
            }
            Object[] smallArray = Arrays.copyOfRange(uniqueValuesForInClause, start, max);
            // add new filter as new data
            // must make a deep copy before attaching the filter otherwise only first 256 records will come
//            newBusinessObjectInfo = ObjectUtils.deepCopy(businessObjectInfo, executionContext);
            // do not use deepcopy to clone. Because deep copy uses serialization and de-serialized objects are prohibited in SQL manipulation
            // See Insecure Interaction - CWE ID 089
            newBusinessObjectInfo = businessObjectInfo.clone(executionContext);
            // add filet with operator IN or NOT_IN
            if ((newBusinessObjectInfo.isJoiningRead()) && (joinClauseToApplyFilter != null)) {
                // find similar join clause on the new cloned business object info
                BaseJoinClause newJoinClauseToApplyFilter = newBusinessObjectInfo.findFirstJoinClauseForJoinAlias(joinClauseToApplyFilter.getJoinAlias());
                newJoinClauseToApplyFilter.addCriteriaFilter(new CriteriaFilter(boAttrNameForInClause, operator, smallArray));
            } else {
                newBusinessObjectInfo.addFilter(boAttrNameForInClause, operator, smallArray);
            }
            if (newBusinessObjectInfo.getPagination().isDefaultPagination()) {
                newBusinessObjectInfo.setReadAllRecords(true);
            }
            // Read reference business objects ids and names
            uniqueBOList.addAll(read(newBusinessObjectInfo, false, connection, executionContext));
            start = start + DSPDMConstants.SQL.MAX_SQL_IN_ARGS;
        }
        return uniqueBOList;
    }

    @Override
    public List<DynamicDTO> readLongRangeUsingCompositeORClause(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter,
                                                                List<String> uniqueBoAttrNames, Collection<DynamicDTO> uniqueBoAttrNamesValues,
                                                                Connection connection, ExecutionContext executionContext) {

        DynamicDTO[] uniqueValuesForCompositeFilter = uniqueBoAttrNamesValues.toArray(new DynamicDTO[0]);
        List<DynamicDTO> uniqueBOList = new ArrayList<>(uniqueValuesForCompositeFilter.length);
        // SQL IN clause has a limit of 256 at max. so split
        BusinessObjectInfo newBusinessObjectInfo = null;
        List<FilterGroup> filterGroups = null;
        DynamicDTO[] smallArray = null;
        for (int start = 0; start < uniqueValuesForCompositeFilter.length; ) {
            int max = start + DSPDMConstants.SQL.MAX_SQL_IN_ARGS;
            if (max > uniqueValuesForCompositeFilter.length) {
                max = uniqueValuesForCompositeFilter.length;
            }
            // make a small array of limited records
            smallArray = Arrays.copyOfRange(uniqueValuesForCompositeFilter, start, max);
            // new create filter groups for this small array
            filterGroups = new ArrayList<>(smallArray.length);
            for (DynamicDTO dynamicDTO : smallArray) {
                CriteriaFilter[] criteriaFilters = new CriteriaFilter[uniqueBoAttrNames.size()];
                int index = 0;
                for (String uniqueBoAttrName : uniqueBoAttrNames) {
                    criteriaFilters[index++] = new CriteriaFilter(uniqueBoAttrName, Operator.EQUALS, new Object[]{dynamicDTO.get(uniqueBoAttrName)});
                }
                filterGroups.add(new FilterGroup(Operator.OR, Operator.AND, criteriaFilters));
            }
            // add new filter as new data
            // must make a deep copy before attaching the filter otherwise only first 256 records will come
//            newBusinessObjectInfo = ObjectUtils.deepCopy(businessObjectInfo, executionContext);
            // do not use deepcopy to clone. Because deep copy uses serialization and de-serialized objects are prohibited in SQL manipulation
            // See Insecure Interaction - CWE ID 089
            newBusinessObjectInfo = businessObjectInfo.clone(executionContext);
            newBusinessObjectInfo.setRecordsAndPages(executionContext, smallArray.length, PagedList.FIRST_PAGE_NUMBER);
            if ((newBusinessObjectInfo.isJoiningRead()) && (joinClauseToApplyFilter != null)) {
                // find similar join clause on the new cloned business object info
                BaseJoinClause newJoinClauseToApplyFilter = newBusinessObjectInfo.findFirstJoinClauseForJoinAlias(joinClauseToApplyFilter.getJoinAlias());
                newJoinClauseToApplyFilter.addFilterGroup(filterGroups);
            } else {
                newBusinessObjectInfo.addFilterGroups(filterGroups);
            }
            // Read reference business objects ids and names
            uniqueBOList.addAll(read(newBusinessObjectInfo, false, connection, executionContext));
            start = start + DSPDMConstants.SQL.MAX_SQL_IN_ARGS;
        }
        return uniqueBOList;
    }

    /* ******************************************************** */
    /* ****************** Utility Methods ********************* */
    /* ******************************************************** */

    private static String getSQLForRead(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        String sql = DSPDMConstants.EMPTY_STRING;
        // SELECT CLAUSE
        sql = sql + getSelectClauseForRead(businessObjectInfo, dynamicDAO, executionContext);
        // FROM CLAUSE
        sql += getFromClause(businessObjectInfo, dynamicDAO, executionContext);
        // WHERE CLAUSE
        sql += getWhereClause(businessObjectInfo, dynamicDAO, executionContext);
        // GROUP BY CLAUSE
        sql += getGroupByClause(businessObjectInfo, dynamicDAO, executionContext);
        // HAVING CLAUSE
        sql += getHavingClause(businessObjectInfo, dynamicDAO, executionContext);
        // ORDER BY CLAUSE
        sql += getOrderByClause(businessObjectInfo, dynamicDAO, executionContext);
        // PAGINATION CLAUSE
        sql += getPaginationClause(businessObjectInfo, dynamicDAO, executionContext);
        return sql;
    }

    private static String getSQLForReadForDistinctCount(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        String sql = DSPDMConstants.EMPTY_STRING;
        // SELECT CLAUSE
        sql = sql + getSelectClauseForRead(businessObjectInfo, dynamicDAO, executionContext);
        // FROM CLAUSE
        sql += getFromClause(businessObjectInfo, dynamicDAO, executionContext);
        // WHERE CLAUSE
        sql += getWhereClause(businessObjectInfo, dynamicDAO, executionContext);
        // GROUP BY CLAUSE
        sql += getGroupByClause(businessObjectInfo, dynamicDAO, executionContext);
        // HAVING CLAUSE
        sql += getHavingClause(businessObjectInfo, dynamicDAO, executionContext);
        return sql;
    }

    private static String getSQLForCount(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        String sql = DSPDMConstants.EMPTY_STRING;
        // SELECT CLAUSE
        sql = sql + getSelectClauseForCount(businessObjectInfo, dynamicDAO, executionContext);
        // FROM CLAUSE
        sql += getFromClause(businessObjectInfo, dynamicDAO, executionContext);
        // WHERE CLAUSE
        sql += getWhereClause(businessObjectInfo, dynamicDAO, executionContext);
        // GROUP BY CLAUSE
        sql += getGroupByClause(businessObjectInfo, dynamicDAO, executionContext);
        // HAVING CLAUSE
        sql += getHavingClause(businessObjectInfo, dynamicDAO, executionContext);
        return sql;
    }

    private static String getSelectClauseForRead(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        String selectClause = "SELECT";
        if (businessObjectInfo.isReadWithDistinct()) {
            selectClause += " DISTINCT";
        }
        List<String> physicalColumnsToSelect = getPhysicalAllColumnsToSelectForRead(businessObjectInfo, dynamicDAO, executionContext);
        if (CollectionUtils.hasValue(physicalColumnsToSelect)) {
            String commaSeparatedSelectList = CollectionUtils.getCommaSeparated(physicalColumnsToSelect);
            if (StringUtils.hasValue(commaSeparatedSelectList)) {
                selectClause += DSPDMConstants.SPACE + commaSeparatedSelectList;
            } else {
                selectClause += " *";
            }
        } else {
            selectClause += " *";
        }
        return selectClause;
    }

    private static String getSelectClauseForCount(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
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

    private static String getFromClause(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        return " FROM " + dynamicDAO.getDatabaseTableName() + (StringUtils.isNullOrEmpty(businessObjectInfo.getAlias()) ? "" : " " + businessObjectInfo.getAlias());
    }

    private static String getWhereClause(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        StringBuffer sbWhereClause = new StringBuffer();
        if (businessObjectInfo.getFilterList() != null) {
            if (CollectionUtils.hasValue(businessObjectInfo.getFilterList().getFilters())) {
                List<String> filtersWithPlaceHolders = getPhysicalColumnsFiltersWithPlaceHolders(businessObjectInfo, String.valueOf(DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER), dynamicDAO, executionContext);
                sbWhereClause.append(CollectionUtils.joinWithAnd(filtersWithPlaceHolders));
            }
            if (CollectionUtils.hasValue(businessObjectInfo.getFilterList().getFilterGroups())) {
                String sqlForFilterGroups = getWhereClauseForFilterGroups(businessObjectInfo.getAlias(),
                        String.valueOf(DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER), businessObjectInfo.getFilterList(), dynamicDAO, executionContext);
                if (StringUtils.hasValue(sqlForFilterGroups)) {
                    // if group filter clause is not the first clause then append AND
                    if (sbWhereClause.length() > 0) {
                        sbWhereClause.append(Operator.AND.getOperator());
                    }
                    sbWhereClause.append(sqlForFilterGroups);
                }
            }
            if (sbWhereClause.length() > 0) {
                sbWhereClause.insert(0, " WHERE ");
            }
        }
        return sbWhereClause.toString();
    }

    private static String getGroupByClause(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        String groupByClause = DSPDMConstants.EMPTY_STRING;
        if ((businessObjectInfo.hasAggregateColumnToSelect()) && (businessObjectInfo.hasSimpleColumnToSelect())) {
            List<String> physicalSimpleColumnsToSelect = getPhysicalSimpleColumnsToSelect(businessObjectInfo, dynamicDAO, executionContext);
            if (CollectionUtils.hasValue(physicalSimpleColumnsToSelect)) {
                String joinedWithComma = CollectionUtils.getCommaSeparated(physicalSimpleColumnsToSelect);
                if (StringUtils.hasValue(joinedWithComma)) {
                    groupByClause = " GROUP BY " + joinedWithComma;
                }
            }
        }
        return groupByClause;
    }

    private static String getHavingClause(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        String havingClause = DSPDMConstants.EMPTY_STRING;
        List<String> filtersWithPlaceHolders = getPhysicalColumnsHavingFiltersWithPlaceHolders(businessObjectInfo, String.valueOf(DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER), dynamicDAO, executionContext);
        if (CollectionUtils.hasValue(filtersWithPlaceHolders)) {
            String joinedWithAnd = CollectionUtils.joinWithAnd(filtersWithPlaceHolders);
            if (StringUtils.hasValue(joinedWithAnd)) {
                havingClause = " HAVING " + joinedWithAnd;
            }
        }
        return havingClause;
    }

    private static String getOrderByClause(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
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
     * @param businessObjectInfo
     * @param executionContext
     * @return
     * @author muhammadimran.ansari
     * @modified made protected from private on 07-Apr-2020 so that can be invoked from derived class of join implementation
     */
    protected static String getPaginationClause(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        String paginationClause = DSPDMConstants.EMPTY_STRING;
        if (!(businessObjectInfo.isReadAllRecords())) {
            // all sql statements are paginated except those with read all records
            // so must add offset even it is zero
            paginationClause += " OFFSET (" + DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER + ") ROWS";
            if ((businessObjectInfo.isReadFirst()) || (businessObjectInfo.isReadUnique())) {
                paginationClause += " FETCH NEXT (" + DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER + ") ROWS ONLY";
            } else if (businessObjectInfo.getPagination().getRecordsToRead() != Integer.MAX_VALUE) {
                paginationClause += " FETCH NEXT (" + DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER + ") ROWS ONLY";
            }
        }
        return paginationClause;
    }

    protected static String getEachFilterClause(String alias, String placeHolder, CriteriaFilter filter,
                                              Map<String, BusinessObjectAttributeDTO> columnsMetadataMap, ExecutionContext executionContext) {
        String physicalColumnName = null;
        if (columnsMetadataMap != null) {
            BusinessObjectAttributeDTO boAttrDTO = columnsMetadataMap.get(filter.getColumnName());
            if (boAttrDTO != null) {
                // column is found. Add physical name from metadata along with the alias if provided
                physicalColumnName = (StringUtils.hasValue(alias) ? alias + DSPDMConstants.DOT : "") + boAttrDTO.getAttributeName();
            } else if (filter.getColumnName().lastIndexOf(DSPDMConstants.DOT) > 0) {
                int indexDot = filter.getColumnName().lastIndexOf(DSPDMConstants.DOT) + 1;
                boAttrDTO = columnsMetadataMap.get(filter.getColumnName().substring(indexDot));
                if (boAttrDTO != null) {
                    // column is found. Add column name as is with the given alias inside the column name
                    physicalColumnName = filter.getColumnName();
                }
            }
            if (boAttrDTO != null) {
                // set java type from metadata
                filter.setSqlDataType(boAttrDTO.getSqlDataType());
                // convert values data type if not already converted in case of internal calls to the service
                if (String.class != boAttrDTO.getJavaDataType()) {
                    Object[] values = filter.getValues();
                    if ((values != null) && (values.length > 0) && (values[0] instanceof String)) {
                        // in case value is string and the metadata type is not string then covert
                        for (Object value : values) {
                            if ((value != null) && (value instanceof String)) {
                                filter.replaceValue(MetadataUtils.convertValueToJavaDataTypeFromString((String) value,
                                        boAttrDTO.getJavaDataType(), boAttrDTO.getBoName(), boAttrDTO.getBoAttrName(), executionContext));
                            }
                        }
                    }
                }
            }
        }
        // check if column name not found in metadata
        if (physicalColumnName == null) {
            // add logical name itself. It might be an expression or constant
            if (StringUtils.isAlphaNumericOrUnderScore(filter.getColumnName())) {
                // add alias in start if alias provided as it is just a column name
                physicalColumnName = "CAST(" + (StringUtils.hasValue(alias) ? alias + DSPDMConstants.DOT : "") + filter.getColumnName() + " AS VARCHAR)";
            } else {
                // do not add alias as it is not a simple column name
                physicalColumnName = "CAST(" + filter.getColumnName() + " AS VARCHAR)";
            }
            // set java type default as String
            filter.setSqlDataType(Types.VARCHAR);
        }
        String filterClause = getSQLOperatorCondition(filter, physicalColumnName, placeHolder, executionContext);
        return filterClause;
    }

    private static List<String> getPhysicalColumnsFiltersWithPlaceHolders(BusinessObjectInfo businessObjectInfo, String placeHolder, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        List<String> physicalFilter = null;
        if ((businessObjectInfo.getFilterList() != null) && (businessObjectInfo.getFilterList().getFilters() != null)) {
            Map<String, BusinessObjectAttributeDTO> columnsMetadataMap = dynamicDAO.getBoAttributeNamesMetadataMap();
            List<CriteriaFilter> filters = businessObjectInfo.getFilterList().getFilters();
            physicalFilter = new ArrayList<>(filters.size());
            String filterClause = null;
            for (CriteriaFilter filter : filters) {
                filterClause = getEachFilterClause(businessObjectInfo.getAlias(), placeHolder, filter, columnsMetadataMap, executionContext);
                physicalFilter.add(filterClause);
            }
        }
        return physicalFilter;
    }

    /**
     *
     * @param alias
     * @param placeHolder
     * @param filterList
     * @param dynamicDAO
     * @param executionContext
     * @return
     */
    protected static String getWhereClauseForFilterGroups(String alias, String placeHolder, FilterList filterList,
                                                          AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        StringBuilder stringBuilder = new StringBuilder();
        if ((filterList != null) && (CollectionUtils.hasValue(filterList.getFilterGroups()))) {
            stringBuilder.append(getWhereClauseForFilterGroupsNonRecursive(alias, placeHolder,
                    filterList.getFilterGroups().toArray(new FilterGroup[0]), dynamicDAO, executionContext));
        }
        return stringBuilder.toString();
    }

    /**
     * non-recurse filter group to get columns filters statement
     *
     * @author Qinghua MA
     */
    private static String getWhereClauseForFilterGroupsNonRecursive(String alias, String placeHolder, FilterGroup[] filterGroups,
                                                                    AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        StringBuilder stringBuilder = new StringBuilder();
        boolean notFirstGroup = false;
        if (CollectionUtils.hasValue(filterGroups)) {
            if(filterGroups.length > 0) {
                stringBuilder.append("(");
            }
            for (FilterGroup childFilterGroup : filterGroups) {
                if (notFirstGroup) {
                    stringBuilder.append(childFilterGroup.getGroupOperator().getOperator());
                } else {
                    notFirstGroup = true;
                }
                // recursive call
                stringBuilder.append(getWhereClauseForOneFilterGroup(alias, placeHolder, childFilterGroup, dynamicDAO, executionContext));
            }
            if(filterGroups.length > 0) {
                stringBuilder.append(")");
            }
        }
        return stringBuilder.toString();
    }

    /**
     * recurse filter group to get columns filters statement
     *
     * @author Qinghua MA
     */
    private static String getWhereClauseForFilterGroupsRecursive(String alias, String placeHolder, FilterGroup[] filterGroups,
                                                                 AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        StringBuilder stringBuilder = new StringBuilder();
        if (CollectionUtils.hasValue(filterGroups)) {
            if(filterGroups.length > 0) {
                stringBuilder.append("(");
            }
            boolean firstGroup = true;
            for (FilterGroup childFilterGroup : filterGroups) {
                if(!firstGroup) {
                    stringBuilder.append(childFilterGroup.getGroupOperator().getOperator());
                } else {
                    firstGroup = false;
                }
                // recursive call
                stringBuilder.append(getWhereClauseForOneFilterGroup(alias, placeHolder, childFilterGroup, dynamicDAO, executionContext));
            }
            if(filterGroups.length > 0) {
                stringBuilder.append(")");
            }
        }
        return stringBuilder.toString();
    }

    /**
     * recurse filter group to get columns filters statement
     *
     * @author Qinghua MA
     */
    private static String getWhereClauseForOneFilterGroup(String alias, String placeHolder, FilterGroup filterGroup, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        StringBuilder stringBuilder = new StringBuilder();
        boolean filtersAdded = false;
        if (CollectionUtils.hasValue(filterGroup.getCriteriaFilters())) {
            // append to the main string builder
            stringBuilder.append("(");
            stringBuilder.append(getWhereClauseForOneFilterGroupCriteriaFilters(alias, filterGroup, placeHolder, dynamicDAO, executionContext));
            stringBuilder.append(")");
            filtersAdded = true;
        }
        if (CollectionUtils.hasValue(filterGroup.getFilterGroups())) {
            if(filtersAdded) {
                stringBuilder.insert(0, "(");
                stringBuilder.append(Operator.AND.getOperator());
            }
            stringBuilder.append(getWhereClauseForFilterGroupsRecursive(alias, placeHolder, filterGroup.getFilterGroups(), dynamicDAO, executionContext));
            if(filtersAdded) {
                stringBuilder.append(")");
            }
        }
        return stringBuilder.toString();
    }

    /**
     * recurse filter group to get columns filters statement
     *
     * @author Qinghua MA
     */
    private static String getWhereClauseForOneFilterGroupCriteriaFilters(String alias, FilterGroup filterGroup, String placeHolder,
                                                                         AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        StringBuilder stringBuilder = new StringBuilder();
        if (CollectionUtils.hasValue(filterGroup.getCriteriaFilters())) {
            Map<String, BusinessObjectAttributeDTO> columnsMetadataMap = (dynamicDAO == null) ? null : dynamicDAO.getBoAttributeNamesMetadataMap();
            String filterClause = null;
            boolean firstFilter = true;
            for (CriteriaFilter filter : filterGroup.getCriteriaFilters()) {
                if (!firstFilter) {
                    stringBuilder.append(filterGroup.getCombineWith().getOperator());
                } else {
                    firstFilter = false;
                }
                filterClause = getEachFilterClause(alias, placeHolder, filter, columnsMetadataMap, executionContext);
                stringBuilder.append(filterClause);
            }
        }
        return stringBuilder.toString();
    }

    private static List<String> getPhysicalColumnsHavingFiltersWithPlaceHolders(BusinessObjectInfo businessObjectInfo, String placeHolder, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        List<String> physicalFilter = null;
        if ((businessObjectInfo.getFilterList() != null) && (businessObjectInfo.getFilterList().getHavingFilters() != null)) {
            Map<String, BusinessObjectAttributeDTO> columnsMetadataMap = (dynamicDAO == null) ? null : dynamicDAO.getBoAttributeNamesMetadataMap();
            List<HavingFilter> havingFilters = businessObjectInfo.getFilterList().getHavingFilters();
            physicalFilter = new ArrayList<>(havingFilters.size());
            String physicalColumnName = null;
            String filterClause = null;

            for (HavingFilter havingFilter : havingFilters) {
                BusinessObjectAttributeDTO boAttrDTO = (columnsMetadataMap == null) ? null : columnsMetadataMap.get(havingFilter.getAggregateColumn().getBoAttrName());
                if (boAttrDTO != null) {
                    // column is found. Add physical name
                    physicalColumnName = havingFilter.getAggregateColumn().getAggregateFunction().getFunctionName() + "(" +
                            (StringUtils.hasValue(businessObjectInfo.getAlias()) ? businessObjectInfo.getAlias() + DSPDMConstants.DOT : "") + boAttrDTO.getAttributeName()
                            + ")";
                } else if (havingFilter.getAggregateColumn().getBoAttrName().lastIndexOf(DSPDMConstants.DOT) > 0) {
                    int indexDot = havingFilter.getAggregateColumn().getBoAttrName().lastIndexOf(DSPDMConstants.DOT) + 1;
                    boAttrDTO = columnsMetadataMap.get(havingFilter.getAggregateColumn().getBoAttrName().substring(indexDot));
                    if (boAttrDTO != null) {
                        // column is found. Add physical name
                        physicalColumnName = havingFilter.getAggregateColumn().getAggregateFunction().getFunctionName() + "("
                                + havingFilter.getAggregateColumn().getBoAttrName() + ")";
                    }
                }
                if (physicalColumnName == null) {
                    // add logical name itself. It might be an expression
                    if (StringUtils.isAlphaNumericOrUnderScore(havingFilter.getAggregateColumn().getBoAttrName())) {
                        // add alias in start if alias provided as it is just a column name
                        physicalColumnName = havingFilter.getAggregateColumn().getAggregateFunction().getFunctionName() + "(" +
                                (StringUtils.hasValue(businessObjectInfo.getAlias()) ? businessObjectInfo.getAlias() + DSPDMConstants.DOT : "")
                                + havingFilter.getAggregateColumn().getBoAttrName()
                                + ")";
                    } else {
                        // do not add alias as it is not a simple column name
                        physicalColumnName = havingFilter.getAggregateColumn().getAggregateFunction().getFunctionName() + "("
                                + havingFilter.getAggregateColumn().getBoAttrName() + ")";
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
                        if (boAttrDTO != null) {
                            havingFilter.setSqlDataType(boAttrDTO.getSqlDataType());
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
                        } else {
                            havingFilter.setSqlDataType(Types.VARCHAR);
                        }
                        break;
                    default:
                        havingFilter.setSqlDataType(Types.VARCHAR);
                }
                // column must be casted to varchar to be compared becuase in case of aggregate functions mostly we do not know the result data type.
                if (havingFilter.getSqlDataType() == Types.VARCHAR) {
                    physicalColumnName = "CAST(" + physicalColumnName + " AS VARCHAR)";
                }
                filterClause = getSQLOperatorCondition(havingFilter, physicalColumnName, placeHolder, executionContext);
                physicalFilter.add("(" + filterClause + ")");
            }
        }
        return physicalFilter;
    }

    /**
     * @param filter
     * @param physicalColumnName
     * @param placeHolder
     * @param executionContext
     * @return
     * @author muhammadimran.ansari
     * @modified made protected from private on 07-Apr-2020 so that can be invoked from derived class of join implementation
     */
    protected static String getSQLOperatorCondition(BaseFilter filter, String physicalColumnName, String placeHolder, ExecutionContext executionContext) {
        String filterClause = null;
        switch (filter.getOperator()) {
            case EQUALS:
            case NOT_EQUALS:
            case GREATER_THAN:
            case LESS_THAN:
            case GREATER_OR_EQUALS:
            case LESS_OR_EQUALS:
                filterClause = getConditionForNormalOperator(filter, physicalColumnName, placeHolder, executionContext);
                break;
            case BETWEEN:
            case NOT_BETWEEN:
                filterClause = getConditionForBetweenOperator(filter, physicalColumnName, placeHolder, executionContext);
                break;
            case LIKE:
            case NOT_LIKE:
            case ILIKE:
            case NOT_ILIKE:
                // IF LIKE is called on a number data type then convert it to VARCHAR and then apply LIKE
                if (DSPDMConstants.SQL.NUMERIC_DATA_TYPES.contains(filter.getSqlDataType())) {
                    physicalColumnName = "CAST(" + physicalColumnName + " AS VARCHAR)";
                    filter.setSqlDataType(Types.VARCHAR);
                }
                filterClause = getConditionForLikeOperator(filter, physicalColumnName, placeHolder, executionContext);
                break;
            case IN:
            case NOT_IN:
                filterClause = getConditionForInOperator(filter, physicalColumnName, placeHolder, executionContext);
                break;
            case JSONB_FIND_EXACT:
                filterClause = getConditionForJsonbFindExactOperator(filter, physicalColumnName, placeHolder, executionContext);
                break;
            case JSONB_FIND_LIKE:
                filterClause = getConditionForJsonbFindLikeOperator(filter, physicalColumnName, placeHolder, executionContext);
                break;
            default:
                throw new DSPDMException("SQL operator '{}' is not supported", executionContext.getExecutorLocale(), filter.getOperator());
        }
        return filterClause;
    }

    private static String getConditionForNormalOperator(BaseFilter filter, String columnName, String placeHolder, ExecutionContext executionContext) {
        String condition = null;
        if ((filter.getValues() != null) && (filter.getValues().length != 1)) {
            throw new DSPDMException("SQL operator '{}' mismatches the number of values provided '{}'", executionContext.getExecutorLocale(), filter.getOperator().name(), filter.getValues().length);
        }
        Object value = (filter.getValues() == null) ? null : filter.getValues()[0];
        if (value == null) {
            if (filter.getOperator() == Operator.EQUALS) {
                condition = columnName + " IS NULL";
            } else if (filter.getOperator() == Operator.NOT_EQUALS) {
                condition = columnName + " IS NOT NULL";
            } else {
                throw new DSPDMException("SQL operator '{}' is not applicable for NULL value", executionContext.getExecutorLocale(), filter.getOperator().name());
            }
        } else {
            condition = columnName + filter.getOperator().getOperator() + placeHolder;
        }
        return condition;
    }

    private static String getConditionForBetweenOperator(BaseFilter filter, String columnName, String placeHolder, ExecutionContext executionContext) {
        String condition = null;
        if (filter.getValues().length != 2) {
            throw new DSPDMException("SQL operator '{}' mismatches the number of values provided '{}'", executionContext.getExecutorLocale(), filter.getOperator().name(), filter.getValues().length);
        }
        if ((filter.getValues()[0] == null) || (filter.getValues()[1] == null)) {
            throw new DSPDMException("SQL operator '{}' does not support NULL values", executionContext.getExecutorLocale(), filter.getOperator().name());
        }
        condition = columnName + filter.getOperator().getOperator() + placeHolder + " AND " + placeHolder;
        return condition;
    }

    private static String getConditionForLikeOperator(BaseFilter filter, String columnName, String placeHolder, ExecutionContext executionContext) {
        String condition = null;
        if (filter.getValues().length != 1) {
            throw new DSPDMException("SQL operator '{}' mismatches the number of values provided '{}'", executionContext.getExecutorLocale(), filter.getOperator().name(), filter.getValues().length);
        }
        Object value = filter.getValues()[0];
        if (value == null) {
            throw new DSPDMException("SQL operator '{}' does not support NULL values", executionContext.getExecutorLocale(), filter.getOperator().name());
        }

        // support numer based like filter as well including string based
        if ((value instanceof String) || (filter.getSqlDataType() == Types.VARCHAR)) {
            String strValue = null;
            if (value instanceof String) {
                strValue = (String) value;
            } else {
                strValue = String.valueOf(value);
            }
            strValue.trim();
            if (!strValue.startsWith("%")) {
                strValue = "%" + strValue;
            }
            if (!strValue.endsWith("%")) {
                strValue = strValue + "%";
            }
            // replace the actual value with the new one
            filter.replaceValue(strValue);
        }
        condition = columnName + filter.getOperator().getOperator() + placeHolder;
        return condition;
    }

    private static String getConditionForInOperator(BaseFilter filter, String columnName, String placeHolder, ExecutionContext executionContext) {
        String condition = null;
        if (filter.getValues().length > DSPDMConstants.SQL.MAX_SQL_IN_ARGS) {
            throw new DSPDMException("SQL operator '{}' does not support more than '{}' values", executionContext.getExecutorLocale(), filter.getOperator().name(), DSPDMConstants.SQL.MAX_SQL_IN_ARGS);
        }
        if (filter.getValues().length > 1) {
            List<String> inClausePlaceHolders = new ArrayList<>(filter.getValues().length);
            for (int i = 0; i < filter.getValues().length; i++) {
                inClausePlaceHolders.add(placeHolder);
            }
            String commaSeparated = CollectionUtils.getCommaSeparated(inClausePlaceHolders);
            condition = columnName + filter.getOperator().getOperator() + "(" + commaSeparated + ")";
        } else if (filter.getValues().length == 1) {
            if (filter.getOperator() == Operator.IN) {
                condition = columnName + Operator.EQUALS.getOperator() + placeHolder;
            } else {
                condition = columnName + Operator.NOT_EQUALS.getOperator() + placeHolder;
            }
        }
        return condition;
    }

    private static String getConditionForJsonbFindExactOperator(BaseFilter filter, String columnName, String placeHolder, ExecutionContext executionContext) {
        String condition = null;
        if (filter.getValues().length > DSPDMConstants.SQL.MAX_SQL_IN_ARGS) {
            throw new DSPDMException("SQL operator '{}' does not support more than '{}' values", executionContext.getExecutorLocale(), filter.getOperator().name(), DSPDMConstants.SQL.MAX_SQL_IN_ARGS);
        }
        List<String> findClausePlaceHolders = new ArrayList<>(filter.getValues().length);
        for (int i = 0; i < filter.getValues().length; i++) {
            findClausePlaceHolders.add(placeHolder);
        }
        String commaSeparated = CollectionUtils.getCommaSeparated(findClausePlaceHolders);
        condition = filter.getOperator().getOperator() + "(" + columnName + ", array[" + commaSeparated + "])";
        return condition;
    }

    private static String getConditionForJsonbFindLikeOperator(BaseFilter filter, String columnName, String placeHolder, ExecutionContext executionContext) {
        String condition = null;
        if (filter.getValues().length > DSPDMConstants.SQL.MAX_SQL_IN_ARGS) {
            throw new DSPDMException("SQL operator '{}' does not support more than '{}' values", executionContext.getExecutorLocale(), filter.getOperator().name(), DSPDMConstants.SQL.MAX_SQL_IN_ARGS);
        }
        String jsonbOrOperatorSeparated = CollectionUtils.joinWith(DSPDMConstants.JsonbOperators.OR.getOperator(), filter.getValues());
        if (!((jsonbOrOperatorSeparated.startsWith("%(")) && (jsonbOrOperatorSeparated.endsWith(")%")))) {
            jsonbOrOperatorSeparated = "%(" + jsonbOrOperatorSeparated + ")%";
            filter.replaceValue(jsonbOrOperatorSeparated);
        }
        condition = columnName + filter.getOperator().getOperator() + placeHolder;
        return condition;
    }

    private static List<String> getPhysicalAllColumnsToSelectForRead(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        List<String> physicalColumnsNames = null;
        if (businessObjectInfo.getSelectList() != null) {
            physicalColumnsNames = new ArrayList<>(businessObjectInfo.getSelectList().getTotalColumnsCount());
            //simple columns
            if (CollectionUtils.hasValue(businessObjectInfo.getSelectList().getColumnsToSelect())) {
                physicalColumnsNames.addAll(getPhysicalSimpleColumnsToSelect(businessObjectInfo, dynamicDAO, executionContext));
            }
            // aggregate columns
            if (CollectionUtils.hasValue(businessObjectInfo.getSelectList().getAggregateColumnsToSelect())) {
                physicalColumnsNames.addAll(getPhysicalAggregateColumnsToSelect(businessObjectInfo, dynamicDAO, executionContext));
            }
        }
        return physicalColumnsNames;
    }

    private static List<String> getPhysicalAllColumnsToSelectForCount(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        List<String> physicalColumnsNames = null;
        if (businessObjectInfo.getSelectList() != null) {
            physicalColumnsNames = new ArrayList<>(businessObjectInfo.getSelectList().getTotalColumnsCount());
            //simple columns
            if (CollectionUtils.hasValue(businessObjectInfo.getSelectList().getColumnsToSelect())) {
                physicalColumnsNames.addAll(getPhysicalSimpleColumnsToSelect(businessObjectInfo, dynamicDAO, executionContext));
            }
            // aggregate columns
            // following code must be commented we cannot apply count over any aggregate column
            // if (CollectionUtils.hasValue(businessObjectInfo.getSelectList().getAggregateColumnsToSelect())) {
            //    physicalColumnsNames.addAll(getPhysicalAggregateColumnsToSelect(businessObjectInfo, executionContext));
            // }
        }
        return physicalColumnsNames;
    }

    private static List<String> getPhysicalSimpleColumnsToSelect(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        List<String> physicalColumnsNames = new ArrayList<>(businessObjectInfo.getSelectList().getColumnsToSelect().size());
        Map<String, BusinessObjectAttributeDTO> columnsMetadataMap = dynamicDAO.getBoAttributeNamesMetadataMap();
        // simple columns            
        List<String> columnsToSelect = businessObjectInfo.getSelectList().getColumnsToSelect();
        for (String bo_attr_name : columnsToSelect) {
            BusinessObjectAttributeDTO boAttrDTO = columnsMetadataMap.get(bo_attr_name);
            if (boAttrDTO != null) {
                // column is found. Add physical name
                physicalColumnsNames.add(boAttrDTO.getAttributeName());
            } else {
                // add logical name itself. It might be an expression
                physicalColumnsNames.add(bo_attr_name);
            }
        }
        return physicalColumnsNames;
    }

    private static List<String> getPhysicalAggregateColumnsToSelect(BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        List<String> physicalColumnsNames = new ArrayList<>(businessObjectInfo.getSelectList().getAggregateColumnsToSelect().size());
        Map<String, BusinessObjectAttributeDTO> columnsMetadataMap = dynamicDAO.getBoAttributeNamesMetadataMap();
        // aggregate columns            
        List<AggregateColumn> aggregateColumnsToSelect = businessObjectInfo.getSelectList().getAggregateColumnsToSelect();
        int index = 0;
        for (AggregateColumn aggregateColumn : aggregateColumnsToSelect) {
            if (StringUtils.isNullOrEmpty(aggregateColumn.getColumnAlias())) {
                // generate an alias and assign it back to column so that can be used in future to get data back from result set
                aggregateColumn.setColumnAlias(aggregateColumn.getAggregateFunction().getFunctionName() + "_" + index++);
            }
            BusinessObjectAttributeDTO boAttrDTO = columnsMetadataMap.get(aggregateColumn.getBoAttrName());
            if (boAttrDTO != null) {
                // column is found. Add physical name                    
                physicalColumnsNames.add(aggregateColumn.getAggregateFunction().getFunctionName() + "(" + boAttrDTO.getAttributeName() + ") AS " + aggregateColumn.getColumnAlias());
            } else {
                // add logical name itself. It might be an expression
                physicalColumnsNames.add(aggregateColumn.getAggregateFunction().getFunctionName() + "(" + aggregateColumn.getBoAttrName() + ") AS " + aggregateColumn.getColumnAlias());
            }
        }
        return physicalColumnsNames;
    }

    private static Map<String, String> getPhysicalColumnsForOrder(BusinessObjectInfo businessObjectInfo,
                                                                  AbstractDynamicDAOImplForRead dynamicDAO,
                                                                  ExecutionContext executionContext) {
        Map<String, String> physicalColumnsAndOrder = null;
        if ((businessObjectInfo.getOrderBy() != null) && (businessObjectInfo.getOrderBy().getOrderMap() != null)) {
            Map<String, BusinessObjectAttributeDTO> columnsMetadataMap = dynamicDAO.getBoAttributeNamesMetadataMap();
            Map<String, Boolean> orderMap = businessObjectInfo.getOrderBy().getOrderMap();
            // use LinkedHashMap to maintain column order
            physicalColumnsAndOrder = new LinkedHashMap<>(orderMap.size());
            String column = null;
            for (String logicalName : orderMap.keySet()) {
                BusinessObjectAttributeDTO boAttrDTO = columnsMetadataMap.get(logicalName);
                if (boAttrDTO != null) {
                    // column is found. Add physical name
                    column = boAttrDTO.getAttributeName();
                    // set NULLS LAST for sql server only in ascending order
                    if (!(businessObjectInfo.isReadWithDistinct())) {
                        if ((boAttrDTO.getJavaDataType() == String.class) || (boAttrDTO.getJavaDataType() == Character.class)) {
                            column = "(CASE WHEN " + column + " IS NULL THEN 2 WHEN " + column + " = '' THEN 1 ELSE 0 END) ASC, " + column;
                        } else {
                            column = "(CASE WHEN " + column + " IS NULL THEN 2 WHEN CAST( " + column + " AS VARCHAR) = '' THEN 1 ELSE 0 END) ASC, " + column;
                        }
                    }
                    physicalColumnsAndOrder.put(column, ((orderMap.get(logicalName)) ? "ASC" : "DESC"));
                } else {
                    // Order must be handle separately for BO_SEARCH due to presence of JSONB
                    if (businessObjectInfo.getBusinessObjectType().equals(DSPDMConstants.BoName.BO_SEARCH)) {
                        physicalColumnsAndOrder.put(DSPDMConstants.BoAttrName.OBJECT_JSONB + Operator.JSONB_DOT.getOperator()
                                + "'" + logicalName + "'", ((orderMap.get(logicalName)) ? "ASC" : "DESC"));
                    } else {
                        column = logicalName;
                        // set NULLS LAST for sql server only in ascending order
                        if (!(businessObjectInfo.isReadWithDistinct())) {
                            column = "(CASE WHEN " + column + " IS NULL THEN 2 WHEN CAST( " + column + " AS VARCHAR) = '' THEN 1 ELSE 0 END) ASC, " + column;
                        }
                        // add logical name itself. It might be an expression
                        physicalColumnsAndOrder.put(column, ((orderMap.get(logicalName)) ? "ASC" : "DESC"));
                    }
                }
            }
        }
        return physicalColumnsAndOrder;
    }

    private static int bindSQLParametersForRead(PreparedStatement pstmt, BusinessObjectInfo businessObjectInfo, boolean hasSQLExpression, ExecutionContext executionContext) {
        // start from 1
        int paramIndex = bindSQLParametersForWhereClause(pstmt, businessObjectInfo, 1, hasSQLExpression, executionContext);
        paramIndex = bindSQLParametersForHavingClause(pstmt, businessObjectInfo, paramIndex, hasSQLExpression, executionContext);
        paramIndex = bindSQLParametersForPagination(pstmt, businessObjectInfo, paramIndex, executionContext);
        return paramIndex;
    }

    private static int bindSQLParametersForCount(PreparedStatement pstmt, BusinessObjectInfo businessObjectInfo, boolean hasSQLExpression, ExecutionContext executionContext) {
        // start from 1
        int paramIndex = bindSQLParametersForWhereClause(pstmt, businessObjectInfo, 1, hasSQLExpression, executionContext);
        paramIndex = bindSQLParametersForHavingClause(pstmt, businessObjectInfo, paramIndex, hasSQLExpression, executionContext);
        return paramIndex;
    }

    protected static int bindSQLParametersForWhereClause(PreparedStatement pstmt, BusinessObjectInfo businessObjectInfo, int paramStartIndex, boolean hasSQLExpression, ExecutionContext executionContext) {
        Map<String, Integer> filterGroupMap = new HashMap<String, Integer>();
        filterGroupMap.put(DSPDMConstants.FILTER_GROUP_KEY, paramStartIndex);
        // bind where clause parameters for dynamic table business object first
        if (businessObjectInfo.hasDynamicJoinsToRead()) {
            for (DynamicJoinClause dynamicJoinClause : businessObjectInfo.getDynamicJoins()) {
                for (DynamicTable dynamicTable : dynamicJoinClause.getDynamicTables()) {
                    if (dynamicTable.getFilterList() != null) {
                        if (CollectionUtils.hasValue(dynamicTable.getFilterList().getFilters())) {
                            bindSQLParametersForWhereClause(pstmt, dynamicTable.getBoName(), dynamicTable.getFilterList().getFilters(), filterGroupMap, hasSQLExpression, executionContext);
                        }
                        // FilterGroups
                        if (CollectionUtils.hasValue(dynamicTable.getFilterList().getFilterGroups())) {
                            RecursiveSQLParametersForWhereClauseFromGroup(pstmt, null, dynamicTable.getFilterList().getFilterGroups(), filterGroupMap, hasSQLExpression, executionContext);
                        }
                        paramStartIndex = filterGroupMap.get(DSPDMConstants.FILTER_GROUP_KEY);
                        if (CollectionUtils.hasValue(dynamicTable.getFilterList().getHavingFilters())) {
                            paramStartIndex = bindSQLParametersForHavingClause(pstmt, dynamicTable.getBoName(), dynamicTable.getFilterList().getHavingFilters(), paramStartIndex, hasSQLExpression, executionContext);
                            filterGroupMap.put(DSPDMConstants.FILTER_GROUP_KEY, paramStartIndex);
                        }
                    }
                }
            }
        }
        // bind where clause parameters for main parent business object
        if ((businessObjectInfo.getFilterList() != null)) {
            if (CollectionUtils.hasValue(businessObjectInfo.getFilterList().getFilters())) {
                bindSQLParametersForWhereClause(pstmt, businessObjectInfo.getBusinessObjectType(), businessObjectInfo.getFilterList().getFilters(), filterGroupMap, hasSQLExpression, executionContext);
            }
            // FilterGroups
            if (CollectionUtils.hasValue(businessObjectInfo.getFilterList().getFilterGroups())) {
                RecursiveSQLParametersForWhereClauseFromGroup(pstmt, businessObjectInfo.getBusinessObjectType(), businessObjectInfo.getFilterList().getFilterGroups(), filterGroupMap, hasSQLExpression, executionContext);
            }
        }
        // bind where clause parameters for simple joined business object
        if (businessObjectInfo.hasSimpleJoinsToRead()) {
            for (SimpleJoinClause simpleJoinClause : businessObjectInfo.getSimpleJoins()) {
                if (simpleJoinClause.getFilterList() != null) {
                    if (CollectionUtils.hasValue(simpleJoinClause.getFilterList().getFilters())) {
                        bindSQLParametersForWhereClause(pstmt, simpleJoinClause.getBoName(), simpleJoinClause.getFilterList().getFilters(), filterGroupMap, hasSQLExpression, executionContext);
                    }
                    // FilterGroups
                    if (CollectionUtils.hasValue(simpleJoinClause.getFilterList().getFilterGroups())) {
                        RecursiveSQLParametersForWhereClauseFromGroup(pstmt, simpleJoinClause.getBoName(), simpleJoinClause.getFilterList().getFilterGroups(), filterGroupMap, hasSQLExpression, executionContext);
                    }
                }
            }
        }
        // bind where clause parameters for dynamic joined business object
        if (businessObjectInfo.hasDynamicJoinsToRead()) {
            for (DynamicJoinClause dynamicJoinClause : businessObjectInfo.getDynamicJoins()) {
                if (dynamicJoinClause.getFilterList() != null) {
                    if (CollectionUtils.hasValue(dynamicJoinClause.getFilterList().getFilters())) {
                        bindSQLParametersForWhereClause(pstmt, null, dynamicJoinClause.getFilterList().getFilters(), filterGroupMap, hasSQLExpression, executionContext);
                    }
                    // FilterGroups
                    if (CollectionUtils.hasValue(dynamicJoinClause.getFilterList().getFilterGroups())) {
                        RecursiveSQLParametersForWhereClauseFromGroup(pstmt, null, dynamicJoinClause.getFilterList().getFilterGroups(), filterGroupMap, hasSQLExpression, executionContext);
                    }
                }
            }
        }
        return filterGroupMap.get(DSPDMConstants.FILTER_GROUP_KEY);
    }

    private static int bindSQLParametersForWhereClause(PreparedStatement pstmt, String boName, List<CriteriaFilter> filters, Map<String, Integer> filterGroupMap, boolean hasSQLExpression, ExecutionContext executionContext) {
        int paramIndex = filterGroupMap.get(DSPDMConstants.FILTER_GROUP_KEY);
        try {
            for (CriteriaFilter filter : filters) {
                if (CollectionUtils.hasValue(filter.getValues())) {
                    for (Object value : filter.getValues()) {
                        if (value == null) {
                            if ((filter.getOperator() == Operator.IN) || (filter.getOperator() == Operator.NOT_IN)) {
                                pstmt.setNull(paramIndex, filter.getSqlDataType());
                                // increment the parameter index
                                paramIndex++;
                            } else {
                                // skip null values because they are already hard coded as is null or is not null
                            }
                        } else {
                            if (hasSQLExpression && (value instanceof SQLExpression)) {
                                SQLExpression expression = (SQLExpression) value;
                                if (expression.getParamAddedCount() > 0) {
                                    for (int i = 0; i < expression.getParamAddedCount(); i++) {
                                        pstmt.setObject(paramIndex, expression.getValues()[i], expression.getValuesSqlDataType()[i]);
                                        // increment the parameter index
                                        paramIndex++;
                                    }
                                }
                            } else {
                                if (Operator.JSONB_FIND_EXACT == filter.getOperator()) {
                                    pstmt.setObject(paramIndex, value, DSPDMConstants.DataTypes.CHARACTER_VARYING.getSqlDataTypes()[0]);
                                } else if (Operator.JSONB_FIND_LIKE == filter.getOperator()) {
                                    pstmt.setObject(paramIndex, value, DSPDMConstants.DataTypes.CHARACTER_VARYING.getSqlDataTypes()[0]);
                                } else {
                                    pstmt.setObject(paramIndex, value, filter.getSqlDataType());
                                }
                                // increment the parameter index
                                paramIndex++;
                            }
                        }
                    }
                } else if ((filter.getOperator() == Operator.IN) || (filter.getOperator() == Operator.NOT_IN)) {
                    if (StringUtils.hasValue(boName)) {
                        throw new DSPDMException("SQL IN clause must have at least one value provided for business object '{}'", executionContext.getExecutorLocale(), boName);
                    } else {
                        throw new DSPDMException("SQL IN clause must have at least one value provided for dynamic join", executionContext.getExecutorLocale());
                    }
                }
            }
            filterGroupMap.put(DSPDMConstants.FILTER_GROUP_KEY, paramIndex);
        } catch (SQLException e) {
            DSPDMException.throwException(e, executionContext);
        }
        return paramIndex;
    }

    // recurse filter groups to get parameters index
    private static void RecursiveSQLParametersForWhereClauseFromGroup(PreparedStatement pstmt, String boName, List<FilterGroup> filterGroups, Map<String, Integer> map, boolean hasSQLExpression, ExecutionContext executionContext) {
        if (CollectionUtils.hasValue(filterGroups)) {
            for (FilterGroup filterGroup : filterGroups) {
                if (CollectionUtils.hasValue(filterGroup.getCriteriaFilters())) {
                    map.put(DSPDMConstants.FILTER_GROUP_KEY, bindSQLParametersForWhereClause(pstmt, boName, Arrays.asList(filterGroup.getCriteriaFilters()), map, hasSQLExpression, executionContext));
                }
                if (CollectionUtils.hasValue(filterGroup.getFilterGroups())) {
                    RecursiveSQLParametersForWhereClauseFromGroup(pstmt, boName, Arrays.asList(filterGroup.getFilterGroups()), map, hasSQLExpression, executionContext);
                }
            }
        }
    }

    protected static int bindSQLParametersForHavingClause(PreparedStatement pstmt, BusinessObjectInfo businessObjectInfo, int paramStartIndex, boolean hasSQLExpression, ExecutionContext executionContext) {
        int paramIndex = paramStartIndex;
        // bind where clause parameters for main parent business object
        if ((businessObjectInfo.getFilterList() != null) && (CollectionUtils.hasValue(businessObjectInfo.getFilterList().getHavingFilters()))) {
            paramIndex = bindSQLParametersForHavingClause(pstmt, businessObjectInfo.getBusinessObjectType(), businessObjectInfo.getFilterList().getHavingFilters(), paramIndex, hasSQLExpression, executionContext);
        }
        // bind having clause parameters for joined business object
        if (businessObjectInfo.hasSimpleJoinsToRead()) {
            for (SimpleJoinClause simpleJoinClause : businessObjectInfo.getSimpleJoins()) {
                if ((simpleJoinClause.getFilterList() != null) && (CollectionUtils.hasValue(simpleJoinClause.getFilterList().getHavingFilters()))) {
                    paramIndex = bindSQLParametersForHavingClause(pstmt, simpleJoinClause.getBoName(), simpleJoinClause.getFilterList().getHavingFilters(), paramIndex, hasSQLExpression, executionContext);
                }
            }
        }
        // bind having clause parameters for dynamic joined business object
        if (businessObjectInfo.hasDynamicJoinsToRead()) {
            for (DynamicJoinClause dynamicJoinClause : businessObjectInfo.getDynamicJoins()) {
                if ((dynamicJoinClause.getFilterList() != null) && (CollectionUtils.hasValue(dynamicJoinClause.getFilterList().getHavingFilters()))) {
                    paramIndex = bindSQLParametersForHavingClause(pstmt, null, dynamicJoinClause.getFilterList().getHavingFilters(), paramIndex, hasSQLExpression, executionContext);
                }
            }
        }
        return paramIndex;
    }

    private static int bindSQLParametersForHavingClause(PreparedStatement pstmt, String boName, List<HavingFilter> havingFilters, int paramStartIndex, boolean hasSQLExpression, ExecutionContext executionContext) {
        int paramIndex = paramStartIndex;
        try {
            for (HavingFilter havingFilter : havingFilters) {
                if (CollectionUtils.hasValue(havingFilter.getValues())) {

                    for (Object value : havingFilter.getValues()) {
                        if (value == null) {
                            if ((havingFilter.getOperator() == Operator.IN) || (havingFilter.getOperator() == Operator.NOT_IN)) {
                                pstmt.setNull(paramIndex, havingFilter.getSqlDataType());
                                // increment the parameter index
                                paramIndex++;
                            } else {
                                // skip null values because they are already hard coded as is null or is not null
                            }
                        } else {
                            if (hasSQLExpression && (value instanceof SQLExpression)) {
                                SQLExpression expression = (SQLExpression) value;
                                if (expression.getParamAddedCount() > 0) {
                                    for (int i = 0; i < expression.getParamAddedCount(); i++) {
                                        pstmt.setObject(paramIndex, expression.getValues()[i], expression.getValuesSqlDataType()[i]);
                                        // increment the parameter index
                                        paramIndex++;
                                    }
                                }
                            } else {
                                pstmt.setObject(paramIndex, value, havingFilter.getSqlDataType());
                                // increment the parameter index
                                paramIndex++;
                            }
                        }
                    }
                } else if ((havingFilter.getOperator() == Operator.IN) || (havingFilter.getOperator() == Operator.NOT_IN)) {
                    if (StringUtils.hasValue(boName)) {
                        throw new DSPDMException("SQL IN clause must have at least one value provided for business object '{}'", executionContext.getExecutorLocale(), boName);
                    } else {
                        throw new DSPDMException("SQL IN clause must have at least one value provided for dynamic join", executionContext.getExecutorLocale());
                    }
                }
            }
        } catch (SQLException e) {
            DSPDMException.throwException(e, executionContext);
        }
        return paramIndex;
    }

    protected static int bindSQLParametersForPagination(PreparedStatement pstmt, BusinessObjectInfo businessObjectInfo, int paramStartIndex, ExecutionContext executionContext) {
        int paramIndex = paramStartIndex;
        try {
            if (!(businessObjectInfo.isReadAllRecords())) {
                // SET OFFSET FIRST
                if (businessObjectInfo.getPagination().getReadFromIndex() > 0) {
                    // SET OFFSET to the proper value
                    pstmt.setInt(paramIndex, businessObjectInfo.getPagination().getReadFromIndex());
                    // increment the parameter index
                    paramIndex++;
                } else {
                    // SET OFFSET to Zero means read from start
                    // offset zero is the requirement of MS SQL Server
                    // it is also added for postgres and others for uniform sql statement and to reduce the sql dialect checks
                    pstmt.setInt(paramIndex, 0);
                    // increment the parameter index
                    paramIndex++;
                }
                // SET FETCH NEXT ROWS ONLY COUNT
                if (businessObjectInfo.isReadFirst()) {
                    // SET FETCH NEXT ROWS ONLY COUNT
                    pstmt.setInt(paramIndex, 1);
                    // increment the parameter index
                    paramIndex++;
                } else if (businessObjectInfo.isReadUnique()) {
                    // SET FETCH NEXT ROWS ONLY COUNT
                    pstmt.setInt(paramIndex, 2);
                    // increment the parameter index
                    paramIndex++;
                } else if (businessObjectInfo.getPagination().getRecordsToRead() != Integer.MAX_VALUE) {
                    // SET FETCH NEXT ROWS ONLY COUNT
                    pstmt.setInt(paramIndex, businessObjectInfo.getPagination().getRecordsToRead());
                    // increment the parameter index
                    paramIndex++;
                }
            }
        } catch (SQLException e) {
            DSPDMException.throwException(e, executionContext);
        }
        return paramIndex;
    }

    private static List<DynamicDTO> extractDynamicDTOListFromResultSet(BusinessObjectInfo businessObjectInfo, ResultSet resultSet, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        List<DynamicDTO> list = new ArrayList<>();
        try {
            if ((businessObjectInfo.getSelectList() != null) && (businessObjectInfo.getSelectList().getTotalColumnsCount() > 0)) {

                LinkedHashSet<String> columnNamesOrderedSet = new LinkedHashSet<>(businessObjectInfo.getSelectList().getTotalColumnsCount());
                // select list is provided by user so iterate over provided column list
                // so that the column order is by sequence number column
                // first add simple columns
                if (CollectionUtils.hasValue(businessObjectInfo.getSelectList().getColumnsToSelect())) {
                    columnNamesOrderedSet.addAll(getPhysicalSimpleColumnsToSelect(businessObjectInfo, dynamicDAO, executionContext));
                }
                // now add aggregate columns using their aliases
                if (CollectionUtils.hasValue(businessObjectInfo.getSelectList().getAggregateColumnsToSelect())) {
                    for (AggregateColumn aggregateColumn : businessObjectInfo.getSelectList().getAggregateColumnsToSelect()) {
                        columnNamesOrderedSet.add(aggregateColumn.getColumnAlias());
                    }
                }
                Map<String, AbstractDynamicDAOImplForRead> dynamicDAOMap = new HashMap<>();
                dynamicDAOMap.put(dynamicDAO.getType(), dynamicDAO);
                while (resultSet.next()) {
                    // order of column is specified in the set
                    DynamicDTO dynamicDTO = extractDynamicDTOFromResultSet(resultSet, columnNamesOrderedSet, new DynamicDTO(dynamicDAO.getType(), dynamicDAO.getPrimaryKeyColumnNames(), executionContext), dynamicDAOMap, executionContext);
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
                while (resultSet.next()) {
                    // order of column is specified in the metadata sequence number 
                    DynamicDTO dto = extractDynamicDTOFromResultSet(resultSet, columnNamesSortedSetFromResultSet, new DynamicDTO(dynamicDAO.getType(), dynamicDAO.getPrimaryKeyColumnNames(), executionContext), dynamicDAO, executionContext);
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

    /**
     * columns order is by given set. columns order inside dynamic dto is preserved
     *
     * @param resultSet
     * @param columnNamesOrderedSetFromResultSet
     * @param dto
     * @return
     */
    protected static DynamicDTO extractDynamicDTOFromResultSet(ResultSet resultSet, LinkedHashSet<String> columnNamesOrderedSetFromResultSet, DynamicDTO dto, Map<String, AbstractDynamicDAOImplForRead> joinAliasToAlreadyJoinedDAOMap, ExecutionContext executionContext) {
        try {
            Object value = null;
            String boAttrName = null;
            BusinessObjectAttributeDTO businessObjectAttributeDTO = null;
            for (String columnName : columnNamesOrderedSetFromResultSet) {
                // iterate over all dao instances to get the appropriate attribute from main or joined DAO instances
                for (AbstractDynamicDAOImplForRead dynamicDAO : joinAliasToAlreadyJoinedDAOMap.values()) {
                    businessObjectAttributeDTO = dynamicDAO.getPhysicalColumnNamesMetadataMap().get(columnName);
                    if (businessObjectAttributeDTO != null) {
                        boAttrName = businessObjectAttributeDTO.getBoAttrName();
                        value = extractColumnValueFromResultSet(columnName, businessObjectAttributeDTO, resultSet, dynamicDAO, executionContext);
                        // found as dynamic dto property
                        break;
                    }
                }
                // if attribute not found in any dao then it means that it might be an alias or expression alias
                if (businessObjectAttributeDTO == null) {
                    boAttrName = columnName;
                    value = resultSet.getObject(columnName);
                    // found as extended column
                }
                // add to dto with maintained order of keys
                dto.putWithOrder(boAttrName, value);
            }
        } catch (SQLException e) {
            DSPDMException.throwException(e, executionContext);
        }
        return dto;
    }

    /**
     * columns order is by metadata sequence number, columns order inside dynamic dto is not preserved
     *
     * @param resultSet
     * @param columnNamesSortedSetFromResultSet
     * @param dto
     * @return
     */
    protected static DynamicDTO extractDynamicDTOFromResultSet(ResultSet resultSet, TreeSet<String> columnNamesSortedSetFromResultSet, DynamicDTO dto, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        try {
            for (BusinessObjectAttributeDTO businessObjectAttributeDTO : dynamicDAO.getBusinessObjectAttributeDTOS()) {
                String columnName = businessObjectAttributeDTO.getAttributeName();
                if (StringUtils.hasValue(columnName)) {
                    if (columnNamesSortedSetFromResultSet.contains(columnName)) {
                        Object value = extractColumnValueFromResultSet(columnName, businessObjectAttributeDTO, resultSet, dynamicDAO, executionContext);
                        dto.put(businessObjectAttributeDTO.getBoAttrName(), value);
                    } else {
                        logger.info("Column '{}' found in metadata but not found in result set", columnName);
                    }
                }
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return dto;
    }

    /**
     * extracts fixed columns from result set and add them to the dto. It is useful in case of dynamic joins
     *
     * @param resultSet
     * @param columnNamesSortedSetFromResultSet
     * @param dto
     * @param selectList
     * @param executionContext
     * @return
     */
    protected static DynamicDTO extractDynamicDTOFromResultSet(ResultSet resultSet, TreeSet<String> columnNamesSortedSetFromResultSet, DynamicDTO dto, SelectList selectList, ExecutionContext executionContext) {
        try {
            for (String columnName : selectList.getColumnsToSelect()) {
                if (columnNamesSortedSetFromResultSet.contains(columnName)) {
                    Object value = resultSet.getObject(columnName);
                    dto.put(columnName, value);
                } else {
                    logger.info("Column '{}' found in select list but not found in result set", columnName);
                }
            }

            for (AggregateColumn aggregateColumn : selectList.getAggregateColumnsToSelect()) {
                if (columnNamesSortedSetFromResultSet.contains(aggregateColumn.getColumnAlias())) {
                    Object value = resultSet.getObject(aggregateColumn.getColumnAlias());
                    dto.put(aggregateColumn.getColumnAlias(), value);
                } else {
                    logger.info("Column '{}' found in select list but not found in result set", aggregateColumn.getColumnAlias());
                }
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return dto;
    }

    /**
     * @param columnName
     * @param businessObjectAttributeDTO
     * @param resultSet
     * @param executionContext
     * @return
     */
    private static Object extractColumnValueFromResultSet(String columnName, BusinessObjectAttributeDTO businessObjectAttributeDTO, ResultSet resultSet, AbstractDynamicDAOImplForRead dynamicDAO, ExecutionContext executionContext) {
        Object value = null;
        try {
            boolean isMSSQLServerDialect = dynamicDAO.isMSSQLServerDialect(dynamicDAO.getType());
            // Special handling needed for byte array. BLOB or byte array alternative in postgres
            if (businessObjectAttributeDTO.getAttributeDatatype().equalsIgnoreCase(DSPDMConstants.DataTypes.JSONB.getAttributeDataType(isMSSQLServerDialect))) {
                value = resultSet.getObject(columnName);
            } else if ((CollectionUtils.contains(DSPDMConstants.DataTypes.BYTEA.getSqlDataTypes(), businessObjectAttributeDTO.getSqlDataType()))
                    || (CollectionUtils.contains(DSPDMConstants.DataTypes.BINARY.getSqlDataTypes(), businessObjectAttributeDTO.getSqlDataType()))
                    || (CollectionUtils.contains(DSPDMConstants.DataTypes.IMAGE.getSqlDataTypes(), businessObjectAttributeDTO.getSqlDataType()))) {
                value = resultSet.getBytes(columnName);
            } else {
                value = resultSet.getObject(columnName, businessObjectAttributeDTO.getJavaDataType());
                if ((value != null) && (value instanceof BigDecimal)) {
                    value = BigDecimal.valueOf(((BigDecimal) value).doubleValue());
                }
            }
        } catch (SQLException e) {
            throw new DSPDMException("An error occurred while reading data for attribute name '{}' and business object name '{}' and metadata data type '{}' and java data type '{}' ",
                    e, executionContext.getExecutorLocale(),
                    columnName, businessObjectAttributeDTO.getBoName(), businessObjectAttributeDTO.getAttributeDatatype(), businessObjectAttributeDTO.getJavaDataType());
        }
        return value;
    }

    private static void setParametersValue(StringBuffer sql, ExecutionContext executionContext, CriteriaFilter filter, AbstractDynamicDAOImplForRead dao) {
        if (CollectionUtils.hasValue(filter.getValues())) {
            for (Object value : filter.getValues()) {
                if ((value == null) && (filter.getOperator() != Operator.IN)
                        && (filter.getOperator() != Operator.NOT_IN)) {
                    // skip null values because they are already hard coded as is null or is not null
                    continue;
                }
                int firstStartIndex = sql.indexOf(String.valueOf(DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER));
                if (firstStartIndex >= 0) {
                    sql.replace(firstStartIndex, firstStartIndex + 1, getSQLMarkerReplacementString(value, dao, executionContext));
                }
            }
        }
    }

    private static String getWhereClauseParamValuesList(String sql, BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForRead dao, ExecutionContext executionContext) {
        StringBuffer sqlSb = new StringBuffer(sql);
        if (businessObjectInfo.getFilterList() != null) {
            if (businessObjectInfo.getFilterList().getFilters() != null) {
                List<CriteriaFilter> filters = businessObjectInfo.getFilterList().getFilters();
                for (CriteriaFilter filter : filters) {
                    setParametersValue(sqlSb, executionContext, filter, dao);
                }
            }
            if (businessObjectInfo.getFilterList().getFilterGroups() != null) {
                RecursiveWhereClauseParamValuesFromGroups(sqlSb, businessObjectInfo.getFilterList().getFilterGroups(), dao, executionContext);
            }
        }
        return sqlSb.toString();
    }

    // recurse filter groups to set parameters values 
    private static void RecursiveWhereClauseParamValuesFromGroups(StringBuffer sql, List<FilterGroup> filterGroups, AbstractDynamicDAOImplForRead dao, ExecutionContext executionContext) {
        if (CollectionUtils.hasValue(filterGroups)) {
            for (FilterGroup filterGroup : filterGroups) {
                if (CollectionUtils.hasValue(filterGroup.getCriteriaFilters())) {
                    for (CriteriaFilter filter : filterGroup.getCriteriaFilters()) {
                        setParametersValue(sql, executionContext, filter, dao);
                    }
                }
                if (CollectionUtils.hasValue(filterGroup.getFilterGroups())) {
                    RecursiveWhereClauseParamValuesFromGroups(sql, Arrays.asList(filterGroup.getFilterGroups()), dao, executionContext);
                }
            }
        }
    }

    private static String getHavingClauseParamValuesList(String sql, BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForRead dao, ExecutionContext executionContext) {
        if ((businessObjectInfo.getFilterList() != null) && (businessObjectInfo.getFilterList().getHavingFilters() != null)) {
            List<HavingFilter> havingFilters = businessObjectInfo.getFilterList().getHavingFilters();
            for (HavingFilter havingFilter : havingFilters) {
                if (CollectionUtils.hasValue(havingFilter.getValues())) {
                    for (Object value : havingFilter.getValues()) {
                        if ((value == null) && (havingFilter.getOperator() != Operator.IN)
                                && (havingFilter.getOperator() != Operator.NOT_IN)) {
                            // skip null values because they are already hard coded as is null or is not null
                            continue;
                        }
                        sql = sql.replaceFirst("\\" + DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER, Matcher.quoteReplacement(getSQLMarkerReplacementString(value, dao, executionContext)));
                    }
                }
            }
        }
        return sql;
    }

    private static String getPaginationParamValuesList(String sql, BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForRead dao, ExecutionContext executionContext) {
        if (!(businessObjectInfo.isReadAllRecords())) {
            if (businessObjectInfo.getPagination().getReadFromIndex() > 0) {
                // SET OFFSET
                sql = sql.replaceFirst("\\" + DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER, Matcher.quoteReplacement(getSQLMarkerReplacementString(businessObjectInfo.getPagination().getReadFromIndex(), dao, executionContext)));
            } else {
                // SET OFFSET
                sql = sql.replaceFirst("\\" + DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER, Matcher.quoteReplacement(getSQLMarkerReplacementString(0, dao, executionContext)));
            }
            if (businessObjectInfo.isReadFirst()) {
                // SET FETCH NEXT ROWS ONLY COUNT
                sql = sql.replaceFirst("\\" + DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER, Matcher.quoteReplacement(getSQLMarkerReplacementString(1, dao, executionContext)));
            } else if (businessObjectInfo.isReadUnique()) {
                // SET FETCH NEXT ROWS ONLY COUNT
                sql = sql.replaceFirst("\\" + DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER, Matcher.quoteReplacement(getSQLMarkerReplacementString(2, dao, executionContext)));
            } else if (businessObjectInfo.getPagination().getRecordsToRead() != Integer.MAX_VALUE) {
                // SET FETCH NEXT ROWS ONLY COUNT
                sql = sql.replaceFirst("\\" + DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER, Matcher.quoteReplacement(getSQLMarkerReplacementString(businessObjectInfo.getPagination().getRecordsToRead(), dao, executionContext)));
            }
        }
        return sql;
    }

    protected static List<String> getColumnNamesFromResultSetMetadata(ResultSet resultSet) throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<String> list = new ArrayList<>(resultSetMetaData.getColumnCount());
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            if (StringUtils.hasValue(resultSetMetaData.getColumnName(i))) {
                list.add(resultSetMetaData.getColumnName(i));
            }
        }
        return list;
    }

    private static String logSQLForRead(String sql, BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForRead dao, ExecutionContext executionContext) {
        String finalSQL = null;
        if ((logger.isInfoEnabled()) || (executionContext.isCollectSQLStats()) || (executionContext.getCollectSQLScriptOptions().isReadEnabled())) {
            sql = getWhereClauseParamValuesList(sql, businessObjectInfo, dao, executionContext);
            sql = getHavingClauseParamValuesList(sql, businessObjectInfo, dao, executionContext);
            sql = getPaginationParamValuesList(sql, businessObjectInfo, dao, executionContext);
            finalSQL = sql;
            logSQL(finalSQL, executionContext);
            // collect sql script only if required
            if (executionContext.getCollectSQLScriptOptions().isReadEnabled()) {
                executionContext.addSqlScript(finalSQL);
            }
        }
        return finalSQL;
    }

    private static String logSQLForCount(String sql, BusinessObjectInfo businessObjectInfo, AbstractDynamicDAOImplForRead dao, ExecutionContext executionContext) {
        String finalSQL = null;
        if ((logger.isInfoEnabled()) || (executionContext.isCollectSQLStats()) || (executionContext.getCollectSQLScriptOptions().isCountEnabled())) {
            sql = getWhereClauseParamValuesList(sql, businessObjectInfo, dao, executionContext);
            sql = getHavingClauseParamValuesList(sql, businessObjectInfo, dao, executionContext);
            finalSQL = sql;
            logSQL(finalSQL, executionContext);
            // collect sql script only if required
            if (executionContext.getCollectSQLScriptOptions().isCountEnabled()) {
                executionContext.addSqlScript(finalSQL);
            }
        }
        return finalSQL;
    }
}
