package com.lgc.dspdm.core.dao.dynamic.metadata.impl;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.CriteriaFilter;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.SelectList;
import com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateColumn;
import com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateFunction;
import com.lgc.dspdm.core.common.data.criteria.join.*;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.*;
import com.lgc.dspdm.core.common.util.metadata.MetadataUtils;
import com.lgc.dspdm.core.dao.BaseDAO;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAO;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAOImpl;
import com.lgc.dspdm.core.dao.dynamic.businessobject.DynamicDAOFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.*;

public abstract class AbstractDBStructureChangeDAO extends BaseDAO {
    private static DSPDMLogger logger = new DSPDMLogger(AbstractDBStructureChangeDAO.class);

    /* ************************************************************ */
    /* ************** BUSINESS METHOD CREATE TABLE **************** */
    /* ************************************************************ */

    /**
     * Create a physical table in database
     *
     * @param dynamicDTO
     * @param connection
     * @param executionContext
     * @return
     */
    protected SaveResultDTO createTable(DynamicDTO dynamicDTO,
                                        List<DynamicDTO> businessObjectAttributes,
                                        List<DynamicDTO> businessObjectRelationships_FK,
                                        List<DynamicDTO> uniqueConstraints,
                                        Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        String tableName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY);
        String boName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        String sql = getCreateTableSQLStatement(dynamicDTO, businessObjectAttributes, businessObjectRelationships_FK,uniqueConstraints, isMSSQLServerDialect(boName));
        long startTime = 0;
        PreparedStatement pstmt = null;
        try {
            logSQL(sql, executionContext);
            // collect sql script only if required
            if(executionContext.getCollectSQLScriptOptions().isDDLEnabled()) {
                executionContext.addSqlScript(sql);
            }
            pstmt = prepareStatementForInsert(sql, boName, executionContext, connection);
            startTime = System.currentTimeMillis();
            // execute batch
            pstmt.executeUpdate();
            logSQLTimeTaken(sql, 1, System.currentTimeMillis() - startTime, executionContext);
            // now update added db columns count
            saveResultDTO.addCreatedDBTablesCount(1);
            logger.info("{} physical table added successfully.", tableName);
            // logging DDL statement
            trackChangeHistory(dynamicDTO, sql, DSPDMConstants.BusinessObjectOperations.DDL_CREATE_TABLE_OPR.getId(), executionContext);
        } catch (Exception e) {
            if (sql != null) {
                logSQLTimeTaken(sql, 1, (System.currentTimeMillis() - startTime), executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeStatement(pstmt, executionContext);
        }
        return saveResultDTO;
    }

    /* ********************************************************** */
    /* ************** BUSINESS METHOD DROP TABLE **************** */
    /* ********************************************************** */

    /**
     * Drops physical database table
     *
     * @param dynamicDTO
     * @param connection
     * @param executionContext
     * @return
     */
    protected SaveResultDTO dropTable(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext) {
        String boName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        String tableName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY);
        String sql = "DROP TABLE " + tableName;
        long startTime = 0;
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        PreparedStatement pstmt = null;
        try {
            logSQL(sql, executionContext);
            // collect sql script only if required
            if(executionContext.getCollectSQLScriptOptions().isDDLEnabled()) {
                executionContext.addSqlScript(sql);
            }
            pstmt = prepareStatementForDelete(sql, boName, executionContext, connection);
            startTime = System.currentTimeMillis();
            // execute batch
            pstmt.executeUpdate();
            logSQLTimeTaken(sql, 1, System.currentTimeMillis() - startTime, executionContext);
            // now update added db columns count
            saveResultDTO.addDroppedDBTablesCount(1);
            logger.info("{} physical table dropped successfully.", tableName);
            // logging DDL statement
            trackChangeHistory(dynamicDTO, sql, DSPDMConstants.BusinessObjectOperations.DDL_DROP_TABLE_OPR.getId(), executionContext);
        } catch (Exception e) {
            if (sql != null) {
                logSQLTimeTaken(sql, 1, (System.currentTimeMillis() - startTime), executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeStatement(pstmt, executionContext);
        }
        return saveResultDTO;
    }

    /**
     * Drops physical database relationship constraint from table
     *
     * @param dynamicDTO
     * @param connection
     * @param executionContext
     * @return
     */
    protected SaveResultDTO dropRelationshipConstraint(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext) {
        String boName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME);
        String tableName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_ENTITY_NAME);
        String constraintName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
        String sql = "ALTER TABLE " + tableName + " DROP CONSTRAINT " + constraintName;
        long startTime = 0;
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        PreparedStatement pstmt = null;
        try {
            logSQL(sql, executionContext);
            // collect sql script only if required
            if(executionContext.getCollectSQLScriptOptions().isDDLEnabled()) {
                executionContext.addSqlScript(sql);
            }
            pstmt = prepareStatementForDelete(sql, boName, executionContext, connection);
            startTime = System.currentTimeMillis();
            // execute batch
            pstmt.executeUpdate();
            logSQLTimeTaken(sql, 1, System.currentTimeMillis() - startTime, executionContext);
            // now update added db columns count
            saveResultDTO.addDroppedDBRelationshipsCount(1);
            logger.info("{} physical relationship constraint dropped successfully.", constraintName);
            // logging DDL statement
            trackChangeHistory(dynamicDTO, sql, DSPDMConstants.BusinessObjectOperations.DDL_DROP_RELATIONSHIP_OPR.getId(), executionContext);
        } catch (Exception e) {
            if (sql != null) {
                logSQLTimeTaken(sql, 1, (System.currentTimeMillis() - startTime), executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeStatement(pstmt, executionContext);
        }
        return saveResultDTO;
    }

    /**
     * Drops physical database unique constraint from table
     *
     * @param dynamicDTO
     * @param connection
     * @param executionContext
     * @return
     */
    protected SaveResultDTO dropUniqueConstraint(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext) {
        String boName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        String tableName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY);
        String constraintName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
        String sql = "ALTER TABLE " + tableName + " DROP CONSTRAINT " + constraintName;
        long startTime = 0;
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        PreparedStatement pstmt = null;
        try {
            logSQL(sql, executionContext);
            // collect sql script only if required
            if(executionContext.getCollectSQLScriptOptions().isDDLEnabled()) {
                executionContext.addSqlScript(sql);
            }
            pstmt = prepareStatementForDelete(sql, boName, executionContext, connection);
            startTime = System.currentTimeMillis();
            // execute batch
            pstmt.executeUpdate();
            logSQLTimeTaken(sql, 1, System.currentTimeMillis() - startTime, executionContext);
            // now update added db columns count
            saveResultDTO.addDroppedDBUniqueConstraintsCount(1);
            logger.info("{} physical unique constraint dropped successfully.", constraintName);
            // logging DDL statement
            trackChangeHistory(dynamicDTO, sql, DSPDMConstants.BusinessObjectOperations.DDL_DROP_UNIQUE_CONSTRAINT_OPR.getId(), executionContext);
        } catch (Exception e) {
            if (sql != null) {
                logSQLTimeTaken(sql, 1, (System.currentTimeMillis() - startTime), executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeStatement(pstmt, executionContext);
        }
        return saveResultDTO;
    }

    /**
     * Drops physical database search indexes from table
     *
     * @param dynamicDTO
     * @param connection
     * @param executionContext
     * @return
     */
    protected SaveResultDTO dropSearchIndex(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext) {
        String boName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        String tableName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY);
        String indexName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.INDEX_NAME);
        String sql = "DROP INDEX " + indexName;
        // sqlserver required tableName to drop index whereas postgresql doesn't
        if(isMSSQLServerDialect(tableName)){
            sql += " ON " + tableName;
        }
        long startTime = 0;
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        PreparedStatement pstmt = null;
        try {
            logSQL(sql, executionContext);
            // collect sql script only if required
            if(executionContext.getCollectSQLScriptOptions().isDDLEnabled()) {
                executionContext.addSqlScript(sql);
            }
            pstmt = prepareStatementForDelete(sql, boName, executionContext, connection);
            startTime = System.currentTimeMillis();
            // execute batch
            pstmt.executeUpdate();
            logSQLTimeTaken(sql, 1, System.currentTimeMillis() - startTime, executionContext);
            // now update added db columns count
            saveResultDTO.addDroppedDBSearchIndexesCount(1);
            logger.info("{} physical search index dropped successfully.", indexName);
            // logging DDL statement
            trackChangeHistory(dynamicDTO, sql, DSPDMConstants.BusinessObjectOperations.DDL_DROP_SEARCH_INDEX_OPR.getId(), executionContext);
        } catch (Exception e) {
            if (sql != null) {
                logSQLTimeTaken(sql, 1, (System.currentTimeMillis() - startTime), executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeStatement(pstmt, executionContext);
        }
        return saveResultDTO;
    }

    protected SaveResultDTO dropDefaultConstraint(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext) {
        String boName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        String tableName = ((String) dynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY)).toLowerCase();
        String columnName = ((String) dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE)).toLowerCase();
        String constraintName = "def_" + tableName+ "_" + columnName;
        String sql = "ALTER TABLE " + tableName + " DROP CONSTRAINT " + constraintName;
        long startTime = 0;
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        PreparedStatement pstmt = null;
        try {
            logSQL(sql, executionContext);
            // collect sql script only if required
            if(executionContext.getCollectSQLScriptOptions().isDDLEnabled()) {
                executionContext.addSqlScript(sql);
            }
            pstmt = prepareStatementForDelete(sql, boName, executionContext, connection);
            startTime = System.currentTimeMillis();
            // execute batch
            pstmt.executeUpdate();
            logSQLTimeTaken(sql, 1, System.currentTimeMillis() - startTime, executionContext);
            // now update added db columns count
            saveResultDTO.addDroppedDBUniqueConstraintsCount(1);
            logger.info("{} physical default constraint dropped successfully.", constraintName);
            // logging DDL statement
            trackChangeHistory(dynamicDTO, sql, DSPDMConstants.BusinessObjectOperations.DDL_DROP_DEFAULT_CONSTRAINT_OPR.getId(), executionContext);
        } catch (Exception e) {
            if (sql != null) {
                logSQLTimeTaken(sql, 1, (System.currentTimeMillis() - startTime), executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeStatement(pstmt, executionContext);
        }
        return saveResultDTO;
    }
    /**
     * add physical referential integrity constraint
     *
     * @param relationships
     * @param connection
     * @param executionContext
     * @return
     */
    protected SaveResultDTO addForeignKeyConstraint(List<DynamicDTO> relationships, Connection connection, ExecutionContext executionContext) {
        DynamicDTO relationship = relationships.get(0);
        String boName = (String) relationship.get(DSPDMConstants.BoAttrName.PARENT_BO_NAME);
        String parentTableName = (String) relationship.get(DSPDMConstants.BoAttrName.PARENT_ENTITY_NAME);
        String childTableName = (String) relationship.get(DSPDMConstants.BoAttrName.CHILD_ENTITY_NAME);
        String constraintName = (String) relationship.get(DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
        String parentColumnNames = null;
        String childColumnNames = null;
        if (relationships.size() == 1) {
            parentColumnNames = (String) relationship.get(DSPDMConstants.BoAttrName.PARENT_ATTRIBUTE_NAME);
            childColumnNames = (String) relationship.get(DSPDMConstants.BoAttrName.CHILD_ATTRIBUTE_NAME);
        } else {
            // now get comma separated column names
            parentColumnNames = CollectionUtils.getCommaSeparated(CollectionUtils.getStringValuesFromList(relationships, DSPDMConstants.BoAttrName.PARENT_ATTRIBUTE_NAME));
            childColumnNames = CollectionUtils.getCommaSeparated(CollectionUtils.getStringValuesFromList(relationships, DSPDMConstants.BoAttrName.CHILD_ATTRIBUTE_NAME));
        }
        StringBuilder sqlBuilder = new StringBuilder(200);
        sqlBuilder.append("ALTER TABLE ").append(childTableName).append(" ADD CONSTRAINT ").append(constraintName);
        sqlBuilder.append(" FOREIGN KEY ").append(DSPDMConstants.OPEN_PARENTHESIS).append(childColumnNames).append(DSPDMConstants.CLOSE_PARENTHESIS);
        sqlBuilder.append(" REFERENCES ").append(parentTableName).append(DSPDMConstants.OPEN_PARENTHESIS).append(parentColumnNames).append(DSPDMConstants.CLOSE_PARENTHESIS);
        String sql = sqlBuilder.toString();
        // execute sql
        long startTime = 0;
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        PreparedStatement pstmt = null;
        try {
            logSQL(sql, executionContext);
            // collect sql script only if required
            if(executionContext.getCollectSQLScriptOptions().isDDLEnabled()) {
                executionContext.addSqlScript(sql);
            }
            pstmt = prepareStatementForInsert(sql, boName, executionContext, connection);
            startTime = System.currentTimeMillis();
            // execute batch
            pstmt.executeUpdate();
            logSQLTimeTaken(sql, 1, System.currentTimeMillis() - startTime, executionContext);
            // now update added db columns count
            saveResultDTO.addCreatedDBRelationshipsCount(1);
            logger.info("{} physical referential integrity constraint added successfully.", constraintName);
            // logging DDL statement
            trackChangeHistory(relationship, sql, DSPDMConstants.BusinessObjectOperations.DDL_ADD_RELATIONSHIP_OPR.getId(), executionContext);
        } catch (Exception e) {
            if (sql != null) {
                logSQLTimeTaken(sql, 1, (System.currentTimeMillis() - startTime), executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeStatement(pstmt, executionContext);
        }
        return saveResultDTO;
    }

    /**
     * add physical unique constraint
     *
     * @param uniqueConstraints
     * @param connection
     * @param executionContext
     * @return
     */
    protected SaveResultDTO addUniqueConstraint(List<DynamicDTO> uniqueConstraints, Connection connection, ExecutionContext executionContext) {
        DynamicDTO uniqueConstraint = uniqueConstraints.get(0);
        String boName = (String) uniqueConstraint.get(DSPDMConstants.BoAttrName.BO_NAME);
        String tableName = (String) uniqueConstraint.get(DSPDMConstants.BoAttrName.ENTITY);
        String constraintName = (String) uniqueConstraint.get(DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
        String columnNames = null;
        if (uniqueConstraints.size() == 1) {
            columnNames = (String) uniqueConstraint.get(DSPDMConstants.BoAttrName.ATTRIBUTE);
        } else {
            // now get comma separated column names
            columnNames = CollectionUtils.getCommaSeparated(CollectionUtils.getStringValuesFromList(uniqueConstraints, DSPDMConstants.BoAttrName.ATTRIBUTE));
        }
        StringBuilder sqlBuilder = new StringBuilder(200);
        sqlBuilder.append("ALTER TABLE ").append(tableName).append(" ADD CONSTRAINT ").append(constraintName);
        sqlBuilder.append(" UNIQUE ").append(DSPDMConstants.OPEN_PARENTHESIS).append(columnNames).append(DSPDMConstants.CLOSE_PARENTHESIS);
        String sql = sqlBuilder.toString();
        // execute sql
        long startTime = 0;
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        PreparedStatement pstmt = null;
        try {
            logSQL(sql, executionContext);
            // collect sql script only if required
            if(executionContext.getCollectSQLScriptOptions().isDDLEnabled()) {
                executionContext.addSqlScript(sql);
            }
            pstmt = prepareStatementForInsert(sql, boName, executionContext, connection);
            startTime = System.currentTimeMillis();
            // execute batch
            pstmt.executeUpdate();
            logSQLTimeTaken(sql, 1, System.currentTimeMillis() - startTime, executionContext);
            // now update added db columns count
            saveResultDTO.addCreatedDBUniqueConstraintsCount(1);
            logger.info("{} physical unique constraint added successfully.", constraintName);
            // logging DDL statement
            trackChangeHistory(uniqueConstraint, sql, DSPDMConstants.BusinessObjectOperations.DDL_ADD_UNIQUE_CONSTRAINT_OPR.getId(), executionContext);
        } catch (Exception e) {
            if (sql != null) {
                logSQLTimeTaken(sql, 1, (System.currentTimeMillis() - startTime), executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeStatement(pstmt, executionContext);
        }
        return saveResultDTO;
    }

    /**
     * add physical search index
     *
     * @param searchIndexes
     * @param connection
     * @param executionContext
     * @return
     */
    protected SaveResultDTO addSearchIndex(List<DynamicDTO> searchIndexes, Connection connection, ExecutionContext executionContext) {
        DynamicDTO searchIndex = searchIndexes.get(0);
        String boName = (String) searchIndex.get(DSPDMConstants.BoAttrName.BO_NAME);
        String tableName = (String) searchIndex.get(DSPDMConstants.BoAttrName.ENTITY);
        String indexName = (String) searchIndex.get(DSPDMConstants.BoAttrName.INDEX_NAME);
        String useCase = (String) searchIndex.get(DSPDMConstants.BoAttrName.USE_CASE);
        String columnNames = null;
        if (searchIndexes.size() == 1) {
            columnNames = (String) searchIndex.get(DSPDMConstants.BoAttrName.ATTRIBUTE);
        } else {
            // now get comma separated column names
            columnNames = CollectionUtils.getCommaSeparated(CollectionUtils.getStringValuesFromList(searchIndexes, DSPDMConstants.BoAttrName.ATTRIBUTE));
        }
        StringBuilder sqlBuilder = new StringBuilder(200);
        sqlBuilder.append("CREATE INDEX ").append(indexName).append(" ON ").append(tableName).append(" ")
                .append(DSPDMConstants.OPEN_PARENTHESIS);
        // since function based indices is applicable for postgres only
        if(!isMSSQLServerDialect(boName) && StringUtils.hasValue(useCase)){
            sqlBuilder.append(useCase).append(DSPDMConstants.OPEN_PARENTHESIS).append(columnNames).
                    append(DSPDMConstants.CLOSE_PARENTHESIS).append(DSPDMConstants.CLOSE_PARENTHESIS);
        }else{
            sqlBuilder.append(columnNames).append(DSPDMConstants.CLOSE_PARENTHESIS);
        }
        String sql = sqlBuilder.toString();
        // execute sql
        long startTime = 0;
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        PreparedStatement pstmt = null;
        try {
            logSQL(sql, executionContext);
            // collect sql script only if required
            if(executionContext.getCollectSQLScriptOptions().isDDLEnabled()) {
                executionContext.addSqlScript(sql);
            }
            pstmt = prepareStatementForInsert(sql, boName, executionContext, connection);
            startTime = System.currentTimeMillis();
            // execute batch
            pstmt.executeUpdate();
            logSQLTimeTaken(sql, 1, System.currentTimeMillis() - startTime, executionContext);
            // now update added db columns count
            saveResultDTO.addCreatedDBSearchIndexesCount(1);
            logger.info("{} physical search index added successfully.", indexName);
            // logging DDL statement
            trackChangeHistory(searchIndex, sql, DSPDMConstants.BusinessObjectOperations.DDL_ADD_SEARCH_INDEX_OPR.getId(), executionContext);
        } catch (Exception e) {
            if (sql != null) {
                logSQLTimeTaken(sql, 1, (System.currentTimeMillis() - startTime), executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeStatement(pstmt, executionContext);
        }
        return saveResultDTO;
    }

    /* ******************************************************************************* */
    /* ************** BUSINESS METHOD ADD COLUMN TO AN EXISTING TABLE **************** */
    /* ******************************************************************************* */

    /**
     * adds a physical column to an existing table in database
     *
     * @param dynamicDTO
     * @param connection
     * @param executionContext
     * @return
     */
    protected SaveResultDTO addColumn(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext) {
        String boName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        String tableName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY);
        String columnName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE);
        String dataType = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE);
        String sql = "ALTER TABLE " + tableName + " ADD " + columnName + " " + dataType;
        long startTime = 0;
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        PreparedStatement pstmt = null;
        try {
            logSQL(sql, executionContext);
            // collect sql script only if required
            if(executionContext.getCollectSQLScriptOptions().isDDLEnabled()) {
                executionContext.addSqlScript(sql);
            }
            pstmt = prepareStatementForInsert(sql, boName, executionContext, connection);
            startTime = System.currentTimeMillis();
            // execute batch
            pstmt.executeUpdate();
            logSQLTimeTaken(sql, 1, System.currentTimeMillis() - startTime, executionContext);
            // now update added db columns count
            saveResultDTO.addAddedDBColumnsCount(1);
            logger.info("{} physical column added successfully.", columnName);
            // logging DDL statement
            trackChangeHistory(dynamicDTO, sql, DSPDMConstants.BusinessObjectOperations.DDL_ADD_COLUMN_OPR.getId(), executionContext);
        } catch (Exception e) {
            if (sql != null) {
                logSQLTimeTaken(sql, 1, (System.currentTimeMillis() - startTime), executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeStatement(pstmt, executionContext);
        }
        return saveResultDTO;
    }

    /* ******************************************************************************* */
    /* ************** BUSINESS METHOD UPDATE COLUMN TO AN EXISTING TABLE **************** */
    /* ******************************************************************************* */

    /**
     * update a physical column to an existing table in database
     *
     * @param dynamicDTO
     * @param connection
     * @param executionContext
     * @return
     */
    protected SaveResultDTO updateColumn(DynamicDTO dynamicDTO, String columnPropertyNameToUpdate, Connection connection, ExecutionContext executionContext) {
        String tableName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY);
        String boName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        String columnName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE);
        String dataType = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE);
        String sql = "";
        if (columnPropertyNameToUpdate.equalsIgnoreCase(DSPDMConstants.BoAttrName.ATTRIBUTE_DEFAULT)) {
            Object newAttributeDefault = dynamicDTO.get(columnPropertyNameToUpdate);
            if (isMSSQLServerDialect(boName)) {
                // In MSSQLServer, if we want to update the default attribute, we have to delete the
                // Handling string and date/time data types as we need to put single quotes ('') around the default value
                if ((newAttributeDefault != null) && ((MetadataUtils.isStringDataType(dataType, boName)) || (MetadataUtils.isDateTimeDataType(dataType, boName)))) {
                    if (newAttributeDefault.toString().toLowerCase().startsWith("next")) {
                        sql = "ALTER TABLE " + tableName + " ADD CONSTRAINT def_" + tableName.toLowerCase() + "_" + columnName.toLowerCase() + " DEFAULT " + newAttributeDefault + " FOR " + columnName;
                    } else {
                        sql = "ALTER TABLE " + tableName + " ADD CONSTRAINT def_" + tableName.toLowerCase() + "_" + columnName.toLowerCase() + " DEFAULT '" + newAttributeDefault + "' FOR " + columnName;
                    }
                } else {
                    sql = "ALTER TABLE " + tableName + " ADD CONSTRAINT def_" + tableName + "_" + columnName + " DEFAULT " + newAttributeDefault + " FOR " + columnName;
                }
            } else {
                // Handling string and date/time data types as we need to put single quotes ('') around the default value
                if ((newAttributeDefault != null) && ((MetadataUtils.isStringDataType(dataType, boName)) || (MetadataUtils.isDateTimeDataType(dataType, boName)))) {
                    if (newAttributeDefault.toString().toLowerCase().startsWith("next")) {
                        sql = "ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " SET DEFAULT " + newAttributeDefault + "";
                    } else {
                        sql = "ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " SET DEFAULT '" + newAttributeDefault + "'";
                    }
                } else {
                    sql = "ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " SET DEFAULT " + newAttributeDefault;
                }
            }
        } else if (columnPropertyNameToUpdate.equalsIgnoreCase(DSPDMConstants.BoAttrName.IS_MANDATORY)) {
            if ((Boolean) dynamicDTO.get(columnPropertyNameToUpdate) == true) {
                if (isMSSQLServerDialect(boName)) {
                    sql = "ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " " + dataType + " NOT NULL";
                } else {
                    sql = "ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " SET NOT NULL";
                }
            } else if ((Boolean) dynamicDTO.get(columnPropertyNameToUpdate) == false) {
                if (isMSSQLServerDialect(boName)) {
                    sql = "ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " " + dataType + " NULL";
                } else {
                    sql = "ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " DROP NOT NULL";
                }
            }
        } else {// attribute datatype case only
            if (isMSSQLServerDialect(boName)) {
                sql = "ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " " + dynamicDTO.get(columnPropertyNameToUpdate);
            } else {
                //recheck of standard format
                sql = "ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " TYPE " + dynamicDTO.get(columnPropertyNameToUpdate) + " USING " + columnName + "::" + dynamicDTO.get(columnPropertyNameToUpdate);
            }
        }

        long startTime = 0;
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        PreparedStatement pstmt = null;
        try {
            logSQL(sql, executionContext);
            // collect sql script only if required
            if (executionContext.getCollectSQLScriptOptions().isDDLEnabled()) {
                executionContext.addSqlScript(sql);
            }
            pstmt = prepareStatementForInsert(sql, boName, executionContext, connection);
            startTime = System.currentTimeMillis();
            // execute batch
            pstmt.executeUpdate();
            logSQLTimeTaken(sql, 1, System.currentTimeMillis() - startTime, executionContext);
            // now update added db columns count
            saveResultDTO.addUpdatedDBColumnsCount(1);
            logger.info("{} physical column updated successfully.", columnName);
            // logging DDL statement
            trackChangeHistory(dynamicDTO, sql, DSPDMConstants.BusinessObjectOperations.DDL_UPDATE_COLUMN_OPR.getId(), executionContext);
        } catch (Exception e) {
            if (sql != null) {
                logSQLTimeTaken(sql, 1, (System.currentTimeMillis() - startTime), executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeStatement(pstmt, executionContext);
        }
        return saveResultDTO;
    }

    /* ********************************************************************************** */
    /* ************** BUSINESS METHOD DROP COLUMN FROM AN EXISTING TABLE **************** */
    /* ********************************************************************************** */

    /**
     * drops a physical column from an existing table in database
     *
     * @param dynamicDTO
     * @param connection
     * @param executionContext
     * @return
     */
    protected SaveResultDTO dropColumn(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext) {
        String boName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        String tableName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY);
        String columnName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE);
        String sql = "ALTER TABLE " + tableName + " DROP COLUMN " + columnName;
        long startTime = 0;
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        PreparedStatement pstmt = null;
        try {
            logSQL(sql, executionContext);
            // collect sql script only if required
            if(executionContext.getCollectSQLScriptOptions().isDDLEnabled()) {
                executionContext.addSqlScript(sql);
            }
            pstmt = prepareStatementForDelete(sql, boName, executionContext, connection);
            startTime = System.currentTimeMillis();
            // execute batch
            pstmt.executeUpdate();
            logSQLTimeTaken(sql, 1, System.currentTimeMillis() - startTime, executionContext);
            // now update dropped db columns count
            saveResultDTO.addDroppedDBColumnsCount(1);
            logger.info("{} physical column dropped successfully.", columnName);
            // logging DDL statement
            trackChangeHistory(dynamicDTO, sql, DSPDMConstants.BusinessObjectOperations.DDL_DROP_COLUMN_OPR.getId(), executionContext);
        } catch (Exception e) {
            if (sql != null) {
                logSQLTimeTaken(sql, 1, (System.currentTimeMillis() - startTime), executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeStatement(pstmt, executionContext);
        }
        return saveResultDTO;
    }

    /* *************************************************************** */
    /* ************** BUSINESS METHOD CREATE SEQUENCE **************** */
    /* *************************************************************** */

    /**
     * created physical sequence database object
     *
     * @param dynamicDTO
     * @param connection
     * @param executionContext
     * @return
     */
    protected SaveResultDTO createSequence(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        String boName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        String sequenceName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.KEY_SEQ_NAME);
        if (StringUtils.hasValue(sequenceName)) {
            String sql = "CREATE SEQUENCE " + sequenceName + " START WITH 1 INCREMENT BY 1";
            if (!isMSSQLServerDialect(boName)) {
                sql = sql + ";ALTER SEQUENCE " + sequenceName + " OWNER TO postgres;";
            }
            long startTime = 0;
            PreparedStatement pstmt = null;
            try {
                logSQL(sql, executionContext);
                // collect sql script only if required
                if(executionContext.getCollectSQLScriptOptions().isDDLEnabled()) {
                    executionContext.addSqlScript(sql);
                }
                pstmt = prepareStatementForInsert(sql, boName, executionContext, connection);
                startTime = System.currentTimeMillis();
                // execute batch
                pstmt.executeUpdate();
                logSQLTimeTaken(sql, 1, System.currentTimeMillis() - startTime, executionContext);
                // now update added db columns count
                saveResultDTO.addCreatedDBSequencesCount(1);
                logger.info("{} physical sequence created successfully.", sequenceName);
                // logging DDL statement
                trackChangeHistory(dynamicDTO, sql, DSPDMConstants.BusinessObjectOperations.DDL_CREATE_SEQUENCE_OPR.getId(), executionContext);
            } catch (Exception e) {
                if (sql != null) {
                    logSQLTimeTaken(sql, 1, (System.currentTimeMillis() - startTime), executionContext);
                }
                DSPDMException.throwException(e, executionContext);
            } finally {
                closeStatement(pstmt, executionContext);
            }
        }
        return saveResultDTO;
    }

    /* ************************************************************* */
    /* ************** BUSINESS METHOD DROP SEQUENCE **************** */
    /* ************************************************************* */

    /**
     * drops physical sequence database object
     *
     * @param dynamicDTO
     * @param connection
     * @param executionContext
     * @return
     */
    protected SaveResultDTO dropSequence(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        String boName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        String sequenceName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.KEY_SEQ_NAME);
        if (StringUtils.hasValue(sequenceName)) {
            String sql = "DROP SEQUENCE " + sequenceName;
            long startTime = 0;
            PreparedStatement pstmt = null;
            try {
                logSQL(sql, executionContext);
                // collect sql script only if required
                if(executionContext.getCollectSQLScriptOptions().isDDLEnabled()) {
                    executionContext.addSqlScript(sql);
                }
                pstmt = prepareStatementForDelete(sql, boName, executionContext, connection);
                startTime = System.currentTimeMillis();
                // execute batch
                pstmt.executeUpdate();
                logSQLTimeTaken(sql, 1, System.currentTimeMillis() - startTime, executionContext);
                // now update added db columns count
                saveResultDTO.addDroppedDBSequencesCount(1);
                logger.info("{} physical sequence dropped successfully.", sequenceName);
                // logging DDL statement
                trackChangeHistory(dynamicDTO, sql, DSPDMConstants.BusinessObjectOperations.DDL_DROP_SEQUENCE_OPR.getId(), executionContext);
            } catch (Exception e) {
                if (sql != null) {
                    logSQLTimeTaken(sql, 1, (System.currentTimeMillis() - startTime), executionContext);
                }
                DSPDMException.throwException(e, executionContext);
            } finally {
                closeStatement(pstmt, executionContext);
            }
        }
        return saveResultDTO;
    }

    /* *************************************************************** */
    /* ***************** PRIVATE LOCAL STATIC METHODS **************** */
    /* *************************************************************** */

    private static String getCreateTableSQLStatement(DynamicDTO businessObjectDynamicDTO,
                                                     List<DynamicDTO> businessObjectAttributes,
                                                     List<DynamicDTO> businessObjectRelationships_FK,
                                                     List<DynamicDTO> uniqueConstraints,
                                                     boolean isMSSQLServerDialect) {

        String tableName = (String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY);
        // create inner small sql clauses
        String columnsSQLStatement = "";
        String pkSQLStatement = "";
        String fkSQLStatement = "";
        String uniqueConstraitSQLStatement="";
        // COLUMNS DEFINITION
        if (CollectionUtils.hasValue(businessObjectAttributes)) {
            columnsSQLStatement = getColumnsDefinitionForCreateTable(businessObjectAttributes, isMSSQLServerDialect);
            // PRIMARY KEY
            pkSQLStatement = getPrimaryKeyDefinitionForCreateTable(tableName, businessObjectAttributes);
            // Foreign KEY
            if (CollectionUtils.hasValue(businessObjectRelationships_FK)) {
                fkSQLStatement = getForeignKeyDefinitionForCreateTable(tableName, businessObjectRelationships_FK);
            }
            //Unique Constraints
            if (CollectionUtils.hasValue(uniqueConstraints)) {
                uniqueConstraitSQLStatement = getUniqueConstraintDefinitionForCreateTable(tableName, uniqueConstraints);
            }
        }

        StringBuilder sql = new StringBuilder(50 + columnsSQLStatement.length() + pkSQLStatement.length() + fkSQLStatement.length()+uniqueConstraitSQLStatement.length());
        sql.append("CREATE TABLE ").append(tableName);
        // COLUMNS DEFINITION
        if (StringUtils.hasValue(columnsSQLStatement)) {
            // TABLE OPEN BRACE
            sql.append(DSPDMConstants.OPEN_PARENTHESIS).append(System.lineSeparator());
            // COLUMNS DEFINITION
            sql.append(columnsSQLStatement);
            // PRIMARY KEY
            if (StringUtils.hasValue(pkSQLStatement)) {
                sql.append(DSPDMConstants.COMMA);
                sql.append(System.lineSeparator());
                sql.append(pkSQLStatement);
            }
            // Foreign KEY
            if (StringUtils.hasValue(fkSQLStatement)) {
                sql.append(DSPDMConstants.COMMA);
                sql.append(System.lineSeparator());
                sql.append(fkSQLStatement);
            }
            // Unique Constraint
            if (StringUtils.hasValue(uniqueConstraitSQLStatement)) {
                sql.append(DSPDMConstants.COMMA);
                sql.append(System.lineSeparator());
                sql.append(uniqueConstraitSQLStatement);
            }
            // TABLE END BRACE
            sql.append(DSPDMConstants.CLOSE_PARENTHESIS);
        }
        // change owner to postgres
        if (!isMSSQLServerDialect) {
            sql.append(";");
            sql.append(System.lineSeparator());
            sql.append("ALTER TABLE ").append(tableName).append(" OWNER TO postgres");
        }
        return sql.toString();
    }

    private static String getColumnsDefinitionForCreateTable(List<DynamicDTO> businessObjectAttributes, boolean isMSSQLServerDialect) {
        StringBuilder sql = new StringBuilder(businessObjectAttributes.size() * 100);
        int remaining = businessObjectAttributes.size();

        String mssqlserverCollation = null;
        if (isMSSQLServerDialect) {
            // if config says that do not use default collation then provide overrided collation
            if (!(ConfigProperties.getInstance().mssqlserver_use_default_collation.getBooleanValue())) {
                if (StringUtils.hasValue(ConfigProperties.getInstance().mssqlserver_overrided_collation_name.getPropertyValue())) {
                    mssqlserverCollation = ConfigProperties.getInstance().mssqlserver_overrided_collation_name.getPropertyValue();
                }
            }
        }
        for (DynamicDTO businessObjectAttrDynamicDTO : businessObjectAttributes) {
            sql.append(businessObjectAttrDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE));
            sql.append(DSPDMConstants.SPACE);
            sql.append(businessObjectAttrDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE));

            // MS SQL Server Collation
            if ((mssqlserverCollation != null) &&
                    (DSPDMConstants.DataTypes.fromAttributeDataType(
                            (String) businessObjectAttrDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE),
                            isMSSQLServerDialect).isStringDataType())) {
                sql.append(DSPDMConstants.SPACE).append("COLLATE ").append(mssqlserverCollation);
            }

            // NOT NULL
            if (Boolean.TRUE.equals(businessObjectAttrDynamicDTO.get(DSPDMConstants.BoAttrName.IS_MANDATORY))) {
                sql.append(DSPDMConstants.SPACE).append("NOT NULL");
            }

            // DEFAULT
            if (StringUtils.hasValue((String) businessObjectAttrDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DEFAULT))) {
                String attrType = (String) businessObjectAttrDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE);
                String defaultValue = ((String) businessObjectAttrDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DEFAULT)).trim();
                String boName = (String) businessObjectAttrDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
                if (defaultValue.contains("(")) {
                    // function or stored procedure call case
                    sql.append(DSPDMConstants.SPACE).append("DEFAULT ").append((String) businessObjectAttrDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DEFAULT)).append("");
                } else {
                    if ((MetadataUtils.isStringDataType(attrType, boName)) || (MetadataUtils.isDateTimeDataType(attrType, boName))) {
                        // string and date time case
                        sql.append(DSPDMConstants.SPACE).append("DEFAULT '").append((String) businessObjectAttrDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DEFAULT)).append("'");
                    } else if ((MetadataUtils.isNumberDataType(attrType, boName)) || (MetadataUtils.isBooleanDataType(attrType, boName))) {
                        // number and boolean
                        sql.append(DSPDMConstants.SPACE).append("DEFAULT ").append((String) businessObjectAttrDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DEFAULT)).append("");
                    }
                }
            }

            // append comma
            if (--remaining > 0) {
                // do not append comma after last element in the list
                sql.append(DSPDMConstants.COMMA);
                sql.append(System.lineSeparator());
            }
        }
        return sql.toString();
    }

    private static String getPrimaryKeyDefinitionForCreateTable(String tableName, List<DynamicDTO> businessObjectAttributes) {
        StringBuilder sql = new StringBuilder(100);
        List<DynamicDTO> primaryKeyAttributes = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(businessObjectAttributes, DSPDMConstants.BoAttrName.IS_PRIMARY_KEY, Boolean.TRUE);
        if (CollectionUtils.hasValue(primaryKeyAttributes)) {
            List<String> primaryKeyAttributeNames = CollectionUtils.getStringValuesFromList(primaryKeyAttributes, DSPDMConstants.BoAttrName.ATTRIBUTE);
            sql.append("CONSTRAINT PK_").append(tableName).append(" PRIMARY KEY ").append(DSPDMConstants.OPEN_PARENTHESIS).append(CollectionUtils.getCommaSeparated(primaryKeyAttributeNames)).append(DSPDMConstants.CLOSE_PARENTHESIS);
        }
        return sql.toString();
    }

    private static String getUniqueConstraintDefinitionForCreateTable(String tableName, List<DynamicDTO> uniqueConstraints) {
        Map<String, List<DynamicDTO>> groupByUniqueConstraintName = CollectionUtils.groupDynamicDTOByPropertyValueIgnoreCase(
                uniqueConstraints, DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
        StringBuilder sql = new StringBuilder(groupByUniqueConstraintName.size() * 200);
        int remaining = groupByUniqueConstraintName.size();
        for (List<DynamicDTO> uniqueConstraintsInANamedGroup : groupByUniqueConstraintName.values()) {
            DynamicDTO uniqueConstraint = uniqueConstraintsInANamedGroup.get(0);
            String constraintName = (String) uniqueConstraint.get(DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
            String columnNames = null;
            if (uniqueConstraintsInANamedGroup.size() == 1) {
                columnNames = (String) uniqueConstraint.get(DSPDMConstants.BoAttrName.ATTRIBUTE);
            } else {
                // now get comma separated column names
                columnNames = CollectionUtils.getCommaSeparated(CollectionUtils.getStringValuesFromList(uniqueConstraintsInANamedGroup, DSPDMConstants.BoAttrName.ATTRIBUTE));
            }
            // now making clause
            sql.append("CONSTRAINT ").append(constraintName).append(" UNIQUE ").append(DSPDMConstants.OPEN_PARENTHESIS);
            sql.append(columnNames).append(DSPDMConstants.CLOSE_PARENTHESIS);
            if (--remaining > 0) {
                // do not append comma after last element in the list
                sql.append(DSPDMConstants.COMMA);
                sql.append(System.lineSeparator());
            }
        }
        return sql.toString();
    }

    private static String getForeignKeyDefinitionForCreateTable(String tableName, List<DynamicDTO> businessObjectRelationships_FK) {
        Map<String, List<DynamicDTO>> groupByRelationshipName = CollectionUtils.groupDynamicDTOByPropertyValueIgnoreCase(businessObjectRelationships_FK, DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME);
        StringBuilder sql = new StringBuilder(groupByRelationshipName.size() * 200);
        int remaining = groupByRelationshipName.size();
        for (List<DynamicDTO> relationshipsInAGroup : groupByRelationshipName.values()) {
            DynamicDTO relationship = relationshipsInAGroup.get(0);
            String parentTableName = (String) relationship.get(DSPDMConstants.BoAttrName.PARENT_ENTITY_NAME);
            String childTableName = (String) relationship.get(DSPDMConstants.BoAttrName.CHILD_ENTITY_NAME);
            String constraintName = (String) relationship.get(DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
            String parentColumnNames = null;
            String childColumnNames = null;
            if (relationshipsInAGroup.size() == 1) {
                // simple relationship
                parentColumnNames = (String) relationship.get(DSPDMConstants.BoAttrName.PARENT_ATTRIBUTE_NAME);
                childColumnNames = (String) relationship.get(DSPDMConstants.BoAttrName.CHILD_ATTRIBUTE_NAME);
            } else {
                // now get comma separated column names to define a composite relationship
                parentColumnNames = CollectionUtils.getCommaSeparated(CollectionUtils.getStringValuesFromList(relationshipsInAGroup, DSPDMConstants.BoAttrName.PARENT_ATTRIBUTE_NAME));
                childColumnNames = CollectionUtils.getCommaSeparated(CollectionUtils.getStringValuesFromList(relationshipsInAGroup, DSPDMConstants.BoAttrName.CHILD_ATTRIBUTE_NAME));
            }

            if (StringUtils.isNullOrEmpty(constraintName)) {
                constraintName = (String) relationship.get(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME);
            }
            // now making clause
            sql.append("CONSTRAINT ").append(constraintName).append(" FOREIGN KEY ").append(DSPDMConstants.OPEN_PARENTHESIS).append(childColumnNames).append(DSPDMConstants.CLOSE_PARENTHESIS);
            sql.append(" REFERENCES ").append(parentTableName).append(DSPDMConstants.OPEN_PARENTHESIS).append(parentColumnNames).append(DSPDMConstants.CLOSE_PARENTHESIS);
            if (--remaining > 0) {
                // do not append comma after last element in the list
                sql.append(DSPDMConstants.COMMA);
                sql.append(System.lineSeparator());
            }
        }
        return sql.toString();
    }

    private void trackChangeHistory(DynamicDTO dynamicDTO, String sql, int businessOperationId, ExecutionContext executionContext){
        // logging DDL statement
        if (isChangeHistoryTrackEnabledForBoName(dynamicDTO.getType())) {
            executionContext.incrementOperationSequenceNumber();// this is for track change history table
            executionContext.incrementDdlOperationCount(); // this is for determining effected row count i.e., without counting ddl statements
            Timestamp timestamp = DateTimeUtils.getCurrentTimestampUTC();
            List<String> primaryKeyColumnNames = dynamicDTO.getPrimaryKeyColumnNames();
            if ((CollectionUtils.hasValue(primaryKeyColumnNames)) && (primaryKeyColumnNames.size() == 1)) {
                // saving target boAttrName against logged ddl statement(s)
                String boAttrName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                if(StringUtils.isNullOrEmpty(boAttrName)){
                    // means we don't have a unique bo_attr_name to log, so logging PK Column here
                    boAttrName = getPKColumnName(dynamicDTO, executionContext);
                }
                //handling case of relationships. saving child bo name in case of relationships
                String boName = dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME) == null ? (String) dynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME) : (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
                // add history dynamic dto to the execution context for logging
                addChangeHistoryDTOToExecutionContext(boName,
                        (Integer) dynamicDTO.getId().getPK()[0],
                        boAttrName,
                        null, // since its an insert, we don't have old value so sending oldValue as null.
                        sql, // DDL statement
                        timestamp,
                        businessOperationId,// business operation id
                        executionContext);
            }
        }
    }
    private static void addChangeHistoryDTOToExecutionContext(String boName, Integer pkId, String boAttrName, Object oldValue, Object newValue, Timestamp timestamp, Integer businessOperationId, ExecutionContext executionContext) {
        if (oldValue != null) {
            if (!(oldValue instanceof String)) {
                oldValue = MetadataUtils.convertValueToStringFromJavaDataType(oldValue, executionContext);
            }
            if (((String) oldValue).length() > 1000) {
                oldValue = ((String) oldValue).substring(0, 999);
            }
        }
        if (newValue != null) {
            if (!(newValue instanceof String)) {
                newValue = MetadataUtils.convertValueToStringFromJavaDataType(newValue, executionContext);
            }
            if (((String) newValue).length() > 1000) {
                newValue = ((String) newValue).substring(0, 999);
            }
        }
        DynamicDTO changeHistoryDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.BUS_OBJ_ATTR_CHANGE_HISTORY, null, executionContext);
        changeHistoryDynamicDTO.put(DSPDMConstants.BoAttrName.R_BUSINESS_OBJECT_OPR_ID, businessOperationId);
        changeHistoryDynamicDTO.put(DSPDMConstants.BoAttrName.BO_NAME, boName);
        changeHistoryDynamicDTO.put(DSPDMConstants.BoAttrName.PK_ID, pkId);// Primary Key
        changeHistoryDynamicDTO.put(DSPDMConstants.BoAttrName.BO_ATTR_NAME, boAttrName);
        changeHistoryDynamicDTO.put(DSPDMConstants.BoAttrName.OLD_VALUE, oldValue);
        changeHistoryDynamicDTO.put(DSPDMConstants.BoAttrName.CHANGED_VALUE, newValue);
        changeHistoryDynamicDTO.put(DSPDMConstants.BoAttrName.OPR_SEQUENCE_NUMBER, executionContext.getOperationSequenceNumber());
        changeHistoryDynamicDTO.put(DSPDMConstants.BoAttrName.ROW_CREATED_DATE, timestamp);
        changeHistoryDynamicDTO.put(DSPDMConstants.BoAttrName.ROW_CREATED_BY, executionContext.getExecutorName());
        executionContext.addBusObjAttrChangeHistoryDTOList(changeHistoryDynamicDTO);
    }

    private static boolean isChangeHistoryTrackEnabledForBoName(String boName) {
        boolean flag = false;
        if (ConfigProperties.getInstance().history_change_track_enabled.getBooleanValue()) {
            if (!(CollectionUtils.containsIgnoreCase(DSPDMConstants.NO_CHANGE_TRACK_BO_NAMES, boName))) {
                flag = true;
            }
        }
        return flag;
    }
    private String getPKColumnName(DynamicDTO dynamicDTO, ExecutionContext executionContext){
        String primaryKeyColumnName = "_";
        // finding primary key column name from children attribute
        if(dynamicDTO.containsKey(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY)){
            // means we need to extract pk from the children packet
            primaryKeyColumnName = getPrimaryKeyFromChildrenObject(dynamicDTO,executionContext);
            if(primaryKeyColumnName.equalsIgnoreCase("-")){
                // we couldn't find PK from the attributes list. Now finding it from the bo name received in dynamicDTO
                IDynamicDAO dynamicDao = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO((String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME), executionContext);
                List<String> primaryKeyColumnNames = dynamicDao.getPrimaryKeyColumnNames();
                if(primaryKeyColumnNames.size() > 0){
                    primaryKeyColumnName = primaryKeyColumnNames.get(0);
                }
            }
        }else{
            IDynamicDAO dynamicDao;
            if(dynamicDTO.containsKey(DSPDMConstants.BoAttrName.CHILD_BO_NAME)){
                // handling relationship. Reading pk column of child bo in case of relationship case
                dynamicDao = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO((String) dynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME), executionContext);
            }else{
                dynamicDao = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO((String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME), executionContext);
            }
            List<String> primaryKeyColumnNames = dynamicDao.getPrimaryKeyColumnNames();
            if(primaryKeyColumnNames.size() > 0){
                primaryKeyColumnName = primaryKeyColumnNames.get(0);
            }
        }
        return primaryKeyColumnName;
    }

    private String getPrimaryKeyFromChildrenObject(DynamicDTO dynamicDTO, ExecutionContext executionContext){
        String primaryKeyColumnName = "-";
        Map<String, DynamicDTO> children = (Map<String, DynamicDTO>) dynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
        if (children.size() > 0) {
            List<DynamicDTO> childrenDTO = (List<DynamicDTO>) children.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
            childrenDTOLoop:
            for (String key : children.keySet()) {
                childrenDTO = (List<DynamicDTO>) children.get(key);
                for (DynamicDTO childDynamicDTO : childrenDTO) {
                    if(childDynamicDTO.containsKey(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY)){
                        if((Boolean) childDynamicDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY) == true){
                            primaryKeyColumnName = (String) childDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE);
                            break childrenDTOLoop;
                        }
                    }
                }
            }
        }
        return primaryKeyColumnName;
    }

    protected List<String[]> getAttributesVsUnitMappings(String boName, Connection connection, ExecutionContext executionContext) {

        /*
            select
            a.OLD_VALUE AS UNIT,b.OLD_VALUE AS BO_ATTR_NAME
            from bus_obj_attr_change_history a
            INNER JOIN bus_obj_attr_change_history b ON b.USER_PERFORMED_OPR_ID = a.USER_PERFORMED_OPR_ID and b.PK_ID = a.PK_ID
            INNER JOIN
            (
               select max(user_performed_opr_id) as last_user_performed_opr_id
               from bus_obj_attr_change_history
               where bo_name = 'BUSINESS OBJECT ATTR'
               AND bo_attr_name = 'BO_NAME'
               AND old_value = 'WELL PROD CYCLE'
               AND r_business_object_opr_id IN (14,20)
            )
            AS c ON a.USER_PERFORMED_OPR_ID = c.last_user_performed_opr_id
            where a.BO_NAME = 'BUSINESS OBJECT ATTR'
            and a.BO_ATTR_NAME = 'UNIT'
            and b.BO_ATTR_NAME = 'BO_ATTR_NAME'
            ;
         */
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BUS_OBJ_ATTR_CHANGE_HISTORY, executionContext);
        businessObjectInfo.setAlias("a");
        businessObjectInfo.setPrependAlias(true);
        businessObjectInfo.setReadAllRecords(true);
        // select a.old_value as unit_name
        businessObjectInfo.addColumnsToSelect(DSPDMConstants.BoAttrName.OLD_VALUE);
        // a.BO_NAME = 'BUSINESS OBJECT ATTR'
        businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.BO_NAME, DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
        // a.BO_ATTR_NAME = 'UNIT'
        businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.BO_ATTR_NAME, DSPDMConstants.BoAttrName.UNIT);
        // *************************** SIMPLE JOIN *************************
        // now prepare join for getting bo_attr_name for the unit
        SimpleJoinClause simpleJoinClause = new SimpleJoinClause(DSPDMConstants.BoName.BUS_OBJ_ATTR_CHANGE_HISTORY, "b", JoinType.INNER);
        // select b.OLD_VALUE AS BO_ATTR_NAME
        simpleJoinClause.setSelectList(new SelectList(Arrays.asList(DSPDMConstants.BoAttrName.OLD_VALUE)));
        // and b.BO_ATTR_NAME = 'BO_ATTR_NAME'
        simpleJoinClause.addCriteriaFilter(new CriteriaFilter(DSPDMConstants.BoAttrName.BO_ATTR_NAME, Operator.EQUALS,
                new Object[]{DSPDMConstants.BoAttrName.BO_ATTR_NAME}));
        // INNER JOIN bus_obj_attr_change_history b ON b.USER_PERFORMED_OPR_ID = a.USER_PERFORMED_OPR_ID and b.PK_ID = a.PK_ID
        simpleJoinClause.setJoiningConditions(Arrays.asList(new JoiningCondition(
                        new JoiningCondition.JoiningConditionOperand("b", DSPDMConstants.BoAttrName.USER_PERFORMED_OPR_ID),
                        Operator.EQUALS,
                        new JoiningCondition.JoiningConditionOperand("a", DSPDMConstants.BoAttrName.USER_PERFORMED_OPR_ID)
                ),
                new JoiningCondition(
                        new JoiningCondition.JoiningConditionOperand("b", DSPDMConstants.BoAttrName.PK_ID),
                        Operator.EQUALS,
                        new JoiningCondition.JoiningConditionOperand("a", DSPDMConstants.BoAttrName.PK_ID)
                )));
        // add join to the business object info
        simpleJoinClause.setJoinOrder(Byte.valueOf("1"));
        businessObjectInfo.addSimpleJoin(simpleJoinClause);
        // *************************** DYNAMIC JOIN *************************
        // select max(user_performed_opr_id) as max_user_performed_opr_id
        DynamicTable dynamicTable = new DynamicTable(DSPDMConstants.BoName.BUS_OBJ_ATTR_CHANGE_HISTORY,
                new SelectList().addAggregateColumnsToSelect(
                        new AggregateColumn(AggregateFunction.MAX,
                                DSPDMConstants.BoAttrName.USER_PERFORMED_OPR_ID,
                                "last_user_performed_opr_id")));
        dynamicTable.setJoinAlias("d");
        // where bo_name = 'BUSINESS OBJECT ATTR'
        dynamicTable.addCriteriaFilter(new CriteriaFilter(DSPDMConstants.BoAttrName.BO_NAME, Operator.EQUALS,
                new Object[]{DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR}));
        // AND bo_attr_name = 'BO_NAME'
        dynamicTable.addCriteriaFilter(new CriteriaFilter(DSPDMConstants.BoAttrName.BO_ATTR_NAME, Operator.EQUALS,
                new Object[]{DSPDMConstants.BoAttrName.BO_NAME}));
        // AND old_value = 'WELL PROD CYCLE'
        dynamicTable.addCriteriaFilter(new CriteriaFilter(DSPDMConstants.BoAttrName.OLD_VALUE, Operator.EQUALS,
                new Object[]{boName}));
        // AND r_business_object_opr_id IN (14,20)
        dynamicTable.addCriteriaFilter(new CriteriaFilter(DSPDMConstants.BoAttrName.R_BUSINESS_OBJECT_OPR_ID, Operator.IN,
                new Object[]{DSPDMConstants.BusinessObjectOperations.DELETE_METADATA_OPR.getId(),
                        DSPDMConstants.BusinessObjectOperations.DELETE_METADATA_FOR_ALL_OPR.getId()}));
        DynamicJoinClause dynamicJoinClause = new DynamicJoinClause("c", JoinType.INNER);
        dynamicJoinClause.setDynamicTables(Arrays.asList(dynamicTable));
        // INNER JOIN (select ...) AS c ON a.USER_PERFORMED_OPR_ID = c.last_user_performed_opr_id
        dynamicJoinClause.setJoiningConditions(Arrays.asList(new JoiningCondition(
                new JoiningCondition.JoiningConditionOperand("a", DSPDMConstants.BoAttrName.USER_PERFORMED_OPR_ID),
                Operator.EQUALS,
                new JoiningCondition.JoiningConditionOperand("c", "last_user_performed_opr_id"))
        ));
        simpleJoinClause.setJoinOrder(Byte.valueOf("2"));
        businessObjectInfo.addDynamicJoin(dynamicJoinClause);

        IDynamicDAOImpl dynamicDao = DynamicDAOFactory.getInstance(executionContext).getDynamicDAOImpl((String) DSPDMConstants.BoName.BUS_OBJ_ATTR_CHANGE_HISTORY, executionContext);
        List<DynamicDTO> dynamicDTOList = dynamicDao.read(businessObjectInfo, false, connection, executionContext);

        List<String[]> attributeVsUnitMappingList = new ArrayList<>(dynamicDTOList.size());
        for (DynamicDTO dynamicDTO : dynamicDTOList) {
            attributeVsUnitMappingList.add(new String[]{
                    (String) dynamicDTO.get("b_" + DSPDMConstants.BoAttrName.OLD_VALUE),
                    (String) dynamicDTO.get("a_" + DSPDMConstants.BoAttrName.OLD_VALUE)
            });
        }
        return attributeVsUnitMappingList;
    }

    protected List<DynamicDTO> restoreBusinessObjectGroupFromHistoryTables(List<DynamicDTO> boList, Connection connection, ExecutionContext executionContext){
        List<DynamicDTO> businessObjectGroupDTOList = new ArrayList<>();
        for(DynamicDTO boDynamicDTO: boList){
            String boName = (String) boDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
             /*
             SELECT
	            a.BO_ATTR_NAME AS a_BO_ATTR_NAME, a.OLD_VALUE AS a_OLD_VALUE
             FROM
                BUS_OBJ_ATTR_CHANGE_HISTORY AS a
                INNER JOIN (
                SELECT
                    MAX( d.USER_PERFORMED_OPR_ID ) AS last_user_performed_opr_id
                FROM
                    BUS_OBJ_ATTR_CHANGE_HISTORY AS d
                WHERE
                    ( d.BO_NAME = 'BUSINESS OBJECT GROUP' )
                    AND ( d.BO_ATTR_NAME = 'BO_NAME' )
                    AND ( d.OLD_VALUE = 'WELL VOL DAILY' )
                    AND (
                    d.R_BUSINESS_OBJECT_OPR_ID IN ( 14, 20 ))) AS c ON a.USER_PERFORMED_OPR_ID = c.last_user_performed_opr_id
            WHERE
                (
                a.BO_NAME = 'BUSINESS OBJECT GROUP')
            */
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BUS_OBJ_ATTR_CHANGE_HISTORY, executionContext);
            businessObjectInfo.setAlias("a");
            businessObjectInfo.setPrependAlias(true);
            businessObjectInfo.setReadAllRecords(true);
            // select a.BO_ATTR_NAME, a.OLD_VALUE
            businessObjectInfo.addColumnsToSelect(Arrays.asList(DSPDMConstants.BoAttrName.BO_ATTR_NAME, DSPDMConstants.BoAttrName.OLD_VALUE));
            // a.BO_NAME = 'BUSINESS OBJECT GROUP'
            businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.BO_NAME, DSPDMConstants.BoName.BUSINESS_OBJECT_GROUP);

            // *************************** DYNAMIC JOIN *************************
            // select max(user_performed_opr_id) as max_user_performed_opr_id
            DynamicTable dynamicTable = new DynamicTable(DSPDMConstants.BoName.BUS_OBJ_ATTR_CHANGE_HISTORY,
                    new SelectList().addAggregateColumnsToSelect(
                            new AggregateColumn(AggregateFunction.MAX,
                                    DSPDMConstants.BoAttrName.USER_PERFORMED_OPR_ID,
                                    "last_user_performed_opr_id")));
            dynamicTable.setJoinAlias("d");
            // where bo_name = 'BUSINESS OBJECT GROUP'
            dynamicTable.addCriteriaFilter(new CriteriaFilter(DSPDMConstants.BoAttrName.BO_NAME, Operator.EQUALS,
                    new Object[]{DSPDMConstants.BoName.BUSINESS_OBJECT_GROUP}));
            // AND bo_attr_name = 'BO_NAME'
            dynamicTable.addCriteriaFilter(new CriteriaFilter(DSPDMConstants.BoAttrName.BO_ATTR_NAME, Operator.EQUALS,
                    new Object[]{DSPDMConstants.BoAttrName.BO_NAME}));
            // AND old_value = 'WELL PROD CYCLE'
            dynamicTable.addCriteriaFilter(new CriteriaFilter(DSPDMConstants.BoAttrName.OLD_VALUE, Operator.EQUALS,
                    new Object[]{boName}));
            // AND r_business_object_opr_id IN (14,20)
            dynamicTable.addCriteriaFilter(new CriteriaFilter(DSPDMConstants.BoAttrName.R_BUSINESS_OBJECT_OPR_ID, Operator.IN,
                    new Object[]{DSPDMConstants.BusinessObjectOperations.DELETE_METADATA_OPR.getId(),
                            DSPDMConstants.BusinessObjectOperations.DELETE_METADATA_FOR_ALL_OPR.getId()}));
            DynamicJoinClause dynamicJoinClause = new DynamicJoinClause("c", JoinType.INNER);
            dynamicJoinClause.setDynamicTables(Arrays.asList(dynamicTable));
            // INNER JOIN (select ...) AS c ON a.USER_PERFORMED_OPR_ID = c.last_user_performed_opr_id
            dynamicJoinClause.setJoiningConditions(Arrays.asList(new JoiningCondition(
                    new JoiningCondition.JoiningConditionOperand("a", DSPDMConstants.BoAttrName.USER_PERFORMED_OPR_ID),
                    Operator.EQUALS,
                    new JoiningCondition.JoiningConditionOperand("c", "last_user_performed_opr_id"))
            ));
            businessObjectInfo.addDynamicJoin(dynamicJoinClause);
            IDynamicDAOImpl dynamicDao = DynamicDAOFactory.getInstance(executionContext).getDynamicDAOImpl((String) DSPDMConstants.BoName.BUS_OBJ_ATTR_CHANGE_HISTORY, executionContext);
            List<DynamicDTO> dynamicDTOList = dynamicDao.read(businessObjectInfo, false, connection, executionContext);
            DynamicDTO boGroupDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.BUSINESS_OBJECT_GROUP, null, executionContext);
            Boolean busObjGroupDtoFound = false;
            for(DynamicDTO dynamicDTO: dynamicDTOList){
                busObjGroupDtoFound = true;
                String bo_attr_name =  (String) dynamicDTO.get("a_" + DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                String old_value = (String) dynamicDTO.get("a_" + DSPDMConstants.BoAttrName.OLD_VALUE);
                if(bo_attr_name.equalsIgnoreCase(DSPDMConstants.BoAttrName.BO_NAME)){
                    //assigning newly created business object id
                    boGroupDynamicDTO.put(DSPDMConstants.BoAttrName.BUSINESS_OBJECT_ID,CollectionUtils.
                            filterFirstDynamicDTOByPropertyNameAndPropertyValue(boList, DSPDMConstants.BoAttrName.BO_NAME, old_value).
                            get(DSPDMConstants.BoAttrName.BUSINESS_OBJECT_ID));
                }
                // ignoring business object id because this id will be newly generated since the bo is newly generated.
                if(!bo_attr_name.equalsIgnoreCase(DSPDMConstants.BoAttrName.BUSINESS_OBJECT_ID)){
                    boGroupDynamicDTO.put(bo_attr_name, old_value);
                }
            }
            if(busObjGroupDtoFound){
                businessObjectGroupDTOList.add(boGroupDynamicDTO);
            }
        }
        return  businessObjectGroupDTOList;
    }
}
