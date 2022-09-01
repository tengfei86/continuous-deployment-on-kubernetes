package com.lgc.dspdm.core.dao.dynamic.businessobject.impl;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.criteria.SQLExpression;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicPK;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectAttributeDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.*;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAOImpl;
import org.postgresql.util.PGobject;

import java.sql.*;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

/**
 * Class to hold common code for insert methods or base methods.
 * These methods will not be public and can only be invoked from the child classes.
 * This class will not contain any public business method
 *
 * @author Muhammad Imran Ansari
 * @date 27-August-2019
 */
public abstract class AbstractDynamicDAOImplForInsert extends AbstractDynamicDAOImplForJoiningRead implements IDynamicDAOImpl {
    private static DSPDMLogger logger = new DSPDMLogger(AbstractDynamicDAOImplForInsert.class);

    protected AbstractDynamicDAOImplForInsert(BusinessObjectDTO businessObjectDTO, ExecutionContext executionContext) {
        super(businessObjectDTO, executionContext);
    }

    /* ******************************************************** */
    /* ****************** Public/Protected Business Methods ********************* */
    /* ******************************************************** */

    protected SaveResultDTO insertImpl(List<DynamicDTO> businessObjectsToSave, boolean hasSQLExpression, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        PreparedStatement pstmt = null;
        String sqlForInsert = null;
        String finalSQL = null;
        long startTime = 0;
        try {
            if (businessObjectsToSave.size() > DSPDMConstants.SQL.MAX_BATCH_INSERT_SIZE) {
                for (int start = 0; start < businessObjectsToSave.size(); ) {
                    int max = start + DSPDMConstants.SQL.MAX_BATCH_INSERT_SIZE;
                    if (max > businessObjectsToSave.size()) {
                        max = businessObjectsToSave.size();
                    }
                    // call recursively
                    saveResultDTO.addResult(insertImpl(businessObjectsToSave.subList(start, max), hasSQLExpression, connection, executionContext));
                    start = start + DSPDMConstants.SQL.MAX_BATCH_INSERT_SIZE;
                }
                // all records inserted
                return saveResultDTO;
            }
            logger.info("Insert : {}", getType());

            String type = this.getBusinessObjectDTO().getBoName();
            if(type == null){
                type = getType();// this will help for inserting metadata
            }
            Set<String> physicalAttributeNames = getPhysicalAttributeNames();
            Map<String, BusinessObjectAttributeDTO> physicalColumnNamesMetadataMap = getPhysicalColumnNamesMetadataMap();

            sqlForInsert = getSQLForInsert(physicalAttributeNames, hasSQLExpression, physicalColumnNamesMetadataMap, businessObjectsToSave.get(0), executionContext);
            pstmt = prepareStatementForInsert(sqlForInsert, type, executionContext, connection);
            // Generate primary key from sequence
            String sequenceName = this.getBusinessObjectDTO().getSequenceName();
            Integer[] nextFromSequence = getNextFromSequenceImpl(type, sequenceName, businessObjectsToSave.size(), connection, executionContext);
            Timestamp timestamp = DateTimeUtils.getCurrentTimestampUTC();
            String user = executionContext.getExecutorName();
            int sequenceIndex = 0;
            List<String> boNames = new ArrayList<>();
            for (DynamicDTO dynamicDTO : businessObjectsToSave) {

                // Fill default values such as creation date etc before setting values to sql statement
                fillDefaultsForInsert(dynamicDTO, timestamp, user, executionContext);
                // set prepared statement parameters
                bindSQLParametersForInsert(pstmt, physicalAttributeNames, hasSQLExpression, physicalColumnNamesMetadataMap, dynamicDTO, new DynamicPK(getType(), nextFromSequence[sequenceIndex]), executionContext);
                // Add row to the batch.
                pstmt.addBatch();
                // LOG SQL Statement
                if (!(executionContext.isIndexAll())) {
                    finalSQL = logSQLForInsert(sqlForInsert, physicalAttributeNames, hasSQLExpression, physicalColumnNamesMetadataMap, dynamicDTO, executionContext);
                }
                sequenceIndex++;
                // set inserted flag
                dynamicDTO.setInserted(true);
                boNames.add(dynamicDTO.getType());
                // Checking if the history is to be logged or not
                if (isChangeHistoryTrackEnabledForBoName(dynamicDTO.getType(), executionContext)) {
                    List<String> primaryKeyColumnNames = getPrimaryKeyColumnNames();
                    if ((CollectionUtils.hasValue(primaryKeyColumnNames)) && (primaryKeyColumnNames.size() == 1)) {
                        String primaryKeyColumnName = primaryKeyColumnNames.get(0);
                        // incrementing sequence number for logging
                        executionContext.incrementOperationSequenceNumber();
                        // add history dynamic dto to the execution context for logging
                        addChangeHistoryDTOToExecutionContext(dynamicDTO.getType(),
                                (Integer) dynamicDTO.getId().getPK()[0],
                                primaryKeyColumnName,
                                null, // since its an insert, we don't have old value so sending oldValue as null.
                                dynamicDTO.get(primaryKeyColumnName),
                                timestamp,
                                DSPDMConstants.BusinessObjectOperations.SAVE_OPR.getId(),
                                executionContext);
                    }
                }
            }
            startTime = System.currentTimeMillis();
            // execute batch
            int[] effectedRowsCounts = pstmt.executeBatch();
            saveResultDTO.setInsertExecuted(true);
            if (!(executionContext.isIndexAll())) {
                logSQLTimeTaken(finalSQL, businessObjectsToSave.size(), System.currentTimeMillis() - startTime, executionContext);
            }
            if (boNames.size() == effectedRowsCounts.length) {
                for (int index = 0; index < effectedRowsCounts.length; index++) {
                    String boName = boNames.get(index);
                    int effectedCount = effectedRowsCounts[index];
                    if (effectedCount > 0) {
                        saveResultDTO.addInsertedRecordsCount(boName, effectedCount);
                    } else if (effectedCount == Statement.EXECUTE_FAILED) {
                        throw new DSPDMException("Insert statement executed but effected row count is not greater than zero, insert count : {}", executionContext.getExecutorLocale(), effectedCount);
                    }
                }
            } else {
                throw new DSPDMException("Insert statement executed but effected row count is not equal to number of records to be inserted. Therefore transaction rollbacked.", executionContext.getExecutorLocale());
            }
            logger.info("{} records inserted successfully.", saveResultDTO.getInsertedRecordsCount());
        } catch (Exception e) {
            if (finalSQL != null) {
                logSQLTimeTaken(finalSQL, businessObjectsToSave.size(), System.currentTimeMillis() - startTime, executionContext);
            } else {
                // add to sql stats
                logSQLTimeTaken(sqlForInsert, 0, 0, executionContext);
                // log in error logs console
                logSQL(sqlForInsert, executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeStatement(pstmt, executionContext);
        }
        return saveResultDTO;
    }

    /**
     * number of records to be inserted in a single request. It will return the same number of sequence values for the current dao
     *
     * @param count
     * @param connection
     * @param executionContext
     * @return
     */
    protected Integer[] getNextFromSequenceImpl(String boName, String sequenceName, int count, Connection connection,
                                                ExecutionContext executionContext) {
        if ((isServiceDBDAO(boName)) || (isPostgresDialect(boName))) {
            return getNextFromSequenceUsingSQLStatement(sequenceName, count, connection, executionContext);
        } else {
            return getNextFromSequenceUsingProcedureCall(sequenceName, count, boName, connection, executionContext);
        }
    }

    /**
     * This method will get the next sequence numbers in bulk by calling a stored procedure
     *
     * @param count number of records to be inserted
     * @param connection
     * @param executionContext
     * @return sequence number CSV
     * @author Muhammad Imran Ansari
     * @since 23-Jul-2020
     */
    private Integer[] getNextFromSequenceUsingProcedureCall(String sequenceName, int count, String type,  Connection connection, ExecutionContext executionContext) {
        Integer[] sequenceNumbers = new Integer[count];
        CallableStatement callableStatement = null;
        ResultSet resultSet = null;
        String procCallForSequenceNumber = null;
        String finalSQL = null;
        long startTime = 0;
        try {
            if (isMSSQLServerDialect(type)) {
                procCallForSequenceNumber = "{call sp_get_nextval_csv(?, ?)}";
            } else {
                throw new DSPDMException("Unknown SQL dialect", executionContext.getExecutorLocale());
            }
            callableStatement = prepareCall(procCallForSequenceNumber, 1, type, executionContext, connection);
            callableStatement.setString(1, sequenceName);
            callableStatement.setInt(2, count);
            finalSQL = logSQLForSequence(procCallForSequenceNumber, sequenceName, count, executionContext);
            // collect sql script only if required
            if(executionContext.getCollectSQLScriptOptions().isReadEnabled()) {
                executionContext.addSqlScript(finalSQL);
            }
            startTime = System.currentTimeMillis();
            resultSet = callableStatement.executeQuery();
            logSQLTimeTaken(finalSQL, 1, System.currentTimeMillis() - startTime, executionContext);
            if (resultSet.next()) {
                String seqNumCSV = resultSet.getString(1);
                if (StringUtils.hasValue(seqNumCSV)) {
                    String[] array = seqNumCSV.split(",");
                    for (int index = 0; index < array.length; index++) {
                        sequenceNumbers[index] = Integer.valueOf(array[index].trim());
                    }
                }
            }
        } catch (Exception e) {
            if (finalSQL != null) {
                logSQLTimeTaken(finalSQL, 1, System.currentTimeMillis() - startTime, executionContext);
            } else {
                // add to sql stats
                logSQLTimeTaken(procCallForSequenceNumber, 0, 0, executionContext);
                // log in error logs console
                logSQL(procCallForSequenceNumber, executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeResultSet(resultSet, executionContext);
            closeStatement(callableStatement, executionContext);
        }
        return sequenceNumbers;
    }

    private Integer[] getNextFromSequenceUsingSQLStatement(String sequenceName, int count, Connection connection, ExecutionContext executionContext) {
        Integer[] sequenceNumbers = new Integer[count];
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        String sqlForSequenceNumber = null;
        String finalSQL = null;
        long startTime = 0;
        try {
            if (isPostgresDialect(getType())) {
                sqlForSequenceNumber = "Select (FUNC_GET_NEXTVAL_CSV(?, ?)) as x";
            } else {
                throw new DSPDMException("Unknown SQL dialect", executionContext.getExecutorLocale());
            }
            pstmt = prepareStatement(sqlForSequenceNumber, 1, getType(), executionContext, connection);
            pstmt.setString(1, sequenceName);
            pstmt.setInt(2, count);
            finalSQL = logSQLForSequence(sqlForSequenceNumber, sequenceName, count, executionContext);
            startTime = System.currentTimeMillis();
            resultSet = pstmt.executeQuery();
            logSQLTimeTaken(finalSQL, 1, System.currentTimeMillis() - startTime, executionContext);
            if (resultSet.next()) {
                String seqNumCSV = resultSet.getString(1);
                if (StringUtils.hasValue(seqNumCSV)) {
                    String[] array = seqNumCSV.split(",");
                    for (int index = 0; index < array.length; index++) {
                        sequenceNumbers[index] = Integer.valueOf(array[index].trim());
                    }
                }
            }
        } catch (Exception e) {
            if (finalSQL != null) {
                logSQLTimeTaken(finalSQL, 1, System.currentTimeMillis() - startTime, executionContext);
            } else {
                // add to sql stats
                logSQLTimeTaken(sqlForSequenceNumber, 0, 0, executionContext);
                // log in error logs console
                logSQL(sqlForSequenceNumber, executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeResultSet(resultSet, executionContext);
            closeStatement(pstmt, executionContext);
        }
        return sequenceNumbers;
    }

    /* ******************************************************** */
    /* ****************** Private Utility Methods ********************* */
    /* ******************************************************** */

    private String getInsertIntoTableClause(Set<String> physicalColumnNames, ExecutionContext executionContext) {
        String commaSeparated = CollectionUtils.getCommaSeparated(physicalColumnNames);
        return "INSERT INTO " + getDatabaseTableName() + "(" + commaSeparated + ")";
    }

    private String getInsertIntoValuesClause(Set<String> physicalColumnNames, ExecutionContext executionContext) {
        Object[] markers = new Character[physicalColumnNames.size()];
        for (int i = 0; i < physicalColumnNames.size(); i++) {
            markers[i] = DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER;
        }
        String commaSeparated = CollectionUtils.getCommaSeparated(markers);
        return " VALUES (" + commaSeparated + ")";
    }

    private String getInsertIntoValuesClauseWithSQLExpression(Set<String> physicalColumnNames, Map<String, BusinessObjectAttributeDTO> physicalColumnNamesMetadataMap, DynamicDTO dynamicDTO, ExecutionContext executionContext) {
        List<String> markers = new ArrayList<>(physicalColumnNames.size());
        for (String physicalColumnName : physicalColumnNames) {
            BusinessObjectAttributeDTO businessObjectAttributeDTO = physicalColumnNamesMetadataMap.get(physicalColumnName);
            Object value = dynamicDTO.get(businessObjectAttributeDTO.getBoAttrName());
            if (value instanceof SQLExpression) {
                SQLExpression expression = (SQLExpression) value;
                if (expression.getParamAddedCount() > 0) {
                    markers.add(expression.getExpression());
                }
            } else {
                markers.add(String.valueOf(DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER));
            }
        }
        String commaSeparated = CollectionUtils.getCommaSeparated(markers);
        return " VALUES (" + commaSeparated + ")";
    }

    private String getSQLForInsert(Set<String> physicalColumnNames, boolean hasSQLExpression, Map<String, BusinessObjectAttributeDTO> physicalColumnNamesMetadataMap, DynamicDTO dynamicDTO, ExecutionContext executionContext) {
        String sql = DSPDMConstants.EMPTY_STRING;
        // INSERT INTO CLAUSE        
        sql = sql + getInsertIntoTableClause(physicalColumnNames, executionContext);
        // VALUES CLAUSE
        if (hasSQLExpression) {
            sql += getInsertIntoValuesClauseWithSQLExpression(physicalColumnNames, physicalColumnNamesMetadataMap, dynamicDTO, executionContext);
        } else {
            sql += getInsertIntoValuesClause(physicalColumnNames, executionContext);
        }
        return sql;
    }

    private void fillDefaultsForInsert(DynamicDTO dynamicDTO, Timestamp timestamp, String user, ExecutionContext executionContext) {

        BusinessObjectAttributeDTO businessObjectAttributeDTO = this.getBoAttributeNamesMetadataMap().get(DSPDMConstants.BoAttrName.ROW_CREATED_DATE);
        if (businessObjectAttributeDTO != null) {
            if (businessObjectAttributeDTO.getSqlDataType() == Types.TIMESTAMP) {
                dynamicDTO.put(DSPDMConstants.BoAttrName.ROW_CREATED_DATE, timestamp);
            } else {
                dynamicDTO.put(DSPDMConstants.BoAttrName.ROW_CREATED_DATE, OffsetDateTime.ofInstant(timestamp.toInstant(), ZoneOffset.UTC));
            }
        }
        businessObjectAttributeDTO = this.getBoAttributeNamesMetadataMap().get(DSPDMConstants.BoAttrName.ROW_CHANGED_DATE);
        if (businessObjectAttributeDTO != null) {
            if (businessObjectAttributeDTO.getSqlDataType() == Types.TIMESTAMP) {
                dynamicDTO.put(DSPDMConstants.BoAttrName.ROW_CHANGED_DATE, timestamp);
            } else {
                dynamicDTO.put(DSPDMConstants.BoAttrName.ROW_CHANGED_DATE, OffsetDateTime.ofInstant(timestamp.toInstant(), ZoneOffset.UTC));
            }
        }
        dynamicDTO.put(DSPDMConstants.BoAttrName.ROW_CREATED_BY, user);
        dynamicDTO.put(DSPDMConstants.BoAttrName.ROW_CHANGED_BY, user);
        // Fill default values from metadata for the column which are null or not present in the dto
        fillDefaultsForInsertFromMetadata(dynamicDTO, executionContext);
    }

    private void fillDefaultsForInsertFromMetadata(DynamicDTO dynamicDTO, ExecutionContext executionContext) {
        if (ConfigProperties.getInstance().fill_default_values_from_metadata.getBooleanValue()) {
            // Fill default values from metadata for the column which are null or not present in the dto
            Map<String, Object> defaultValuesMap = getDefaultValuesMap();
            String boAttrName = null;
            Object value = null;
            for (Map.Entry<String, Object> entry : defaultValuesMap.entrySet()) {
                boAttrName = entry.getKey();
                value = dynamicDTO.get(boAttrName);
                if (value == null) {
                    dynamicDTO.put(boAttrName, entry.getValue());
                } else if (value instanceof java.lang.String) {
                    if (((String) value).trim().length() == 0) {
                        dynamicDTO.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
    }

    private int bindSQLParametersForInsert(PreparedStatement pstmt, Set<String> physicalAttributeNames, boolean hasSQLExpression,
                                           Map<String, BusinessObjectAttributeDTO> physicalColumnNamesMetadataMap,
                                           DynamicDTO dynamicDTO, DynamicPK dynamicPK, ExecutionContext executionContext) {
        // create an indexer to support composite primary keys
        int compositePrimaryKeyIndex = 0;
        // sql parameters index starts from 1
        int paramIndex = 1;
        try {
            BusinessObjectAttributeDTO boAttrDTO = null;
            for (String physicalAttributeName : physicalAttributeNames) {
                boAttrDTO = physicalColumnNamesMetadataMap.get(physicalAttributeName);
                Object value = null;
                if (boAttrDTO.getPrimaryKey()) {
                    Object[] pkValues = dynamicPK.getPK();
                    if ((pkValues == null) || (compositePrimaryKeyIndex >= pkValues.length)) {
                        throw new DSPDMException("Given primary key values are fewer than primary key columns defined in metadata for bo name : '{}' required : '{}' provided : '{}'", executionContext.getExecutorLocale(), boAttrDTO.getBoName(), compositePrimaryKeyIndex, (pkValues == null ? 0 : pkValues.length));
                    } else {
                        value = pkValues[compositePrimaryKeyIndex];
                        // Set primary key in business object to be used in read back
                        dynamicDTO.put(boAttrDTO.getBoAttrName(), value);
                        compositePrimaryKeyIndex++;
                    }
                } else {
                    value = dynamicDTO.get(boAttrDTO.getBoAttrName());
                }
                if (value == null) {
                    pstmt.setNull(paramIndex, boAttrDTO.getSqlDataType());
                    // increment the parameter index
                    paramIndex++;
                } else {
                    // paramIndex will be incremented inside the function being called to set value
                    paramIndex = setSQLParameterValueForIndex(pstmt, paramIndex, value, boAttrDTO.getAttributeDatatype(), boAttrDTO.getSqlDataType(), hasSQLExpression, (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME), executionContext);
                }
            }
        } catch (SQLException e) {
            DSPDMException.throwException(e, executionContext);
        }
        return paramIndex;
    }

    protected int setSQLParameterValueForIndex(final PreparedStatement pstmt, int paramIndex, final Object value, final String attributeDataType, final int sqlDataType, final boolean hasSQLExpression,String type, ExecutionContext executionContext) {
        try {
            boolean isMSSQLServerDialect = isMSSQLServerDialect(type);
            if (hasSQLExpression && (value instanceof SQLExpression)) {
                SQLExpression expression = (SQLExpression) value;
                if (expression.getParamAddedCount() > 0) {
                    for (int i = 0; i < expression.getParamAddedCount(); i++) {
                        if (DSPDMConstants.DataTypes.JSONB.getAttributeDataType(isMSSQLServerDialect).equalsIgnoreCase(attributeDataType)) {
                            PGobject pgObject = new PGobject();
                            pgObject.setType(DSPDMConstants.DataTypes.JSONB.getAttributeDataType(isMSSQLServerDialect));
                            String jsonbValue = (String) expression.getValues()[i];
                            if (jsonbValue.length() > ConfigProperties.getInstance().max_jsonb_length_to_save.getIntegerValue()) {
                                throw new DSPDMException("Save operation failed. JSONB size exceeding max allowed limit '{}'", executionContext.getExecutorLocale(), ConfigProperties.getInstance().max_jsonb_length_to_save.getIntegerValue());
                            }
                            pgObject.setValue(jsonbValue);
                            pstmt.setObject(paramIndex, pgObject, expression.getValuesSqlDataType()[i]);
                        } else if ((DSPDMConstants.DataTypes.BYTEA.getAttributeDataType(isMSSQLServerDialect).equalsIgnoreCase(attributeDataType))
                                || (DSPDMConstants.DataTypes.BINARY.getAttributeDataType(isMSSQLServerDialect).equalsIgnoreCase(attributeDataType))
                                || (DSPDMConstants.DataTypes.IMAGE.getAttributeDataType(isMSSQLServerDialect).equalsIgnoreCase(attributeDataType))) {
                            byte[] bytes = (byte[]) expression.getValues()[i];
                            if (bytes.length > ConfigProperties.getInstance().max_bytes_to_save.getIntegerValue()) {
                                throw new DSPDMException("Save operation failed. Binary data size exceeding max allowed limit '{}'", executionContext.getExecutorLocale(), ConfigProperties.getInstance().max_bytes_to_save.getIntegerValue());
                            }
                            pstmt.setBytes(paramIndex, bytes);
                        } else {
                            pstmt.setObject(paramIndex, expression.getValues()[i], expression.getValuesSqlDataType()[i]);
                        }
                        // increment the parameter index
                        paramIndex++;
                    }
                }
            } else {
                if (DSPDMConstants.DataTypes.JSONB.getAttributeDataType(isMSSQLServerDialect).equalsIgnoreCase(attributeDataType)) {
                    PGobject pgObject = new PGobject();
                    pgObject.setType(DSPDMConstants.DataTypes.JSONB.getAttributeDataType(isMSSQLServerDialect));
                    String jsonbValue = (String) value;
                    if (jsonbValue.length() > ConfigProperties.getInstance().max_jsonb_length_to_save.getIntegerValue()) {
                        throw new DSPDMException("Save operation failed. JSONB size exceeding max allowed limit '{}'", executionContext.getExecutorLocale(), ConfigProperties.getInstance().max_jsonb_length_to_save.getIntegerValue());
                    }
                    pgObject.setValue(jsonbValue);
                    pstmt.setObject(paramIndex, pgObject, sqlDataType);
                } else if ((DSPDMConstants.DataTypes.BYTEA.getAttributeDataType(isMSSQLServerDialect).equalsIgnoreCase(attributeDataType))
                        || (DSPDMConstants.DataTypes.BINARY.getAttributeDataType(isMSSQLServerDialect).equalsIgnoreCase(attributeDataType))
                        || (DSPDMConstants.DataTypes.IMAGE.getAttributeDataType(isMSSQLServerDialect).equalsIgnoreCase(attributeDataType))) {
                    byte[] bytes = (byte[]) value;
                    if (bytes.length > ConfigProperties.getInstance().max_bytes_to_save.getIntegerValue()) {
                        throw new DSPDMException("Save operation failed. Binary data size exceeding max allowed limit '{}'", executionContext.getExecutorLocale(), ConfigProperties.getInstance().max_bytes_to_save.getIntegerValue());
                    }
                    pstmt.setBytes(paramIndex, bytes);
                } else {
                    pstmt.setObject(paramIndex, value, sqlDataType);
                }
                // increment the parameter index
                paramIndex++;
            }
        } catch (SQLException e) {
            throw new DSPDMException("Unable to set prepared statement parameter value '{}' with data type '{}' at parameter index '{}'", e, executionContext.getExecutorLocale(), value, attributeDataType, paramIndex);
        }
        return paramIndex;
    }

    private String logSQLForInsert(String sql, Set<String> physicalAttributeNames, boolean hasSQLExpression, Map<String, BusinessObjectAttributeDTO> physicalColumnNamesMetadataMap, DynamicDTO businessObject, ExecutionContext executionContext) {
        String finalSQL = null;
        if ((logger.isInfoEnabled()) || (executionContext.isCollectSQLStats()) || (executionContext.getCollectSQLScriptOptions().isInsertEnabled())) {
            BusinessObjectAttributeDTO businessObjectAttributeDTO = null;
            Object value = null;
            for (String physicalAttributeName : physicalAttributeNames) {
                businessObjectAttributeDTO = physicalColumnNamesMetadataMap.get(physicalAttributeName);
                value = businessObject.get(businessObjectAttributeDTO.getBoAttrName());
                if (hasSQLExpression && (value instanceof SQLExpression)) {
                    SQLExpression expression = (SQLExpression) value;
                    if (expression.getParamAddedCount() > 0) {
                        for (int i = 0; i < expression.getParamAddedCount(); i++) {
                            sql = sql.replaceFirst("\\?", Matcher.quoteReplacement(getSQLMarkerReplacementString(expression.getValues()[i],this, executionContext)));
                        }
                    }
                } else {
                    sql = sql.replaceFirst("\\?", Matcher.quoteReplacement(getSQLMarkerReplacementString(value,this, executionContext)));
                }
            }
            finalSQL = sql;
            logSQL(finalSQL, executionContext);
            // collect sql script only if required
            if(executionContext.getCollectSQLScriptOptions().isInsertEnabled()) {
                executionContext.addSqlScript(finalSQL);
            }
        }
        return finalSQL;
    }

    private String logSQLForSequence(String sql, String sequenceName, int count, ExecutionContext executionContext) {
        String finalSQL = null;
        if ((logger.isInfoEnabled()) || (executionContext.isCollectSQLStats())
                || (executionContext.getCollectSQLScriptOptions().isReadEnabled())
                || (executionContext.getCollectSQLScriptOptions().isInsertEnabled())) {
            sql = sql.replaceFirst("\\?", Matcher.quoteReplacement(getSQLMarkerReplacementString(sequenceName,this, executionContext)));
            sql = sql.replaceFirst("\\?", Matcher.quoteReplacement(getSQLMarkerReplacementString(count, this,executionContext)));
            finalSQL = sql;
            logSQL(finalSQL, executionContext);
            // collect sql script only if required
            if ((executionContext.getCollectSQLScriptOptions().isReadEnabled()) || (executionContext.getCollectSQLScriptOptions().isInsertEnabled())) {
                executionContext.addSqlScript(finalSQL);
            }
        }
        return finalSQL;
    }
}
