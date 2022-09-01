package com.lgc.dspdm.core.dao.dynamic.businessobject.impl;

import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.criteria.SQLExpression;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectAttributeDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.DateTimeUtils;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAOImpl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

public abstract class AbstractDynamicDAOImplForDelete extends AbstractDynamicDAOImplForUpdate implements IDynamicDAOImpl {
    private static DSPDMLogger logger = new DSPDMLogger(AbstractDynamicDAOImplForDelete.class);

    protected AbstractDynamicDAOImplForDelete(BusinessObjectDTO businessObjectDTO, ExecutionContext executionContext) {
        super(businessObjectDTO, executionContext);
    }

    /* ******************************************************** */
    /* ****************** Public/Protected Business Methods ********************* */
    /* ******************************************************** */

    protected SaveResultDTO deleteImpl(List<DynamicDTO> businessObjectsToDelete, boolean hasSQLExpression, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        PreparedStatement pstmt = null;
        String sqlForDelete = null;
        String finalSQL = null;
        long startTime = 0;
        try {
            if (businessObjectsToDelete.size() > DSPDMConstants.SQL.MAX_BATCH_DELETE_SIZE) {
                for (int start = 0; start < businessObjectsToDelete.size(); ) {
                    int max = start + DSPDMConstants.SQL.MAX_BATCH_DELETE_SIZE;
                    if (max > businessObjectsToDelete.size()) {
                        max = businessObjectsToDelete.size();
                    }
                    // call recursively
                    saveResultDTO.addResult(deleteImpl(businessObjectsToDelete.subList(start, max), hasSQLExpression, connection, executionContext));
                    start = start + DSPDMConstants.SQL.MAX_BATCH_DELETE_SIZE;
                }
                // all records deleted
                return saveResultDTO;
            }
            logger.info("delete : {}", getType());


            List<BusinessObjectAttributeDTO> primaryKeyColumns = getPrimaryKeyColumns();
            Map<String, BusinessObjectAttributeDTO> boAttributeNamesMetadataMap = getBoAttributeNamesMetadataMap();
            sqlForDelete = getSQLForDelete(primaryKeyColumns, executionContext);
            pstmt = prepareStatementForDelete(sqlForDelete, getType(), executionContext, connection);

            Timestamp timestamp = DateTimeUtils.getCurrentTimestampUTC();
            String user = executionContext.getExecutorName();

            List<String> boNames=new ArrayList<>();
            for (DynamicDTO dynamicDTO : businessObjectsToDelete) {
                dynamicDTO.setPrimaryKeyColumnNames(this.getPrimaryKeyColumnNames());
                // Fill default values such as creation date etc before setting values to sql statement
                fillDefaultsForDelete(dynamicDTO, timestamp, user, executionContext);
                // set prepared statement parameters
                bindSQLParametersForDelete(pstmt, dynamicDTO, hasSQLExpression, primaryKeyColumns,boAttributeNamesMetadataMap, timestamp, executionContext);
                // Add row to the batch.
                pstmt.addBatch();
                // LOG SQL Statement
                finalSQL = logSQLForDelete(sqlForDelete, dynamicDTO, hasSQLExpression, primaryKeyColumns, executionContext);
                // set deleted flag
                dynamicDTO.setDeleted(true);
                boNames.add(dynamicDTO.getType());
            }
            startTime = System.currentTimeMillis();
            // execute batch
            int[] effectedRowsCounts = pstmt.executeBatch();
            logSQLTimeTaken(finalSQL, businessObjectsToDelete.size(), System.currentTimeMillis() - startTime, executionContext);
			if (boNames.size() == effectedRowsCounts.length) {
				for (int index = 0; index < effectedRowsCounts.length; index++) {
					String boName = boNames.get(index);
					int effectedCount = effectedRowsCounts[index];
					if (effectedCount > 0) {
						saveResultDTO.addDeletedRecordsCount(boName,effectedCount);
					} else {
						throw new DSPDMException("Delete statement executed but effected row count is not greater than zero, delete count : {}", executionContext.getExecutorLocale(), effectedCount);
					}
				}
			}else {
            	throw new DSPDMException("Delete statement executed but effected row count is not equal to the records to be deleted. Therefore transaction is rollbacked.", executionContext.getExecutorLocale());
            }            
            saveResultDTO.setDeleteExecuted(true);
            logger.info("{} records deleted successfully.", saveResultDTO.getDeletedRecordsCountByBoName());
        } catch (Exception e) {
            if (finalSQL != null) {
                logSQLTimeTaken(finalSQL, businessObjectsToDelete.size(), System.currentTimeMillis() - startTime, executionContext);
            }  else {
                // add to sql stats
                logSQLTimeTaken(sqlForDelete, 0, 0, executionContext);
                // log in error logs console
                logSQL(sqlForDelete, executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeStatement(pstmt, executionContext);
        }
        return saveResultDTO;
    }

    protected SaveResultDTO deleteAllImpl(Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        PreparedStatement pstmt = null;
        String sqlForDelete = null;
        long startTime = 0;
        try {
            if (!(getType().equals(DSPDMConstants.BoName.BO_SEARCH))) {
                throw new DSPDMException("Direct deletion of all the records is allowed only for business object '{}'.",
                        executionContext.getExecutorLocale(), DSPDMConstants.BoName.BO_SEARCH);
            } else {
                logger.info("Going to delete all the business objects of type '{}'.", DSPDMConstants.BoName.BO_SEARCH);
            }
            sqlForDelete = "DELETE FROM " + getBusinessObjectDTO().getEntityName();
            pstmt = prepareStatementForDelete(sqlForDelete, getBusinessObjectDTO().getBoName(), executionContext, connection);
            startTime = System.currentTimeMillis();
            int effectedRowsCounts = pstmt.executeUpdate();
            logSQLTimeTaken(sqlForDelete, effectedRowsCounts, System.currentTimeMillis() - startTime, executionContext);
            saveResultDTO.addDeletedRecordsCount(getBusinessObjectDTO().getBoName(),effectedRowsCounts);
            saveResultDTO.setDeleteExecuted(true);
            logger.info("{} records deleted successfully.", effectedRowsCounts);
        } catch (Exception e) {
            if (sqlForDelete != null) {
                logSQLTimeTaken(sqlForDelete, 0, System.currentTimeMillis() - startTime, executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeStatement(pstmt, executionContext);
        }
        return saveResultDTO;
    }
    
    /* ******************************************************** */
    /* ********************** HARD DELETE ********************* */
    /* ******************************************************** */

    private String getSQLForDelete(List<BusinessObjectAttributeDTO> primaryKeyColumns, ExecutionContext executionContext) {
        List<String> whereClauses = new ArrayList<>(primaryKeyColumns.size());
        for (BusinessObjectAttributeDTO primaryKeyDTO : primaryKeyColumns) {
            whereClauses.add(primaryKeyDTO.getAttributeName() + " = " + DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER);
        }
        String deleteSQL = "DELETE FROM " + getDatabaseTableName();
        if (CollectionUtils.hasValue(whereClauses)) {
            deleteSQL = deleteSQL + " WHERE " + CollectionUtils.joinWithAnd(whereClauses);
        }
        return deleteSQL;
    }

    private void fillDefaultsForDelete(DynamicDTO dynamicDTO, Timestamp timestamp, String user, ExecutionContext executionContext) {
        dynamicDTO.put(DSPDMConstants.BoAttrName.ROW_CHANGED_DATE, timestamp);
        dynamicDTO.put(DSPDMConstants.BoAttrName.ROW_CHANGED_BY, user);
    }

    private int bindSQLParametersForDelete(PreparedStatement pstmt, DynamicDTO dynamicDTO, boolean hasSQLExpression, List<BusinessObjectAttributeDTO> primaryKeyColumns,Map<String, BusinessObjectAttributeDTO> boAttributeNamesMetadataMap, Timestamp timestamp, ExecutionContext executionContext) {
        // create an indexer to support composite primary keys
        int compositePrimaryKeyIndex = 0;
        // sql parameters index starts from 1
        int paramIndex = 1;
        try {
            Object value = null;
            for (BusinessObjectAttributeDTO primaryKeyDTO : primaryKeyColumns) {
                value = dynamicDTO.get(primaryKeyDTO.getBoAttrName());
                if (value == null) {
                    pstmt.setNull(paramIndex, primaryKeyDTO.getSqlDataType());
                    // increment the parameter index
                    paramIndex++;
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
                        pstmt.setObject(paramIndex, value, primaryKeyDTO.getSqlDataType());
                        // increment the parameter index
                        paramIndex++;
                    }
                }
            }
        } catch (SQLException e) {
            DSPDMException.throwException(e, executionContext);
        }
        return paramIndex;
    }

    private String logSQLForDelete(String sql, DynamicDTO businessObject, boolean hasSQLExpression, List<BusinessObjectAttributeDTO> primaryKeyColumns, ExecutionContext executionContext) {
        String finalSQL = null;
        if ((logger.isInfoEnabled()) || (executionContext.isCollectSQLStats()) || (executionContext.getCollectSQLScriptOptions().isDeleteEnabled())) {
            Object value = null;
            for (BusinessObjectAttributeDTO primaryKeyDTO : primaryKeyColumns) {
                value = businessObject.get(primaryKeyDTO.getBoAttrName());
                if (hasSQLExpression && (value instanceof SQLExpression)) {
                    SQLExpression expression = (SQLExpression) value;
                    if (expression.getParamAddedCount() > 0) {
                        for (int i = 0; i < expression.getParamAddedCount(); i++) {
                            sql = sql.replaceFirst("\\?", Matcher.quoteReplacement(getSQLMarkerReplacementString(expression.getValues()[i], this, executionContext)));
                        }
                    }
                } else {
                    sql = sql.replaceFirst("\\?", Matcher.quoteReplacement(getSQLMarkerReplacementString(value,this, executionContext)));
                }
            }
            finalSQL = sql;
            logSQL(finalSQL, executionContext);
            // collect sql script only if required
            if(executionContext.getCollectSQLScriptOptions().isDeleteEnabled()) {
                executionContext.addSqlScript(finalSQL);
            }
        }
        return finalSQL;
    }
}
