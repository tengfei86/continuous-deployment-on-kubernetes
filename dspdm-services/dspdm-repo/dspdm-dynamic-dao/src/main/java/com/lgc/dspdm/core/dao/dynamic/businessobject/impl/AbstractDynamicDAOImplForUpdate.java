package com.lgc.dspdm.core.dao.dynamic.businessobject.impl;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicPK;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectAttributeDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.*;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAOImpl;

import java.sql.*;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

/**
 * Class to hold common code for update methods or base methods.
 * These methods will not be public and can only be invoked from the child classes.
 * This class will not contain any public business method
 *
 * @author Muhammad Imran Ansari
 * @date 27-August-2019
 */
public abstract class AbstractDynamicDAOImplForUpdate extends AbstractDynamicDAOImplForInsert implements IDynamicDAOImpl {
    private static DSPDMLogger logger = new DSPDMLogger(AbstractDynamicDAOImplForUpdate.class);

    protected AbstractDynamicDAOImplForUpdate(BusinessObjectDTO businessObjectDTO, ExecutionContext executionContext) {
        super(businessObjectDTO, executionContext);
    }

    /* ******************************************************** */
    /* ****************** Public/Protected Business Methods ********************* */
    /* ******************************************************** */

    /**
     * @param businessObjectsToUpdate
     * @param readBeforeUpdate        do a read and compare to check anything changed
     * @param doTinyUpdate            flag to issue a short update statement only for changed fields
     * @param connection
     * @param executionContext
     * @return
     */
    public SaveResultDTO updateImpl(List<DynamicDTO> businessObjectsToUpdate, boolean readBeforeUpdate, boolean doTinyUpdate, boolean hasSQLExpression, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        {
            List<DynamicDTO> finalBusinessObjectsToUpdate = businessObjectsToUpdate;
            // check if read copy before update is enabled
            Map<DynamicPK, DynamicDTO> boCopyMapFromRead = null;
            if (readBeforeUpdate) {
                logger.info("Going to read a copy of records from db before update because only changed records will be updated for BO : {}", getType());
                boCopyMapFromRead = readCopyAsMap(finalBusinessObjectsToUpdate, connection, executionContext);
                // compare and filter out the objects which are same and will not effect 
                finalBusinessObjectsToUpdate = filterBusinessObjectsForUpdate(finalBusinessObjectsToUpdate, boCopyMapFromRead, executionContext);
            }
            if (finalBusinessObjectsToUpdate.size() > 0) {
                // call underlying update api on filtered records
                if (doTinyUpdate) {
                    DynamicDTO copyDTOFromRead = null;
                    for (DynamicDTO dynamicDTO : finalBusinessObjectsToUpdate) {
                        copyDTOFromRead = (boCopyMapFromRead == null) ? null : boCopyMapFromRead.get(dynamicDTO.getId());
                        saveResultDTO.addUpdatedRecordsCount(dynamicDTO.getType(),updateOne(dynamicDTO, copyDTOFromRead, connection, executionContext));
                    }
                } else {
					updateListInBatch(saveResultDTO, finalBusinessObjectsToUpdate, hasSQLExpression, connection, executionContext);
                }
                saveResultDTO.setUpdateExecuted(true);
            }
            // Set ignored records count
            if (finalBusinessObjectsToUpdate.size() != businessObjectsToUpdate.size()) {
				for (DynamicDTO dynamicDTO : businessObjectsToUpdate) {
					if (!finalBusinessObjectsToUpdate.contains(dynamicDTO)) {
						saveResultDTO.addIgnoredRecordsCount(dynamicDTO.getType(), 1);
					}
				}
            }
        }
        return saveResultDTO;
    }

    /**
     * Deletes the provided list from the database. Strategy depends upon implementation hard delete or soft delete
     *
     * @param businessObjectsToDelete
     * @param connection
     * @param executionContext
     * @return
     * @author Muhammad Imran Ansari
     * @since 17-Oct-2019
     */
    public SaveResultDTO softDeleteImpl(List<DynamicDTO> businessObjectsToDelete, boolean hasSQLExpression, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        performSoftDeleteInBatch(saveResultDTO, businessObjectsToDelete, hasSQLExpression, connection, executionContext);
        // set delete operation executed to true
        saveResultDTO.setDeleteExecuted(true);
        // Set ignored records count
//        if (businessObjectsToDelete.size() != saveResultDTO.getDeletedRecordsCount()) {
//            saveResultDTO.addIgnoredRecordsCount(businessObjectsToDelete.size() - saveResultDTO.getDeletedRecordsCount());
//        }
        if (businessObjectsToDelete.size() != saveResultDTO.getDeletedRecordsCount()) {
        	for(DynamicDTO dynamicDTO:businessObjectsToDelete) {
        		String boName=dynamicDTO.getType();
        		if(saveResultDTO.getIgnoredRecordsCountByBoName().containsKey(boName)) {
        			saveResultDTO.addIgnoredRecordsCount(boName, -1);
        		}else {
        			saveResultDTO.addIgnoredRecordsCount(boName, saveResultDTO.getDeletedRecordsCountByBoName().get(boName));
        		}
        	}
        }
        return saveResultDTO;
    }

    /* ******************************************************** */
    /* ************** Private Utility Methods ***************** */
    /* ******************************************************** */


    private String getUpdateTableClause(ExecutionContext executionContext) {
        return "UPDATE " + getDatabaseTableName();
    }

    private void fillDefaultsForUpdate(DynamicDTO dynamicDTO, Timestamp timestamp, String user, ExecutionContext executionContext) {
        BusinessObjectAttributeDTO businessObjectAttributeDTO = this.getBoAttributeNamesMetadataMap().get(DSPDMConstants.BoAttrName.ROW_CHANGED_DATE);
        if (businessObjectAttributeDTO != null) {
            if (businessObjectAttributeDTO.getSqlDataType() == Types.TIMESTAMP) {
                dynamicDTO.put(DSPDMConstants.BoAttrName.ROW_CHANGED_DATE, timestamp);
            } else {
                dynamicDTO.put(DSPDMConstants.BoAttrName.ROW_CHANGED_DATE, OffsetDateTime.ofInstant(timestamp.toInstant(), ZoneOffset.UTC));
            }
        }
        dynamicDTO.put(DSPDMConstants.BoAttrName.ROW_CHANGED_BY, user);
    }

    private boolean isValueChangedForAttribute(String boAttrName, DynamicDTO dynamicDTO, DynamicDTO copyDTOFromReadToCompare) {
        boolean flag = true;
        if (copyDTOFromReadToCompare != null) {
            if (ObjectUtils.areTwoValuesSame(dynamicDTO.get(boAttrName), copyDTOFromReadToCompare.get(boAttrName))) {
                flag = false;
            }
        }
        return flag;
    }

    /* ******************************************************** */
    /* **************** Update whole object ******************* */
    /* ******** Update All the field of an object ************* */
    /* ******************************************************** */

    private void updateListInBatch(SaveResultDTO saveResultDTO, List<DynamicDTO> businessObjectsToUpdate, boolean hasSQLExpression, Connection connection, ExecutionContext executionContext) {        
        PreparedStatement pstmt = null;
        String sqlForUpdate = null;
        String finalSQL = null;
        long startTime = 0;
        try {
            logger.info("Update List Batch: {}", getType());
            Set<String> physicalAttributeNames = getPhysicalAttributeNames();
            Map<String, BusinessObjectAttributeDTO> physicalColumnNamesMetadataMap = getPhysicalColumnNamesMetadataMap();
            sqlForUpdate = getSQLForUpdate(physicalAttributeNames, physicalColumnNamesMetadataMap, executionContext);
            pstmt = prepareStatementForUpdate(sqlForUpdate,getType(), executionContext, connection);
            // Fill default values such as updation date etc before setting values to sql statement
            Timestamp timestamp = DateTimeUtils.getCurrentTimestampUTC();
            String user = executionContext.getExecutorName();
            
            List<String> boNames=new ArrayList<>();
            for (DynamicDTO dynamicDTO : businessObjectsToUpdate) {
                fillDefaultsForUpdate(dynamicDTO, timestamp, user, executionContext);
                // set prepared statement parameters
                bindSQLParametersForUpdate(pstmt, physicalAttributeNames, hasSQLExpression, physicalColumnNamesMetadataMap, dynamicDTO, executionContext);
                // Add row to the batch.
                pstmt.addBatch();
                // LOG SQL Statement
                finalSQL = logSQLForUpdate(sqlForUpdate, physicalAttributeNames, physicalColumnNamesMetadataMap, dynamicDTO, executionContext);
                dynamicDTO.setUpdated(true);
                boNames.add(dynamicDTO.getType());
            }
            startTime = System.currentTimeMillis();
            // execute batch
            int[] effectedRowsCounts = pstmt.executeBatch();
            logSQLTimeTaken(finalSQL, businessObjectsToUpdate.size(), System.currentTimeMillis() - startTime, executionContext);
			if (boNames.size() == effectedRowsCounts.length) {
				for (int index = 0; index < effectedRowsCounts.length; index++) {
					String boName = boNames.get(index);
					int effectedCount = effectedRowsCounts[index];
					if (effectedCount > 0) {
						saveResultDTO.addUpdatedRecordsCount(boName,effectedCount);
					} else if (effectedCount == Statement.EXECUTE_FAILED) {
						throw new DSPDMException("Update statement executed but effected row count is not greater than zero, update count : {}",executionContext.getExecutorLocale(), effectedCount);
					}
				}
			} else {
				throw new DSPDMException("Update statement executed but effected object size is not equal to the dynamicDTO",executionContext.getExecutorLocale());
			}
            logger.info("{} records updated successfully.", saveResultDTO.getUpdatedRecordsCount());
        } catch (Exception e) {
            if(finalSQL != null) {
                logSQLTimeTaken(finalSQL, businessObjectsToUpdate.size(), System.currentTimeMillis() - startTime, executionContext);
            } else {
                // add to sql stats
                logSQLTimeTaken(sqlForUpdate, 0, 0, executionContext);
                // log in error logs console
                logSQL(sqlForUpdate, executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeStatement(pstmt, executionContext);
        }
    }

    private String getSQLForUpdate(Set<String> physicalColumnNames, Map<String, BusinessObjectAttributeDTO> physicalColumnNamesMetadataMap, ExecutionContext executionContext) {
        String sql = DSPDMConstants.EMPTY_STRING;
        //Update Clause
        sql = sql + getUpdateTableClause(executionContext);
        //Set Value Clause
        sql += getSetClauseForUpdate(physicalColumnNames, physicalColumnNamesMetadataMap, executionContext);
        return sql;
    }

    private String getSetClauseForUpdate(Set<String> physicalColumnNames, Map<String, BusinessObjectAttributeDTO> physicalColumnNamesMetadataMap, ExecutionContext executionContext) {

        List<String> setClauses = new ArrayList<>(physicalColumnNames.size());
        List<String> whereClauses = new ArrayList<>();
        BusinessObjectAttributeDTO businessObjectAttributeDTO = null;
        for (String physicalColumnName : physicalColumnNames) {
            businessObjectAttributeDTO = physicalColumnNamesMetadataMap.get(physicalColumnName);
            if (businessObjectAttributeDTO != null) {
                if (businessObjectAttributeDTO.getPrimaryKey()) {
                    whereClauses.add(physicalColumnName + " = " + DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER);
                } else if ((!businessObjectAttributeDTO.getReadOnly()) || (businessObjectAttributeDTO.isUpdateAuditField())) {
                    setClauses.add(physicalColumnName + " = " + DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER);
                }
            }
        }
        String setClause = " SET " + CollectionUtils.getCommaSeparated(setClauses);
        if (CollectionUtils.hasValue(whereClauses)) {
            setClause = setClause + " WHERE " + CollectionUtils.joinWithAnd(whereClauses);
        }
        return setClause;
    }

    private int bindSQLParametersForUpdate(PreparedStatement pstmt, Set<String> physicalAttributeNames, boolean hasSQLExpression, Map<String, BusinessObjectAttributeDTO> physicalColumnNamesMetadataMap, DynamicDTO dynamicDTO, ExecutionContext executionContext) {
        int paramIndex = 1;
        try {
            BusinessObjectAttributeDTO boAttrDTO = null;
            List<BusinessObjectAttributeDTO> pkBOAttrDTOList = new ArrayList<>(3);
            for (String physicalAttributeName : physicalAttributeNames) {
                boAttrDTO = physicalColumnNamesMetadataMap.get(physicalAttributeName);
                if (boAttrDTO != null) {
                    if (boAttrDTO.getPrimaryKey()) {
                        pkBOAttrDTOList.add(boAttrDTO);
                    } else if ((!boAttrDTO.getReadOnly()) || (boAttrDTO.isUpdateAuditField())) {
                        Object value = null;
                        value = dynamicDTO.get(boAttrDTO.getBoAttrName());
                        if (value == null) {
                            pstmt.setNull(paramIndex, boAttrDTO.getSqlDataType());
                            // increment the parameter index
                            paramIndex++;
                        } else {
                            // paramIndex will be incremented inside the function being called to set value
                            paramIndex = setSQLParameterValueForIndex(pstmt, paramIndex, value, boAttrDTO.getAttributeDatatype(), boAttrDTO.getSqlDataType(), hasSQLExpression, this.getType(), executionContext);
                        }
                    }
                }
            }
            // setting primary key in where clause of update statement
            if (CollectionUtils.hasValue(pkBOAttrDTOList)) {
                for (BusinessObjectAttributeDTO pkBOAttrDTO : pkBOAttrDTOList) {
                    Object value = dynamicDTO.get(pkBOAttrDTO.getBoAttrName());
                    // paramIndex will be incremented inside the function being called to set value
                    paramIndex = setSQLParameterValueForIndex(pstmt, paramIndex, value, pkBOAttrDTO.getAttributeDatatype(), pkBOAttrDTO.getSqlDataType(), hasSQLExpression, this.getType(),executionContext);
                }
            }
        } catch (SQLException e) {
            DSPDMException.throwException(e, executionContext);
        }
        return paramIndex;
    }

    /**
     * Reads the dynamic dtos from database for the given list using primary keys and compare the records and returns only the changed records
     *
     * @param businessObjectsToUpdate
     * @return
     */
    private List<DynamicDTO> filterBusinessObjectsForUpdate(List<DynamicDTO> businessObjectsToUpdate, Map<DynamicPK, DynamicDTO> boCopyMapFromRead, ExecutionContext executionContext) {
        List<DynamicDTO> filteredBusinessObjectsToUpdate = new ArrayList<>(businessObjectsToUpdate.size());
        if (CollectionUtils.hasValue(boCopyMapFromRead)) {
            for (DynamicDTO dtoToUpdate : businessObjectsToUpdate) {
                DynamicDTO dtoFromRead = boCopyMapFromRead.get(dtoToUpdate.getId());
                if (dtoFromRead == null) {
                    throw new DSPDMException("Cannot perform update on a business object of type '{}'. No business object already exists with the id : '{}'", executionContext.getExecutorLocale(), getType(), dtoToUpdate.getId());
                    // filteredBusinessObjectsToUpdate.add(dtoToUpdate);
                } else if (!compareDynamicDTOAttributes(dtoToUpdate, dtoFromRead)) {
                    filteredBusinessObjectsToUpdate.add(dtoToUpdate);
                }
            }
        } else {
            if (businessObjectsToUpdate.size() == 1) {
                throw new DSPDMException("Cannot perform update on a business object of type '{}'. No business object already exists with the id : '{}'", executionContext.getExecutorLocale(), getType(), businessObjectsToUpdate.get(0).getId());
            } else {
                throw new DSPDMException("Cannot perform update on a business object of type '{}'. No business object already exists with the given id.", executionContext.getExecutorLocale(), getType());
            }
            // return the same list back because nothing filtered
            // filteredBusinessObjectsToUpdate = businessObjectsToUpdate;
        }
        return filteredBusinessObjectsToUpdate;
    }

    /**
     * compares the two business objects on the basis of metadata and returns true if both are similar/same
     * Field other than those registered in metadata are ignored
     *
     * @param dto1
     * @param dto2
     * @return
     */
    private boolean compareDynamicDTOAttributes(DynamicDTO dto1, DynamicDTO dto2) {
        boolean same = true;
        // get attribute names from metadata for this DAO
        Map<String, BusinessObjectAttributeDTO> boAttributeNamesMetadataMap = getBoAttributeNamesMetadataMap();
        BusinessObjectAttributeDTO businessObjectAttributeDTO = null;
        // Just iterate on the fields which exists in dto1 and do not iterate over all the metadata fields
        for (String key : dto1.keySet()) {
            businessObjectAttributeDTO = boAttributeNamesMetadataMap.get(key);
            // check if it is a metadata attribute then proceed otherwise ignore
            if ((businessObjectAttributeDTO != null) && (!(businessObjectAttributeDTO.getReadOnly()))) {
                // skip ROW_CHANGED_BY and ROW_CHANGED_DATE changes because these are system provided fields
                Object obj1 = dto1.get(key);
                Object obj2 = dto2.get(key);
                same = ObjectUtils.areTwoValuesSame(obj1, obj2);
                if (!same) {
                    break;
                }
            }
        }
        return same;
    }

    private String logSQLForUpdate(String sql, Set<String> physicalAttributeNames, Map<String, BusinessObjectAttributeDTO> physicalColumnNamesMetadataMap, DynamicDTO businessObject, ExecutionContext executionContext) {
        String finalSQL = null;
        if ((logger.isInfoEnabled()) || (executionContext.isCollectSQLStats()) || (executionContext.getCollectSQLScriptOptions().isUpdateEnabled())) {
            List<BusinessObjectAttributeDTO> pkBOAttrDTOList = new ArrayList<>(3);
            BusinessObjectAttributeDTO businessObjectAttributeDTO = null;
            Object value = null;
            for (String physicalAttributeName : physicalAttributeNames) {
                businessObjectAttributeDTO = physicalColumnNamesMetadataMap.get(physicalAttributeName);
                if (businessObjectAttributeDTO != null) {
                    if (businessObjectAttributeDTO.getPrimaryKey()) {
                        pkBOAttrDTOList.add(businessObjectAttributeDTO);
                    } else if ((!businessObjectAttributeDTO.getReadOnly()) || (businessObjectAttributeDTO.isUpdateAuditField())) {
                        value = businessObject.get(businessObjectAttributeDTO.getBoAttrName());
                        sql = sql.replaceFirst("\\?", Matcher.quoteReplacement(getSQLMarkerReplacementString(value,this, executionContext)));
                    }
                }
            }
            if (CollectionUtils.hasValue(pkBOAttrDTOList)) {
                for (BusinessObjectAttributeDTO pkBOAttrDTO : pkBOAttrDTOList) {
                    value = businessObject.get(pkBOAttrDTO.getBoAttrName());
                    sql = sql.replaceFirst("\\?", Matcher.quoteReplacement(getSQLMarkerReplacementString(value,this, executionContext)));
                }
            }
            finalSQL = sql;
            logSQL(finalSQL, executionContext);
            // collect sql script only if required
            if (executionContext.getCollectSQLScriptOptions().isUpdateEnabled()) {
                executionContext.addSqlScript(finalSQL);
            }
        }
        return finalSQL;
    }

    /* ******************************************************** */
    /* ********************** TINY UPDATE ********************* */
    /* Means update a few fields (not all the fields) of a business object */
    /* ******************************************************** */

    private int updateOne(DynamicDTO dynamicDTO, DynamicDTO copyDTOFromReadToCompare, Connection connection, ExecutionContext executionContext) {
        int updatedCount = 0;
        PreparedStatement pstmt = null;
        String sqlForUpdate = null;
        String finalSQL = null;
        long startTime = 0;
        try {
            logger.info("Update One : {}", getType());
            Map<String, BusinessObjectAttributeDTO> boAttributeNamesMetadataMap = getBoAttributeNamesMetadataMap();
            // Fill default values such as changed date etc before setting values to sql statement
            // fill defaults must be done before sql statement creation
            Timestamp timestamp = DateTimeUtils.getCurrentTimestampUTC();
            String user = executionContext.getExecutorName();
            fillDefaultsForUpdate(dynamicDTO, timestamp, user, executionContext);
            // create SQL statement for update changed fields only
            sqlForUpdate = getTinySQLForUpdate(dynamicDTO, copyDTOFromReadToCompare, boAttributeNamesMetadataMap, timestamp, executionContext);

            pstmt = prepareStatementForUpdate(sqlForUpdate, this.getType(), executionContext, connection);
            // set prepared statement parameters
            bindSQLParametersForTinyUpdate(pstmt, false, dynamicDTO, copyDTOFromReadToCompare, boAttributeNamesMetadataMap, executionContext);

            // LOG SQL Statement
            finalSQL = logSQLForTinyUpdate(sqlForUpdate, dynamicDTO, copyDTOFromReadToCompare, boAttributeNamesMetadataMap, executionContext);
            startTime = System.currentTimeMillis();
            // execute batch
            updatedCount = pstmt.executeUpdate();
            logSQLTimeTaken(finalSQL, 1,System.currentTimeMillis() - startTime, executionContext);
            dynamicDTO.setUpdated(true);
            logger.info("{} records updated successfully.", updatedCount);
        } catch (Exception e) {
            if(finalSQL != null) {
                logSQLTimeTaken(finalSQL, 1, System.currentTimeMillis() - startTime, executionContext);
            } else {
                // add to sql stats
                logSQLTimeTaken(sqlForUpdate, 0, 0, executionContext);
                // log in error logs console
                logSQL(sqlForUpdate, executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeStatement(pstmt, executionContext);
        }
        return updatedCount;
    }

    private String getTinySQLForUpdate(DynamicDTO dynamicDTO, DynamicDTO copyDTOFromReadToCompare, Map<String, BusinessObjectAttributeDTO> boAttributeNamesMetadataMap, Timestamp timestamp, ExecutionContext executionContext) {
        String sql = DSPDMConstants.EMPTY_STRING;
        //Update Clause
        sql = sql + getUpdateTableClause(executionContext);
        //Set Value Clause
        sql += getTinySetClauseForUpdate(dynamicDTO, copyDTOFromReadToCompare, boAttributeNamesMetadataMap, timestamp, executionContext);
        return sql;
    }

    private String getTinySetClauseForUpdate(DynamicDTO dynamicDTO, DynamicDTO copyDTOFromReadToCompare, Map<String, BusinessObjectAttributeDTO> boAttributeNamesMetadataMap, Timestamp timestamp, ExecutionContext executionContext) {

        List<String> setClauses = new ArrayList<>(boAttributeNamesMetadataMap.size());
        List<String> whereClauses = new ArrayList<>();
        BusinessObjectAttributeDTO businessObjectAttributeDTO = null;
        boolean first = true;
        for (Map.Entry<String, Object> entry : dynamicDTO.entrySet()) {
            businessObjectAttributeDTO = boAttributeNamesMetadataMap.get(entry.getKey());
            if (businessObjectAttributeDTO != null) {
                if (businessObjectAttributeDTO.getPrimaryKey()) {
                    whereClauses.add(businessObjectAttributeDTO.getAttributeName() + " = " + DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER);
                } else if ((!businessObjectAttributeDTO.getReadOnly()) || (businessObjectAttributeDTO.isUpdateAuditField())) {
                    if (isValueChangedForAttribute(entry.getKey(), dynamicDTO, copyDTOFromReadToCompare)) {
                        setClauses.add(businessObjectAttributeDTO.getAttributeName() + " = " + DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER);
                        if (isChangeHistoryTrackEnabledForBoName(dynamicDTO.getType(), executionContext)) {
                            if (!DSPDMConstants.BoAttrName.isAuditField(businessObjectAttributeDTO.getBoAttrName())) {
                                if(first) {
                                    first = false;
                                    // do not increment for each attribute change
                                    executionContext.incrementOperationSequenceNumber();
                                }
                                addChangeHistoryDTOToExecutionContext(dynamicDTO.getType(),
                                        (Integer) copyDTOFromReadToCompare.getId().getPK()[0],
                                        businessObjectAttributeDTO.getBoAttrName(),
                                        copyDTOFromReadToCompare.get(businessObjectAttributeDTO.getBoAttrName()),
                                        dynamicDTO.get(businessObjectAttributeDTO.getBoAttrName()),
                                        timestamp,
                                        DSPDMConstants.BusinessObjectOperations.UPDATE_OPR.getId(),
                                        executionContext);
                            }
                        }
                    }
                }
            }
        }
        String setClause = " SET " + CollectionUtils.getCommaSeparated(setClauses);
        if (CollectionUtils.hasValue(whereClauses)) {
            setClause = setClause + " WHERE " + CollectionUtils.joinWithAnd(whereClauses);
        }
        return setClause;
    }

    private int bindSQLParametersForTinyUpdate(PreparedStatement pstmt, boolean hasSQLExpression, DynamicDTO dynamicDTO, DynamicDTO copyDTOFromReadToCompare, Map<String, BusinessObjectAttributeDTO> boAttributeNamesMetadataMap, ExecutionContext executionContext) {
        int paramIndex = 1;
        try {
            BusinessObjectAttributeDTO boAttrDTO = null;
            List<BusinessObjectAttributeDTO> pkBOAttrDTOList = new ArrayList<>(3);
            for (Map.Entry<String, Object> entry : dynamicDTO.entrySet()) {
                boAttrDTO = boAttributeNamesMetadataMap.get(entry.getKey());
                if (boAttrDTO != null) {
                    if (boAttrDTO.getPrimaryKey()) {
                        pkBOAttrDTOList.add(boAttrDTO);
                    } else if ((!boAttrDTO.getReadOnly()) || (boAttrDTO.isUpdateAuditField())) {
                        Object value = entry.getValue();
                        if (isValueChangedForAttribute(entry.getKey(), dynamicDTO, copyDTOFromReadToCompare)) {
                            if (value == null) {
                                pstmt.setNull(paramIndex, boAttrDTO.getSqlDataType());
                                // increment the parameter index
                                paramIndex++;
                            } else {
                                // paramIndex will be incremented inside the function being called to set value
                                paramIndex = setSQLParameterValueForIndex(pstmt, paramIndex, value, boAttrDTO.getAttributeDatatype(), boAttrDTO.getSqlDataType(), hasSQLExpression, this.getType(),executionContext);
                            }
                        }
                    }
                }
            }
            // setting primary key in where clause
            if (CollectionUtils.hasValue(pkBOAttrDTOList)) {
                for (BusinessObjectAttributeDTO pkBOAttrDTO : pkBOAttrDTOList) {
                    Object value = dynamicDTO.get(pkBOAttrDTO.getBoAttrName());
                    // paramIndex will be incremented inside the function being called to set value
                    paramIndex = setSQLParameterValueForIndex(pstmt, paramIndex, value, pkBOAttrDTO.getAttributeDatatype(), pkBOAttrDTO.getSqlDataType(), hasSQLExpression, this.getType(), executionContext);
                }
            }
        } catch (Throwable e) {
            DSPDMException.throwException(e, executionContext);
        }
        return paramIndex;
    }

    private String logSQLForTinyUpdate(String sql, DynamicDTO dynamicDTO, DynamicDTO copyDTOFromReadToCompare, Map<String, BusinessObjectAttributeDTO> boAttributeNamesMetadataMap, ExecutionContext executionContext) {
        String finalSQL = null;
        if ((logger.isInfoEnabled()) || (executionContext.isCollectSQLStats()) || (executionContext.getCollectSQLScriptOptions().isUpdateEnabled())) {
            BusinessObjectAttributeDTO boAttrDTO = null;
            List<BusinessObjectAttributeDTO> pkBOAttrDTOList = new ArrayList<>(3);
            for (Map.Entry<String, Object> entry : dynamicDTO.entrySet()) {
                boAttrDTO = boAttributeNamesMetadataMap.get(entry.getKey());
                if (boAttrDTO != null) {
                    if (boAttrDTO.getPrimaryKey()) {
                        pkBOAttrDTOList.add(boAttrDTO);
                    } else if ((!boAttrDTO.getReadOnly()) || (boAttrDTO.isUpdateAuditField())) {
                        if (isValueChangedForAttribute(entry.getKey(), dynamicDTO, copyDTOFromReadToCompare)) {
                            sql = sql.replaceFirst("\\?", Matcher.quoteReplacement(getSQLMarkerReplacementString(entry.getValue(),this, executionContext)));
                        }
                    }
                }
            }
            // setting primary key in where clause
            if (CollectionUtils.hasValue(pkBOAttrDTOList)) {
                for (BusinessObjectAttributeDTO pkBOAttrDTO : pkBOAttrDTOList) {
                    Object value = dynamicDTO.get(pkBOAttrDTO.getBoAttrName());
                    sql = sql.replaceFirst("\\?", Matcher.quoteReplacement(getSQLMarkerReplacementString(value,this, executionContext)));
                }
            }
            finalSQL = sql;
            logSQL(finalSQL, executionContext);
            // collect sql script only if required
            if (executionContext.getCollectSQLScriptOptions().isUpdateEnabled()) {
                executionContext.addSqlScript(finalSQL);
            }
        }
        return finalSQL;
    }

    /* ******************************************************** */
    /* ********************** SOFT DELETE ********************* */
    /* ******************************************************** */

    private void performSoftDeleteInBatch(SaveResultDTO saveResultDTO, List<DynamicDTO> businessObjectsToDelete, boolean hasSQLExpression, Connection connection, ExecutionContext executionContext) {
        int totalRowsDeleted = 0;
        PreparedStatement pstmt = null;
        String sqlForSoftDelete = null;
        String finalSQL = null;
        long startTime = 0;
        try {
            logger.info("Soft Delete List Batch: {}", getType());
            sqlForSoftDelete = getSQLForSoftDelete(executionContext);
            pstmt = prepareStatementForUpdate(sqlForSoftDelete,getType(), executionContext, connection);
            Timestamp timestamp = DateTimeUtils.getCurrentTimestampUTC();
            String user = executionContext.getExecutorName();
            List<String> boNames=new ArrayList<>();
            for (DynamicDTO dynamicDTO : businessObjectsToDelete) {
                dynamicDTO.setPrimaryKeyColumnNames(this.getPrimaryKeyColumnNames());
                dynamicDTO.put(DSPDMConstants.BoAttrName.IS_ACTIVE, false);
                // Fill default values such as creation date etc before setting values to sql statement
                fillDefaultsForUpdate(dynamicDTO, timestamp, user, executionContext);
                // set prepared statement parameters
                bindSQLParametersForSoftDelete(pstmt, dynamicDTO, executionContext);
                // Add row to the batch.
                pstmt.addBatch();
                // LOG SQL Statement
                finalSQL = logSQLForSoftDelete(sqlForSoftDelete, dynamicDTO, executionContext);
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
						saveResultDTO.addDeletedRecordsCount(boName, effectedCount);
					} else if (effectedCount == Statement.EXECUTE_FAILED) {
						throw new DSPDMException("Soft delete statement executed but effected row count is not greater than zero, update count : {}",executionContext.getExecutorLocale(), effectedCount);
					}
				}
			} else {
				throw new DSPDMException("Soft delete statement executed but effected object size is not equal to the dynamicDTO",executionContext.getExecutorLocale());
			} 
            logger.info("{} records soft deleted successfully.", totalRowsDeleted);
        } catch (Exception e) {
            if(finalSQL != null) {
                logSQLTimeTaken(finalSQL, businessObjectsToDelete.size(), System.currentTimeMillis() - startTime, executionContext);
            } else {
                // add to sql stats
                logSQLTimeTaken(sqlForSoftDelete, 0, 0, executionContext);
                // log in error logs console
                logSQL(sqlForSoftDelete, executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeStatement(pstmt, executionContext);
        }
    }

    private String getSQLForSoftDelete(ExecutionContext executionContext) {
        String sql = DSPDMConstants.EMPTY_STRING;
        //Update Clause
        sql = sql + getUpdateTableClause(executionContext);
        //Set Value Clause
        sql += getSetClauseForSoftDelete(executionContext);
        return sql;
    }

    private String getSetClauseForSoftDelete(ExecutionContext executionContext) {

        Map<String, BusinessObjectAttributeDTO> boAttributeNamesMetadataMap = getBoAttributeNamesMetadataMap();
        BusinessObjectAttributeDTO isActiveBusinessObjectAttributeDTO = boAttributeNamesMetadataMap.get(DSPDMConstants.BoAttrName.IS_ACTIVE);
        BusinessObjectAttributeDTO rowChangedByBusinessObjectAttributeDTO = boAttributeNamesMetadataMap.get(DSPDMConstants.BoAttrName.ROW_CHANGED_BY);
        BusinessObjectAttributeDTO rowChangedDateBusinessObjectAttributeDTO = boAttributeNamesMetadataMap.get(DSPDMConstants.BoAttrName.ROW_CHANGED_DATE);
        List<String> setClauses = new ArrayList<>(3);
        if (isActiveBusinessObjectAttributeDTO != null) {
            setClauses.add(isActiveBusinessObjectAttributeDTO.getAttributeName() + " = " + DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER);
        } else {
            throw new DSPDMException("Cannot perform soft delete on business object '{}'. Attribute '{}' does not exist.", executionContext.getExecutorLocale(), getType(), DSPDMConstants.BoAttrName.IS_ACTIVE);
        }
        if (rowChangedByBusinessObjectAttributeDTO != null) {
            setClauses.add(rowChangedByBusinessObjectAttributeDTO.getAttributeName() + " = " + DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER);
        }
        if (rowChangedDateBusinessObjectAttributeDTO != null) {
            setClauses.add(rowChangedDateBusinessObjectAttributeDTO.getAttributeName() + " = " + DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER);
        }

        List<String> whereClauses = new ArrayList<>(getPrimaryKeyColumns().size() + 1);
        // add a condition that is_active = true
        whereClauses.add(isActiveBusinessObjectAttributeDTO.getAttributeName() + " = " + DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER);
        for (BusinessObjectAttributeDTO primaryKeyBusinessObjectAttributeDTO : getPrimaryKeyColumns()) {
            whereClauses.add(primaryKeyBusinessObjectAttributeDTO.getAttributeName() + " = " + DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER);
        }

        String setClause = " SET " + CollectionUtils.getCommaSeparated(setClauses);
        if (CollectionUtils.hasValue(whereClauses)) {
            setClause = setClause + " WHERE " + CollectionUtils.joinWithAnd(whereClauses);
        }

        return setClause;
    }

    private int bindSQLParametersForSoftDelete(PreparedStatement pstmt, DynamicDTO dynamicDTO, ExecutionContext executionContext) {
        int paramIndex = 1;
        try {
            Map<String, BusinessObjectAttributeDTO> boAttributeNamesMetadataMap = getBoAttributeNamesMetadataMap();
            BusinessObjectAttributeDTO isActiveBusinessObjectAttributeDTO = boAttributeNamesMetadataMap.get(DSPDMConstants.BoAttrName.IS_ACTIVE);
            BusinessObjectAttributeDTO rowChangedByBusinessObjectAttributeDTO = boAttributeNamesMetadataMap.get(DSPDMConstants.BoAttrName.ROW_CHANGED_BY);
            BusinessObjectAttributeDTO rowChangedDateBusinessObjectAttributeDTO = boAttributeNamesMetadataMap.get(DSPDMConstants.BoAttrName.ROW_CHANGED_DATE);

            pstmt.setObject(paramIndex++, DSPDMConstants.DEFAULT_VALUES.IS_ACTIVE_FALSE, isActiveBusinessObjectAttributeDTO.getSqlDataType());
            if (rowChangedByBusinessObjectAttributeDTO != null) {
                pstmt.setObject(paramIndex++, dynamicDTO.get(DSPDMConstants.BoAttrName.ROW_CHANGED_BY), rowChangedByBusinessObjectAttributeDTO.getSqlDataType());
            }
            if (rowChangedDateBusinessObjectAttributeDTO != null) {
                pstmt.setObject(paramIndex++, dynamicDTO.get(DSPDMConstants.BoAttrName.ROW_CHANGED_DATE), rowChangedDateBusinessObjectAttributeDTO.getSqlDataType());
            }
            // NOW SET BIND PARAMETERS FOR WHERE CLAUSE
            pstmt.setObject(paramIndex++, DSPDMConstants.DEFAULT_VALUES.IS_ACTIVE_TRUE, isActiveBusinessObjectAttributeDTO.getSqlDataType());
            // NOW BIND PARAMETERS FOR PRIMARY KEY
            for (BusinessObjectAttributeDTO primaryKeyBusinessObjectAttributeDTO : getPrimaryKeyColumns()) {
                pstmt.setObject(paramIndex++, dynamicDTO.get(primaryKeyBusinessObjectAttributeDTO.getBoAttrName()), primaryKeyBusinessObjectAttributeDTO.getSqlDataType());
            }
        } catch (Throwable e) {
            DSPDMException.throwException(e, executionContext);
        }
        return paramIndex;
    }

    private String logSQLForSoftDelete(String sql, DynamicDTO dynamicDTO, ExecutionContext executionContext) {
        String finalSQL = null;
        if ((logger.isInfoEnabled()) || (executionContext.isCollectSQLStats())
                || ((executionContext.getCollectSQLScriptOptions().isUpdateEnabled())
                || (executionContext.getCollectSQLScriptOptions().isDeleteEnabled()))) {
            Map<String, BusinessObjectAttributeDTO> boAttributeNamesMetadataMap = getBoAttributeNamesMetadataMap();
            BusinessObjectAttributeDTO isActiveBusinessObjectAttributeDTO = boAttributeNamesMetadataMap.get(DSPDMConstants.BoAttrName.IS_ACTIVE);
            BusinessObjectAttributeDTO rowChangedByBusinessObjectAttributeDTO = boAttributeNamesMetadataMap.get(DSPDMConstants.BoAttrName.ROW_CHANGED_BY);
            BusinessObjectAttributeDTO rowChangedDateBusinessObjectAttributeDTO = boAttributeNamesMetadataMap.get(DSPDMConstants.BoAttrName.ROW_CHANGED_DATE);

            // FIRST REPLACE BINDED PARAMETERS FOR SET CLAUSE            
            sql = sql.replaceFirst("\\?", Matcher.quoteReplacement(getSQLMarkerReplacementString(DSPDMConstants.DEFAULT_VALUES.IS_ACTIVE_FALSE,this, executionContext)));
            if (rowChangedByBusinessObjectAttributeDTO != null) {
                sql = sql.replaceFirst("\\?", Matcher.quoteReplacement(getSQLMarkerReplacementString(dynamicDTO.get(DSPDMConstants.BoAttrName.ROW_CHANGED_BY),this, executionContext)));
            }
            if (rowChangedDateBusinessObjectAttributeDTO != null) {
                sql = sql.replaceFirst("\\?", Matcher.quoteReplacement(getSQLMarkerReplacementString(dynamicDTO.get(DSPDMConstants.BoAttrName.ROW_CHANGED_DATE),this, executionContext)));
            }
            // NOW REPLACE BINDED PARAMETERS FOR WHERE CLAUSE
            sql = sql.replaceFirst("\\?", Matcher.quoteReplacement(getSQLMarkerReplacementString(DSPDMConstants.DEFAULT_VALUES.IS_ACTIVE_TRUE,this, executionContext)));
            // NOW REPLACE BINDED PARAMETERS FOR PRIMARY KEY
            for (BusinessObjectAttributeDTO primaryKeyBusinessObjectAttributeDTO : getPrimaryKeyColumns()) {
                sql = sql.replaceFirst("\\?", Matcher.quoteReplacement(getSQLMarkerReplacementString(dynamicDTO.get(primaryKeyBusinessObjectAttributeDTO.getBoAttrName()),this, executionContext)));
            }
            finalSQL = sql;
            logSQL(finalSQL, executionContext);
            // collect sql script only if required
            if ((executionContext.getCollectSQLScriptOptions().isUpdateEnabled()) || (executionContext.getCollectSQLScriptOptions().isDeleteEnabled())) {
                executionContext.addSqlScript(finalSQL);
            }
        }
        return finalSQL;
    }
}
