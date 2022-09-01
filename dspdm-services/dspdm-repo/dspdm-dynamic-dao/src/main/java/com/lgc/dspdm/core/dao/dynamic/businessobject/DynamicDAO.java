package com.lgc.dspdm.core.dao.dynamic.businessobject;

import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.join.BaseJoinClause;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicPK;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAO;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAOImpl;
import com.lgc.dspdm.core.dao.dynamic.businessobject.impl.AbstractDynamicDAOImpl;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Muhammad Imran Ansari
 * @date 17-Jun-2019
 */
public class DynamicDAO extends AbstractDynamicDAOImpl implements IDynamicDAO {
    private static DSPDMLogger logger = new DSPDMLogger(DynamicDAO.class);

    public DynamicDAO(BusinessObjectDTO businessObjectDTO, ExecutionContext executionContext) {
        // bo Name as type
        super(businessObjectDTO, executionContext);
    }

    @Override
    public boolean isSearchDAO(String boName) {
        return super.isSearchDAO(boName);
    }

    /*

    @Override
    public boolean isPostgresDialect() {
        return super.isPostgresDialect();
    }*/

    @Override
    public boolean isMSSQLServerDialect(String type) {
        return super.isMSSQLServerDialect(type);
    }

    @Override
    public DynamicDTO readMetadataBusinessObject(ExecutionContext executionContext) {
        return super.getBusinessObject();
    }

    @Override
    public List<String> readMetadataBOAttrNames(ExecutionContext executionContext) {
        return super.getMetadataBOAttrNames();
    }

    @Override
    public Map<String, Object> getDefaultValuesMap(ExecutionContext executionContext) {
        return super.getDefaultValuesMap();
    }

    @Override
    public List<DynamicDTO> readMetadata(ExecutionContext executionContext) {
        return super.getMetadataList();
    }

    @Override
    public Map<String, DynamicDTO> readMetadataMap(ExecutionContext executionContext) {
        return super.getMetadataMap();
    }

    @Override
    public List<DynamicDTO> readActiveMetadata(ExecutionContext executionContext) {
        return super.getActiveMetadataList();
    }

    @Override
    public List<DynamicDTO> readMetadataConstraints(ExecutionContext executionContext) {
        return super.getMetadataConstraintsList();
    }

    @Override
    public List<DynamicDTO> readActiveMetadataConstraints(ExecutionContext executionContext) {
        return super.getActiveMetadataConstraintsList();
    }

    @Override
    public List<DynamicDTO> readActiveMetadataSearchIndexes(ExecutionContext executionContext) {
        return super.getActiveMetadataSearchIndexesList();
    }

    @Override
    public List<DynamicDTO> readMetadataRelationships(ExecutionContext executionContext) {
        return super.getMetadataRelationshipsList();
    }

    @Override
    public Map<String, List<DynamicDTO>> readMetadataParentRelationships(ExecutionContext executionContext) {
        return super.getMetadataParentRelationshipsMap();
    }

    @Override
    public Map<String, List<DynamicDTO>> readMetadataChildRelationships(ExecutionContext executionContext) {
        return super.getMetadataChildRelationshipsMap();
    }

    @Override
    public int count(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        int count = 0;
        Connection connection = null;
        try {
            connection = getReadOnlyConnection(getType(), executionContext);
            count = super.count(businessObjectInfo, false, connection, executionContext);
        } finally {
            closeConnection(getType(), connection, executionContext);
        }
        return count;
    }

    @Override
    public List<DynamicDTO> read(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        List<DynamicDTO> list = null;
        Connection connection = null;
        try {
            connection = getReadOnlyConnection(getType(), executionContext);
            list = super.read(businessObjectInfo, false, connection, executionContext);
        } finally {
            closeConnection(getType(), connection, executionContext);
        }
        return list;
    }

    @Override
    public DynamicDTO readOne(final DynamicPK dynamicPK, ExecutionContext executionContext) {
        DynamicDTO dynamicDTO = null;
        Connection connection = null;
        try {
            connection = getReadOnlyConnection(getType(), executionContext);
            dynamicDTO = super.readOne(dynamicPK, connection, executionContext);
        } finally {
            closeConnection(getType(), connection, executionContext);
        }
        return dynamicDTO;
    }

    @Override
    public List<DynamicDTO> readCopy(List<DynamicDTO> businessObjectsToRead, ExecutionContext executionContext) {
        List<DynamicDTO> list = null;
        Connection connection = null;
        try {
            connection = getReadOnlyConnection(getType(), executionContext);
            list = super.readCopy(businessObjectsToRead, connection, executionContext);
        } finally {
            closeConnection(getType(), connection, executionContext);
        }
        return list;
    }

    @Override
    public List<DynamicDTO> readCopyRetainChildren(List<DynamicDTO> businessObjectsToRead, ExecutionContext executionContext) {
        List<DynamicDTO> list = null;
        Connection connection = null;
        try {
            connection = getReadOnlyConnection(getType(), executionContext);
            list = super.readCopyRetainChildren(businessObjectsToRead, connection, executionContext);
        } finally {
            closeConnection(getType(), connection, executionContext);
        }
        return list;
    }

    @Override
    public Map<DynamicPK, DynamicDTO> readCopyAsMap(List<DynamicDTO> businessObjectsToRead, ExecutionContext executionContext) {
        Map<DynamicPK, DynamicDTO> dynamicPKDynamicDTOMap = null;
        Connection connection = null;
        try {
            connection = getReadOnlyConnection(getType(), executionContext);
            dynamicPKDynamicDTOMap = super.readCopyAsMap(businessObjectsToRead, connection, executionContext);
        } finally {
            closeConnection(getType(), connection, executionContext);
        }
        return dynamicPKDynamicDTOMap;
    }

    @Override
    public List<DynamicDTO> readCopyWithCustomFilter(List<DynamicDTO> boListToReadCopy, ExecutionContext executionContext, String... boAttrNamesToVerify) {
        List<DynamicDTO> list = null;
        Connection connection = null;
        try {
            connection = getReadOnlyConnection(getType(), executionContext);
            list = super.readCopyAndVerifyBoAttrNamesExists(boListToReadCopy, connection, executionContext, boAttrNamesToVerify);
        } finally {
            closeConnection(getType(), connection, executionContext);
        }
        return list;
    }

    @Override
    public List<DynamicDTO> readLongRangeUsingInClause(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter,
                                                       String boAttrNameForInClause, Object[] uniqueValuesForInClause,
                                                       Operator operator, ExecutionContext executionContext) {
        List<DynamicDTO> list = null;
        Connection connection = null;
        try {
            connection = getReadOnlyConnection(getType(), executionContext);
            list = super.readLongRangeUsingInClause(businessObjectInfo, joinClauseToApplyFilter, boAttrNameForInClause,
                    uniqueValuesForInClause, operator, connection, executionContext);
        } finally {
            closeConnection(getType(), connection, executionContext);
        }
        return list;
    }

    @Override
    public List<DynamicDTO> readLongRangeUsingInClauseAndExistingFilters(BusinessObjectInfo businessObjectInfo,
                                                                         BaseJoinClause joinClauseToApplyFilter,
                                                                         String boAttrNameForInClause,
                                                                         Object[] uniqueValuesForInClause,
                                                                         Operator operator,
                                                                         ExecutionContext executionContext) {
        List<DynamicDTO> list = null;
        Connection connection = null;
        try {
            connection = getReadOnlyConnection(getType(), executionContext);
            list = super.readLongRangeUsingInClauseAndExistingFilters(businessObjectInfo, joinClauseToApplyFilter,
                    boAttrNameForInClause, uniqueValuesForInClause, operator, connection, executionContext);
        } finally {
            closeConnection(getType(), connection, executionContext);
        }
        return list;
    }

    @Override
    public List<DynamicDTO> readLongRangeUsingCompositeORClause(BusinessObjectInfo businessObjectInfo,
                                                                BaseJoinClause joinClauseToApplyFilter,
                                                                List<String> uniqueBoAttrNames,
                                                                Collection<DynamicDTO> uniqueBoAttrNamesValues,
                                                                ExecutionContext executionContext) {
        List<DynamicDTO> list = null;
        Connection connection = null;
        try {
            connection = getReadOnlyConnection(getType(), executionContext);
            list = super.readLongRangeUsingCompositeORClause(businessObjectInfo, joinClauseToApplyFilter, uniqueBoAttrNames,
                    uniqueBoAttrNamesValues, connection, executionContext);
        } finally {
            closeConnection(getType(), connection, executionContext);
        }
        return list;
    }

    @Override
    public SaveResultDTO saveOrUpdate(List<DynamicDTO> businessObjectsToSave, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        Connection connection = null;
        try {
            connection = getReadWriteConnection(getType(), executionContext);
            saveResultDTO = super.saveOrUpdate(businessObjectsToSave, false, connection, executionContext);
        } finally {
            closeConnection(getType(), connection, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public Integer[] getNextFromSequence(String boName, String sequenceName, int count, ExecutionContext executionContext) {
        Integer[] sequenceNumbers = null;
        Connection connection = null;
        try {
            connection = getReadWriteConnection(getType(), executionContext);
            sequenceNumbers = super.getNextFromSequence(boName, sequenceName, count, connection, executionContext);
        } finally {
            closeConnection(getType(), connection, executionContext);
        }
        return sequenceNumbers;
    }

    @Override
    public Integer[] getNextFromSequenceFromDataModelDB(String boName, String sequenceName, int count, ExecutionContext executionContext) {
        Integer[] sequenceNumbers = null;
        Connection dataModelDBConnection = null;
        try {
            dataModelDBConnection = getDataModelDBReadOnlyConnection(executionContext);
            sequenceNumbers = super.getNextFromSequence(boName, sequenceName, count, dataModelDBConnection, executionContext);
        } finally {
            closeDataModelDBConnection(dataModelDBConnection, executionContext);
        }
        return sequenceNumbers;
    }

    @Override
    public SaveResultDTO delete(List<DynamicDTO> businessObjectsToDelete, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        Connection connection = null;
        try {
            connection = getReadWriteConnection(getType(), executionContext);
            saveResultDTO = super.delete(businessObjectsToDelete, false, connection, executionContext);
        } finally {
            closeConnection(getType(), connection, executionContext);
        }
        return saveResultDTO;
    }
    
    @Override
    public SaveResultDTO deleteAll(ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        Connection connection = null;
        try {
            connection = getReadWriteConnection(getType(), executionContext);
            saveResultDTO = super.deleteAll(connection, executionContext);
        } finally {
            closeConnection(getType(), connection, executionContext);
        }
        return saveResultDTO;
    }
    
    @Override
    public SaveResultDTO softDelete(List<DynamicDTO> businessObjectsToDelete, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        Connection connection = null;
        try {
            connection = getReadWriteConnection(getType(), executionContext);
            saveResultDTO = super.softDelete(businessObjectsToDelete, false, connection, executionContext);
        } finally {
            closeConnection(getType(), connection, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO addCustomAttributes(List<DynamicDTO> businessObjectsToSave, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        Connection dataModelDBConnection = null;
        Connection serviceDBConnection = null;
        try {
            dataModelDBConnection = getDataModelDBReadWriteConnection(executionContext);
            serviceDBConnection = getServiceDBReadWriteConnection(executionContext);
            saveResultDTO = super.addCustomAttributes(businessObjectsToSave, serviceDBConnection, dataModelDBConnection, executionContext);
        } finally {
            closeDataModelDBConnection(dataModelDBConnection, executionContext);
            closeServiceDBConnection(serviceDBConnection, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO updateCustomAttributes(List<DynamicDTO> businessObjectsToUpdate, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        Connection serviceDBConnection = null;
        Connection dataModelDBConnection = null;
        try {
            serviceDBConnection = getServiceDBReadWriteConnection(executionContext);
            dataModelDBConnection = getDataModelDBReadWriteConnection(executionContext);
            saveResultDTO = super.updateCustomAttributes(businessObjectsToUpdate, serviceDBConnection, dataModelDBConnection, executionContext);
        } finally {
            closeServiceDBConnection(serviceDBConnection, executionContext);
            closeDataModelDBConnection(dataModelDBConnection, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO deleteCustomAttributes(List<DynamicDTO> businessObjectsToSave, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        Connection serviceDBConnection = null;
        Connection dataModelDBConnection = null;
        try {
            serviceDBConnection = getServiceDBReadWriteConnection(executionContext);
            dataModelDBConnection = getDataModelDBReadWriteConnection(executionContext);
            saveResultDTO = super.deleteCustomAttributes(businessObjectsToSave, serviceDBConnection, dataModelDBConnection, executionContext);
        } finally {
            closeServiceDBConnection(serviceDBConnection, executionContext);
            closeDataModelDBConnection(dataModelDBConnection, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO introduceNewBusinessObjects(List<DynamicDTO> businessObjectsToSave, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        Connection dataModelDBConnection = null;
        Connection serviceDBConnection = null;
        try {
            dataModelDBConnection = getDataModelDBReadWriteConnection( executionContext);
            serviceDBConnection = getServiceDBReadWriteConnection(executionContext);
            saveResultDTO = super.introduceNewBusinessObjects(businessObjectsToSave, dataModelDBConnection, serviceDBConnection, executionContext);
        } finally {
            closeDataModelDBConnection(dataModelDBConnection, executionContext);
            closeServiceDBConnection(serviceDBConnection, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO dropBusinessObjects(List<DynamicDTO> businessObjectsToBeDropped, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        Connection dataModelDBConnection = null;
        Connection serviceDBConnection = null;
        try {
            serviceDBConnection = getServiceDBReadWriteConnection(executionContext);
            dataModelDBConnection = getDataModelDBReadWriteConnection(executionContext);
            // verify that the physical table has no data if full drop option is not set to true
            if (!executionContext.isFullDrop()) {
                // verify that business objects has no data otherwise throw error
                BusinessObjectInfo businessObjectInfo = null;
                IDynamicDAOImpl dynamicDAOImpl = null;
                String boName = null;
                for (DynamicDTO businessObjectToBeDropped : businessObjectsToBeDropped) {
                    boName = (String) businessObjectToBeDropped.get(DSPDMConstants.BoAttrName.BO_NAME);
                    // do not call read/count on the current dao but get the dynamic dao for the given business object name and then call read/count on it
                    dynamicDAOImpl = getDynamicDAOImpl(boName, executionContext);
                    businessObjectInfo = new BusinessObjectInfo(boName, executionContext);
                    boolean businessObjectHasData = false;
                    if (isMSSQLServerDialect(boName)) {
                        // as MS SQL Server does not support fetch and offset statement without using order by clause.
                        // if we use fetch first record on sql server then order by will be appied first
                        // Order by is an expensive operation os for MS SQL Server we will use count and for other
                        // sql dialects we will use fetch first record to check the existence of the data
                        if (dynamicDAOImpl.count(businessObjectInfo, false, dataModelDBConnection, executionContext) > 0) {
                            businessObjectHasData = true;
                        }
                    } else {
                        // count will be expensive than reading first record therefore read first
                        businessObjectInfo.setReadFirst(true);
                        if (CollectionUtils.hasValue(dynamicDAOImpl.read(businessObjectInfo, false, dataModelDBConnection, executionContext))) {
                            businessObjectHasData = true;
                        }
                    }
                    // if list has data then throw error
                    if (businessObjectHasData) {
                        throw new DSPDMException("Business object {} contains data and cannot be dropped directly. Please delete data first and then try again.",
                                executionContext.getExecutorLocale(), boName);
                    }
                }
            }
            saveResultDTO = super.dropBusinessObjects(businessObjectsToBeDropped, dataModelDBConnection, serviceDBConnection, executionContext);
        } finally {
            closeServiceDBConnection(serviceDBConnection, executionContext);
            closeDataModelDBConnection(dataModelDBConnection, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO dropBusinessObjectRelationships(List<DynamicDTO> businessObjectsRelationshipsToBeDropped, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        Connection dataModelDBConnection = null;
        Connection serviceDBConnection = null;
        try {
            //connection = getReadWriteConnection(getType(), executionContext);
            dataModelDBConnection = getDataModelDBReadWriteConnection(executionContext);
            serviceDBConnection = getServiceDBReadWriteConnection(executionContext);
            // call the service to do it
            saveResultDTO = super.dropBusinessObjectRelationships(businessObjectsRelationshipsToBeDropped, dataModelDBConnection, serviceDBConnection, executionContext);
        } finally {
            //closeConnection(getType(), connection, executionContext);
            closeDataModelDBConnection(dataModelDBConnection, executionContext);
            closeServiceDBConnection(serviceDBConnection, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO addBusinessObjectRelationships(List<DynamicDTO> busObjRelationshipsToBeCreated, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        Connection dataModelDBConnection = null;
        Connection serviceDBConnection = null;
        try {
            //connection = getReadWriteConnection(getType(), executionContext);
            dataModelDBConnection = getDataModelDBReadWriteConnection(executionContext);
            serviceDBConnection = getServiceDBReadWriteConnection(executionContext);
            // call the service to do it
            saveResultDTO = super.addBusinessObjectRelationships(busObjRelationshipsToBeCreated, dataModelDBConnection, serviceDBConnection, executionContext);
        } finally {
            //closeConnection(getType(), connection, executionContext);
            closeDataModelDBConnection(dataModelDBConnection, executionContext);
            closeServiceDBConnection(serviceDBConnection, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO addUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeCreated, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        Connection serviceDBConnection = null;
        Connection dataModelDBConnection = null;
        try {
            serviceDBConnection = getServiceDBReadWriteConnection(executionContext);
            dataModelDBConnection = getDataModelDBReadWriteConnection(executionContext);
            // call the service to do it
            saveResultDTO = super.addUniqueConstraints(busObjAttrUniqueConstraintsToBeCreated, serviceDBConnection, dataModelDBConnection, executionContext);
        } finally {
            closeServiceDBConnection(serviceDBConnection, executionContext);
            closeDataModelDBConnection(dataModelDBConnection, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO addSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeCreated, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        Connection serviceDBConnection = null;
        Connection dataModelDBConnection = null;
        try {
            serviceDBConnection = getServiceDBReadWriteConnection(executionContext);
            dataModelDBConnection = getDataModelDBReadWriteConnection(executionContext);
            // call the service to do it
            saveResultDTO = super.addSearchIndexes(busObjAttrSearchIndexesToBeCreated, serviceDBConnection, dataModelDBConnection, executionContext);
        } finally {
            closeServiceDBConnection(serviceDBConnection, executionContext);
            closeDataModelDBConnection(dataModelDBConnection, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO dropUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeDropped, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        Connection serviceDBConnection = null;
        Connection dataModelDBConnection = null;
        try {
            //connection = getReadWriteConnection(getType(), executionContext);
            serviceDBConnection = getServiceDBReadWriteConnection(executionContext);
            dataModelDBConnection = getDataModelDBReadWriteConnection(executionContext);
            // call the service to do it
            saveResultDTO = super.dropUniqueConstraints(busObjAttrUniqueConstraintsToBeDropped, serviceDBConnection, dataModelDBConnection, executionContext);
        } finally {
            closeServiceDBConnection(serviceDBConnection, executionContext);
            closeDataModelDBConnection(dataModelDBConnection, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO dropSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeDropped, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        Connection serviceDBCConnection = null;
        Connection dataModelDBCConnection = null;
        try {
            //connection = getReadWriteConnection(getType(), executionContext);
            serviceDBCConnection = getServiceDBReadWriteConnection(executionContext);
            dataModelDBCConnection = getDataModelDBReadWriteConnection(executionContext);
            // call the service to do it
            saveResultDTO = super.dropSearchIndexes(busObjAttrSearchIndexesToBeDropped, serviceDBCConnection, dataModelDBCConnection, executionContext);
        } finally {
            closeServiceDBConnection(serviceDBCConnection, executionContext);
            closeDataModelDBConnection(dataModelDBCConnection, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO generateMetadataForExistingTable(List<DynamicDTO> boListToGenerateMetadata, Set<String> exclusionList, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        Connection serviceDBConnection = null;
        Connection dataModelDBConnection = null;
        try {
            serviceDBConnection = getServiceDBReadWriteConnection(executionContext);
            dataModelDBConnection = getDataModelDBReadOnlyConnection(executionContext);
            saveResultDTO = super.generateMetadataForExistingTable(boListToGenerateMetadata, exclusionList, serviceDBConnection, dataModelDBConnection, executionContext);
        } finally {
            closeServiceDBConnection(serviceDBConnection, executionContext);
            closeDataModelDBConnection(dataModelDBConnection, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO deleteMetadataForExistingTable(List<DynamicDTO> boListToDeleteMetadata, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        Connection serviceDBConnection = null;
        try {
            serviceDBConnection = getServiceDBReadWriteConnection(executionContext);
            saveResultDTO = super.deleteMetadataForExistingTable(boListToDeleteMetadata, serviceDBConnection, executionContext);
        } finally {
            closeServiceDBConnection(serviceDBConnection, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO addBusinessObjectGroup(List<DynamicDTO> busObjGroupsToBeCreated, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        Connection serviceDBConnection = null;
        try {
            serviceDBConnection = getServiceDBReadWriteConnection(executionContext);
            saveResultDTO = super.addBusinessObjectGroup(busObjGroupsToBeCreated, serviceDBConnection, executionContext);
        } finally {
            closeServiceDBConnection(serviceDBConnection, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public List<String> getAllEntityNamesListOfConnectedDB(DynamicDTO dynamicDTO, Set<String> exclusionList, ExecutionContext executionContext) {
        List<String> entityNamesList = null;
        Connection dataModelDBConnection = null;
        try {
            dataModelDBConnection = getDataModelDBReadOnlyConnection(executionContext);
            entityNamesList = super.getAllEntityNamesListOfConnectedDB(dynamicDTO, exclusionList, dataModelDBConnection, executionContext);
        } finally {
            closeDataModelDBConnection(dataModelDBConnection, executionContext);
        }
        return entityNamesList;
    }

    @Override
    public String getCurrentUserDBSchemaName(ExecutionContext executionContext) {
       String currentUserDBSchemaName = null;
        Connection connection = null;
        try {
            // sending null since we need data model db connection (connection to user db i.e., physical db)
            connection = getReadOnlyConnection(null, executionContext);
            currentUserDBSchemaName = super.getCurrentDBSchemaName(connection,executionContext);
        } finally {
            closeConnection(getType(), connection, executionContext);
        }
        return currentUserDBSchemaName;
    }

    @Override
    public String getCurrentUserDBName(ExecutionContext executionContext) {
        String currentUserDBName = null;
        Connection connection = null;
        try {
            // sending null since we need data model db connection (connection to user db i.e., physical db)
            connection = getReadOnlyConnection(null, executionContext);
            currentUserDBName = super.getCurrentUserDBName(connection,executionContext);
        } finally {
            closeConnection(getType(), connection, executionContext);
        }
        return currentUserDBName;
    }
}
