package com.lgc.dspdm.core.dao.dynamic.businessobject.impl;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.*;
import com.lgc.dspdm.core.common.util.metadata.MetadataUtils;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAO;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAOImpl;
import com.lgc.dspdm.core.dao.dynamic.businessobject.DynamicDAOFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Class to hold common code for utility methods or base methods.
 * These methods will not be public and can only be invoked from the child classes.
 * This class will not contain any public business method
 *
 * @author Muhammad Imran Ansari
 * @date 03-May-2021
 */
public abstract class AbstractDynamicDAOImplForReverseEngineerMetadata extends AbstractDynamicDAOImplForStructureChange implements IDynamicDAOImpl {
    private static DSPDMLogger logger = new DSPDMLogger(AbstractDynamicDAOImplForReverseEngineerMetadata.class);

    protected AbstractDynamicDAOImplForReverseEngineerMetadata(BusinessObjectDTO businessObjectDTO, ExecutionContext executionContext) {
        super(businessObjectDTO, executionContext);
    }

    /* ***************************************************************************** */
    /* ************** BUSINESS METHOD DROP BUSINESS OBJECT METADATA **************** */
    /* ***************************************************************************** */

    @Override
    public SaveResultDTO deleteMetadataForExistingTable(List<DynamicDTO> boListToDeleteMetadata, Connection serviceDBConnection, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        // delete business object
        rootSaveResultDTO.addResult(deleteMetadata(boListToDeleteMetadata, serviceDBConnection, executionContext));
        return rootSaveResultDTO;
    }

    private static SaveResultDTO deleteMetadata(List<DynamicDTO> boListToDelete, Connection
            serviceDBConnection, ExecutionContext executionContext) {
        // deciding operation id
        if(executionContext.getDeleteMetadataForAll()){
            executionContext.setCurrentOperationId(DSPDMConstants.BusinessObjectOperations.DELETE_METADATA_FOR_ALL_OPR.getId());
        }else{
            executionContext.setCurrentOperationId(DSPDMConstants.BusinessObjectOperations.DELETE_METADATA_OPR.getId());
        }

        IDynamicDAOImpl businessObjectDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUSINESS_OBJECT, executionContext);
        // extract all the business object names from the list to read actual records
        List<String> boNamesFromList = CollectionUtils.getStringValuesFromList(boListToDelete, DSPDMConstants.BoAttrName.BO_NAME);
        // find primary keys (and related details for logging) from database to delete against bo names
        List<DynamicDTO> finalListToDelete = businessObjectDAO.read(
                new BusinessObjectInfo(DSPDMConstants.BoName.BUSINESS_OBJECT, executionContext)
                        .addFilter(DSPDMConstants.BoAttrName.BO_NAME, Operator.IN, boNamesFromList)
                        .setReadAllRecords(true), false, serviceDBConnection, executionContext);
        if (finalListToDelete.size() != boListToDelete.size()) {
            throw new DSPDMException("Not same number of business objects found for the names '{}'", executionContext.getExecutorLocale(), CollectionUtils.getCommaSeparated(boNamesFromList));
        }
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        Map<String, List<DynamicDTO>> childrenMap = null;
        for (DynamicDTO businessObjectToDelete : finalListToDelete) {
            // delete children
            deleteMetadataChildrenOnly(businessObjectToDelete, serviceDBConnection, executionContext);
        }
        // delete business object
        saveResultDTO.addResult(businessObjectDAO.delete(finalListToDelete, false, serviceDBConnection, executionContext));
        // clear old list and add new objects to be deleted.
        boListToDelete.clear();
        boListToDelete.addAll(finalListToDelete);
        // Metadata deleted successfully
        //logging now
        trackChangeHistory(saveResultDTO,executionContext.getCurrentOperationId(), boListToDelete, serviceDBConnection, executionContext);
        return saveResultDTO;
    }

    private static SaveResultDTO deleteMetadataChildrenOnly(DynamicDTO businessObjectToDelete, Connection
            connection, ExecutionContext executionContext) {
        IDynamicDAOImpl businessObjectAttrDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, executionContext);
        IDynamicDAOImpl busObjAttrUniqueConstraintsDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, executionContext);
        IDynamicDAOImpl businessObjectGroupDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUSINESS_OBJECT_GROUP, executionContext);
        IDynamicDAOImpl busObjRelationshipDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, executionContext);
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        Map<String, List<DynamicDTO>> childrenMap = new LinkedHashMap<>();
        String boName = (String) businessObjectToDelete.get(DSPDMConstants.BoAttrName.BO_NAME);
        businessObjectToDelete.put(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY, childrenMap);
        // start deletion in reverse order means child will be deleted first
        // 1. delete business object child relationships
        List<DynamicDTO> childRelationships = busObjRelationshipDAO.read(
                new BusinessObjectInfo(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, executionContext)
                        .addFilter(DSPDMConstants.BoAttrName.CHILD_BO_NAME, boName)
                        .setReadAllRecords(true)
                , false, connection, executionContext);
        if (CollectionUtils.hasValue(childRelationships)) {
            saveResultDTO.addResult(busObjRelationshipDAO.delete(childRelationships, false, connection, executionContext));
            childrenMap.put(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, childRelationships);
        }
        // 2. delete business object parent relationships
        List<DynamicDTO> parentRelationships = busObjRelationshipDAO.read(
                new BusinessObjectInfo(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, executionContext)
                        .addFilter(DSPDMConstants.BoAttrName.PARENT_BO_NAME, boName)
                        .setReadAllRecords(true)
                , false, connection, executionContext);
        if (CollectionUtils.hasValue(parentRelationships)) {
            saveResultDTO.addResult(busObjRelationshipDAO.delete(parentRelationships, false, connection, executionContext));
            if (CollectionUtils.hasValue(childRelationships)) {
                childRelationships.addAll(parentRelationships);
            } else {
                childrenMap.put(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, parentRelationships);
            }
        }

        // 3. delete unique constraints
        List<DynamicDTO> constraints = busObjAttrUniqueConstraintsDAO.read(
                new BusinessObjectInfo(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, executionContext)
                        .addFilter(DSPDMConstants.BoAttrName.BO_NAME, boName)
                        .setReadAllRecords(true)
                , false, connection, executionContext);
        if (CollectionUtils.hasValue(constraints)) {
            saveResultDTO.addResult(busObjAttrUniqueConstraintsDAO.delete(constraints, false, connection, executionContext));
            childrenMap.put(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, constraints);
        }

        // 4. attributes will be deleted after the deletion of constraints and relationships
        // 4. delete attributes
        List<DynamicDTO> attributes = businessObjectAttrDAO.read(
                new BusinessObjectInfo(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, executionContext)
                        .addFilter(DSPDMConstants.BoAttrName.BO_NAME, boName)
                        .setReadAllRecords(true)
                , false, connection, executionContext);
        if (CollectionUtils.hasValue(attributes)) {
            saveResultDTO.addResult(businessObjectAttrDAO.delete(attributes, false, connection, executionContext));
            childrenMap.put(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, attributes);
        }

        // 5. delete business object group entries
        List<DynamicDTO> groups = businessObjectGroupDAO.read(
                new BusinessObjectInfo(DSPDMConstants.BoName.BUSINESS_OBJECT_GROUP, executionContext)
                        .addFilter(DSPDMConstants.BoAttrName.BO_NAME, boName)
                        .setReadAllRecords(true)
                , false, connection, executionContext);
        if (CollectionUtils.hasValue(groups)) {
            saveResultDTO.addResult(businessObjectGroupDAO.delete(groups, false, connection, executionContext));
            childrenMap.put(DSPDMConstants.BoName.BUSINESS_OBJECT_GROUP, groups);
        }
        return saveResultDTO;
    }


    /* ***************************************************************************** */
    /* ************** GENERATE METADATA FOR EXISTING BUSINESS METHOD *************** */
    /* ***************************************************************************** */

    @Override
    public SaveResultDTO generateMetadataForExistingTable(List<DynamicDTO> boListToGenerateMetadata, Set<String> exclusionList,
                                                          Connection serviceDBConnection, Connection dataModelDBConnection, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        if (CollectionUtils.hasValue(boListToGenerateMetadata)) {
            try {
                DatabaseMetaData metaData = dataModelDBConnection.getMetaData();
                List<DynamicDTO> businessObjectAttrDynamicDTOListToSave = new ArrayList<>();
                List<DynamicDTO> busObjRelationshipDynamicDTOListToBeSaved = new ArrayList<>();
                List<DynamicDTO> busObjUniqueConstraintsDynamicDTOListToBeSaved = new ArrayList<>();
                List<String> alreadyGeneratedMetadataRelationships = new ArrayList<>();
                Map<String, DynamicDTO> alreadyGeneratedMetadataBusinessObjects = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                for (DynamicDTO businessObjectDynamicDTO : boListToGenerateMetadata) {
                    String databaseName = (String) businessObjectDynamicDTO.get(DSPDMConstants.DATABASE_NAME);
                    String schemaName = (String) businessObjectDynamicDTO.get(DSPDMConstants.SCHEMA_NAME);
                    String tableName = ((String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY));
                    // database connection metadata is case sensitive
                    // first checking table name with lower case
                    try (ResultSet tablesResultSetLowerCase = metaData.getTables(databaseName.toLowerCase(), schemaName.toLowerCase(), tableName.toLowerCase(), null)) {
                        if (tablesResultSetLowerCase.next()) {
                            tableName = tableName.toLowerCase();
                            businessObjectDynamicDTO.put(DSPDMConstants.DATABASE_NAME, databaseName.toLowerCase());
                            businessObjectDynamicDTO.put(DSPDMConstants.SCHEMA_NAME, schemaName.toLowerCase());
                            generateMetadataForSingleTable_generatedMetadataForExistingTable(tableName, businessObjectDynamicDTO,
                                    metaData, businessObjectAttrDynamicDTOListToSave, busObjRelationshipDynamicDTOListToBeSaved,
                                    busObjUniqueConstraintsDynamicDTOListToBeSaved, boListToGenerateMetadata, alreadyGeneratedMetadataRelationships,
                                    alreadyGeneratedMetadataBusinessObjects, exclusionList, serviceDBConnection, executionContext);
                        } else {
                            // table name not found in database connection metadata with lower case
                            // now try checking table name with upper case
                            try (ResultSet tablesResultSetUpperCase = metaData.getTables(databaseName.toUpperCase(), schemaName.toUpperCase(), tableName.toUpperCase(), null)) {
                                if (tablesResultSetUpperCase.next()) {
                                    tableName = tableName.toUpperCase();
                                    businessObjectDynamicDTO.put(DSPDMConstants.DATABASE_NAME, databaseName.toUpperCase());
                                    businessObjectDynamicDTO.put(DSPDMConstants.SCHEMA_NAME, schemaName.toUpperCase());
                                    generateMetadataForSingleTable_generatedMetadataForExistingTable(tableName, businessObjectDynamicDTO,
                                            metaData, businessObjectAttrDynamicDTOListToSave, busObjRelationshipDynamicDTOListToBeSaved,
                                            busObjUniqueConstraintsDynamicDTOListToBeSaved, boListToGenerateMetadata,
                                            alreadyGeneratedMetadataRelationships,alreadyGeneratedMetadataBusinessObjects,
                                            exclusionList, serviceDBConnection, executionContext);
                                } else {
                                    throw new DSPDMException("Cannot generate metadata for table '{}'. Reason table does not exist", Locale.getDefault(), tableName);
                                }
                            }
                        }
                    }
                    String[] referenceTablePrefixes = ConfigProperties.getInstance().generate_metadata_reference_table_prefix.getPropertyValue().split(",");
                    for (String referenceTablePrefix : referenceTablePrefixes) {
                        if (CollectionUtils.startWithEqualIgnoreCase(tableName, referenceTablePrefix)) {
                            businessObjectDynamicDTO.put(DSPDMConstants.BoAttrName.IS_REFERENCE_TABLE, true);
                        }
                    }
                }
                // now all the metadata objects have be created but they are not yet saved.
                //saving business object
                rootSaveResultDTO.addResult(saveOrUpdate(boListToGenerateMetadata, false, serviceDBConnection, executionContext));
                // restore business object group info (if available) in the history tables
                IDynamicDAOImpl businessObjectGroupDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUSINESS_OBJECT_GROUP, executionContext);
                List<DynamicDTO> restoredBusinessObjectGroupsList = restoreBusinessObjectGroupFromHistory(boListToGenerateMetadata, serviceDBConnection, executionContext);
                businessObjectGroupDAO.saveOrUpdate(restoredBusinessObjectGroupsList, false, serviceDBConnection, executionContext);
                if(CollectionUtils.hasValue(restoredBusinessObjectGroupsList)){
                    // making sure bus obj group information is available in the read back
                    saveBusinessObjectGroupInformationToTheBoList(boListToGenerateMetadata, restoredBusinessObjectGroupsList);
                }
                //rootSaveResultDTO.addResult(saveOrUpdate(boListToGenerateMetadata, false, metadataDBConnection, executionContext));
                //now saving children . save all children of current children type
                IDynamicDAOImpl businessObjectAttrDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, executionContext);
                businessObjectAttrDAO.saveOrUpdate(businessObjectAttrDynamicDTOListToSave, false, serviceDBConnection, executionContext);
                //createBusinessObjectAttrMetadata_generateMetadataForExistingTable(boListToGenerateMetadata, connection, executionContext);
                // Going to create relationship DTOs while considering current boName as parent. Hence we are finding children of current parent.
                // save all relationships using relationship dynamic dao
                IDynamicDAOImpl busObjRelationshipDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, executionContext);
                busObjRelationshipDAO.saveOrUpdate(busObjRelationshipDynamicDTOListToBeSaved, false, serviceDBConnection, executionContext);
                updateBusinessObjectAttributesAddReferenceData_generateMetadataForExistingTable(boListToGenerateMetadata, businessObjectAttrDynamicDTOListToSave, serviceDBConnection, executionContext);
                // save all unique constraints using unique constraints dynamic dao
                IDynamicDAOImpl busObjAttrUniqueConstraintDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, executionContext);
                busObjAttrUniqueConstraintDAO.saveOrUpdate(busObjUniqueConstraintsDynamicDTOListToBeSaved, false, serviceDBConnection, executionContext);
                //logging
                int operationId = 0;
                if (executionContext.getGenerateMetadataForAll()) {
                    operationId = DSPDMConstants.BusinessObjectOperations.GENERATE_METADATA_FOR_ALL_OPR.getId();
                } else {
                    operationId = DSPDMConstants.BusinessObjectOperations.GENERATE_METADATA_OPR.getId();
                }
                trackChangeHistory(rootSaveResultDTO, operationId, boListToGenerateMetadata, serviceDBConnection, executionContext);
            } catch (java.sql.SQLException e) {
                DSPDMException.throwException(e, executionContext);
            }
        }
        return rootSaveResultDTO;
    }


    /**
     * it will create just objects in memory. it will not call any save or update operation
     * it will create attributes metadata but unsaved
     * it will create relationship metadata but unsaved
     * it will createunique constraints metadata but unsaved
     *
     * @param tableName
     * @param businessObjectDynamicDTO
     * @param metaData
     * @param busObjAttrDynamicDTOListToBeSaved
     * @param busObjRelationshipDynamicDTOListToBeSaved
     * @param executionContext
     */
    private  void generateMetadataForSingleTable_generatedMetadataForExistingTable(String tableName, DynamicDTO businessObjectDynamicDTO,
                                                                                         DatabaseMetaData metaData, List<DynamicDTO> busObjAttrDynamicDTOListToBeSaved,
                                                                                         List<DynamicDTO> busObjRelationshipDynamicDTOListToBeSaved,
                                                                                         List<DynamicDTO> busObjUniqueConstraintsDynamicDTOListToBeSaved,
                                                                                         List<DynamicDTO> boListToGenerateMetadata,
                                                                                         List<String> alreadyGeneratedMetadataRelationships,
                                                                                         Map<String, DynamicDTO> alreadyGeneratedMetadataBusinessObjects,
                                                                                         Set<String> exclusionList, Connection connection,
                                                                                         ExecutionContext executionContext) {
        // prepare attributes metadata
        List<DynamicDTO> generatedMetadataForAttributes = generateAttributesMetadata_generatedMetadataForExistingTable(tableName, businessObjectDynamicDTO, metaData, executionContext);
        //DynamicDAOFactory.getInstance(executionContext).getBusinessObjects(executionContext).addAll(generatedMetadataForAttributes);
        restoreAttributesUnitsData_generatedMetadataForExistingTable(generatedMetadataForAttributes, connection, executionContext);
        // prepare relationships (columns)
        List<DynamicDTO> generatedParentRelationships = generateRelationshipsMetadata_generatedMetadataForExistingTable(tableName, false,
                businessObjectDynamicDTO, generatedMetadataForAttributes, metaData, null,
                boListToGenerateMetadata, alreadyGeneratedMetadataRelationships, alreadyGeneratedMetadataBusinessObjects,
                exclusionList, executionContext);
        // child relationships
        List<DynamicDTO> generatedChildRelationships = generateRelationshipsMetadata_generatedMetadataForExistingTable(tableName, true,
                businessObjectDynamicDTO, generatedMetadataForAttributes, metaData, generatedParentRelationships,
                boListToGenerateMetadata, alreadyGeneratedMetadataRelationships, alreadyGeneratedMetadataBusinessObjects,
                exclusionList, executionContext);
        // merge both parent and child together
        List<DynamicDTO> combinedParentChildRelationships = new ArrayList<>(generatedParentRelationships.size() + generatedChildRelationships.size());
        combinedParentChildRelationships.addAll(generatedParentRelationships);
        combinedParentChildRelationships.addAll(generatedChildRelationships);
        // generate unique constraints
        List<DynamicDTO> generateMetadataUniqueConstraints = generateUniqueConstraintsMetadata_generatedMetadataForExistingTable(tableName, businessObjectDynamicDTO, metaData,
                generatedMetadataForAttributes, executionContext);
        // create child map and put in business object DTO
        Map<String, Object> childrenMapToSave = new LinkedHashMap<>();
        childrenMapToSave.put(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, generatedMetadataForAttributes);
        childrenMapToSave.put(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, combinedParentChildRelationships);
        childrenMapToSave.put(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, generateMetadataUniqueConstraints);
        businessObjectDynamicDTO.putWithOrder(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY, childrenMapToSave);
        // add attribute and relationships to the global list.
        busObjAttrDynamicDTOListToBeSaved.addAll(generatedMetadataForAttributes);
        busObjRelationshipDynamicDTOListToBeSaved.addAll(combinedParentChildRelationships);
        busObjUniqueConstraintsDynamicDTOListToBeSaved.addAll(generateMetadataUniqueConstraints);
        // adding already generated bo to the list. This will help us to identify already generated relationships/PKs
        alreadyGeneratedMetadataBusinessObjects.put(tableName, businessObjectDynamicDTO);
    }

    private List<DynamicDTO> generateAttributesMetadata_generatedMetadataForExistingTable(
            String tableName,
            DynamicDTO businessObjectDynamicDTO,
            DatabaseMetaData metaData,
            ExecutionContext executionContext) {
        List<DynamicDTO> attributesMetadata = new ArrayList<>();
        ResultSet pkResultSet = null;
        ResultSet columnResultSet = null;
        String boName = (String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        try {
            IDynamicDAO boAttrNameDAO = DynamicDAOFactory.getInstance(executionContext)
                    .getDynamicDAO(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, executionContext);
            String databaseName = ((String) businessObjectDynamicDTO.get(DSPDMConstants.DATABASE_NAME));
            String schemaName = ((String) businessObjectDynamicDTO.get(DSPDMConstants.SCHEMA_NAME));
            pkResultSet = metaData.getPrimaryKeys(databaseName, schemaName, tableName);
            columnResultSet = metaData.getColumns(databaseName, schemaName, tableName, null);
            while (columnResultSet.next()) {
                DynamicDTO attributeDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, boAttrNameDAO.getPrimaryKeyColumnNames(), executionContext);
                // fill business object attribute dynamic dto from parent business object dto
                attributeDynamicDTO.put(DSPDMConstants.BoAttrName.BO_NAME, businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME));
                attributeDynamicDTO.put(DSPDMConstants.BoAttrName.BO_DISPLAY_NAME, businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_DISPLAY_NAME));
                attributeDynamicDTO.put(DSPDMConstants.BoAttrName.ENTITY, businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY));
                // fill from database metadata for the current column
                generateOneAttributeMetadataFromDB(tableName, attributeDynamicDTO, columnResultSet, boAttrNameDAO, executionContext);
                // set control type
                generateMetadataControlTypeForAttributes(attributeDynamicDTO, boAttrNameDAO);
                // set primary key flag
                List<String> primaryKeyColumnNamesFromDB = getPrimaryKeyColumnNamesFromDB(pkResultSet, boAttrNameDAO,
                        boName, executionContext);
                String currentColumnName = (String) attributeDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE);
                if (CollectionUtils.containsIgnoreCase(primaryKeyColumnNamesFromDB, currentColumnName)) {
                    attributeDynamicDTO.put(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY, true);
                    attributeDynamicDTO.put(DSPDMConstants.BoAttrName.IS_UPLOAD_NEEDED, false);
                    attributeDynamicDTO.put(DSPDMConstants.BoAttrName.IS_HIDDEN, true);
                    attributeDynamicDTO.put(DSPDMConstants.BoAttrName.IS_INTERNAL, true);
                } else {
                    attributeDynamicDTO.put(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY, false);
                }
                // add to list
                attributesMetadata.add(attributeDynamicDTO);
            }
        } catch (Throwable exception) {
            DSPDMException.throwException(exception, executionContext);
        } finally {
            closeResultSet(pkResultSet, executionContext);
            closeResultSet(columnResultSet, executionContext);
        }
        return attributesMetadata;
    }

    private void restoreAttributesUnitsData_generatedMetadataForExistingTable(List<DynamicDTO> generatedMetadataForAttributes, Connection connection, ExecutionContext executionContext){
        String boName = (String) generatedMetadataForAttributes.get(0).get(DSPDMConstants.BoAttrName.BO_NAME);
        List<String[]> attributeVsUnitMappings = getAttributeUnitMapping(boName, connection, executionContext);
        // String[0] has attribute name and String[1] has unit
        for(String[] mapping : attributeVsUnitMappings){
            DynamicDTO attrDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(generatedMetadataForAttributes, DSPDMConstants.BoAttrName.BO_ATTR_NAME, mapping[0]);
            attrDTO.put(DSPDMConstants.BoAttrName.UNIT, mapping[1]);
        }
    }

    private static List<DynamicDTO> filterRelationshipsForTableName(String childTableName, String parentTableName, List<DynamicDTO> relationships) {
        List<DynamicDTO> filteredRelationships = new ArrayList<>();
        if (relationships != null) {
            for (DynamicDTO dynamicDTO : relationships) {
                if (((childTableName == null) || ((String) dynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_ENTITY_NAME)).equalsIgnoreCase(childTableName))
                        && ((parentTableName == null) || ((String) dynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_ENTITY_NAME)).equalsIgnoreCase(parentTableName))) {
                    filteredRelationships.add(dynamicDTO);
                }
            }
        }
        return filteredRelationships;
    }

    private List<DynamicDTO> generateRelationshipsMetadata_generatedMetadataForExistingTable(
            String currentTableName, boolean isParent, DynamicDTO businessObjectDynamicDTO,
            List<DynamicDTO> generatedMetadataForAttributeList, DatabaseMetaData metaData,
            List<DynamicDTO> generatedParentRelationships, List<DynamicDTO> boListToGenerateMetadata,
            List<String> alreadyGeneratedMetadataRelationships, Map<String, DynamicDTO> alreadyGeneratedMetadataBusinessObjects,
            Set<String> exclusionList, ExecutionContext executionContext) {
        List<DynamicDTO> recursiveRelationships = filterRelationshipsForTableName(currentTableName, currentTableName, generatedParentRelationships);
        List<DynamicDTO> generatedRelationshipMetadata = new ArrayList<>();
        ResultSet relationshipResultSet = null;
        try {
            String schemaName = ((String) businessObjectDynamicDTO.get(DSPDMConstants.SCHEMA_NAME));
            String databaseName = ((String) businessObjectDynamicDTO.get(DSPDMConstants.DATABASE_NAME));
            String boName = (String)businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
            IDynamicDAO boBusObjRelationshipDAO = DynamicDAOFactory.getInstance(executionContext)
                    .getDynamicDAO(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, executionContext);
            if (isParent) {
                relationshipResultSet = metaData.getCrossReference(databaseName, schemaName, currentTableName, databaseName, schemaName, null);
            } else {
                relationshipResultSet = metaData.getCrossReference(databaseName, schemaName, null, databaseName, schemaName, currentTableName);
            }
            HashMap<String, DynamicDTO> busObjConstraintRelationshipMap = new HashMap<>();
            List<DynamicDTO> businessObjects = DynamicDAOFactory.getInstance(executionContext).getBusinessObjects(executionContext);

            while (relationshipResultSet.next()) {
                boolean isRecursiveRelationship = false;
                // get result set column name for parent table name
                String columnNameForParentTableName = DSPDMConstants.REVERSE_ENGINEER_METADATA.BusObjRelationshipToColumnNameMapping.getColumnName(
                        DSPDMConstants.BoAttrName.PARENT_BO_NAME, boBusObjRelationshipDAO.isMSSQLServerDialect(boName));
                // result set column name for parent table name
                String columnNameForChildTableName = DSPDMConstants.REVERSE_ENGINEER_METADATA.BusObjRelationshipToColumnNameMapping.getColumnName(
                        DSPDMConstants.BoAttrName.CHILD_BO_NAME, boBusObjRelationshipDAO.isMSSQLServerDialect(boName));
                // read values from result set
                String parentTableName = ((String) relationshipResultSet.getObject(columnNameForParentTableName));
                String childTableName = ((String) relationshipResultSet.getObject(columnNameForChildTableName));
                if (executionContext.getGenerateMetadataForAll()) {
                    // if any table (child or parent) found in relationship and is in exclusion list as well, we need to skip that relationship
                    if (exclusionList.contains(parentTableName) || exclusionList.contains(childTableName)) {
                        continue;
                    }
                }
                if (!(parentTableName.equalsIgnoreCase(childTableName))) {
                    // first check that the parent business object exists in db
                    Optional<DynamicDTO> parentBusinessObjectDTO = businessObjects.stream().filter(dynamicDTO -> parentTableName.equalsIgnoreCase((String) dynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY))).findFirst();
                    if (!parentBusinessObjectDTO.isPresent()) {
                        // parent business object does not exist in db so now checking from current list in memory
                        parentBusinessObjectDTO = boListToGenerateMetadata.stream().filter(dynamicDTO -> parentTableName.equalsIgnoreCase((String) dynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY))).findFirst();
                        if (!parentBusinessObjectDTO.isPresent()) {
                            throw new DSPDMException("Cannot generate metadata for child entity name '{}' due to a relationship failure because business object does not exist for parent entity name '{}'", executionContext.getExecutorLocale(), childTableName, parentTableName);
                        }
                    }
                } else {
                    // handling recursive relationship case
                    if (CollectionUtils.hasValue(recursiveRelationships)) {
                        String columnNameForChildAttribute = DSPDMConstants.REVERSE_ENGINEER_METADATA.BusObjRelationshipToColumnNameMapping.getColumnName(
                                DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME, boBusObjRelationshipDAO.isMSSQLServerDialect(boName));
                        String childAttributeName = (String) relationshipResultSet.getObject(columnNameForChildAttribute);
                        List<String> childAttributeNamesFromRecursiveRelationships = CollectionUtils.getStringValuesFromList(recursiveRelationships, DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
                        if (CollectionUtils.containsIgnoreCase(childAttributeNamesFromRecursiveRelationships, childAttributeName)) {
                            // checking if the current recursive relationship has already been made, in that case, skipping that by calling next relationship
                            logger.info("Recursive relationship already reverse engineered with parent relationships for entity name '{}'", parentTableName);
                            continue;
                        }
                    }
                    isRecursiveRelationship = true;
                }

                boolean skipThisRelationship = false;
                DynamicDTO relationshipDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, boBusObjRelationshipDAO.getPrimaryKeyColumnNames(), executionContext);
                for (DSPDMConstants.REVERSE_ENGINEER_METADATA.BusObjRelationshipToColumnNameMapping mapping : DSPDMConstants.REVERSE_ENGINEER_METADATA.BusObjRelationshipToColumnNameMapping.values()) {
                    //String columnNameInMetadataResultSet = (boBusObjRelationshipDAO.isMSSQLServerDialect()) ? mapping.getMsSQLServerColumnName() : mapping.getPostgresColumnName();
                    switch (mapping) {
                        case BUS_OBJ_RELATIONSHIP_NAME:
                            // For composite key check we have to see if we get duplication in constraint name, then relationship name should be same for both of the constraints
                            // Also, the relationship name will be appended with the current (duplicate constraint name) and will be saved
                            String columnNameForChildEntity = DSPDMConstants.REVERSE_ENGINEER_METADATA.BusObjRelationshipToColumnNameMapping.getColumnName(
                                    DSPDMConstants.BoAttrName.CHILD_ENTITY_NAME, boBusObjRelationshipDAO.isMSSQLServerDialect(boName));
                            String columnNameForChildAttribute = DSPDMConstants.REVERSE_ENGINEER_METADATA.BusObjRelationshipToColumnNameMapping.getColumnName(
                                    DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME, boBusObjRelationshipDAO.isMSSQLServerDialect(boName));
                            String columnNameForConstraintName = DSPDMConstants.REVERSE_ENGINEER_METADATA.BusObjRelationshipToColumnNameMapping.getColumnName(
                                    DSPDMConstants.BoAttrName.CONSTRAINT_NAME, boBusObjRelationshipDAO.isMSSQLServerDialect(boName));
                            String columnValueForConstraintName = (String) relationshipResultSet.getObject(columnNameForConstraintName);
                            // if(true) means this constraint name is already appeared before so it means this is a composite relationship. What we'll be doing is
                            // appending the latest child_bo_attr_name with the existing relationship that will return us a new relationship name. Now save
                            // the constraint with new relationship and and update any previous existence of the existing relationship name
                            if (CollectionUtils.containsKeyIgnoreCase(busObjConstraintRelationshipMap, columnValueForConstraintName)) {
                                DynamicDTO alreadyGeneratedRelationshipDto = busObjConstraintRelationshipMap.get(columnValueForConstraintName);
                                String existingRelationshipName = (String) alreadyGeneratedRelationshipDto.get(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME);
                                // already FK appended since its existingRelationshipName so no need of append of 'FK_'
                                String newRelationshipName = existingRelationshipName + "_" + relationshipResultSet.getObject(columnNameForChildAttribute);
                                //replacing old relationship name with new one
                                relationshipDynamicDTO.put(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME, newRelationshipName);
                                alreadyGeneratedRelationshipDto.put(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME, newRelationshipName);
                            } else {
                                // if relationship is already starting with fk, no need to create a new one, use existing relationship name
                                if (columnValueForConstraintName != null && columnValueForConstraintName.toLowerCase().startsWith("fk")) {
                                    relationshipDynamicDTO.put(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME, columnValueForConstraintName);
                                    busObjConstraintRelationshipMap.put(columnValueForConstraintName, relationshipDynamicDTO);
                                } else {
                                    String relationshipName = "FK_" + relationshipResultSet.getObject(columnNameForChildEntity) + "_" + relationshipResultSet.getObject(columnNameForChildAttribute);
                                    busObjConstraintRelationshipMap.put(columnValueForConstraintName, relationshipDynamicDTO);
                                    relationshipDynamicDTO.put(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME, relationshipName);
                                }
                            }
                            break;
                        case IS_PRIMARY_KEY_RELATIONSHIP:
                            if (isParent) {
                                String currentColumnName = DSPDMConstants.REVERSE_ENGINEER_METADATA.BusObjRelationshipToColumnNameMapping.getColumnName(
                                        DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME, boBusObjRelationshipDAO.isMSSQLServerDialect(boName));
                                String pkColumnName = ((String) relationshipResultSet.getObject(currentColumnName));
                                List<DynamicDTO> pkDtoList = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(
                                        generatedMetadataForAttributeList, DSPDMConstants.BoAttrName.IS_PRIMARY_KEY, true);
                                List<String> pkAttributeNames = CollectionUtils.getStringValuesFromList(pkDtoList, DSPDMConstants.BoAttrName.ATTRIBUTE);
                                if (CollectionUtils.containsIgnoreCase(pkAttributeNames, pkColumnName)) {
                                    relationshipDynamicDTO.put(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, Boolean.TRUE);
                                } else {
                                    relationshipDynamicDTO.put(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, Boolean.FALSE);
                                }
                            } else {
                                String rsColumnNameForParentAttributeName = DSPDMConstants.REVERSE_ENGINEER_METADATA.BusObjRelationshipToColumnNameMapping.getColumnName(
                                        DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME, boBusObjRelationshipDAO.isMSSQLServerDialect(boName));
                                String parentAttributeName = ((String) relationshipResultSet.getObject(rsColumnNameForParentAttributeName));
                                // if the relationship is recursive, we don't have the newly created bo in dynamic factory,
                                // so iterating the current bo for getting primary key information
                                if (isRecursiveRelationship) {
                                    boolean isPKRelationship = false;
                                    List<DynamicDTO> pkDTOList = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(
                                            generatedMetadataForAttributeList, DSPDMConstants.BoAttrName.IS_PRIMARY_KEY, true);
                                    if (CollectionUtils.hasValue(pkDTOList)) {
                                        List<String> pkAttributeNames = CollectionUtils.getStringValuesFromList(pkDTOList, DSPDMConstants.BoAttrName.ATTRIBUTE);
                                        if (CollectionUtils.containsIgnoreCase(pkAttributeNames, parentAttributeName)) {
                                            isPKRelationship = true;
                                        }
                                    }
                                    relationshipDynamicDTO.put(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, isPKRelationship);
                                } else {
                                    if (executionContext.getGenerateMetadataForAll()) {
                                        DynamicDTO alreadyGeneratedDynamicDTO = alreadyGeneratedMetadataBusinessObjects.get(parentTableName);
                                        if (alreadyGeneratedDynamicDTO != null) {
                                            Map<String, Object> childrenMap = (Map<String, Object>) alreadyGeneratedDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
                                            List<DynamicDTO> parentTableAttrList = (List<DynamicDTO>) childrenMap.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
                                            List<DynamicDTO> parentPkAttrList = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(parentTableAttrList, DSPDMConstants.BoAttrName.IS_PRIMARY_KEY, true);
                                            if (CollectionUtils.hasValue(parentPkAttrList)) {
                                                List<String> parentPkColumnNames = CollectionUtils.getStringValuesFromList(parentPkAttrList, DSPDMConstants.BoAttrName.ATTRIBUTE);
                                                if (CollectionUtils.containsIgnoreCase(parentPkColumnNames, parentAttributeName)) {
                                                    relationshipDynamicDTO.put(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, Boolean.TRUE);
                                                } else {
                                                    relationshipDynamicDTO.put(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, Boolean.FALSE);
                                                }
                                            } else {
                                                relationshipDynamicDTO.put(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, Boolean.FALSE);
                                            }
                                        } else {
                                            relationshipDynamicDTO.put(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, Boolean.FALSE);
                                        }
                                    } else {
                                        // checking first that if the parent table part of the request then use it
                                        DynamicDTO alreadyGeneratedDynamicDTO = alreadyGeneratedMetadataBusinessObjects.get(parentTableName);
                                        if (alreadyGeneratedDynamicDTO != null) {
                                            Map<String, Object> childrenMap = (Map<String, Object>) alreadyGeneratedDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
                                            List<DynamicDTO> parentTableAttrList = (List<DynamicDTO>) childrenMap.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
                                            List<DynamicDTO> parentPkAttrList = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(parentTableAttrList, DSPDMConstants.BoAttrName.IS_PRIMARY_KEY, true);
                                            if (CollectionUtils.hasValue(parentPkAttrList)) {
                                                List<String> parentPkColumnNames = CollectionUtils.getStringValuesFromList(parentPkAttrList, DSPDMConstants.BoAttrName.ATTRIBUTE);
                                                if (CollectionUtils.containsIgnoreCase(parentPkColumnNames, parentAttributeName)) {
                                                    relationshipDynamicDTO.put(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, Boolean.TRUE);
                                                } else {
                                                    relationshipDynamicDTO.put(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, Boolean.FALSE);
                                                }
                                            } else {
                                                relationshipDynamicDTO.put(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, Boolean.FALSE);
                                            }
                                        } else {
                                            Optional<DynamicDTO> parentBusinessObjectDTO = businessObjects.stream().filter(dynamicDTO ->
                                                    parentTableName.equalsIgnoreCase((String) dynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY))).findFirst();
                                            if (!parentBusinessObjectDTO.isPresent()) {
                                                throw new DSPDMException("Cannot generate metadata for child entity name '{}' due to a relationship failure because business object does not exist for parent entity name '{}'", executionContext.getExecutorLocale(), childTableName, parentTableName);
                                            } else {
                                                // parent table exists in db
                                                List<String> primaryKeyColumnNames = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO((String)
                                                        parentBusinessObjectDTO.get().get(DSPDMConstants.BoAttrName.BO_NAME), executionContext).getPrimaryKeyColumnNames();
                                                if (CollectionUtils.containsIgnoreCase(primaryKeyColumnNames, parentAttributeName)) {
                                                    relationshipDynamicDTO.put(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, Boolean.TRUE);
                                                } else {
                                                    relationshipDynamicDTO.put(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, Boolean.FALSE);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            break;
                        case PARENT_BO_NAME:
                            String parentBoName = null;
                            if ((isParent) && (StringUtils.hasValue((String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME)))) {
                                // parent is same
                                parentBoName = (String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
                            } else {
                                // find parent in request
                                Optional<DynamicDTO> parentBusinessObjectDTO = boListToGenerateMetadata.stream().filter(dynamicDTO ->
                                        parentTableName.equalsIgnoreCase((String) dynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY))).findFirst();
                                if (parentBusinessObjectDTO.isPresent()) {
                                    parentBoName = (String) parentBusinessObjectDTO.get().get(DSPDMConstants.BoAttrName.BO_NAME);
                                } else {
                                    // find parent in db
                                    parentBusinessObjectDTO = businessObjects.stream().filter(dynamicDTO ->
                                            parentTableName.equalsIgnoreCase((String) dynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY))).findFirst();
                                    if (parentBusinessObjectDTO.isPresent()) {
                                        parentBoName = (String) parentBusinessObjectDTO.get().get(DSPDMConstants.BoAttrName.BO_NAME);
                                    } else if (!(childTableName.equalsIgnoreCase(parentTableName))) {
                                        // if not recursive relationship then throw error
                                        // parent business object does not exist in db so skipping the relationship.
                                        throw new DSPDMException("Cannot generate metadata for child entity name '{}' "
                                                + "due to a relationship failure because parent business object metadata "
                                                + "does not exist for parent entity name '{}'",
                                                executionContext.getExecutorLocale(), childTableName, parentTableName);
                                    }
                                }
                            }
                            if (StringUtils.isNullOrEmpty(parentBoName)) {
                                // generate from table name
                                String parentBoNameColumnName = (boBusObjRelationshipDAO.isMSSQLServerDialect(boName)) ? mapping.getMsSQLServerColumnName() : mapping.getPostgresColumnName();
                                parentBoName = ((String) relationshipResultSet.getObject(parentBoNameColumnName)).replaceAll("_", " ").toUpperCase();
                            }
                            relationshipDynamicDTO.put(mapping.getBoBusObjRelationshipAttrName(), parentBoName);
                            break;
                        case CHILD_BO_NAME:
                            String childBoName = null;
                            if ((!isParent) && (StringUtils.hasValue((String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME)))) {
                                // child is same current
                                childBoName = (String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
                            } else {
                                // find parent in request
                                Optional<DynamicDTO> childBusinessObjectDTO = boListToGenerateMetadata.stream().filter(dynamicDTO ->
                                        childTableName.equalsIgnoreCase((String) dynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY))).findFirst();
                                if (childBusinessObjectDTO.isPresent()) {
                                    childBoName = (String) childBusinessObjectDTO.get().get(DSPDMConstants.BoAttrName.BO_NAME);
                                } else {
                                    // find parent in db
                                    childBusinessObjectDTO = businessObjects.stream().filter(dynamicDTO ->
                                            childTableName.equalsIgnoreCase((String) dynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY))).findFirst();
                                    if (childBusinessObjectDTO.isPresent()) {
                                        childBoName = (String) childBusinessObjectDTO.get().get(DSPDMConstants.BoAttrName.BO_NAME);
                                    } else if (!(childTableName.equalsIgnoreCase(parentTableName))) {
                                        // if recursive relationship then do not skip otherwise skip
                                        // child table metadata do not exist therefore skip this relationship to be saved
                                        skipThisRelationship = true;
                                        logger.info("A relationship has been skipped in metadata generation "
                                                        + "for parent entity name '{}' because child entity name '{}' metadata"
                                                        + " not found in db and not found in this request",
                                                executionContext.getExecutorLocale(), parentTableName, childTableName);
                                        // break inner loop started to generate columns data of relationship object
                                        break;
                                    }
                                }
                            }
                            if (StringUtils.isNullOrEmpty(childBoName)) {
                                // generate from table name
                                String childBoNameColumnName = (boBusObjRelationshipDAO.isMSSQLServerDialect(boName)) ? mapping.getMsSQLServerColumnName() : mapping.getPostgresColumnName();
                                childBoName = ((String) relationshipResultSet.getObject(childBoNameColumnName)).replaceAll("_", " ").toUpperCase();
                            }
                            relationshipDynamicDTO.put(mapping.getBoBusObjRelationshipAttrName(), childBoName);
                            break;
                        case PARENT_BO_ATTR_NAME:
                            String parentAttrName = null;
                            String parentAttrNameColumnName = (boBusObjRelationshipDAO.isMSSQLServerDialect(boName)) ? mapping.getMsSQLServerColumnName() : mapping.getPostgresColumnName();
                            parentAttrName = ((String) relationshipResultSet.getObject(parentAttrNameColumnName)).toUpperCase();
                            relationshipDynamicDTO.put(mapping.getBoBusObjRelationshipAttrName(), parentAttrName);
                            if (!(childTableName.equalsIgnoreCase(parentTableName))) {
                                if (!isParent) {
                                    // check if this attribute involved in relationship really exists in parent metadata. if not then throw error
                                    // checking first that if the parent table is already a part of the request then use it
                                    DynamicDTO alreadyGeneratedDynamicDTO = alreadyGeneratedMetadataBusinessObjects.get(parentTableName);
                                    if (alreadyGeneratedDynamicDTO != null) {
                                        Map<String, Object> childrenMap = (Map<String, Object>) alreadyGeneratedDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
                                        List<DynamicDTO> parentTableAttrList = (List<DynamicDTO>) childrenMap.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
                                        List<String> parentAttrList = CollectionUtils.getStringValuesFromList(parentTableAttrList, DSPDMConstants.BoAttrName.ATTRIBUTE);
                                        // throw error in case parent attribute does not exist and fail the process
                                        if (!(CollectionUtils.containsIgnoreCase(parentAttrList, parentAttrName))) {
                                            throw new DSPDMException("Cannot generate metadata for child entity name '{}' "
                                                    + "due to a relationship failure because parent business object attribute '{}' "
                                                    + "does not exist in parent entity '{}'", executionContext.getExecutorLocale()
                                                    , childTableName, parentAttrName, parentTableName);
                                        }
                                    } else {
                                        Optional<DynamicDTO> parentBusinessObjectDTO = businessObjects.stream().filter(dynamicDTO ->
                                                parentTableName.equalsIgnoreCase((String) dynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY))).findFirst();
                                        if (parentBusinessObjectDTO.isPresent()) {
                                            // parent table exists in db
                                            List<String> parentAttrList = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO((String)
                                                    parentBusinessObjectDTO.get().get(DSPDMConstants.BoAttrName.BO_NAME), executionContext).readMetadataBOAttrNames(executionContext);
                                            if (!(CollectionUtils.containsIgnoreCase(parentAttrList, parentAttrName))) {
                                                throw new DSPDMException("Cannot generate metadata for child entity name '{}' "
                                                        + "due to a relationship failure because parent business object attribute '{}' "
                                                        + "does not exist in parent entity '{}'", executionContext.getExecutorLocale()
                                                        , childTableName, parentAttrName, parentTableName);
                                            }
                                        } else {
                                            throw new DSPDMException("Cannot generate metadata for child entity name '{}' "
                                                    + "due to a relationship failure because parent business object '{}'"
                                                    + " does not exist", executionContext.getExecutorLocale(), childTableName, parentTableName);
                                        }
                                    }
                                }
                            }
                            break;
                        case CHILD_BO_ATTR_NAME:
                            String childAttrName = null;
                            String childAttrNameColumnName = (boBusObjRelationshipDAO.isMSSQLServerDialect(boName)) ? mapping.getMsSQLServerColumnName() : mapping.getPostgresColumnName();
                            childAttrName = ((String) relationshipResultSet.getObject(childAttrNameColumnName)).toUpperCase();
                            relationshipDynamicDTO.put(mapping.getBoBusObjRelationshipAttrName(), childAttrName);

                            if (!(childTableName.equalsIgnoreCase(parentTableName))) {
                                if (isParent) {
                                    // check if this attribute involved in relationship really exists in child metadata only in case child metadata already exists. if not then throw error
                                    // checking first that if the child table is already a part of the request then use it
                                    DynamicDTO alreadyGeneratedDynamicDTO = alreadyGeneratedMetadataBusinessObjects.get(childTableName);
                                    if (alreadyGeneratedDynamicDTO != null) {
                                        Map<String, Object> childrenMap = (Map<String, Object>) alreadyGeneratedDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
                                        List<DynamicDTO> childTableAttrList = (List<DynamicDTO>) childrenMap.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
                                        List<String> childAttrList = CollectionUtils.getStringValuesFromList(childTableAttrList, DSPDMConstants.BoAttrName.ATTRIBUTE);
                                        // throw error in case parent attribute does not exist and fail the process
                                        if (!(CollectionUtils.containsIgnoreCase(childAttrList, childAttrName))) {
                                            throw new DSPDMException("Cannot generate metadata for parent entity name '{}' "
                                                    + "due to a relationship failure because child business object '{}' exists in request"
                                                    + " but child business object attribute '{}' does not exist", executionContext.getExecutorLocale()
                                                    , parentTableName, childTableName, childAttrName);
                                        }
                                    } else {
                                        Optional<DynamicDTO> childBusinessObjectDTO = businessObjects.stream().filter(dynamicDTO ->
                                                childTableName.equalsIgnoreCase((String) dynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY))).findFirst();
                                        if (childBusinessObjectDTO.isPresent()) {
                                            // parent table exists in db
                                            List<String> childAttrList = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO((String)
                                                    childBusinessObjectDTO.get().get(DSPDMConstants.BoAttrName.BO_NAME), executionContext).readMetadataBOAttrNames(executionContext);
                                            if (!(CollectionUtils.containsIgnoreCase(childAttrList, childAttrName))) {
                                                throw new DSPDMException("Cannot generate metadata for parent entity name '{}' "
                                                        + "due to a relationship failure because child business object '{}' found"
                                                        + " but child business object attribute '{}' does not exist", executionContext.getExecutorLocale()
                                                        , parentTableName, childTableName, childAttrName);
                                            }
                                        } else {
                                            // if not recursive then skip
                                            // child table metadata do not exist therefore skip this relationship to be saved
                                            skipThisRelationship = true;
                                            logger.info("A relationship has been skipped in metadata generation "
                                                            + "for parent entity name '{}' because child entity name '{}' metadata"
                                                            + " not found in db and not found in this request",
                                                    executionContext.getExecutorLocale(), parentTableName, childTableName);
                                            // break inner loop started to generate columns data of relationship object
                                            break;
                                        }
                                    }
                                }
                            }
                            break;
                        case CHILD_ORDER:
                            String childBoNameOfRelationship = (String) relationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME);
                            String parentBoNameOfRelationship = (String) relationshipDynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_NAME);
                            Integer childOrderFromRequestPacket = getChildOrderFromAlreadyGeneratedRelationshipsFromRequestPacket(generatedRelationshipMetadata,
                                    parentBoNameOfRelationship, childBoNameOfRelationship);
                            if (childOrderFromRequestPacket == null) {
                                // means childOrder not found in request packet, now searching in db, and even if not found, will give
                                // childOrder value of sequence in which relationship is rea from DB.
                                Map<String, List<DynamicDTO>> childRelationships = null;
                                try {
                                    childRelationships = DynamicDAOFactory.getInstance(executionContext).
                                            getDynamicDAO(parentBoNameOfRelationship, executionContext).readMetadataChildRelationships(executionContext);
                                } catch (Exception e) {
                                    // eat exception because the relationship information is neither found in request
                                    // packet, nor in db, Now we'll give childOrder the max value i.e., least priority
                                }
                                if (CollectionUtils.hasValue(childRelationships)) {
                                    if(childRelationships.containsKey(childBoNameOfRelationship)){
                                        Integer childOrderFromDB = (Integer) childRelationships.get(childBoNameOfRelationship).get(0).get(DSPDMConstants.BoAttrName.CHILD_ORDER);
                                        relationshipDynamicDTO.put(DSPDMConstants.BoAttrName.CHILD_ORDER, childOrderFromDB);
                                    } else {
                                        // set default order to zero
                                        relationshipDynamicDTO.put(DSPDMConstants.BoAttrName.CHILD_ORDER, 0);
                                    }
                                } else {
                                    // set default order to zero
                                    relationshipDynamicDTO.put(DSPDMConstants.BoAttrName.CHILD_ORDER, 0);
                                }
                            } else {
                                relationshipDynamicDTO.put(DSPDMConstants.BoAttrName.CHILD_ORDER, childOrderFromRequestPacket);
                            }
                            break;
                        default:
                            String columnName = (boBusObjRelationshipDAO.isMSSQLServerDialect(boName)) ? mapping.getMsSQLServerColumnName() : mapping.getPostgresColumnName();
                            relationshipDynamicDTO.put(mapping.getBoBusObjRelationshipAttrName(), relationshipResultSet.getObject(columnName));
                    }
                }
                if (!skipThisRelationship) {
                    // this updated is applied with generateMetadataForAll and for generateMetadataForExistingTable. the following check is used to remove any redundant relationships.
                    String relationshipUix = ((String) relationshipDynamicDTO.get(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME) +
                            relationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME) + relationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME));
                    if (!alreadyGeneratedMetadataRelationships.contains(relationshipUix)) {
                        alreadyGeneratedMetadataRelationships.add(relationshipUix);
                        generatedRelationshipMetadata.add(relationshipDynamicDTO);
                    }
                }
            }
        } catch (Throwable exception) {
            DSPDMException.throwException(exception, executionContext);
        } finally {
            closeResultSet(relationshipResultSet, executionContext);
        }
        return generatedRelationshipMetadata;
    }

    private Integer getChildOrderFromAlreadyGeneratedRelationshipsFromRequestPacket(List<DynamicDTO> generatedRelationships,
                                                        String parentBoName, String childBoName){
        if(CollectionUtils.hasValue(generatedRelationships)){
            for(DynamicDTO dynamicDTO: generatedRelationships){
                String parentBoNameFromDTO = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_NAME);
                String childBoNameFromDTO = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME);
                if(parentBoName.equalsIgnoreCase(parentBoNameFromDTO) && childBoName.equalsIgnoreCase(childBoNameFromDTO)){
                    return (Integer) dynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_ORDER);
                }
            }
        }
        return null;
    }

    private static void generateOneAttributeMetadataFromDB(String tableName, DynamicDTO attributeDynamicDTO, ResultSet columnResultSet,
                                                           IDynamicDAO boAttrNameDAO, ExecutionContext executionContext) throws SQLException {
        // fill from database metadata fot the current column
        int sqlDataType;
        DSPDMConstants.DataTypes dataTypeFromSQLDataType = null;
        String boName = (String) attributeDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        for (DSPDMConstants.REVERSE_ENGINEER_METADATA.BoAttrNameToColumnNameMapping mapping : DSPDMConstants.REVERSE_ENGINEER_METADATA.BoAttrNameToColumnNameMapping.values()) {
            String columnNameInMetadataResultSet = (boAttrNameDAO.isMSSQLServerDialect(boName)) ? mapping.getMsSQLServerColumnName() : mapping.getPostgresColumnName();
            switch (mapping) {
                case ATTRIBUTE_DATATYPE:
                    sqlDataType = NumberUtils.convertToInteger(columnResultSet.getObject(columnNameInMetadataResultSet), executionContext);
                    dataTypeFromSQLDataType = DSPDMConstants.DataTypes.getDataTypeFromSQLDataType(sqlDataType);
                    if (dataTypeFromSQLDataType != null) {
                        String dataType = dataTypeFromSQLDataType.getAttributeDataType(boAttrNameDAO.isMSSQLServerDialect(boName));
                        if ((dataTypeFromSQLDataType.isStringDataTypeWithLength()) || (dataTypeFromSQLDataType.isFloatingDataType())) {
                            // now checking column length/size
                            DSPDMConstants.REVERSE_ENGINEER_METADATA.BoAttrNameToColumnNameMapping columnSizeMapping = DSPDMConstants.REVERSE_ENGINEER_METADATA.BoAttrNameToColumnNameMapping.MAX_ALLOWED_LENGTH;
                            columnNameInMetadataResultSet = (boAttrNameDAO.isMSSQLServerDialect(boName)) ? columnSizeMapping.getMsSQLServerColumnName() : columnSizeMapping.getPostgresColumnName();
                            Integer maxAllowedLength = NumberUtils.convertToInteger(columnResultSet.getObject(columnNameInMetadataResultSet), executionContext);
                            // now checking column decimal length/size
                            DSPDMConstants.REVERSE_ENGINEER_METADATA.BoAttrNameToColumnNameMapping decimalSizeMapping = DSPDMConstants.REVERSE_ENGINEER_METADATA.BoAttrNameToColumnNameMapping.MAX_ALLOWED_DECIMAL_PLACES;
                            columnNameInMetadataResultSet = (boAttrNameDAO.isMSSQLServerDialect(boName)) ? decimalSizeMapping.getMsSQLServerColumnName() : decimalSizeMapping.getPostgresColumnName();
                            Integer maxAllowedDecimalLength = NumberUtils.convertToInteger(columnResultSet.getObject(columnNameInMetadataResultSet), executionContext);
                            if ((maxAllowedLength != null) && (maxAllowedLength > 0)) {
                                if ((dataTypeFromSQLDataType.isFloatingDataType()) && ((maxAllowedDecimalLength != null) && (maxAllowedDecimalLength > 0))) {
                                    dataType = dataType + "(" + maxAllowedLength + "," + maxAllowedDecimalLength + ")";
                                } else {
                                    dataType = dataType + "(" + maxAllowedLength + ")";
                                }
                            }
                        }
                        attributeDynamicDTO.put(mapping.getBoAttrName(), dataType);
                    } else {
                        columnNameInMetadataResultSet = (boAttrNameDAO.isMSSQLServerDialect(boName)) ? DSPDMConstants.REVERSE_ENGINEER_METADATA.BoAttrNameToColumnNameMapping.ATTRIBUTE.getMsSQLServerColumnName() : DSPDMConstants.REVERSE_ENGINEER_METADATA.BoAttrNameToColumnNameMapping.ATTRIBUTE.getPostgresColumnName();
                        String tableColumnName = columnResultSet.getString(columnNameInMetadataResultSet);
                        throw new DSPDMException("Cannot generate metadata for table or view '{}'. Found an unsupported sql data type code '{}' for column name '{}'",
                                executionContext.getExecutorLocale(), tableName, sqlDataType, tableColumnName);
                    }
                    break;
                case ATTRIBUTE_DEFAULT:
                    Object attributeDefault = columnResultSet.getObject(columnNameInMetadataResultSet);
                    if (attributeDefault != null) {
                        if ((attributeDefault instanceof String) && (((String) attributeDefault).contains("("))) {
                            String defaultValue = ((String) attributeDefault).trim();
                            if ((defaultValue.charAt(0) == '(') && (defaultValue.charAt(defaultValue.length() - 1) == ')')) {
                                // this check is specially for MS SQL Server
                                // case sample value : ((1))
                                if ((defaultValue.charAt(1) == '(') && (defaultValue.charAt(defaultValue.length() - 2) == ')')) {
                                    defaultValue = defaultValue.substring(2, defaultValue.length() - 2);
                                } else {
                                    // case sample value : (NEXT VALUE FOR [seq_well])
                                    defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
                                }
                            }
                            // case sample value : nextval('well_view'::regclass)
                            // function or stored procedure call case
                            attributeDynamicDTO.put(mapping.getBoAttrName(), defaultValue);
                            // sql.append(DSPDMConstants.SPACE).append("DEFAULT ").append((String) businessObjectAttrDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DEFAULT)).append("");
                        } else if ((attributeDefault instanceof String) && (((String) attributeDefault).contains("::"))) {
                            // case sample value : 'test_well_name'::character varying
                            String defaultValue = ((String) attributeDefault).trim();
                            String[] splittedDefaultValue = defaultValue.split("::");
                            attributeDynamicDTO.put(mapping.getBoAttrName(), splittedDefaultValue[0].replaceAll("\'", ""));
                        } else {
                            // case sample value : 1
                            String attrType = (String) attributeDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE);
                            if ((MetadataUtils.isStringDataType(attrType, boName)) || (MetadataUtils.isDateTimeDataType(attrType, boName))) {
                                // string and date time case
                                attributeDynamicDTO.put(mapping.getBoAttrName(), "'" + attributeDefault + "'");
                                //sql.append(DSPDMConstants.SPACE).append("DEFAULT '").append((String) businessObjectAttrDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DEFAULT)).append("'");
                            } else if ((MetadataUtils.isNumberDataType(attrType, boName)) || (MetadataUtils.isBooleanDataType(attrType, boName))) {
                                // number and boolean
                                attributeDynamicDTO.put(mapping.getBoAttrName(), attributeDefault);
                                //sql.append(DSPDMConstants.SPACE).append("DEFAULT ").append((String) businessObjectAttrDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DEFAULT)).append("");
                            }
                        }
                    }
                    break;
                case IS_MANDATORY:
                    Object isNullable = columnResultSet.getObject(columnNameInMetadataResultSet);
                    if (isNullable != null) {
                        if (boAttrNameDAO.isMSSQLServerDialect(boName)) {
                            attributeDynamicDTO.put(mapping.getBoAttrName(), "YES".equalsIgnoreCase((String) isNullable) ? 0 : 1);
                            // The below case is to handle IS_NOT_NULL case. if mandatory field is yes, then the attribute should be IS_NOT_NULL as true
                            attributeDynamicDTO.put(DSPDMConstants.BoAttrName.IS_NOT_NULL, "YES".equalsIgnoreCase((String) isNullable) ? 0 : 1);
                        } else {
                            attributeDynamicDTO.put(mapping.getBoAttrName(), "YES".equalsIgnoreCase((String) isNullable) ? false : true);
                            // The below case is to handle IS_NOT_NULL case. if mandatory field is NO, then the attribute should be IS_NOT_NULL as false
                            attributeDynamicDTO.put(DSPDMConstants.BoAttrName.IS_NOT_NULL, "YES".equalsIgnoreCase((String) isNullable) ? false : true);
                        }
                    }
                    break;
                case ATTRIBUTE_DESC:
                    attributeDynamicDTO.put(mapping.getBoAttrName(), columnResultSet.getObject(columnNameInMetadataResultSet));
                    break;
                case SEQUENCE_NUM:
                    attributeDynamicDTO.put(mapping.getBoAttrName(), columnResultSet.getObject(columnNameInMetadataResultSet));
                    break;
                case ENTITY:
                case ATTRIBUTE:
                case BO_ATTR_NAME:
                    String name = (String) columnResultSet.getObject(columnNameInMetadataResultSet);
                    if (name != null) {
                        name = name.trim().toUpperCase();
                    }
                    attributeDynamicDTO.put(mapping.getBoAttrName(), name);
                    break;
                case ATTRIBUTE_DISPLAYNAME:
                    String attributeDisplayName = (String) columnResultSet.getObject(columnNameInMetadataResultSet);
                    if (attributeDisplayName != null) {
                        attributeDisplayName = StringUtils.toCamelCase(attributeDisplayName.trim().replaceAll("_"," "));
                    }
                    attributeDynamicDTO.put(mapping.getBoAttrName(), attributeDisplayName);
                    break;

                default:
                    attributeDynamicDTO.put(mapping.getBoAttrName(), columnResultSet.getObject(columnNameInMetadataResultSet));
            }
        }
    }

    private static List<String> getPrimaryKeyColumnNamesFromDB(ResultSet pkResultSet, IDynamicDAO boAttrNameDAO,String boName, ExecutionContext executionContext) throws SQLException {
        String columnNameForResultSet = DSPDMConstants.REVERSE_ENGINEER_METADATA.BoAttrNameToColumnNameMapping.getColumnName(
                DSPDMConstants.BoAttrName.BO_ATTR_NAME, boAttrNameDAO.isMSSQLServerDialect(boName));
        List<String> list = new ArrayList<>();
        while (pkResultSet.next()) {
            list.add((String) pkResultSet.getObject(columnNameForResultSet));
        }
        return list;
    }

    private static void generateMetadataControlTypeForAttributes(DynamicDTO attributeDynamicDTO, IDynamicDAO boAttrNameDAO) {
        String attributeDatatype = (String) attributeDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE);
        String boName = (String) attributeDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        DSPDMConstants.DataTypes dataType = DSPDMConstants.DataTypes.fromAttributeDataType(attributeDatatype, boAttrNameDAO.isMSSQLServerDialect(boName));
        attributeDynamicDTO.put(DSPDMConstants.BoAttrName.CONTROL_TYPE, dataType.getControlType());
    }

    private List<DynamicDTO> generateUniqueConstraintsMetadata_generatedMetadataForExistingTable(String tableName,
                                                                                                 DynamicDTO businessObjectDynamicDTO, DatabaseMetaData metaData,
                                                                                                 List<DynamicDTO> generatedMetadataForAttributeList, ExecutionContext executionContext) {
        List<DynamicDTO> uniqueConstraintsMetadataList = new ArrayList<>();
        ResultSet uniqueConstraintsResultSet = null;
        try {
            IDynamicDAO busObjAttrUniqueConstraintsDAO = DynamicDAOFactory.getInstance(executionContext)
                    .getDynamicDAO(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, executionContext);
            String schemaName = ((String) businessObjectDynamicDTO.get(DSPDMConstants.SCHEMA_NAME));
            String dataBaseName = ((String) businessObjectDynamicDTO.get(DSPDMConstants.DATABASE_NAME));
            uniqueConstraintsResultSet = metaData.getIndexInfo(dataBaseName, schemaName, tableName, true, false);
            DynamicDTO uniqueConstraintDynamicDTO = null;
            while (uniqueConstraintsResultSet.next()) {
                // fill from database metadata for the current column
                uniqueConstraintDynamicDTO = createOneUniqueConstraintDTO_generatedMetadataForExistingTable(uniqueConstraintsResultSet, businessObjectDynamicDTO,
                        generatedMetadataForAttributeList, busObjAttrUniqueConstraintsDAO, tableName, executionContext);
                if (uniqueConstraintDynamicDTO != null) {
                    uniqueConstraintsMetadataList.add(uniqueConstraintDynamicDTO);
                }
            }
        } catch (Throwable exception) {
            DSPDMException.throwException(exception, executionContext);
        } finally {
            closeResultSet(uniqueConstraintsResultSet, executionContext);
        }
        return uniqueConstraintsMetadataList;
    }

    private static DynamicDTO createOneUniqueConstraintDTO_generatedMetadataForExistingTable(ResultSet uniqueConstraintResultSet, DynamicDTO businessObjectDynamicDTO,
                                                                                             List<DynamicDTO> generatedMetadataForAttributeList, IDynamicDAO busObjAttrUniqueConstraintsDAO,
                                                                                             String tableName, ExecutionContext executionContext) throws SQLException {
        DynamicDTO uniqueConstraintDynamicDTO = null;
        String boName = (String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        String rsColumnNameForUniqueConstraintAttributeName = DSPDMConstants.REVERSE_ENGINEER_METADATA.BusObjAttrUniqueConstraintToColumnNameMapping.
                getColumnName(DSPDMConstants.BoAttrName.BO_ATTR_NAME, busObjAttrUniqueConstraintsDAO.isMSSQLServerDialect(boName));
        String attributeNameForUniqueConstraint = (String) uniqueConstraintResultSet.getObject(rsColumnNameForUniqueConstraintAttributeName);
        // case TABLE_CAT : dspdm_qa_demo,			TABLE_SCHEM : dbo,			TABLE_NAME : well_copy,			NON_UNIQUE : null,			INDEX_QUALIFIER : null,			INDEX_NAME : null,			TYPE : 0,			ORDINAL_POSITION : null,			COLUMN_NAME : null,			ASC_OR_DESC : null,			CARDINALITY : 0,			PAGES : 0,			FILTER_CONDITION : null,			,
        // in metadata service, very first row only return table catalog, table schema and table name, rest of the fields are null, to get the details regarding unique indexing, we need to bypass very first row.
        List<DynamicDTO> pkDTOList = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(generatedMetadataForAttributeList, DSPDMConstants.BoAttrName.IS_PRIMARY_KEY, true);
        List<String> pkAttributeNames = CollectionUtils.getStringValuesFromList(pkDTOList, DSPDMConstants.BoAttrName.ATTRIBUTE);
        // Ignoring primary key columns
        if (attributeNameForUniqueConstraint != null && !CollectionUtils.containsIgnoreCase(pkAttributeNames, attributeNameForUniqueConstraint)) {
            String rsColumnNameForConstraintName = DSPDMConstants.REVERSE_ENGINEER_METADATA.BusObjAttrUniqueConstraintToColumnNameMapping
                    .getColumnName(DSPDMConstants.BoAttrName.CONSTRAINT_NAME, busObjAttrUniqueConstraintsDAO.isMSSQLServerDialect(boName));
            String constraintName = (String) uniqueConstraintResultSet.getObject(rsColumnNameForConstraintName);
            // create new unique constraints DTO
            uniqueConstraintDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, busObjAttrUniqueConstraintsDAO.getPrimaryKeyColumnNames(), executionContext);
            uniqueConstraintDynamicDTO.put(DSPDMConstants.BoAttrName.BO_NAME, businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME));
            uniqueConstraintDynamicDTO.put(DSPDMConstants.BoAttrName.BO_ATTR_NAME, attributeNameForUniqueConstraint.toUpperCase());
            uniqueConstraintDynamicDTO.put(DSPDMConstants.BoAttrName.ENTITY, businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY));
            uniqueConstraintDynamicDTO.put(DSPDMConstants.BoAttrName.ATTRIBUTE, attributeNameForUniqueConstraint.toUpperCase());
            uniqueConstraintDynamicDTO.put(DSPDMConstants.BoAttrName.CONSTRAINT_NAME, constraintName);
        }
        return uniqueConstraintDynamicDTO;
    }

    @Override
    public SaveResultDTO addBusinessObjectGroup(List<DynamicDTO> busObjGroupsToBeCreated, Connection serviceDBConnection, ExecutionContext executionContext){
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        executionContext.setCurrentOperationId(DSPDMConstants.BusinessObjectOperations.ADD_BUS_OBJ_GROUP.getId());
        IDynamicDAOImpl dynamicDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUSINESS_OBJECT_GROUP, executionContext);
        saveResultDTO.addResult(dynamicDAO.saveOrUpdate(busObjGroupsToBeCreated, false, serviceDBConnection, executionContext));
        //logging
        trackChangeHistory(saveResultDTO, executionContext.getCurrentOperationId(), busObjGroupsToBeCreated, serviceDBConnection, executionContext);
        return saveResultDTO;
    }

    @Override
    public List<String> getAllEntityNamesListOfConnectedDB(DynamicDTO dynamicDTO, Set<String> exclusionList,
                                                            Connection dataModelDBConnection, ExecutionContext executionContext) {
        List<String> entityNamesList = null;
        LinkedList<String> listInRelationshipOrder = null;
        try {
            DatabaseMetaData metaData = dataModelDBConnection.getMetaData();
            // getting all table names of the provided/connected db
            entityNamesList = entityNamesList(metaData, dynamicDTO, exclusionList, executionContext);
            listInRelationshipOrder = new LinkedList<>();
            List<String[]> allRelationships = null;
            // now we'll be fetching relationship among the entityNamesList retrieved above and will sort in a way that
            // parent will always come before child (i.e., sorted on the basis of parent/child relationship)
            allRelationships = readAllRelationships(metaData, dynamicDTO, entityNamesList, exclusionList, executionContext);

            boolean isFirstEntry = true;
            for (String[] parentChildRelationship : allRelationships) {
                String parentTableName = parentChildRelationship[0];
                String childTableName = parentChildRelationship[1];
                if (isFirstEntry) {
                    isFirstEntry = false;
                    listInRelationshipOrder.add(parentTableName);
                    listInRelationshipOrder.add(childTableName);
                    entityNamesList.remove(parentTableName);
                    entityNamesList.remove(childTableName);
                    continue;
                }
                if (!listInRelationshipOrder.contains(parentTableName)) {
                    listInRelationshipOrder.addFirst(parentTableName);
                    entityNamesList.remove(parentTableName);
                }
                if (!listInRelationshipOrder.contains(childTableName)) {
                    listInRelationshipOrder.addLast(childTableName);
                    entityNamesList.remove(childTableName);
                }
                if (listInRelationshipOrder.contains(parentTableName) && !listInRelationshipOrder.contains(childTableName)) {
                    // putting child table just after parent table name
                    listInRelationshipOrder.add(listInRelationshipOrder.indexOf(parentTableName) + 1, childTableName);
                    entityNamesList.remove(childTableName);
                } else if (listInRelationshipOrder.contains(childTableName) && !listInRelationshipOrder.contains(parentTableName)) {
                    // putting parent table just before child table name
                    listInRelationshipOrder.add(listInRelationshipOrder.indexOf(childTableName), parentTableName);
                    entityNamesList.remove(parentTableName);
                } else if (listInRelationshipOrder.contains(parentTableName) && listInRelationshipOrder.contains(childTableName)) {
                    if (listInRelationshipOrder.indexOf(parentTableName) > listInRelationshipOrder.indexOf(childTableName)) {
                        listInRelationshipOrder.remove(parentTableName);
                        listInRelationshipOrder.add(listInRelationshipOrder.indexOf(childTableName), parentTableName);
                    }
                }
            }
        } catch (java.sql.SQLException e) {
            DSPDMException.throwException(e, executionContext);
        }
        // adding tables having no relationship to the start of the listInRelationshipOrder list
        listInRelationshipOrder.addAll(0, entityNamesList);
        return listInRelationshipOrder;
    }

    private List<String> entityNamesList(DatabaseMetaData databaseMetaData, DynamicDTO dynamicDTO, Set<String> exclusionList,
                                         ExecutionContext executionContext) {
        List<String> entityNamesList = new ArrayList<>();
        String[] types = null;
        String tableType = (String) dynamicDTO.get(DSPDMConstants.TABLE_TYPE);
        switch (tableType) {
            case DSPDMConstants.TABLE:
                types = new String[]{"TABLE"};
                break;
            case DSPDMConstants.VIEW:
                types = new String[]{"VIEW"};
                break;
            default:
                types = new String[]{"TABLE", "VIEW"};
        }

        try (ResultSet resultSet = databaseMetaData.getTables((String) dynamicDTO.get(DSPDMConstants.DATABASE_NAME),
                (String) dynamicDTO.get(DSPDMConstants.SCHEMA_NAME), null, types)) {
            while (resultSet.next()) {
                String tableName = (String) resultSet.getObject("TABLE_NAME");
                if (exclusionList == null || !exclusionList.contains(tableName)) {
                    entityNamesList.add(tableName);
                }
            }
        } catch (SQLException e) {
            DSPDMException.throwException(e, executionContext);
        }
        return entityNamesList;
    }

    private List<String[]> readAllRelationships(DatabaseMetaData databaseMetaData, DynamicDTO dynamicDTO,
                                                List<String> entityNamesList, Set<String> exclusionList,
                                                ExecutionContext executionContext) {
        List<String[]> parentChildRelationshipList = new ArrayList<>();
        String databaseName = (String) dynamicDTO.get(DSPDMConstants.DATABASE_NAME);
        String databaseSchemaName = (String) dynamicDTO.get(DSPDMConstants.SCHEMA_NAME);
        TreeSet<String> alreadyFetchedRelationships = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (String entity : entityNamesList) {
            // if an entity is in exclusion list, we're not calling for its relationship.
            if (exclusionList.contains(entity)) {
                continue;
            }
            try (ResultSet resultSet = databaseMetaData.getCrossReference(databaseName, databaseSchemaName, entity,
                    databaseName, databaseSchemaName, null);) {
                while (resultSet.next()) {
                    String parentTableName = (String) resultSet.getObject(DSPDMConstants.SchemaName.PKTABLE_NAME.toLowerCase());
                    String childTableName = (String) resultSet.getObject(DSPDMConstants.SchemaName.FOREIGNKEY_TABLE.toLowerCase());
                    if (exclusionList == null || !(((exclusionList.contains(parentTableName)) ||
                            exclusionList.contains(childTableName))) && !alreadyFetchedRelationships.contains(parentTableName + "_" + childTableName)) {
                        String[] parentChildRelationship = new String[2];
                        parentChildRelationship[0] = parentTableName;
                        parentChildRelationship[1] = childTableName;
                        parentChildRelationshipList.add(parentChildRelationship);
                        alreadyFetchedRelationships.add(parentTableName + "_" + childTableName);
                    }
                }
            } catch (SQLException e) {
                DSPDMException.throwException(e, executionContext);
            }
        }
        return parentChildRelationshipList;
    }

    /**
     * it will update restored business object group information for each bo in 'boListToGenerateMetadata' so that it can be available in the read back.
     *
     * @param boListToGenerateMetadata
     * @param restoredBusinessObjectGroupsList
     */
    private void saveBusinessObjectGroupInformationToTheBoList(List<DynamicDTO> boListToGenerateMetadata, List<DynamicDTO> restoredBusinessObjectGroupsList) {
        for (DynamicDTO dynamicDTO : boListToGenerateMetadata) {
            ((Map<String, Object>) dynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY)).put(DSPDMConstants.BoName.BUSINESS_OBJECT_GROUP
                    , CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(restoredBusinessObjectGroupsList,
                            DSPDMConstants.BoAttrName.BO_NAME, dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME)));
        }
    }
}
