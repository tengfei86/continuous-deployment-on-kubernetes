package com.lgc.dspdm.repo.delegate.metadata.boattr.write;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.config.ConnectionProperties;
import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAO;
import com.lgc.dspdm.core.dao.dynamic.businessobject.DynamicDAOFactory;

import java.util.*;

/**
 * @author Muhammad Imran Ansari
 */
public class MetadataBOAttrWriteDelegateImpl implements IMetadataBOAttrWriteDelegate {
    private static IMetadataBOAttrWriteDelegate singleton = null;

    private MetadataBOAttrWriteDelegateImpl() {

    }

    public static IMetadataBOAttrWriteDelegate getInstance() {
        if (singleton == null) {
            singleton = new MetadataBOAttrWriteDelegateImpl();
        }
        return singleton;
    }

    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */

    @Override
    public SaveResultDTO addCustomAttributes(List<DynamicDTO> boListToSave, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        Map<String, Integer> maxSeqNumForBoNameMap = new HashMap<>();

        if (CollectionUtils.hasValue(boListToSave)) {
            Map<String, BusinessObjectDTO> businessObjectsMap = DynamicDAOFactory.getInstance(executionContext).getBusinessObjectsMap(executionContext);

            IDynamicDAO boAttrDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, executionContext);
            List<DynamicDTO> boAttrUnitRelationshipListToSave = new ArrayList<>();
            for (DynamicDTO dynamicDTO : boListToSave) {
                String boName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
                BusinessObjectDTO businessObjectDTO = businessObjectsMap.get(boName);
                if(businessObjectDTO== null) {
                    throw new DSPDMException("Cannot add custom attribute. No business object found with name '{}'", executionContext.getExecutorLocale(), boName);
                }
                // fill dto values from parent dto
                fillCustomAttributeDTOFromBusinessObject(dynamicDTO, businessObjectDTO, executionContext);
            }
            // fix attribute data type as per the current sql dialect
            for(DynamicDTO dynamicDTO:boListToSave) {
                String dataType = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE);
                dataType = DSPDMConstants.DataTypes.fixDataTypeError(dataType);

				// default data types are for postgre
				// now we need to convert postgres data type to the sql server supported data type
				// to get the enum from string data type must send is ms sql as false
				String boName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
				String attributeDisplayName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME);
				dataType = getAppropriateDatabaseAttributeDataType(dataType, attributeDisplayName, boName, executionContext, boAttrDAO.isMSSQLServerDialect(boName));
				dynamicDTO.put(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE, dataType);
 
            }
            // save first the parent metadata attribute objects
            rootSaveResultDTO.addResult(boAttrDAO.addCustomAttributes(boListToSave, executionContext));
            // set data in read back if required
            if (executionContext.isReadBack()) {
                rootSaveResultDTO.addDataFromReadBack(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, boListToSave);
            }
            // now save the child objects metadata unit attribute relationship objects
            if (boAttrUnitRelationshipListToSave.size() > 0) {
                IDynamicDAO boAttrUnitDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR_UNIT, executionContext);
                rootSaveResultDTO.addResult(boAttrUnitDAO.saveOrUpdate(boAttrUnitRelationshipListToSave, executionContext));
                if (executionContext.isReadBack()) {
                    rootSaveResultDTO.addDataFromReadBack(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR_UNIT, boAttrUnitRelationshipListToSave);
                }
            }
        }
        return rootSaveResultDTO;
    }

    private void fillCustomAttributeDTOFromBusinessObject(DynamicDTO dynamicDTO, BusinessObjectDTO businessObjectDTO, ExecutionContext executionContext) {
        boolean isMSSQLServerDialect = ConnectionProperties.isMSSQLServerDialect();
        // attributes coming from parent table
        dynamicDTO.put(DSPDMConstants.BoAttrName.BO_NAME, businessObjectDTO.getBoName());
        dynamicDTO.put(DSPDMConstants.BoAttrName.BO_DISPLAY_NAME, businessObjectDTO.getBoDisplayName());
        dynamicDTO.put(DSPDMConstants.BoAttrName.BO_DESC, businessObjectDTO.getBoDesc());
        dynamicDTO.put(DSPDMConstants.BoAttrName.IS_ACTIVE, businessObjectDTO.getActive());
        dynamicDTO.put(DSPDMConstants.BoAttrName.ENTITY, businessObjectDTO.getEntityName());
        dynamicDTO.put(DSPDMConstants.BoAttrName.SCHEMA_NAME, businessObjectDTO.getSchemaName());
        dynamicDTO.put(DSPDMConstants.BoAttrName.IS_CUSTOM_ATTRIBUTE, true);

        if (dynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME) == null) {
            String boAttrName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME);
            boAttrName = boAttrName.replaceAll(" ", "_").toUpperCase();
            dynamicDTO.put(DSPDMConstants.BoAttrName.BO_ATTR_NAME, boAttrName);
        }

        if (dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE) == null) {
            dynamicDTO.put(DSPDMConstants.BoAttrName.ATTRIBUTE, dynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME));
        }
        if (dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE) == null) {
            dynamicDTO.put(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE, DSPDMConstants.DataTypes.CHARACTER_VARYING.getAttributeDataType(isMSSQLServerDialect) + "(100)");
        } else {
            String dataType = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE);
            dynamicDTO.put(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE, dataType.trim().toLowerCase());
        }
        if (dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DESC) == null) {
            dynamicDTO.put(DSPDMConstants.BoAttrName.ATTRIBUTE_DESC, dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE));
        }
        dynamicDTO.putIfAbsent(DSPDMConstants.BoAttrName.CONTROL_TYPE, DSPDMConstants.ControlTypes.INPUT);
        dynamicDTO.putIfAbsent(DSPDMConstants.BoAttrName.IS_HIDDEN, false);
        dynamicDTO.putIfAbsent(DSPDMConstants.BoAttrName.IS_MANDATORY, false);
        dynamicDTO.putIfAbsent(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY, false);
        dynamicDTO.putIfAbsent(DSPDMConstants.BoAttrName.IS_REFERENCE_IND, false);
        dynamicDTO.putIfAbsent(DSPDMConstants.BoAttrName.IS_SORTABLE, true);
        dynamicDTO.putIfAbsent(DSPDMConstants.BoAttrName.IS_UPLOAD_NEEDED, true);
        dynamicDTO.putIfAbsent(DSPDMConstants.BoAttrName.IS_INTERNAL, false);
        dynamicDTO.putIfAbsent(DSPDMConstants.BoAttrName.IS_READ_ONLY, false);
    }

    // Get the enum by converting data type to the database supported type
    private String getAppropriateDatabaseAttributeDataType(String dataType, String attributeDisplayName, String boName, ExecutionContext executionContext,Boolean isMSSQLServerDialect) {
        DSPDMConstants.DataTypes dataTypeEnum = DSPDMConstants.DataTypes.fromAttributeDataType(dataType, isMSSQLServerDialect);
        if (dataTypeEnum == null) {
            throw new DSPDMException("Invalid data type '{}' for attribute '{}' and business object '{}'", executionContext.getExecutorLocale(), dataType, attributeDisplayName, boName);
        } else {
            if (ConfigProperties.getInstance().use_nvarchar_for_mssqlserver.getBooleanValue()) {
                if (dataTypeEnum == DSPDMConstants.DataTypes.CHARACTER_VARYING) {
                    dataTypeEnum = DSPDMConstants.DataTypes.N_CHARACTER_VARYING;
                } else if (dataTypeEnum == DSPDMConstants.DataTypes.CHARACTER) {
                    dataTypeEnum = DSPDMConstants.DataTypes.N_CHARACTER;
                } else if (dataTypeEnum == DSPDMConstants.DataTypes.TEXT) {
                    dataTypeEnum = DSPDMConstants.DataTypes.N_TEXT;
                }
            }
            return dataType;
        }
    }

    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */

    @Override
    public SaveResultDTO updateCustomAttributes(List<DynamicDTO> boListToUpdate, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        if (CollectionUtils.hasValue(boListToUpdate)) {
            IDynamicDAO boAttrDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, executionContext);
            // read copy of same records from database
            List<DynamicDTO> boListToUpdateReadFromDatabase = boAttrDAO.readCopyWithCustomFilter(boListToUpdate, executionContext, DSPDMConstants.BoAttrName.BO_NAME, DSPDMConstants.BoAttrName.BO_ATTR_NAME);
            if (boListToUpdate.size() != boListToUpdateReadFromDatabase.size()) {
                if (boListToUpdate.size() == 1) {
                    DynamicDTO attrDynamicDTO = boListToUpdate.get(0);
                    throw new DSPDMException("Attribute '{}' does not exist in business object '{}'. Please check bo name and attribute name values. "
                            + "Update operation aborted.", executionContext.getExecutorLocale(),
                            attrDynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME),
                            attrDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME));
                } else {
                    throw new DSPDMException("Attribute(s) do not exist. Please check bo name and their attribute names values. "
                            + "Update operation aborted.", executionContext.getExecutorLocale(), boListToUpdateReadFromDatabase.size());
                }
            }
            // start update process
            rootSaveResultDTO.addResult(boAttrDAO.updateCustomAttributes(boListToUpdate,executionContext));
            // set data in read back if required
            if (executionContext.isReadBack()) {
                rootSaveResultDTO.addDataFromReadBack(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, boListToUpdate);
            }
        }
        return rootSaveResultDTO;
    }
    @Override
    public SaveResultDTO deleteCustomAttributes(List<DynamicDTO> boListToDelete, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        if (CollectionUtils.hasValue(boListToDelete)) {
            IDynamicDAO boAttrDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, executionContext);

            // read copy of same records from database
            List<DynamicDTO> boListToDeleteReadFromDatabase = boAttrDAO.readCopyWithCustomFilter(boListToDelete, executionContext, DSPDMConstants.BoAttrName.BO_NAME, DSPDMConstants.BoAttrName.BO_ATTR_NAME);
            if (boListToDelete.size() != boListToDeleteReadFromDatabase.size()) {
                if (boListToDelete.size() == 1) {
                    DynamicDTO attrDynamicDTO = boListToDelete.get(0);
                    throw new DSPDMException("Custom attribute '{}' does not exist in business object '{}'. Please check bo name and attribute name values. "
                            + "Delete operation aborted.", executionContext.getExecutorLocale(),
                            attrDynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME),
                            attrDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME));
                } else {
                    throw new DSPDMException("Custom attribute(s) do not exist. Please check bo name and their attribute names values. "
                            + "Delete operation aborted.", executionContext.getExecutorLocale(), boListToDeleteReadFromDatabase.size());
                }
            }
            validateCustomAttributesBeforeDeletion(boListToDeleteReadFromDatabase, executionContext);
            // start delete process. Del the main parent attributes
            rootSaveResultDTO.addResult(boAttrDAO.deleteCustomAttributes(boListToDeleteReadFromDatabase, executionContext));
            // set data in read back if required
            if (executionContext.isReadBack()) {
                rootSaveResultDTO.addDataFromReadBack(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, boListToDeleteReadFromDatabase);
            }
        }
        return rootSaveResultDTO;
    }

    private void validateCustomAttributesBeforeDeletion(List<DynamicDTO> boListToDelete, ExecutionContext executionContext) {
        for (DynamicDTO dynamicDTO : boListToDelete) {
            if (!((Boolean) dynamicDTO.get(DSPDMConstants.BoAttrName.IS_CUSTOM_ATTRIBUTE))) {
                throw new DSPDMException("Cannot delete a default attribute. '{}' is not a custom attribute",
                        executionContext.getExecutorLocale(), dynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME));
            }
        }
    }

    @Override
    public SaveResultDTO introduceNewBusinessObjects(List<DynamicDTO> boListToSave, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        if (CollectionUtils.hasValue(boListToSave)) {
            IDynamicDAO businessObjectDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUSINESS_OBJECT, executionContext);
            rootSaveResultDTO.addResult(businessObjectDAO.introduceNewBusinessObjects(boListToSave, executionContext));
            // set data in read back if required
            if (executionContext.isReadBack()) {
                rootSaveResultDTO.addDataFromReadBack(DSPDMConstants.BoName.BUSINESS_OBJECT, boListToSave);
            }
        }
        return rootSaveResultDTO;
    }

    @Override
    public SaveResultDTO dropBusinessObjects(List<DynamicDTO> businessObjectsToBeDropped, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        if (CollectionUtils.hasValue(businessObjectsToBeDropped)) {
            IDynamicDAO businessObjectDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUSINESS_OBJECT, executionContext);
            rootSaveResultDTO.addResult(businessObjectDAO.dropBusinessObjects(businessObjectsToBeDropped, executionContext));
            // set data in read back if required
            if (executionContext.isReadBack()) {
                rootSaveResultDTO.addDataFromReadBack(DSPDMConstants.BoName.BUSINESS_OBJECT, businessObjectsToBeDropped);
            }
        }
        return rootSaveResultDTO;
    }

    @Override
    public SaveResultDTO dropBusinessObjectRelationships(List<DynamicDTO> businessObjectsRelationshipsToBeDropped, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        if (CollectionUtils.hasValue(businessObjectsRelationshipsToBeDropped)) {
            IDynamicDAO businessObjectRelationshipDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, executionContext);
            rootSaveResultDTO.addResult(businessObjectRelationshipDAO.dropBusinessObjectRelationships(businessObjectsRelationshipsToBeDropped, executionContext));
            // set data in read back if required
            if (executionContext.isReadBack()) {
                rootSaveResultDTO.addDataFromReadBack(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, businessObjectsRelationshipsToBeDropped);
            }
        }
        return rootSaveResultDTO;
    }

    @Override
    public SaveResultDTO addBusinessObjectRelationships(List<DynamicDTO> busObjRelationshipsToCreate, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        if (CollectionUtils.hasValue(busObjRelationshipsToCreate)) {
            IDynamicDAO businessObjectRelationshipDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, executionContext);
            rootSaveResultDTO.addResult(businessObjectRelationshipDAO.addBusinessObjectRelationships(busObjRelationshipsToCreate, executionContext));
            // set data in read back if required
            if (executionContext.isReadBack()) {
                rootSaveResultDTO.addDataFromReadBack(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, busObjRelationshipsToCreate);
            }
        }
        return rootSaveResultDTO;
    }

    @Override
    public SaveResultDTO addUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeCreated, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        if (CollectionUtils.hasValue(busObjAttrUniqueConstraintsToBeCreated)) {
            IDynamicDAO businessObjectAttrUniqueConstraintsDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, executionContext);
            rootSaveResultDTO.addResult(businessObjectAttrUniqueConstraintsDAO.addUniqueConstraints(busObjAttrUniqueConstraintsToBeCreated, executionContext));
            // set data in read back if required
            if (executionContext.isReadBack()) {
                rootSaveResultDTO.addDataFromReadBack(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, busObjAttrUniqueConstraintsToBeCreated);
            }
        }
        return rootSaveResultDTO;
    }

    @Override
    public SaveResultDTO addSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeCreated, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        if (CollectionUtils.hasValue(busObjAttrSearchIndexesToBeCreated)) {
            IDynamicDAO businessObjectAttrSearchIndexesDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES, executionContext);
            rootSaveResultDTO.addResult(businessObjectAttrSearchIndexesDAO.addSearchIndexes(busObjAttrSearchIndexesToBeCreated, executionContext));
            // set data in read back if required
            if (executionContext.isReadBack()) {
                rootSaveResultDTO.addDataFromReadBack(DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES, busObjAttrSearchIndexesToBeCreated);
            }
        }
        return rootSaveResultDTO;
    }

    @Override
    public SaveResultDTO dropUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeDropped, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        if (CollectionUtils.hasValue(busObjAttrUniqueConstraintsToBeDropped)) {
            IDynamicDAO businessObjectAttrUniqueConstraintsDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, executionContext);
            rootSaveResultDTO.addResult(businessObjectAttrUniqueConstraintsDAO.dropUniqueConstraints(busObjAttrUniqueConstraintsToBeDropped, executionContext));
            // set data in read back if required
            if (executionContext.isReadBack()) {
                rootSaveResultDTO.addDataFromReadBack(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, busObjAttrUniqueConstraintsToBeDropped);
            }
        }
        return rootSaveResultDTO;
    }

    @Override
    public SaveResultDTO dropSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeDropped, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        if (CollectionUtils.hasValue(busObjAttrSearchIndexesToBeDropped)) {
            IDynamicDAO businessObjectAttrSearchIndexesDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES, executionContext);
            rootSaveResultDTO.addResult(businessObjectAttrSearchIndexesDAO.dropSearchIndexes(busObjAttrSearchIndexesToBeDropped, executionContext));
            // set data in read back if required
            if (executionContext.isReadBack()) {
                rootSaveResultDTO.addDataFromReadBack(DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES, busObjAttrSearchIndexesToBeDropped);
            }
        }
        return rootSaveResultDTO;
    }

    @Override
    public SaveResultDTO generateMetadataForExistingTable(List<DynamicDTO> boListToGenerateMetadata, Set<String> exclusionList, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        if (CollectionUtils.hasValue(boListToGenerateMetadata)) {
            IDynamicDAO businessObjectDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUSINESS_OBJECT, executionContext);
            rootSaveResultDTO.addResult(businessObjectDAO.generateMetadataForExistingTable(boListToGenerateMetadata, exclusionList, executionContext));
            // set data in read back if required
            if (executionContext.isReadBack()) {
                rootSaveResultDTO.addDataFromReadBack(DSPDMConstants.BoName.BUSINESS_OBJECT, boListToGenerateMetadata);
            }
        }
        return rootSaveResultDTO;
    }

    @Override
    public SaveResultDTO deleteMetadataForExistingTable(List<DynamicDTO> boListToDeleteMetadata, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        if (CollectionUtils.hasValue(boListToDeleteMetadata)) {
            // read metadata map to get primary key values
            Map<String, BusinessObjectDTO> businessObjectsMap = DynamicDAOFactory.getInstance(executionContext).getBusinessObjectsMap(executionContext);
            String boName = null;
            // fill primary key values in the list to be deleted
            for(DynamicDTO businessObjectDTO: boListToDeleteMetadata) {
                boName = (String) businessObjectDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
                businessObjectDTO.put(DSPDMConstants.BoAttrName.BUSINESS_OBJECT_ID, businessObjectsMap.get(boName).getId());
            }
            // pk values has been set now call the dao layer
            IDynamicDAO businessObjectDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUSINESS_OBJECT, executionContext);
            // call db
            rootSaveResultDTO.addResult(businessObjectDAO.deleteMetadataForExistingTable(boListToDeleteMetadata, executionContext));
            // set data in read back if required
            if (executionContext.isReadBack()) {
                rootSaveResultDTO.addDataFromReadBack(DSPDMConstants.BoName.BUSINESS_OBJECT, boListToDeleteMetadata);
            }
        }
        return rootSaveResultDTO;
    }

    @Override
    public SaveResultDTO addBusinessObjectGroups(List<DynamicDTO> busObjGroupsToBeCreated, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        if (CollectionUtils.hasValue(busObjGroupsToBeCreated)) {
            IDynamicDAO businessObjectDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUSINESS_OBJECT_GROUP, executionContext);
            rootSaveResultDTO.addResult(businessObjectDAO.addBusinessObjectGroup(busObjGroupsToBeCreated, executionContext));
            // set data in read back if required
            if (executionContext.isReadBack()) {
                rootSaveResultDTO.addDataFromReadBack(DSPDMConstants.BoName.BUSINESS_OBJECT_GROUP, busObjGroupsToBeCreated);
            }
        }
        return rootSaveResultDTO;
    }
}
