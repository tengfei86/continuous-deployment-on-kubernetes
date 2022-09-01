package com.lgc.dspdm.core.dao.dynamic.businessobject.impl;

import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.SQLExpression;
import com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateColumn;
import com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateFunction;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectAttributeDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.core.common.util.metadata.MetadataRelationshipUtils;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAO;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAOImpl;
import com.lgc.dspdm.core.dao.dynamic.businessobject.DynamicDAOFactory;
import com.lgc.dspdm.core.dao.dynamic.metadata.impl.MetadataStructureChangeDAOImpl;

import java.sql.Connection;
import java.sql.Types;
import java.util.*;

/**
 * Class to hold common code for utility methods or base methods.
 * These methods will not be public and can only be invoked from the child classes.
 * This class will not contain any public business method
 *
 * @author Muhammad Imran Ansari
 * @date 27-August-2019
 */
public abstract class AbstractDynamicDAOImplForStructureChange extends AbstractDynamicDAOImplForDelete implements IDynamicDAOImpl {
    private static DSPDMLogger logger = new DSPDMLogger(AbstractDynamicDAOImplForStructureChange.class);

    protected AbstractDynamicDAOImplForStructureChange(BusinessObjectDTO businessObjectDTO, ExecutionContext executionContext) {
        super(businessObjectDTO, executionContext);
    }

    /* ***************************************************************************** */
    /* ************** BUSINESS METHOD INTRODUCE NEW BUSINESS OBJECT **************** */
    /* ***************************************************************************** */

    @Override
    public SaveResultDTO introduceNewBusinessObjects(List<DynamicDTO> boListToSave, Connection dataModelDBConnection,
                                                     Connection serviceDBConnection, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        // setting current operation id  in execution context for logging purpose
        executionContext.setCurrentOperationId(DSPDMConstants.BusinessObjectOperations.INTRODUCE_BUSINESS_OBJECT_OPR.getId());
        if (CollectionUtils.hasValue(boListToSave)) {
            boolean hasSQLExpression = false;
            for (DynamicDTO dynamicDTO : boListToSave) {
                dynamicDTO.setPrimaryKeyColumnNames(this.getPrimaryKeyColumnNames());
            }

            // create metadata
            rootSaveResultDTO.addResult(createMetadata_IntroduceNewBusinessObjects(boListToSave, hasSQLExpression,
                    serviceDBConnection, isMSSQLServerDialect((String)boListToSave.get(0).get(DSPDMConstants.BoAttrName.BO_NAME)),
                    executionContext));
            // Metadata saved successfully now add table to physical database
            rootSaveResultDTO.addResult(addPhysicalTableInDatabase(boListToSave, dataModelDBConnection, executionContext));
            if(rootSaveResultDTO.isAnyRecordInserted() || rootSaveResultDTO.isAnyRecordUpdated()){
                // Logging Business Operation
                trackChangeHistory(rootSaveResultDTO, executionContext.getCurrentOperationId(), boListToSave, serviceDBConnection, executionContext);
            }
        }
        return rootSaveResultDTO;
    }

    /**
     * Create all the data records related to metadata
     * This method saves metadata in BUSINESS_OBJECT table
     * This method saves metadata in BUSINESS_OBJECT_ATTR table
     * This method saves metadata in BUS_OBJ_RELATIONSHIP table
     *
     * @param boListToSave
     * @param hasSQLExpression
     * @param connection
     * @param executionContext
     * @return
     */
    private static SaveResultDTO createMetadata_IntroduceNewBusinessObjects(List<DynamicDTO> boListToSave, boolean hasSQLExpression, Connection connection,
                                                                            Boolean isMSSQLServerDialect, ExecutionContext executionContext) {
        IDynamicDAO businessObjectDAO = getDynamicDAO(DSPDMConstants.BoName.BUSINESS_OBJECT, executionContext);
        // map to hold all the children relationships for the current parent bo
        Map<String, List<DynamicDTO>> metadataChildRelationshipsMap = businessObjectDAO.readMetadataChildRelationships(executionContext);
        if (CollectionUtils.isNullOrEmpty(metadataChildRelationshipsMap)) {
            throw new DSPDMException("Cannot introduce new business object. No child relationships found for business object '{}'", executionContext.getExecutorLocale(), DSPDMConstants.BoName.BUSINESS_OBJECT);
        }
        List<DynamicDTO> boAndBoAttrRelationships = metadataChildRelationshipsMap.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
        if (CollectionUtils.isNullOrEmpty(boAndBoAttrRelationships)) {
            throw new DSPDMException("Cannot introduce new business object. No child relationships found for parent business object '{}' and child business object '{}'",
                    executionContext.getExecutorLocale(), DSPDMConstants.BoName.BUSINESS_OBJECT, DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
        }

        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // save business objects
        // call with impl interface to provide the already opened db connection
        saveResultDTO.addResult(((IDynamicDAOImpl) businessObjectDAO).saveOrUpdate(boListToSave, hasSQLExpression, connection, executionContext));
        // save business object attributes
        saveResultDTO.addResult(createBusinessObjectAttrMetadata_IntroduceNewBusinessObjects(boListToSave, boAndBoAttrRelationships, connection, isMSSQLServerDialect, executionContext));
        // save business object attributes Unique Constraints
        saveResultDTO.addResult(createBusinessObjectAttributesUniqueConstraintsMetadata_IntroduceNewBusinessObjects(boListToSave, connection, executionContext));
        // save business object attributes Search Indexes
        saveResultDTO.addResult(createBusinessObjectAttributesSearchIndexesMetadata_IntroduceNewBusinessObjects(boListToSave, connection, executionContext));
        // save business object relationships
        saveResultDTO.addResult(createBusinessObjectRelationshipMetadata_IntroduceNewBusinessObjects(boListToSave, connection, executionContext));
        // update business object attributes reference data for the attributes having relationships
        saveResultDTO.addResult(updateBusinessObjectAttributesAddReferenceData_IntroduceNewBusinessObjects(boListToSave, connection, executionContext));
        //save the business object group
        saveResultDTO.addResult(creatBusinessObjectGroups_IntroduceNewBusinessObjects(boListToSave, connection, executionContext));
        // Metadata saved successfully now add table to physical database

        return saveResultDTO;
    }

    /**
     * Creates just attribute metadata
     * This method saves metadata in BUSINESS_OBJECT_ATTR table only
     *
     * @param parentBoListAlreadySaved
     * @param boAndBoAttrRelationships
     * @param connection
     * @param executionContext
     * @return
     */
    private static SaveResultDTO createBusinessObjectAttrMetadata_IntroduceNewBusinessObjects(List<DynamicDTO> parentBoListAlreadySaved,
                                                                                              List<DynamicDTO> boAndBoAttrRelationships,
                                                                                              Connection connection, Boolean isMSSQLServerDialect,
                                                                                              ExecutionContext executionContext) {

        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // map to hold child business object map
        Map<String, List<DynamicDTO>> childrenMapToSave = null;
        // list to hold all the child business objects to be saved for a particular child bo type
        List<DynamicDTO> businessObjectAttrDynamicDTOListToSave = null;
        // save only children data if exists for the given parent list
        // now iterate over all the parents which are already saved.
        for (DynamicDTO businessObjectDynamicDTO : parentBoListAlreadySaved) {
            String boName = (String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
            childrenMapToSave = (Map<String, List<DynamicDTO>>) businessObjectDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
            if (CollectionUtils.isNullOrEmpty(childrenMapToSave)) {
                throw new DSPDMException("Cannot introduce new business object '{}', mandatory children map not found", executionContext.getExecutorLocale(), boName);
            }
            // get all attributes of current business object
            businessObjectAttrDynamicDTOListToSave = childrenMapToSave.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
            if (CollectionUtils.isNullOrEmpty(businessObjectAttrDynamicDTOListToSave)) {
                throw new DSPDMException("Cannot introduce new business object '{}', no attribute details found in children map", executionContext.getExecutorLocale(), boName);
            }

            // now iterate over all the children to be saved of the current child bo name/type
            for (DynamicDTO businessObjectAttrDynamicDTO : businessObjectAttrDynamicDTOListToSave) {
                // now iterate over all the relationship defined fields between this parent and child.
                // If there is a value for parent then copy it to child record also
                for (DynamicDTO relationshipDTO : boAndBoAttrRelationships) {
                    if (businessObjectDynamicDTO.containsKey(relationshipDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME))) {
                        Object parentIdOrName = businessObjectDynamicDTO.get(relationshipDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME));
                        // do not write in reference bo name. we will write this value in relationships writing
                        if (!(DSPDMConstants.BoAttrName.REFERENCE_BO_NAME.equals(relationshipDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME)))) {
                            businessObjectAttrDynamicDTO.put((String) relationshipDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME), parentIdOrName);
                        }
                    }
                }
                // Handling attribute datatype as per sql dialect.
                if(!StringUtils.isNullOrEmpty((String) businessObjectAttrDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE))) {
                    String currentAttributeDataType = (String) businessObjectAttrDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE);
                    StringBuilder updatedAttributeDataTypeWithRespectToSQLServerDialect = new StringBuilder();
                    updatedAttributeDataTypeWithRespectToSQLServerDialect.append(DSPDMConstants.DataTypes.
                            fromAttributeDataType(currentAttributeDataType, isMSSQLServerDialect).getAttributeDataType(isMSSQLServerDialect));
                    if(DSPDMConstants.DataTypes.fromAttributeDataType(currentAttributeDataType, isMSSQLServerDialect).isStringDataType()
                            ||DSPDMConstants.DataTypes.fromAttributeDataType(currentAttributeDataType, isMSSQLServerDialect).isNumericDataType()){
                        // checking if the attribute datatype is of string or numeric datatype. If yes, then we need to check if size
                        // is provided then put size information in updated attribute type
                        if(currentAttributeDataType.contains("(") && currentAttributeDataType.endsWith(")")){
                            // the below code will fetch attribute size. e.g., if varchar(50) is given in request, attribute size will be 50
                            // with applied regex, we'll have varchar at 0 index and 50 at 1 index. and for numeric case, suppose we'll get
                            // numeric(10,5), 0 index will have numeric and 1st index will have 10,5
                            String attributeSize = currentAttributeDataType.split("[\\\\(||//)]")[1];
                            updatedAttributeDataTypeWithRespectToSQLServerDialect.append("(").append(attributeSize).append(")");
                        }
                    }
                    businessObjectAttrDynamicDTO.put(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE, updatedAttributeDataTypeWithRespectToSQLServerDialect.toString());
                }
            }
            // save all children of current children type
            IDynamicDAOImpl businessObjectAttrDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, executionContext);
            saveResultDTO.addResult(businessObjectAttrDAO.saveOrUpdate(businessObjectAttrDynamicDTOListToSave, false, connection, executionContext));
        }
        return saveResultDTO;
    }

    /**
     * This method saves metadata in BUS_OBJ_RELATIONSHIP table only
     *
     * @param parentBoListAlreadySaved
     * @param connection
     * @param executionContext
     * @return
     */
    private static SaveResultDTO createBusinessObjectRelationshipMetadata_IntroduceNewBusinessObjects(List<DynamicDTO> parentBoListAlreadySaved, Connection connection, ExecutionContext executionContext) {

        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // map to hold child business object map
        Map<String, List<DynamicDTO>> childrenMapToSave = null;
        // list to hold all the child business objects to be saved for a particular child bo type
        List<DynamicDTO> busObjRelationshipDynamicDTOListToSave = null;
        List<DynamicDTO> businessObjectAttrDynamicDTOListAlreadySaved = null;
        // save only children data if exists for the given parent list
        // now iterate over all the parents which are already saved.
        for (DynamicDTO businessObjectDynamicDTO : parentBoListAlreadySaved) {
            String boName = (String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
            childrenMapToSave = (Map<String, List<DynamicDTO>>) businessObjectDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
            // if no data in map then go back with no relationships
            if (CollectionUtils.hasValue(childrenMapToSave)) {
                // get all business object attributes of current business object
                businessObjectAttrDynamicDTOListAlreadySaved = childrenMapToSave.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
                // get all business object relationships of current business object
                busObjRelationshipDynamicDTOListToSave = childrenMapToSave.get(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP);
                // check first that the list has some relationships to be saved.
                if (CollectionUtils.hasValue(busObjRelationshipDynamicDTOListToSave)) {
                    String parentBoName = null;
                    String childBoName = null;
                    String parentBoAttrName = null;
                    String childBoAttrName = null;
                    // objects
                    DynamicDTO parentBusinessObjectDTO = null;
                    DynamicDTO childBusinessObjectDTO = null;
                    DynamicDTO childBusinessObjectAttrDTO = null;
                    DynamicDTO parentBusinessObjectAttrDTO = null;

                    // now iterate over all the children to be saved of the current child bo name/type
                    for (DynamicDTO busObjRelationshipDynamicDTO : busObjRelationshipDynamicDTOListToSave) {
                        // now bring child entity names and set in the relationship dynamic dto
                        childBoName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME);
                        childBoAttrName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
                        // current business object dto is the child business object dto. It will not be available in the DAO factory because not yet saved and committed and refreshed metadata
                        childBusinessObjectDTO = businessObjectDynamicDTO;
                        childBusinessObjectAttrDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(businessObjectAttrDynamicDTOListAlreadySaved, DSPDMConstants.BoAttrName.BO_ATTR_NAME, childBoAttrName);
                        // now bring parent entity names and set in the relationship dynamic dto
                        parentBoName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_NAME);
                        parentBoAttrName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME);
                        // identifying child order
                        if(busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_ORDER) == null){
                            // means child_order is not provided. We need to determine childOrder ourselves now
                            // parent/child relationship might be in request packet having child order, in that case we need to put same child order
                            // for current relationship which is of other attribute of same parent/child relationship
                            Integer childOrderFromRequestPacket = getChildOrderFromRequestPacket(busObjRelationshipDynamicDTOListToSave, parentBoName, childBoName);
                            if(childOrderFromRequestPacket == null){
                                //means childOrder Not Found from request packet, now we'll search in db
                                Map<String, List<DynamicDTO>> childRelationships = null;
                                try{
                                    childRelationships = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(parentBoName, executionContext).readMetadataChildRelationships(executionContext);
                                }catch (Exception e){
                                    // each exception because no parent relationship is found, this is the case when there's a recursive relationship with he same bo that is in request packet
                                }
                                if(CollectionUtils.hasValue(childRelationships)){
                                    // if parent relationship exists in db then fist we'll see if parent/child relationship exists, if so, we'll get same child order,
                                    // else we'll get maximum child order from db and will assign the max value.
                                    if(childRelationships.containsKey(childBoName)){
                                        Integer childOrderFromDB = (Integer) childRelationships.get(childBoName).get(0).get(DSPDMConstants.BoAttrName.CHILD_ORDER);
                                        busObjRelationshipDynamicDTO.put(DSPDMConstants.BoAttrName.CHILD_ORDER, childOrderFromDB);
                                    }else{
                                        // set default child order to zero
                                        busObjRelationshipDynamicDTO.put(DSPDMConstants.BoAttrName.CHILD_ORDER, 0);
                                    }
                                }else{
                                    // at this point, childOrder is neither in request packet nor in db
                                    // set default child order to zero
                                    busObjRelationshipDynamicDTO.put(DSPDMConstants.BoAttrName.CHILD_ORDER, 0);
                                }
                            }else{
                                busObjRelationshipDynamicDTO.put(DSPDMConstants.BoAttrName.CHILD_ORDER, childOrderFromRequestPacket);
                            }
                        }
                        // read dynamic dao of each one and get main business object dto
                        IDynamicDAO parentDynamicDAO = null;
                        try {
                            // read dynamic dao of each one and get main business object dto
                            parentDynamicDAO = getDynamicDAO(parentBoName, executionContext);
                        } catch (DSPDMException e) {
                            // clear the object if not initialized inside the loop
                        }
                        if (parentDynamicDAO != null) {
                            // parent dao is found it means it is already s registered business object
                            parentBusinessObjectDTO = parentDynamicDAO.readMetadataBusinessObject(executionContext);
                            parentBusinessObjectAttrDTO = parentDynamicDAO.readMetadataMap(executionContext).get(parentBoAttrName);
                        } else if (parentBoName.equalsIgnoreCase(childBoName)) {
                            // self join case
                            parentBusinessObjectDTO = childBusinessObjectDTO;
                            parentBusinessObjectAttrDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(businessObjectAttrDynamicDTOListAlreadySaved, DSPDMConstants.BoAttrName.BO_ATTR_NAME, parentBoAttrName);
                        } else {
                            // case when parent is in same json request and already validated above
                            parentBusinessObjectDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(parentBoListAlreadySaved, DSPDMConstants.BoAttrName.BO_NAME, parentBoName);
                            Map<String, List<DynamicDTO>> children = (Map<String, List<DynamicDTO>>) parentBusinessObjectDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
                            List<DynamicDTO> parentAttributes = children.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
                            parentBusinessObjectAttrDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(parentAttributes, DSPDMConstants.BoAttrName.BO_ATTR_NAME, parentBoAttrName);
                        }
                        // !IMPORTANT fill remaining data
                        fillBusinessObjectRelationshipMetadata(busObjRelationshipDynamicDTO, parentBusinessObjectDTO, childBusinessObjectDTO, parentBusinessObjectAttrDTO, childBusinessObjectAttrDTO);
                    }
                    // save all relationships using relationship dynamic dao
                    IDynamicDAOImpl busObjRelationshipDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, executionContext);
                    saveResultDTO.addResult(busObjRelationshipDAO.saveOrUpdate(busObjRelationshipDynamicDTOListToSave, false, connection, executionContext));
                }
            }
        }
        return saveResultDTO;
    }

    /**
     * This method saves metadata in BUS_OBJ_ATTR_UNIQ_CONSTRAINTS table only
     *
     * @param parentBoListAlreadySaved
     * @param connection
     * @param executionContext
     * @return
     */
    private static SaveResultDTO createBusinessObjectAttributesUniqueConstraintsMetadata_IntroduceNewBusinessObjects(List<DynamicDTO> parentBoListAlreadySaved, Connection connection, ExecutionContext executionContext) {

        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // map to hold child business object map
        Map<String, List<DynamicDTO>> childrenMapToSave = null;
        // list to hold all the child business objects to be saved for a particular child bo type
        List<DynamicDTO> busObjAttrUniqConstraintsDynamicDTOListToSave = null;
        // save only children data if exists for the given parent list
        // now iterate over all the parents which are already saved.
        for (DynamicDTO businessObjectDynamicDTO : parentBoListAlreadySaved) {
            childrenMapToSave = (Map<String, List<DynamicDTO>>) businessObjectDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
            // if no data in map then go back with no unique constraints
            if (CollectionUtils.hasValue(childrenMapToSave)) {
                // get all business object attributes unique constraints of current business object
                busObjAttrUniqConstraintsDynamicDTOListToSave = childrenMapToSave.get(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS);
                // check first that the list has some unique constraints to be saved.
                if (CollectionUtils.hasValue(busObjAttrUniqConstraintsDynamicDTOListToSave)) {
                    String boName=null;
                    String boAttrName = null;
                    // dao
                    IDynamicDAO dynamicDAO = null;
                    // objects
                    DynamicDTO businessObjectDTO = null;
                    DynamicDTO businessObjectAttrDTO = null;
                    // now iterate over all the children to be saved of the current child bo name/type
                    for (DynamicDTO busObjAttrUniqConstraintsDynamicDTO : busObjAttrUniqConstraintsDynamicDTOListToSave) {
                        // now bring child entity names and set in the unique constraints dynamic dto
                        boName = (String) busObjAttrUniqConstraintsDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
                        boAttrName = (String) busObjAttrUniqConstraintsDynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                        busObjAttrUniqConstraintsDynamicDTO.put(DSPDMConstants.BoAttrName.BO_NAME,boName);
                        busObjAttrUniqConstraintsDynamicDTO.put(DSPDMConstants.BoAttrName.BO_ATTR_NAME,boAttrName);
                        try {
                            // read dynamic dao of each one and get main business object dto
                            dynamicDAO = getDynamicDAO(boName, executionContext);
                        } catch (DSPDMException e) {
                            // clear the object if not initialized inside the loop
                            dynamicDAO = null;
                        }
                        if (dynamicDAO != null) {
                            // get attribute metadata
                            businessObjectDTO=dynamicDAO.readMetadataBusinessObject(executionContext);
                            businessObjectAttrDTO = dynamicDAO.readMetadataMap(executionContext).get(boAttrName);
                        }else{
                            // case when bo is in same json request and already validated above
                            businessObjectDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(parentBoListAlreadySaved, DSPDMConstants.BoAttrName.BO_NAME, boName);
                            Map<String, List<DynamicDTO>> children = (Map<String, List<DynamicDTO>>) businessObjectDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
                            List<DynamicDTO> attributes = children.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
                            businessObjectAttrDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(attributes, DSPDMConstants.BoAttrName.BO_ATTR_NAME, boAttrName);
                        }
                        // !IMPORTANT fill remaining data
                        fillBusObjAttrUniqueConstraintMetadata(busObjAttrUniqConstraintsDynamicDTO,businessObjectAttrDTO);
                    }
                    // save all business object attributes unique constraints
                    IDynamicDAOImpl busObjAttrUniqConstraintsDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, executionContext);
                    saveResultDTO.addResult(busObjAttrUniqConstraintsDAO.saveOrUpdate(busObjAttrUniqConstraintsDynamicDTOListToSave, false, connection, executionContext));
                }
            }
        }
        return saveResultDTO;
    }

    /**
     * This method saves metadata in BUS_OBJ_ATTR_SEARCH_INDEXES table only
     *
     * @param parentBoListAlreadySaved
     * @param connection
     * @param executionContext
     * @return
     */
    private static SaveResultDTO createBusinessObjectAttributesSearchIndexesMetadata_IntroduceNewBusinessObjects(List<DynamicDTO> parentBoListAlreadySaved, Connection connection, ExecutionContext executionContext) {

        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // map to hold child business object map
        Map<String, List<DynamicDTO>> childrenMapToSave = null;
        // list to hold all the child business objects to be saved for a particular child bo type
        List<DynamicDTO> busObjAttrSearchIndexesDynamicDTOListToSave = null;
        // save only children data if exists for the given parent list
        // now iterate over all the parents which are already saved.
        for (DynamicDTO businessObjectDynamicDTO : parentBoListAlreadySaved) {
            childrenMapToSave = (Map<String, List<DynamicDTO>>) businessObjectDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
            // if no data in map then go back with no unique constraints
            if (CollectionUtils.hasValue(childrenMapToSave)) {
                // get all business object attributes search indexes of current business object
                busObjAttrSearchIndexesDynamicDTOListToSave = childrenMapToSave.get(DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES);
                // check first that the list has some search indexes to be saved.
                if (CollectionUtils.hasValue(busObjAttrSearchIndexesDynamicDTOListToSave)) {
                    String boName=null;
                    String boAttrName = null;
                    // dao
                    IDynamicDAO dynamicDAO = null;
                    // objects
                    DynamicDTO businessObjectDTO = null;
                    DynamicDTO businessObjectAttrDTO = null;
                    // now iterate over all the children to be saved of the current child bo name/type
                    for (DynamicDTO busObjAttrSearchIndexesDynamicDTO : busObjAttrSearchIndexesDynamicDTOListToSave) {
                        // now bring child entity names and set in the search index dynamic dto
                        boName = (String) busObjAttrSearchIndexesDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
                        boAttrName = (String) busObjAttrSearchIndexesDynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                        busObjAttrSearchIndexesDynamicDTO.put(DSPDMConstants.BoAttrName.BO_NAME,boName);
                        busObjAttrSearchIndexesDynamicDTO.put(DSPDMConstants.BoAttrName.BO_ATTR_NAME,boAttrName);
                        try {
                            // read dynamic dao of each one and get main business object dto
                            dynamicDAO = getDynamicDAO(boName, executionContext);
                        } catch (DSPDMException e) {
                            // clear the object if not initialized inside the loop
                            dynamicDAO = null;
                        }
                        if (dynamicDAO != null) {
                            // get attribute metadata
                            businessObjectDTO=dynamicDAO.readMetadataBusinessObject(executionContext);
                            businessObjectAttrDTO = dynamicDAO.readMetadataMap(executionContext).get(boAttrName);
                        }else{
                            // case when bo is in same json request and already validated above
                            businessObjectDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(parentBoListAlreadySaved, DSPDMConstants.BoAttrName.BO_NAME, boName);
                            Map<String, List<DynamicDTO>> children = (Map<String, List<DynamicDTO>>) businessObjectDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
                            List<DynamicDTO> attributes = children.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
                            businessObjectAttrDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(attributes, DSPDMConstants.BoAttrName.BO_ATTR_NAME, boAttrName);
                        }
                        // !IMPORTANT fill remaining data
                        fillBusObjAttrSearchIndexMetadata(busObjAttrSearchIndexesDynamicDTO,businessObjectAttrDTO, executionContext);
                    }
                    // save all business object attributes search indexes
                    IDynamicDAOImpl busObjAttrSearchIndexesDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES, executionContext);
                    saveResultDTO.addResult(busObjAttrSearchIndexesDAO.saveOrUpdate(busObjAttrSearchIndexesDynamicDTOListToSave, false, connection, executionContext));
                }
            }
        }
        return saveResultDTO;
    }

    /**
     * This method updates the reference fields for the business object attribute metadata which has relationship on it.
     * The attributes include: reference_bo_name, reference_bo_attr_value, reference_bo_attr_label, and is_reference_ind, control type
     *
     * @param parentBoListAlreadySaved
     * @param connection
     * @param executionContext
     * @return
     */
    protected static SaveResultDTO updateBusinessObjectAttributesAddReferenceData_generateMetadataForExistingTable(List<DynamicDTO> parentBoListAlreadySaved,
                                                                                                                   List<DynamicDTO> businessObjectAttrDynamicDTOListToSave,
                                                                                                                   Connection connection,
                                                                                                                   ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // create an empty list to add all the objects to be updated
        List<DynamicDTO> businessObjectAttrDynamicDTOListToUpdate = new ArrayList<>();
        // now iterate over all the parents which are already saved.
        for (DynamicDTO businessObjectDynamicDTO : parentBoListAlreadySaved) {
            String currentBoName = (String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
            // get children map from each parent
            Map<String, List<DynamicDTO>> childrenMapToSave = (Map<String, List<DynamicDTO>>) businessObjectDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
            // get all business object relationships of current business object from the children map
            List<DynamicDTO> busObjRelationshipDynamicDTOListAlreadySaved = childrenMapToSave.get(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP);
            // check first that the list has some relationships already saved. If yes then continue
            if (CollectionUtils.hasValue(busObjRelationshipDynamicDTOListAlreadySaved)) {
                // get all business object attributes of current business object which are already saved above
                List<DynamicDTO> businessObjectAttrDynamicDTOListAlreadySaved = childrenMapToSave.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
                // now iterate over all the children to be saved of the current child bo name/type
                for (DynamicDTO busObjRelationshipDynamicDTO : busObjRelationshipDynamicDTOListAlreadySaved) {
                    String childBoName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME);
                    String childBoAttrName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
                    // get child business object attribute dto from the already saved list. This dto will be updated
                    DynamicDTO childBusinessObjectAttrDTOToUpdate = null;
                    if (currentBoName.equalsIgnoreCase(childBoName)) {
                        childBusinessObjectAttrDTOToUpdate = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue
                                (businessObjectAttrDynamicDTOListAlreadySaved, DSPDMConstants.BoAttrName.BO_ATTR_NAME, childBoAttrName);
                    } else {
                        if(executionContext.getGenerateMetadataForAll()){
                            Optional<DynamicDTO> first = businessObjectAttrDynamicDTOListToSave.stream().
                                    filter(dynamicDTO -> childBoName.equalsIgnoreCase((String) dynamicDTO.
                                            get(DSPDMConstants.BoAttrName.BO_NAME))).filter(dynamicDTO ->
                                    childBoAttrName.equalsIgnoreCase((String) dynamicDTO.
                                            get(DSPDMConstants.BoAttrName.ATTRIBUTE))).findFirst();
                            if(!first.isPresent()){
                                continue;
                            }else{
                                childBusinessObjectAttrDTOToUpdate = first.get();
                            }
                        }else{
                            childBusinessObjectAttrDTOToUpdate = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(childBoName, executionContext).readMetadataMap(executionContext).get(childBoAttrName);
                        }
                    }
                    // now bring parent entity names and set in the relationship dynamic dto
                    String referenceBoName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_NAME);
                    // read dynamic dao of each one and get main business object dto
                    IDynamicDAO referenceDAO = null;
                    try {
                        // get parent dao in a try catch and eat any exception if it comes.
                        referenceDAO = getDynamicDAO(referenceBoName, executionContext);
                    } catch (DSPDMException e) {
                        // eat exception that the parent business object does not exists.
                        // in this case parent business object is already in the above list
                    }
                    // get primary key column names of the parent/reference business object
                    List<String> referenceDAOPKAttrNames = null;
                    if (referenceDAO != null) {
                        referenceDAOPKAttrNames = referenceDAO.getPrimaryKeyColumnNames();
                    } else {
                        DynamicDTO parentDynamicDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(parentBoListAlreadySaved, DSPDMConstants.BoAttrName.BO_NAME, referenceBoName);
                        Map<String, List<DynamicDTO>> parentDynamicDTOChildrenMap = (Map<String, List<DynamicDTO>>) parentDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
                        List<DynamicDTO> parentBusinessObjectAttrDynamicDTOList = parentDynamicDTOChildrenMap.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
                        List<DynamicDTO> parentBusinessObjectAttrPKDynamicDTOList = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(parentBusinessObjectAttrDynamicDTOList, DSPDMConstants.BoAttrName.IS_PRIMARY_KEY, true);
                        referenceDAOPKAttrNames = CollectionUtils.getStringValuesFromList(parentBusinessObjectAttrPKDynamicDTOList, DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                    }

                    // assign reference dao primary key in value column. If more than one then it will be comma separated
                    String referenceBoAttrValue = (CollectionUtils.hasValue(referenceDAOPKAttrNames)) ? CollectionUtils.getCommaSeparated(referenceDAOPKAttrNames) : null;
                    // now we calculate the value of the reference bo attr label field
                    String referenceBoAttrLabel = null;
                    if (Boolean.TRUE.equals(busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP))) {
                        // if current relationship is a primary key relationship then extract the label value from the unique constraints
                        // read all unique constraints in a list
                        List<DynamicDTO> uniqueConstraints = null;
                        if (referenceDAO != null) {
                            uniqueConstraints = referenceDAO.readMetadataConstraints(executionContext);
                        } else {
                            DynamicDTO parentDynamicDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(parentBoListAlreadySaved, DSPDMConstants.BoAttrName.BO_NAME, referenceBoName);
                            Map<String, List<DynamicDTO>> parentDynamicDTOChildrenMap = (Map<String, List<DynamicDTO>>) parentDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
                            uniqueConstraints = parentDynamicDTOChildrenMap.get(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS);
                            if (uniqueConstraints == null) {
                                uniqueConstraints = new ArrayList<>();
                            }
                        }
                        // group all unique constraints on the basis of constraint name
                        Map<Object, List<DynamicDTO>> constraintsByName = CollectionUtils.groupDynamicDTOByPropertyValue(uniqueConstraints, DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
                        int size = 0;
                        // iterate on each group and try to use the best smaller size group
                        for (List<DynamicDTO> constraints : constraintsByName.values()) {
                            // first time write the value in the label field
                            // if there is already a value and then has more columns than the current one then use the current one as best unique constraint
                            if ((referenceBoAttrLabel == null) || (size > constraints.size())) {
                                referenceBoAttrLabel = CollectionUtils.getCommaSeparated(CollectionUtils.getStringValuesFromList(constraints, DSPDMConstants.BoAttrName.BO_ATTR_NAME));
                                // assign the current list size
                                size = constraints.size();
                            }
                        }
                        if(StringUtils.isNullOrEmpty(referenceBoAttrLabel)) {
                            // if no unique constraint then set same value
                            referenceBoAttrLabel = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME);
                        }
                        // as it is a primary key based relationship therefore mark the foreign key hidden
                        childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.IS_UPLOAD_NEEDED, false);
                        childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.IS_HIDDEN, true);
                        childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.IS_INTERNAL, true);
                        // for pk relationships related bo attr name is always null
                        childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.RELATED_BO_ATTR_NAME, null);
                    } else {
                        // as current relationship is a non-pk relationship then the parent attribute name in the relationship is the reference label.
                        referenceBoAttrLabel = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME);
                        // if this relationship is a non-pk relationship then change control type to auto complete to generate a combo box on a front end
                        childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.CONTROL_TYPE, DSPDMConstants.ControlTypes.AUTO_COMPLETE);

                        String relatedBoAttrName = getRelatedBoAttrNameFromRelationships(busObjRelationshipDynamicDTO,
                                busObjRelationshipDynamicDTOListAlreadySaved, executionContext);
                        childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.RELATED_BO_ATTR_NAME, relatedBoAttrName);
                    }
                    // now set other remaining reference values.
                    childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.REFERENCE_BO_NAME, referenceBoName);
                    childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_VALUE, referenceBoAttrValue);
                    childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_LABEL, referenceBoAttrLabel);
                    childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.IS_REFERENCE_IND, true);
                    // add the attribute dynamic dto to the update candidate list
                    businessObjectAttrDynamicDTOListToUpdate.add(childBusinessObjectAttrDTOToUpdate);
                }
            }
        }
        // if there are some attributes to be updated then update them
        if (CollectionUtils.hasValue(businessObjectAttrDynamicDTOListToUpdate)) {
            // update all the fileterd attributes reference data
            IDynamicDAOImpl businessObjectAttrDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, executionContext);
            saveResultDTO.addResult(businessObjectAttrDAO.saveOrUpdate(businessObjectAttrDynamicDTOListToUpdate, false, connection, executionContext));
        }
        return saveResultDTO;
    }

    private static String findBestMatchedRelatedBoAttrName(String boAttrNameAgainstWhichRelatedBoAttrNameIsToBePut, List<DynamicDTO> pkRelationshipDtoList) {
        // use sorted map and integer as key
        TreeMap<Integer, String> relatedBoAttrNameScoreMap = new TreeMap<>();
        for (DynamicDTO relationshipDynamicDTO : pkRelationshipDtoList) {
            String candidateRelatedBoAttrName = (String) relationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
            String[] candidateRelatedBoAttrNamePieces = (candidateRelatedBoAttrName).split("_");
            int matchScore = 0;
            for (String str : candidateRelatedBoAttrNamePieces) {
                if (boAttrNameAgainstWhichRelatedBoAttrNameIsToBePut.contains(str)) {
                    matchScore += 1;
                }
            }
            // do not overwrite existing score value if same score already exists.
            // if score is same then use strategy first come, first served
            if (!(relatedBoAttrNameScoreMap.containsKey(matchScore))) {
                relatedBoAttrNameScoreMap.put(matchScore, candidateRelatedBoAttrName);
            }
        }
        // pick last entry value and send as it is a sorted map
        return relatedBoAttrNameScoreMap.lastEntry().getValue();
    }

    protected static SaveResultDTO updateBusinessObjectAttributesAddReferenceData_IntroduceNewBusinessObjects(List<DynamicDTO> parentBoListAlreadySaved,
                                                                                                              Connection connection,
                                                                                                              ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // create an empty list to add all the objects to be updated
        List<DynamicDTO> businessObjectAttrDynamicDTOListToUpdate = new ArrayList<>();
        // now iterate over all the parents which are already saved.
        for (DynamicDTO businessObjectDynamicDTO : parentBoListAlreadySaved) {
            String currentBoName = (String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
            // get children map from each parent
            Map<String, List<DynamicDTO>> childrenMapToSave = (Map<String, List<DynamicDTO>>) businessObjectDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
            // get all business object relationships of current business object from the children map
            List<DynamicDTO> busObjRelationshipDynamicDTOListAlreadySaved = childrenMapToSave.get(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP);
            // check first that the list has some relationships already saved. If yes then continue
            if (CollectionUtils.hasValue(busObjRelationshipDynamicDTOListAlreadySaved)) {
                // get all business object attributes of current business object which are already saved above
                List<DynamicDTO> businessObjectAttrDynamicDTOListAlreadySaved = childrenMapToSave.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
                // now iterate over all the children to be saved of the current child bo name/type
                for (DynamicDTO busObjRelationshipDynamicDTO : busObjRelationshipDynamicDTOListAlreadySaved) {
                    String childBoName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME);
                    String childBoAttrName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
                    // get child business object attribute dto from the already saved list. This dto will be updated
                    DynamicDTO childBusinessObjectAttrDTOToUpdate = null;
                    if (currentBoName.equalsIgnoreCase(childBoName)) {
                        childBusinessObjectAttrDTOToUpdate = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue
                                (businessObjectAttrDynamicDTOListAlreadySaved, DSPDMConstants.BoAttrName.BO_ATTR_NAME, childBoAttrName);
                    } else {
                            childBusinessObjectAttrDTOToUpdate = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(childBoName, executionContext).readMetadataMap(executionContext).get(childBoAttrName);
                    }
                    // now bring parent entity names and set in the relationship dynamic dto
                    String referenceBoName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_NAME);
                    // read dynamic dao of each one and get main business object dto
                    IDynamicDAO referenceDAO = null;
                    try {
                        // get parent dao in a try catch and eat any exception if it comes.
                        referenceDAO = getDynamicDAO(referenceBoName, executionContext);
                    } catch (DSPDMException e) {
                        // eat exception that the parent business object does not exists.
                        // in this case parent business object is already in the above list
                    }
                    // get primary key column names of the parent/reference business object
                    List<String> referenceDAOPKAttrNames = null;
                    if (referenceDAO != null) {
                        referenceDAOPKAttrNames = referenceDAO.getPrimaryKeyColumnNames();
                    } else {
                        DynamicDTO parentDynamicDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(parentBoListAlreadySaved, DSPDMConstants.BoAttrName.BO_NAME, referenceBoName);
                        Map<String, List<DynamicDTO>> parentDynamicDTOChildrenMap = (Map<String, List<DynamicDTO>>) parentDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
                        List<DynamicDTO> parentBusinessObjectAttrDynamicDTOList = parentDynamicDTOChildrenMap.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
                        List<DynamicDTO> parentBusinessObjectAttrPKDynamicDTOList = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(parentBusinessObjectAttrDynamicDTOList, DSPDMConstants.BoAttrName.IS_PRIMARY_KEY, true);
                        referenceDAOPKAttrNames = CollectionUtils.getStringValuesFromList(parentBusinessObjectAttrPKDynamicDTOList, DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                    }

                    // assign reference dao primary key in value column. If more than one then it will be comma separated
                    String referenceBoAttrValue = (CollectionUtils.hasValue(referenceDAOPKAttrNames)) ? CollectionUtils.getCommaSeparated(referenceDAOPKAttrNames) : null;
                    // now we calculate the value of the reference bo attr label field
                    String referenceBoAttrLabel = null;
                    if (Boolean.TRUE.equals(busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP))) {
                        // if current relationship is a primary key relationship then extract the label value from the unique constraints
                        // read all unique constraints in a list
                        List<DynamicDTO> uniqueConstraints = null;
                        if (referenceDAO != null) {
                            uniqueConstraints = referenceDAO.readMetadataConstraints(executionContext);
                        } else {
                            DynamicDTO parentDynamicDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(parentBoListAlreadySaved, DSPDMConstants.BoAttrName.BO_NAME, referenceBoName);
                            Map<String, List<DynamicDTO>> parentDynamicDTOChildrenMap = (Map<String, List<DynamicDTO>>) parentDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
                            uniqueConstraints = parentDynamicDTOChildrenMap.get(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS);
                            if (uniqueConstraints == null) {
                                uniqueConstraints = new ArrayList<>();
                            }
                        }
                        // group all unique constraints on the basis of constraint name
                        Map<Object, List<DynamicDTO>> constraintsByName = CollectionUtils.groupDynamicDTOByPropertyValue(uniqueConstraints, DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
                        int size = 0;
                        // iterate on each group and try to use the best smaller size group
                        for (List<DynamicDTO> constraints : constraintsByName.values()) {
                            // first time write the value in the label field
                            // if there is already a value and then has more columns than the current one then use the current one as best unique constraint
                            if ((referenceBoAttrLabel == null) || (size > constraints.size())) {
                                referenceBoAttrLabel = CollectionUtils.getCommaSeparated(CollectionUtils.getStringValuesFromList(constraints, DSPDMConstants.BoAttrName.BO_ATTR_NAME));
                                // assign the current list size
                                size = constraints.size();
                            }
                        }
                        if(StringUtils.isNullOrEmpty(referenceBoAttrLabel)) {
                            // if no unique constraint then set same value
                            referenceBoAttrLabel = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME);
                        }
                        // as it is a primary key based relationship therefore mark the foreign key hidden
                        childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.IS_UPLOAD_NEEDED, false);
                        childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.IS_HIDDEN, true);
                        childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.IS_INTERNAL, true);
                        // for pk relationships related bo attr name is always null
                        childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.RELATED_BO_ATTR_NAME, null);
                    } else {
                        // as current relationship is a non-pk relationship then the parent attribute name in the relationship is the reference label.
                        referenceBoAttrLabel = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME);
                        // if this relationship is a non-pk relationship then change control type to auto complete to generate a combo box on a front end
                        childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.CONTROL_TYPE, DSPDMConstants.ControlTypes.AUTO_COMPLETE);
                        // we must set relation bo attr name for all no pk relationships and use user provided value first if it is there
                        if(StringUtils.isNullOrEmpty((String)childBusinessObjectAttrDTOToUpdate.get(DSPDMConstants.BoAttrName.RELATED_BO_ATTR_NAME))){
                            String relatedBoAttrName = getRelatedBoAttrNameFromRelationships(busObjRelationshipDynamicDTO,
                                    busObjRelationshipDynamicDTOListAlreadySaved, executionContext);
                            childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.RELATED_BO_ATTR_NAME, relatedBoAttrName);
                        }
                    }
                    // now set other remaining reference values.
                    childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.REFERENCE_BO_NAME, referenceBoName);
                    childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_VALUE, referenceBoAttrValue);
                    childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_LABEL, referenceBoAttrLabel);
                    childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.IS_REFERENCE_IND, true);

                    // add the attribute dynamic dto to the update candidate list
                    businessObjectAttrDynamicDTOListToUpdate.add(childBusinessObjectAttrDTOToUpdate);
                }
            }
        }
        // if there are some attributes to be updated then update them
        if (CollectionUtils.hasValue(businessObjectAttrDynamicDTOListToUpdate)) {
            // update all the fileterd attributes reference data
            IDynamicDAOImpl businessObjectAttrDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, executionContext);
            saveResultDTO.addResult(businessObjectAttrDAO.saveOrUpdate(businessObjectAttrDynamicDTOListToUpdate, false, connection, executionContext));
        }
        return saveResultDTO;
    }

    /**
     * This method updates the reference fields for the business object attribute metadata for which the relationship is going to be added
     * The attributes include: reference_bo_name, reference_bo_attr_value, reference_bo_attr_label, and is_reference_ind, control type
     *
     * @param busObjRelationshipDynamicDTOListAlreadySaved
     * @param connection
     * @param executionContext
     * @return
     */
    private static SaveResultDTO updateBusinessObjectAttributesAddReferenceData_AddBusinessObjectRelationship(
            List<DynamicDTO> busObjRelationshipDynamicDTOListAlreadySaved,
            Connection connection,
            ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // check first that the list has some relationships already saved. If yes then continue
        if (CollectionUtils.hasValue(busObjRelationshipDynamicDTOListAlreadySaved)) {
            IDynamicDAOImpl boAttrDaoImpl = getDynamicDAOImpl(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, executionContext);
            IDynamicDAOImpl busObjRelationshipDaoImpl = getDynamicDAOImpl(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, executionContext);
            // create an empty list to add all the objects to be updated
            List<DynamicDTO> businessObjectAttrDynamicDTOListToUpdate = new ArrayList<>();
            // get all business object attributes of current business object which are already saved above
            List<DynamicDTO> businessObjectAttrDynamicDTOListAlreadySaved = null;
            // now iterate over all the children to be saved of the current child bo name/type
            for (DynamicDTO busObjRelationshipDynamicDTO : busObjRelationshipDynamicDTOListAlreadySaved) {
                String childBoName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME);
                String childBoAttrName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
                // get child business object attribute dto from cache. This dto will be updated
                DynamicDTO childBusinessObjectAttrDTOToUpdate = getDynamicDAO(childBoName, executionContext).readMetadataMap(executionContext).get(childBoAttrName);
                // read copy from db and do not use cache copy
                childBusinessObjectAttrDTOToUpdate = boAttrDaoImpl.readOne(childBusinessObjectAttrDTOToUpdate.getId(), connection, executionContext);

                // now bring parent entity names and set in the relationship dynamic dto
                String referenceBoName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_NAME);
                // read dynamic dao of each one and get main business object dto
                IDynamicDAO referenceDAO = getDynamicDAO(referenceBoName, executionContext);
                // get primary key column names of the parent/reference business object
                List<String> referenceDAOPKAttrNames = referenceDAO.getPrimaryKeyColumnNames();

                // assign reference dao primary key in value column. If more than one then it will be comma separated
                String referenceBoAttrValue = (CollectionUtils.hasValue(referenceDAOPKAttrNames)) ? CollectionUtils.getCommaSeparated(referenceDAOPKAttrNames) : null;
                // now we calculate the value of the reference bo attr label field
                String referenceBoAttrLabel = null;
                if (Boolean.TRUE.equals(busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP))) {
                    // if current relationship is a primary key relationship then extract the label value from the unique constraints
                    // read all unique constraints in a list
                    List<DynamicDTO> uniqueConstraints = referenceDAO.readMetadataConstraints(executionContext);
                    // group all unique constraints on the basis of constraint name
                    Map<Object, List<DynamicDTO>> constraintsByName = CollectionUtils.groupDynamicDTOByPropertyValue(uniqueConstraints, DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
                    int size = 0;
                    // iterate on each group and try to use the best smaller size group
                    for (List<DynamicDTO> constraints : constraintsByName.values()) {
                        // find best unique constraint of reference business object on the basis of size of attributes involved
                        // first time write the value in the label field
                        // if there is already a value and then has more columns than the current one then use the current one as best unique constraint
                        if ((referenceBoAttrLabel == null) || (size > constraints.size())) {
                            referenceBoAttrLabel = CollectionUtils.getCommaSeparated(CollectionUtils.getStringValuesFromList(constraints, DSPDMConstants.BoAttrName.BO_ATTR_NAME));
                            // assign the current list size
                            size = constraints.size();
                        }
                    }
                    // as it is a primary key based relationship therefore mark the foreign key hidden
                    childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.IS_UPLOAD_NEEDED, false);
                    childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.IS_HIDDEN, true);
                    childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.IS_INTERNAL, true);
                    // for pk relationships related bo attr name is always null
                    childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.RELATED_BO_ATTR_NAME, null);
                } else {
                    // as current relationship is a non-pk relationship then the parent attribute name in the relationship is the reference label.
                    referenceBoAttrLabel = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME);
                    // if this relationship is a non-pk relationship then change control type to auto complete to generate a combo box on a front end
                    childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.CONTROL_TYPE, DSPDMConstants.ControlTypes.AUTO_COMPLETE);
                    // for non-pk relationships we must set related bo attr name
                    if(StringUtils.isNullOrEmpty((String)childBusinessObjectAttrDTOToUpdate.get(DSPDMConstants.BoAttrName.RELATED_BO_ATTR_NAME))){
                        // do not use the already saved relationships list coming from up because it is possible that
                        // this time in this request user adds only the non-pk relationship
                        // read all pk relationships from db on same connection for same parent and child
                        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, executionContext)
                                .addFilter(DSPDMConstants.BoAttrName.PARENT_BO_NAME, referenceBoName)
                                .addFilter(DSPDMConstants.BoAttrName.CHILD_BO_NAME, childBoName)
                                .addFilter(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, true)
                                .setReadAllRecords(true);
                        List<DynamicDTO> allPKRelationshipsForSameChildAndParent = busObjRelationshipDaoImpl.read(businessObjectInfo,
                                false, connection, executionContext);
                        String relatedBoAttrName = getRelatedBoAttrNameFromRelationships(busObjRelationshipDynamicDTO,
                                allPKRelationshipsForSameChildAndParent, executionContext);
                        childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.RELATED_BO_ATTR_NAME, relatedBoAttrName);
                    }
                }
                // now set other remaining reference values.
                childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.REFERENCE_BO_NAME, referenceBoName);
                childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_VALUE, referenceBoAttrValue);
                childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_LABEL, referenceBoAttrLabel);
                childBusinessObjectAttrDTOToUpdate.put(DSPDMConstants.BoAttrName.IS_REFERENCE_IND, true);
                                // add the attribute dynamic dto to the update candidate list
                businessObjectAttrDynamicDTOListToUpdate.add(childBusinessObjectAttrDTOToUpdate);
            }
            // if there are some attributes to be updated then update them
            if (CollectionUtils.hasValue(businessObjectAttrDynamicDTOListToUpdate)) {
                // update all the filtered attributes reference data
                IDynamicDAOImpl businessObjectAttrDAO = boAttrDaoImpl;
                saveResultDTO.addResult(businessObjectAttrDAO.saveOrUpdate(businessObjectAttrDynamicDTOListToUpdate, false, connection, executionContext));
            }
        }
        return saveResultDTO;
    }

    /**
    * Creates business object groups
    * This method saves business object groups
    *
    * @param parentBoListAlreadySaved
    * @param connection
    * @param executionContext
    * @return
    */
   private static SaveResultDTO creatBusinessObjectGroups_IntroduceNewBusinessObjects(List<DynamicDTO> parentBoListAlreadySaved, Connection connection, ExecutionContext executionContext) {
		SaveResultDTO saveResultDTO = new SaveResultDTO();
		// map to hold child business object map
		Map<String, List<DynamicDTO>> childrenMapToSave = null;
		// list to hold all the child business objects to be saved for a particular child bo type
		List<DynamicDTO> businessObjectGroupDTOListToSave = null;
		// save only children data if exists for the given parent list, now iterate over all the parents which are already saved.
		for (DynamicDTO businessObjectDynamicDTO : parentBoListAlreadySaved) {
			Object boId = businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BUSINESS_OBJECT_ID);
			Object boName = businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
			Object displayName = businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_DISPLAY_NAME);
			Object isActive = businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.IS_ACTIVE);
            childrenMapToSave = (Map<String, List<DynamicDTO>>) businessObjectDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
			// if no data in map then go back with no business object group
			if (CollectionUtils.hasValue(childrenMapToSave)) {
				// get all business object groups of current business object
				businessObjectGroupDTOListToSave = childrenMapToSave.get(DSPDMConstants.BoName.BUSINESS_OBJECT_GROUP);
				// check first that the list has business object group to be saved.
				if (CollectionUtils.hasValue(businessObjectGroupDTOListToSave)) {
					// now iterate over all the business object groups to be saved of the current child bo name/type
					for (DynamicDTO businessObjectGroup : businessObjectGroupDTOListToSave) {
						businessObjectGroup.put(DSPDMConstants.BoAttrName.BUSINESS_OBJECT_ID, boId);
						businessObjectGroup.put(DSPDMConstants.BoAttrName.BO_NAME, boName);
						businessObjectGroup.put(DSPDMConstants.BoAttrName.DISPLAY_NAME, displayName);
						businessObjectGroup.put(DSPDMConstants.BoAttrName.IS_ACTIVE, isActive);
					}
					// save all business object groups
					IDynamicDAOImpl busObjGroupDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUSINESS_OBJECT_GROUP, executionContext);
					saveResultDTO.addResult(busObjGroupDAO.saveOrUpdate(businessObjectGroupDTOListToSave, false, connection, executionContext));
				}
			}
		}
		return saveResultDTO;
   }

    /**
     * This method clears the reference fields for the business object attribute metadata which had relationship on it
     *
     * @param businessObjectsRelationshipsToBeDropped
     * @param connection
     * @param executionContext
     * @return
     */
    private static SaveResultDTO updateBusinessObjectAttributesClearReferenceData(List<DynamicDTO> businessObjectsRelationshipsToBeDropped, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();

        if (CollectionUtils.hasValue(businessObjectsRelationshipsToBeDropped)) {
            List<DynamicDTO> businessObjectAttrDynamicDTOListToUpdate = new ArrayList<>(businessObjectsRelationshipsToBeDropped.size());
            String childBoName = null;
            String childBoAttrName = null;
            IDynamicDAO childBusinessObjectDAO = null;
            DynamicDTO childBusinessObjectAttrDTO = null;
            // now iterate over all the relationships to be dropped
            for (DynamicDTO busObjRelationshipDynamicDTO : businessObjectsRelationshipsToBeDropped) {
                childBoName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME);
                childBoAttrName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
                childBusinessObjectDAO = getDynamicDAO(childBoName, executionContext);
                childBusinessObjectAttrDTO = childBusinessObjectDAO.readMetadataMap(executionContext).get(childBoAttrName);
                // clone because actual one might be being used by other threads/transactions right now
                childBusinessObjectAttrDTO = new DynamicDTO(childBusinessObjectAttrDTO, executionContext);
                // clear reference data
                childBusinessObjectAttrDTO.put(DSPDMConstants.BoAttrName.REFERENCE_BO_NAME, null);
                childBusinessObjectAttrDTO.put(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_VALUE, null);
                childBusinessObjectAttrDTO.put(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_LABEL, null);
                childBusinessObjectAttrDTO.put(DSPDMConstants.BoAttrName.IS_REFERENCE_IND, false);
                childBusinessObjectAttrDTO.put(DSPDMConstants.BoAttrName.RELATED_BO_ATTR_NAME, null);

                if (DSPDMConstants.ControlTypes.AUTO_COMPLETE.equalsIgnoreCase((String) childBusinessObjectAttrDTO.get(DSPDMConstants.BoAttrName.CONTROL_TYPE))) {
                    String attributeDatatype = (String) childBusinessObjectAttrDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE);
                    DSPDMConstants.DataTypes dataType = DSPDMConstants.DataTypes.fromAttributeDataType(attributeDatatype, childBusinessObjectDAO.isMSSQLServerDialect(childBoName));
                    childBusinessObjectAttrDTO.put(DSPDMConstants.BoAttrName.CONTROL_TYPE, dataType.getControlType());
                }
                // as it is not a relationship anymore therefore mark the foreign key visible
                childBusinessObjectAttrDTO.put(DSPDMConstants.BoAttrName.IS_UPLOAD_NEEDED, true);
                childBusinessObjectAttrDTO.put(DSPDMConstants.BoAttrName.IS_HIDDEN, false);
                childBusinessObjectAttrDTO.put(DSPDMConstants.BoAttrName.IS_INTERNAL, false);

                // add to list to update all together
                businessObjectAttrDynamicDTOListToUpdate.add(childBusinessObjectAttrDTO);
            }
            // save all relationships using relationship dynamic dao
            IDynamicDAOImpl businessObjectAttrDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, executionContext);
            saveResultDTO.addResult(businessObjectAttrDAO.saveOrUpdate(businessObjectAttrDynamicDTOListToUpdate, false, connection, executionContext));
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO addCustomAttributes(List<DynamicDTO> boListToSave, Connection serviceDBConnection, Connection dataModelDBConnection,
                                             ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        // setting current operation id  in execution context for logging purpose
        executionContext.setCurrentOperationId(DSPDMConstants.BusinessObjectOperations.ADD_CUSTOM_ATTR_OPR.getId());
//        Map<String, Integer> maxSeqNumForBoNameMap = new HashMap<>();
        if (CollectionUtils.hasValue(boListToSave)) {
            boolean hasSQLExpression = false;
            for (DynamicDTO dynamicDTO : boListToSave) {
                dynamicDTO.setPrimaryKeyColumnNames(this.getPrimaryKeyColumnNames());
                String boName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
                boolean sqlExpressionUsed = fillNextMaxBoAttrSequenceNumberIfRequired(dynamicDTO, serviceDBConnection, executionContext);
                if (sqlExpressionUsed) {
                    hasSQLExpression = true;
                }
            }
            // save metadata
            rootSaveResultDTO.addResult(saveOrUpdate(boListToSave, hasSQLExpression, serviceDBConnection, executionContext));
            // Metadata saved successfully now add column to physical database table
            rootSaveResultDTO.addResult(addPhysicalColumnInDatabaseTable(boListToSave, dataModelDBConnection, executionContext));
            //logging
            trackChangeHistory(rootSaveResultDTO, executionContext.getCurrentOperationId(), boListToSave, serviceDBConnection, executionContext);
        }
        return rootSaveResultDTO;
    }

    private boolean fillNextMaxBoAttrSequenceNumberIfRequired(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext) {
        boolean sqlExpressionUsed = false;
        // set next sequence number
        dynamicDTO.setPrimaryKeyColumnNames(this.getPrimaryKeyColumnNames());
        if (dynamicDTO.getId() == null) {
            // set incase of insert only not in update case
            BusinessObjectAttributeDTO seqNumBoAttrDTO = getColumn(DSPDMConstants.BoAttrName.SEQUENCE_NUM);
            if (seqNumBoAttrDTO != null) {
                // set only in case sequence number is a column// get bo name
                sqlExpressionUsed = true;
                // put expression in dto
                dynamicDTO.put(DSPDMConstants.BoAttrName.SEQUENCE_NUM, getNextMaxBoAttrSequenceNumber(dynamicDTO, sqlExpressionUsed, connection, executionContext));
            }
        }
        return sqlExpressionUsed;
    }

    private Object getNextMaxBoAttrSequenceNumber(DynamicDTO dynamicDTO, boolean useInnerSQLForMax, Connection connection, ExecutionContext executionContext) {
        Object nextMaxBoAttrSequenceNumber = null;
        String boNameForNewAttribute = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        if (useInnerSQLForMax) {
            nextMaxBoAttrSequenceNumber = getNextMaxBoAttrSequenceNumber(boNameForNewAttribute, executionContext);
        } else {
            // get max from map        
            Integer maxSequenceNumber = getMetadataBoAttrMaxSequenceNumber(boNameForNewAttribute, connection, executionContext);
            if (maxSequenceNumber == null) {
                maxSequenceNumber = 0;
            }
            nextMaxBoAttrSequenceNumber = maxSequenceNumber + 1;
        }
        return nextMaxBoAttrSequenceNumber;
    }

    private SQLExpression getNextMaxBoAttrSequenceNumber(String boName, ExecutionContext executionContext) {

        BusinessObjectAttributeDTO seqNumBoAttrDTO = getColumn(DSPDMConstants.BoAttrName.SEQUENCE_NUM);
        BusinessObjectAttributeDTO boNameBoAttrDTO = getColumn(DSPDMConstants.BoAttrName.BO_NAME);
        // SQL: select COALESCE(max(sequence_num), 0) + 1 from BUSINESS_OBJECT_ATTR where BO_NAME = 'WELL';
        String maxSequenceSQL = "SELECT COALESCE(MAX(" + seqNumBoAttrDTO.getAttributeName() + "), 0) + 1 FROM "
                + seqNumBoAttrDTO.getEntityName()
                + " WHERE " + boNameBoAttrDTO.getAttributeName() + " = " + DSPDMConstants.SQL.SQL_PARAM_PLACEHOLDER;
        SQLExpression sqlExpression = new SQLExpression(maxSequenceSQL, 1);
        sqlExpression.addParam(boName, Types.VARCHAR);
        return sqlExpression;
    }

    private Integer getMetadataBoAttrMaxSequenceNumber(String boName, Connection connection, ExecutionContext executionContext) {
        Integer maxSequenceNumber = null;
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, executionContext);
        businessObjectInfo.addAggregateColumnToSelect(new AggregateColumn(AggregateFunction.MAX, DSPDMConstants.BoAttrName.SEQUENCE_NUM, "Max Sequence Number"));
        businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.BO_NAME, boName);
        businessObjectInfo.setReadFirst(true);
        List<DynamicDTO> dtoList = read(businessObjectInfo, false, connection, executionContext);
        if (CollectionUtils.hasValue(dtoList)) {
            DynamicDTO dynamicDTO = dtoList.get(0);
            if ((dynamicDTO != null) && (dynamicDTO.get("Max Sequence Number") != null)) {
                maxSequenceNumber = (Integer) dynamicDTO.get("Max Sequence Number");
            }
        }
        return maxSequenceNumber;
    }


    private SaveResultDTO addPhysicalColumnInDatabaseTable(List<DynamicDTO> boListToSave, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        for (DynamicDTO dynamicDTO : boListToSave) {
            saveResultDTO.addResult(MetadataStructureChangeDAOImpl.getInstance().addPhysicalColumnInDatabaseTable(dynamicDTO, connection, executionContext));
        }
        return saveResultDTO;
    }

    private SaveResultDTO updatePhysicalColumnInDatabaseTable(List<DynamicDTO> boListToUpdate, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        for (DynamicDTO dynamicDTO : boListToUpdate) {
            if (((Boolean) dynamicDTO.get(DSPDMConstants.UpdateCustomAttributeFlags.IS_ATTR_DEFAULT_UPDATE_REQUESTED)) && (dynamicDTO.containsKey(DSPDMConstants.BoAttrName.ATTRIBUTE_DEFAULT))) {
                saveResultDTO.addResult(MetadataStructureChangeDAOImpl.getInstance().updatePhysicalColumnInDatabaseTable(dynamicDTO, DSPDMConstants.BoAttrName.ATTRIBUTE_DEFAULT, connection, executionContext));
            }
            if (dynamicDTO.get(DSPDMConstants.BoAttrName.IS_MANDATORY) != null) {
                saveResultDTO.addResult(MetadataStructureChangeDAOImpl.getInstance().updatePhysicalColumnInDatabaseTable(dynamicDTO, DSPDMConstants.BoAttrName.IS_MANDATORY, connection, executionContext));
            }
            if (((Boolean) dynamicDTO.get(DSPDMConstants.UpdateCustomAttributeFlags.IS_ATTR_DATATYPE_UPDATE_REQUESTED)) && (StringUtils.hasValue((String) dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE)))) {
                saveResultDTO.addResult(MetadataStructureChangeDAOImpl.getInstance().updatePhysicalColumnInDatabaseTable(dynamicDTO, DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE, connection, executionContext));
            }
        }
        return saveResultDTO;
    }

    private SaveResultDTO addPhysicalTableInDatabase(List<DynamicDTO> boListToSave, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // map to hold child business object map
        Map<String, List<DynamicDTO>> childrenMapToSave = null;
        // list to hold all the business object attributes to be saved for a particular bo
        List<DynamicDTO> businessObjectAttributes = null;
        // list to hold all the business object relationships to be saved for a particular bo
        List<DynamicDTO> businessObjectRelationships_FK = null;
        // list to hold all the business object attributes unique constraints to be saved for a particular bo
        List<DynamicDTO> uniqueConstraints = null;
        // list to hold all the business object attributes search indexes to be saved for a particular bo
        List<DynamicDTO> searchIndexes = null;
        for (DynamicDTO businessObjectDynamicDTO : boListToSave) {
            childrenMapToSave = (Map<String, List<DynamicDTO>>) businessObjectDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
            businessObjectAttributes = childrenMapToSave.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
            businessObjectRelationships_FK = childrenMapToSave.get(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP);
            uniqueConstraints=childrenMapToSave.get(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS);
            searchIndexes=childrenMapToSave.get(DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES);
            saveResultDTO.addResult(MetadataStructureChangeDAOImpl.getInstance().addPhysicalTableInDatabase(businessObjectDynamicDTO,
                    businessObjectAttributes, businessObjectRelationships_FK,uniqueConstraints, searchIndexes, connection, executionContext));
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO updateCustomAttributes(List<DynamicDTO> boListToUpdate, Connection serviceDBConnection,
                                                Connection dataModelDBConnection, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        // setting current operation id  in execution context for logging purpose
        executionContext.setCurrentOperationId(DSPDMConstants.BusinessObjectOperations.UPDATE_CUSTOM_ATTR_OPR.getId());
        if (CollectionUtils.hasValue(boListToUpdate)) {
            IDynamicDAO boAttrDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, executionContext);
            // read bo attr ids toUpdate
            List<DynamicDTO> boAttrDynamicDTOListFromRead = boAttrDAO.readCopyWithCustomFilter(boListToUpdate, executionContext,
                    DSPDMConstants.BoAttrName.BO_NAME, DSPDMConstants.BoAttrName.BO_ATTR_NAME);
            Map<String, List<DynamicDTO>> boAttrDynamicDTOListFromReadMapByBOName =
                    CollectionUtils.prepareIgnoreCaseMapOfListFromStringValuesOfKey(boAttrDynamicDTOListFromRead, DSPDMConstants.BoAttrName.BO_NAME);

            // the following list will contain cloned DTOs while will be used to update physical table columns
            List<DynamicDTO> dynamicDTOListForPhysicalTableUpdate = new ArrayList<>();
            // Cloning Ids and related required data only
            for (DynamicDTO boAttrDynamicDTOToUpdate : boListToUpdate) {
                boolean isAttributeDefaultUpdateRequested = false;
                boolean isAttributeDataTypeUpdateRequested = true;
                // validating that if we get a request to update unit, then we must make sure there's no data inside physical table against that attribute
                if(!StringUtils.isNullOrEmpty((String) boAttrDynamicDTOToUpdate.get(DSPDMConstants.BoAttrName.UNIT))){
                    String boName = (String) boAttrDynamicDTOToUpdate.get(DSPDMConstants.BoAttrName.BO_NAME);
                    String boAttrName = (String) boAttrDynamicDTOToUpdate.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                    BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(boName, executionContext).setReadFirst(true);
                    businessObjectInfo.addFilter(boAttrName, Operator.NOT_EQUALS, null);
                    List<DynamicDTO> list = getDynamicDAO(boName, executionContext).read(businessObjectInfo, executionContext);
                    if(CollectionUtils.hasValue(list)) {
                        throw new DSPDMException("Cannot update unit. Attribute '{}' has already data inside it.",
                                Locale.getDefault(),boAttrName);
                    }
                }
                boAttrDynamicDTOListFromRead = boAttrDynamicDTOListFromReadMapByBOName.get(boAttrDynamicDTOToUpdate.get(DSPDMConstants.BoAttrName.BO_NAME));
                DynamicDTO boAttrDynamicDTOFromRead = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(boAttrDynamicDTOListFromRead,
                        DSPDMConstants.BoAttrName.BO_ATTR_NAME, boAttrDynamicDTOToUpdate.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME));
                boAttrDynamicDTOToUpdate.put(DSPDMConstants.BoAttrName.BUSINESS_OBJECT_ATTR_ID, boAttrDynamicDTOFromRead.get(DSPDMConstants.BoAttrName.BUSINESS_OBJECT_ATTR_ID));
                boAttrDynamicDTOToUpdate.put(DSPDMConstants.BoAttrName.ENTITY, boAttrDynamicDTOFromRead.get(DSPDMConstants.BoAttrName.ENTITY));
                boAttrDynamicDTOToUpdate.put(DSPDMConstants.BoAttrName.ATTRIBUTE, boAttrDynamicDTOFromRead.get(DSPDMConstants.BoAttrName.ATTRIBUTE));
                boAttrDynamicDTOToUpdate.setPrimaryKeyColumnNames(boAttrDynamicDTOFromRead.getPrimaryKeyColumnNames());// we need pk for logging purpose
                // if datatype is not in the request, we need to read it from database just like we did for business_object_attr_ids
                if (StringUtils.isNullOrEmpty((String) boAttrDynamicDTOToUpdate.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE))) {
                    isAttributeDataTypeUpdateRequested = false;
                    boAttrDynamicDTOToUpdate.put(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE, boAttrDynamicDTOFromRead.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE));
                } else {
                    String newDataType = (String) boAttrDynamicDTOToUpdate.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE);
                    String oldDataType = (String) boAttrDynamicDTOFromRead.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE);
                    if (newDataType.equalsIgnoreCase(oldDataType)) {
                        isAttributeDataTypeUpdateRequested = false;
                    }
                }
                // Checking if attribute default is already present or not? This check is applied for sql server only since we've to see if we get a request
                // to update the attribute default and if the column already have default set, need to drop that default and set a new default value
                if (boAttrDynamicDTOToUpdate.containsKey(DSPDMConstants.BoAttrName.ATTRIBUTE_DEFAULT)) {
                    Object newAttributeDefault = boAttrDynamicDTOToUpdate.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DEFAULT);
                    Object oldAttributeDefault = boAttrDynamicDTOFromRead.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DEFAULT);
                    if ((newAttributeDefault == null) && (oldAttributeDefault == null)) {
                        isAttributeDefaultUpdateRequested = false;
                    } else if ((newAttributeDefault != null) && (oldAttributeDefault == null)) {
                        isAttributeDefaultUpdateRequested = true;
                    } else if ((newAttributeDefault == null) && (oldAttributeDefault != null)) {
                        isAttributeDefaultUpdateRequested = true;
                    } else if (newAttributeDefault.toString().equalsIgnoreCase(oldAttributeDefault.toString())) {
                        isAttributeDefaultUpdateRequested = false;
                    } else {
                        isAttributeDefaultUpdateRequested = true;
                    }
                    if ((isAttributeDefaultUpdateRequested) && (StringUtils.hasValue((String) oldAttributeDefault)) && (isMSSQLServerDialect((String) boAttrDynamicDTOFromRead.get(DSPDMConstants.BoAttrName.BO_NAME)))) {
                        // means we already have default value set which is different from what we get to update. We need to delete the old one now from physical table
                        rootSaveResultDTO.addResult(MetadataStructureChangeDAOImpl.getInstance().
                                dropDefaultConstraintForMSSQLServerOnly(boAttrDynamicDTOFromRead, dataModelDBConnection, executionContext));
                    }
                }
                // we are cloning the dynamic DTOs because we need to keep track of what attribute properties are being
                // requested to push updates on physical table's columns and what are not.
                // if we don't clone like below, the boList will be updated and we cannot decide which attribute properties were requested.
                if (isPhysicalColumnChangeRequested(boAttrDynamicDTOToUpdate)) {
                    DynamicDTO dynamicDTOForPhysicalTableUpdate = (DynamicDTO) boAttrDynamicDTOToUpdate.clone();
                    // the following custom keys will be used updatePhysicalColumnInDatabaseTable to determine is attr_datatype or attr_default is updated or note
                    dynamicDTOForPhysicalTableUpdate.put(DSPDMConstants.UpdateCustomAttributeFlags.IS_ATTR_DATATYPE_UPDATE_REQUESTED, isAttributeDataTypeUpdateRequested);
                    dynamicDTOForPhysicalTableUpdate.put(DSPDMConstants.UpdateCustomAttributeFlags.IS_ATTR_DEFAULT_UPDATE_REQUESTED, isAttributeDefaultUpdateRequested);
                    dynamicDTOListForPhysicalTableUpdate.add(dynamicDTOForPhysicalTableUpdate);
                }
            }
            // save metadata
            rootSaveResultDTO.addResult(saveOrUpdate(boListToUpdate, false, serviceDBConnection, executionContext));
            // we should be updating physical table only once we get anything changed in the metadata table
            if (rootSaveResultDTO.isAnyRecordUpdated()) {
                // Metadata updated successfully now update column of physical database table
                rootSaveResultDTO.addResult(updatePhysicalColumnInDatabaseTable(dynamicDTOListForPhysicalTableUpdate, dataModelDBConnection, executionContext));
            }
            //logging
            trackChangeHistory(rootSaveResultDTO, executionContext.getCurrentOperationId(), boListToUpdate, serviceDBConnection, executionContext);
        }
        return rootSaveResultDTO;
    }

    private boolean isPhysicalColumnChangeRequested(DynamicDTO dynamicDTO) {
        if (dynamicDTO.containsKey(DSPDMConstants.BoAttrName.ATTRIBUTE_DEFAULT)) {
            return true;
        } else if (dynamicDTO.get(DSPDMConstants.BoAttrName.IS_MANDATORY) != null) {
            return true;
        } else if (!StringUtils.isNullOrEmpty((String) dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE))) {
            return true;
        } else {
            logger.info("No attribute change found that should issue an alter statement to the physical table for the bo attr name '{}' ", dynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME));
            return false;
        }
    }

    /* ******************************************************************** */
    /* ************** BUSINESS METHOD DELETE CUSTOM ATTRIBUTE ************* */
    /* ******************************************************************** */

    /**
     * It adds relationship metadata
     * it adds physical database constraint on the child table
     *
     * @param businessObjectsRelationshipsToBeCreated
     * @param dataModelDBConnection
     * @param serviceDBConnection
     * @param executionContext
     * @return
     */
    @Override
    public SaveResultDTO addBusinessObjectRelationships(List<DynamicDTO> businessObjectsRelationshipsToBeCreated, Connection dataModelDBConnection,
                                                        Connection serviceDBConnection, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        // setting current operation id  in execution context for logging purpose
        executionContext.setCurrentOperationId(DSPDMConstants.BusinessObjectOperations.ADD_BUS_OBJ_RELATIONSHIPS_OPR.getId());
        // 1. create relationship metadata
        rootSaveResultDTO.addResult(createBusinessObjectRelationshipMetadata_AddBusinessObjectRelationships(businessObjectsRelationshipsToBeCreated, serviceDBConnection, executionContext));
        if (rootSaveResultDTO.isAnyRecordInserted()) {
            // Metadata deleted successfully now delete physical relationship foreign key constraint from database
            Map<String, List<DynamicDTO>> groupByRelationshipName = CollectionUtils.groupDynamicDTOByPropertyValueIgnoreCase(businessObjectsRelationshipsToBeCreated, DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME);
            for (List<DynamicDTO> relationshipsInANamedGroup : groupByRelationshipName.values()) {
                // 2. create relationship
                rootSaveResultDTO.addResult(MetadataStructureChangeDAOImpl.getInstance().addPhysicalRelationshipToDatabase(relationshipsInANamedGroup, dataModelDBConnection, executionContext));
            }
            // Logging Business Operation
            trackChangeHistory(rootSaveResultDTO, executionContext.getCurrentOperationId(),businessObjectsRelationshipsToBeCreated,
                    serviceDBConnection, executionContext);
        }
        return rootSaveResultDTO;
    }

    private static SaveResultDTO createBusinessObjectRelationshipMetadata_AddBusinessObjectRelationships(List<DynamicDTO> businessObjectsRelationshipsToBeCreated, Connection connection, ExecutionContext executionContext) {

        SaveResultDTO saveResultDTO = new SaveResultDTO();

        if (CollectionUtils.hasValue(businessObjectsRelationshipsToBeCreated)) {

            String parentBoName = null;
            String childBoName = null;
            String parentBoAttrName = null;
            String childBoAttrName = null;
            // dao
            IDynamicDAO parentDynamicDAO = null;
            IDynamicDAO childDynamicDAO = null;
            // objects
            DynamicDTO parentBusinessObjectDTO = null;
            DynamicDTO childBusinessObjectDTO = null;
            DynamicDTO parentBusinessObjectAttrDTO = null;
            DynamicDTO childBusinessObjectAttrDTO = null;

            // now iterate over all the children to be saved of the current child bo name/type
            for (DynamicDTO busObjRelationshipDynamicDTO : businessObjectsRelationshipsToBeCreated) {
                parentBoName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_NAME);
                childBoName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME);
                parentBoAttrName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME);
                childBoAttrName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
                if(busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_ORDER) == null) {
                    // means child_order is not provided. We need to determine ourselves now
                    // parent/child relationship might be in request packet having child order, in that case we need to put same child order
                    // for current relationship which is of other attribute of same parent/child relationship
                    Integer childOrderFromRequestPacket = getChildOrderFromRequestPacket(businessObjectsRelationshipsToBeCreated, parentBoName, childBoName);
                    if (childOrderFromRequestPacket == null) {
                        //means childOrder Not Found from request packet, now we'll search in db
                        Map<String, List<DynamicDTO>> childRelationships = null;
                        try {
                            childRelationships = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(parentBoName, executionContext).readMetadataChildRelationships(executionContext);
                        } catch (Exception e) {
                            // each exception because no parent relationship is found, this is the case when there's a recursive relationship with he same bo that is in request packet
                        }
                        if (CollectionUtils.hasValue(childRelationships)) {
                            // if parent relationship exists in db then fist we'll see if parent/child relationship exists, if so, we'll get same child order,
                            // else we'll get maximum child order from db and will assign the max value.
                            if (childRelationships.containsKey(childBoName)) {
                                Integer childOrderFromDB = (Integer) childRelationships.get(childBoName).get(0).get(DSPDMConstants.BoAttrName.CHILD_ORDER);
                                busObjRelationshipDynamicDTO.put(DSPDMConstants.BoAttrName.CHILD_ORDER, childOrderFromDB);
                            } else {
                                // set default child order to zero
                                busObjRelationshipDynamicDTO.put(DSPDMConstants.BoAttrName.CHILD_ORDER, 0);
                            }
                        } else {
                            // at this point, childOrder is neither in request packet nor in db
                            // set default child order to zero
                            busObjRelationshipDynamicDTO.put(DSPDMConstants.BoAttrName.CHILD_ORDER, 0);
                        }
                    } else {
                        busObjRelationshipDynamicDTO.put(DSPDMConstants.BoAttrName.CHILD_ORDER, childOrderFromRequestPacket);
                    }
                }

                // read dynamic dao of each one
                parentDynamicDAO = getDynamicDAO(parentBoName, executionContext);
                childDynamicDAO = getDynamicDAO(childBoName, executionContext);
                // get main business object dto
                parentBusinessObjectDTO = parentDynamicDAO.readMetadataBusinessObject(executionContext);
                childBusinessObjectDTO = childDynamicDAO.readMetadataBusinessObject(executionContext);
                // get attribute metddata
                parentBusinessObjectAttrDTO = parentDynamicDAO.readMetadataMap(executionContext).get(parentBoAttrName);
                childBusinessObjectAttrDTO = childDynamicDAO.readMetadataMap(executionContext).get(childBoAttrName);
                // !IMPORTANT fill remaining data
                fillBusinessObjectRelationshipMetadata(busObjRelationshipDynamicDTO, parentBusinessObjectDTO, childBusinessObjectDTO, parentBusinessObjectAttrDTO, childBusinessObjectAttrDTO);
            }
            // save all relationships using relationship dynamic dao
            IDynamicDAOImpl busObjRelationshipDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, executionContext);
            saveResultDTO.addResult(busObjRelationshipDAO.saveOrUpdate(businessObjectsRelationshipsToBeCreated, false, connection, executionContext));
            //update reference columns in attribute metadata for those where relationship has already been added
            updateBusinessObjectAttributesAddReferenceData_AddBusinessObjectRelationship(businessObjectsRelationshipsToBeCreated, connection, executionContext);
        }
        return saveResultDTO;
    }

    private static Integer getChildOrderFromRequestPacket(List<DynamicDTO> relationshipListToSave, String parentBoName, String childBoName){
        if(CollectionUtils.hasValue(relationshipListToSave)){
            for(DynamicDTO dynamicDTO: relationshipListToSave){
                String parentBoNameFromDTO = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_NAME);
                String childBoNameFromDTO = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME);
                if(parentBoName.equalsIgnoreCase(parentBoNameFromDTO) && childBoName.equalsIgnoreCase(childBoNameFromDTO)){
                    return (Integer) dynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_ORDER);
                }
            }
        }
        return null;
    }

    private static void fillBusinessObjectRelationshipMetadata(DynamicDTO busObjRelationshipDynamicDTO,
                                                               DynamicDTO parentBusinessObjectDTO,
                                                               DynamicDTO childBusinessObjectDTO,
                                                               DynamicDTO parentBusinessObjectAttrDTO,
                                                               DynamicDTO childBusinessObjectAttrDTO) {

        // if child entity has a value then set in relationship metadata
        if (StringUtils.hasValue((String) childBusinessObjectDTO.get(DSPDMConstants.BoAttrName.ENTITY))) {
            busObjRelationshipDynamicDTO.put(DSPDMConstants.BoAttrName.CHILD_ENTITY_NAME, childBusinessObjectDTO.get(DSPDMConstants.BoAttrName.ENTITY));
        }
        // if parent entity has a value then set in relationship metadata
        if (StringUtils.hasValue((String) parentBusinessObjectDTO.get(DSPDMConstants.BoAttrName.ENTITY))) {
            busObjRelationshipDynamicDTO.put(DSPDMConstants.BoAttrName.PARENT_ENTITY_NAME, parentBusinessObjectDTO.get(DSPDMConstants.BoAttrName.ENTITY));
        }

        // if child entity has a value then set in relationship metadata
        if (StringUtils.hasValue((String) childBusinessObjectAttrDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE))) {
            busObjRelationshipDynamicDTO.put(DSPDMConstants.BoAttrName.CHILD_ATTRIBUTE_NAME, childBusinessObjectAttrDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE));
        }
        // if parent entity has a value then set in relationship metadata
        if (StringUtils.hasValue((String) parentBusinessObjectAttrDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE))) {
            busObjRelationshipDynamicDTO.put(DSPDMConstants.BoAttrName.PARENT_ATTRIBUTE_NAME, parentBusinessObjectAttrDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE));
        }
        // if parent bo attr name is a primary key in its business object them mark this relationship as a primary key relationship
        if (Boolean.TRUE.equals(parentBusinessObjectAttrDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY))) {
            busObjRelationshipDynamicDTO.put(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, Boolean.TRUE);
        } else {
            busObjRelationshipDynamicDTO.put(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, Boolean.FALSE);
        }
        // relationship name if not already provided
        if (StringUtils.isNullOrEmpty((String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME))) {
            busObjRelationshipDynamicDTO.put(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME, "FK_" + childBusinessObjectDTO.get(DSPDMConstants.BoAttrName.ENTITY) + "_" + childBusinessObjectAttrDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE));
        }

        // set constraint name from relationship name
        String constraintName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME);
        if (constraintName.contains(" ")) {
            constraintName = constraintName.trim().replaceAll(" ", "_");
        }
        busObjRelationshipDynamicDTO.put(DSPDMConstants.BoAttrName.CONSTRAINT_NAME, constraintName);
    }

    /**
     * It drops relationship metadata
     * it drops physical database constraint from the chile table
     *
     * @param businessObjectsRelationshipsToBeDropped
     * @param dataModelDBConnection
     * @param serviceDBConnection
     * @param executionContext
     * @return
     */
    @Override
    public SaveResultDTO dropBusinessObjectRelationships
    (List<DynamicDTO> businessObjectsRelationshipsToBeDropped, Connection dataModelDBConnection, Connection serviceDBConnection
            , ExecutionContext executionContext) {
        // read relationships from database on the basis of relationship name and constraint name
        List<DynamicDTO> finalRelationshipsToBeDropped = readFinalBusinessObjectRelationshipsToDropFromDB(businessObjectsRelationshipsToBeDropped, serviceDBConnection, executionContext);
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        // setting current operation id  in execution context for logging purpose
        executionContext.setCurrentOperationId(DSPDMConstants.BusinessObjectOperations.DROP_BUS_OBJ_RELATIONSHIPS_OPR.getId());
        // clear reference fields from attribute metadata.
        rootSaveResultDTO.addResult(updateBusinessObjectAttributesClearReferenceData(finalRelationshipsToBeDropped, serviceDBConnection, executionContext));
        // delete relationship metadata
        IDynamicDAOImpl businessObjectRelationshipDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, executionContext);
        rootSaveResultDTO.addResult(businessObjectRelationshipDAO.delete(finalRelationshipsToBeDropped, false, serviceDBConnection, executionContext));
        if (!rootSaveResultDTO.isAnyRecordDeleted()) {
            throw new DSPDMException("Unable to delete business object relationships. Relationships metadata might be already deleted. Please refresh metadata and try again.",
                    executionContext.getExecutorLocale(), finalRelationshipsToBeDropped.size());
        } else {
            // create a set to hold dropped relationship names so that if a name exists more than once then the drop db constraint is not invoked on it again
            Set<String> droppedRelationshipNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            // Metadata deleted successfully now delete physical relationship foreign key constraint from database
            for (DynamicDTO businessObjectsRelationship : finalRelationshipsToBeDropped) {
                String relationshipNameToBeDropped = (String) businessObjectsRelationship.get((DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME));
                if (!droppedRelationshipNames.contains(relationshipNameToBeDropped)) {
                    rootSaveResultDTO.addResult(MetadataStructureChangeDAOImpl.getInstance().deletePhysicalRelationshipFromDatabase(businessObjectsRelationship, dataModelDBConnection, executionContext));
                    droppedRelationshipNames.add(relationshipNameToBeDropped);
                }
            }
            // Logging Business Operation
            trackChangeHistory(rootSaveResultDTO, executionContext.getCurrentOperationId(), businessObjectsRelationshipsToBeDropped,
                    serviceDBConnection, executionContext);
        }
        return rootSaveResultDTO;
    }

    /**
     * It reads relations from data store on the basis of relationship name
     *
     * @param businessObjectsRelationshipsToBeDropped
     * @param connection
     * @param executionContext
     * @return
     */
    private static List<DynamicDTO> readFinalBusinessObjectRelationshipsToDropFromDB
    (List<DynamicDTO> businessObjectsRelationshipsToBeDropped, Connection connection, ExecutionContext
            executionContext) {

        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, executionContext);
        // ready first 10 records as max
        businessObjectInfo.addPagesToRead(executionContext, 10, 1);
        IDynamicDAOImpl dynamicDAOImpl = getDynamicDAOImpl(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, executionContext);
        String relationshipName = null;
        String constraintName = null;
        List<DynamicDTO> relationshipsAfterReadFromDB = null;
        List<DynamicDTO> finalRelationshipsToBeDropped = new ArrayList<>(businessObjectsRelationshipsToBeDropped.size());
        for (DynamicDTO businessObjectRelationshipToBeDropped : businessObjectsRelationshipsToBeDropped) {
            relationshipName = (String) businessObjectRelationshipToBeDropped.get(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME);
            constraintName = (String) businessObjectRelationshipToBeDropped.get(DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
            // clear all existing filters
            if ((businessObjectInfo.getFilterList() != null) && (businessObjectInfo.getFilterList().getFilters() != null)) {
                businessObjectInfo.getFilterList().getFilters().clear();
            }
            if (StringUtils.hasValue(relationshipName)) {
                businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME, relationshipName);
            } else if (StringUtils.hasValue(constraintName)) {
                businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.CONSTRAINT_NAME, constraintName);
            } else {
                throw new DSPDMException("Cannot drop business object relationship. There is no relationship or constraint name provided.",
                        executionContext.getExecutorLocale(), relationshipName);
            }
            // do not call read/count on the current dao but get the dynamic dao for the given business object name and then call read/count on it
            relationshipsAfterReadFromDB = dynamicDAOImpl.read(businessObjectInfo, false, connection, executionContext);
            if (CollectionUtils.isNullOrEmpty(relationshipsAfterReadFromDB)) {
                if (StringUtils.hasValue(relationshipName)) {
                    throw new DSPDMException("Cannot drop business object relationship. No relationship with name '{}' exists.",
                            executionContext.getExecutorLocale(), relationshipName);
                } else if (StringUtils.hasValue(constraintName)) {
                    throw new DSPDMException("Cannot drop business object relationship. No constraint with name '{}' exists.",
                            executionContext.getExecutorLocale(), constraintName);
                }
            } else {
                finalRelationshipsToBeDropped.addAll(relationshipsAfterReadFromDB);
            }
        }
        return finalRelationshipsToBeDropped;
    }

    @Override
    public SaveResultDTO dropBusinessObjects(List<DynamicDTO> boListToDelete, Connection dataModelDBConnection,
                                             Connection serviceDBConnection, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        // setting current operation id  in execution context for logging purpose
        executionContext.setCurrentOperationId(DSPDMConstants.BusinessObjectOperations.DROP_BUSINESS_OBJECT_OPR.getId());
        // delete metadata
        rootSaveResultDTO.addResult(deleteMetadata(boListToDelete, serviceDBConnection, executionContext));
        if (!rootSaveResultDTO.isAnyRecordDeleted()) {
            throw new DSPDMException("Unable to delete business object metadata. Business object might be already deleted. Please refresh metadata and try again.",
                    executionContext.getExecutorLocale(), boListToDelete.size());
        } else {
            // Metadata deleted successfully now delete physical database table
            for (DynamicDTO dynamicDTO : boListToDelete) {
                rootSaveResultDTO.addResult(MetadataStructureChangeDAOImpl.getInstance().deletePhysicalTableFromDatabase(dynamicDTO, dataModelDBConnection, executionContext));
            }
            // Logging Business Operation
            trackChangeHistory(rootSaveResultDTO, executionContext.getCurrentOperationId(),boListToDelete,
                    serviceDBConnection, executionContext);
        }
        return rootSaveResultDTO;
    }

    private static SaveResultDTO deleteMetadata(List<DynamicDTO> boListToDelete, Connection
            connection, ExecutionContext executionContext) {
        IDynamicDAOImpl businessObjectDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUSINESS_OBJECT, executionContext);
        // extract all the business object names from the list to read actual records
        List<String> boNamesFromList = CollectionUtils.getStringValuesFromList(boListToDelete, DSPDMConstants.BoAttrName.BO_NAME);
        // find primary keys (and related records for logging purpose)from database to delete against bo names
        List<DynamicDTO> finalListToDelete = businessObjectDAO.read(
                new BusinessObjectInfo(DSPDMConstants.BoName.BUSINESS_OBJECT, executionContext)
                        .addFilter(DSPDMConstants.BoAttrName.BO_NAME, Operator.IN, boNamesFromList)
                        .setReadAllRecords(true), false, connection, executionContext);
        if (finalListToDelete.size() != boListToDelete.size()) {
            throw new DSPDMException("Not same number of business objects found for the names '{}'", executionContext.getExecutorLocale(), CollectionUtils.getCommaSeparated(boNamesFromList));
        }

        SaveResultDTO saveResultDTO = new SaveResultDTO();
        Map<String, List<DynamicDTO>> childrenMap = null;
        for (DynamicDTO businessObjectToDelete : finalListToDelete) {
            // delete children
            deleteMetadataChildrenOnly(businessObjectToDelete, connection, executionContext);
        }
        // delete business object
        saveResultDTO.addResult(businessObjectDAO.delete(finalListToDelete, false, connection, executionContext));
        // clear old list and add new objects to be deleted.
        boListToDelete.clear();
        boListToDelete.addAll(finalListToDelete);
        // Metadata deleted successfully
        return saveResultDTO;
    }

    private static SaveResultDTO deleteMetadataChildrenOnly(DynamicDTO businessObjectToDelete, Connection
            connection, ExecutionContext executionContext) {
        IDynamicDAOImpl businessObjectAttrDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, executionContext);
        IDynamicDAOImpl busObjAttrUniqueConstraintsDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, executionContext);
        IDynamicDAOImpl busObjAttrSearchIndexesDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES, executionContext);
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

        // 4. delete search indexes
        List<DynamicDTO> indexes = busObjAttrSearchIndexesDAO.read(
                new BusinessObjectInfo(DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES, executionContext)
                        .addFilter(DSPDMConstants.BoAttrName.BO_NAME, boName)
                        .setReadAllRecords(true)
                , false, connection, executionContext);
        if (CollectionUtils.hasValue(indexes)) {
            saveResultDTO.addResult(busObjAttrSearchIndexesDAO.delete(indexes, false, connection, executionContext));
            childrenMap.put(DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES, indexes);
        }

        // 5. attributes will be deleted after the deletion of constraints and relationships
        // 5. delete attributes
        List<DynamicDTO> attributes = businessObjectAttrDAO.read(
                new BusinessObjectInfo(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, executionContext)
                        .addFilter(DSPDMConstants.BoAttrName.BO_NAME, boName)
                        .setReadAllRecords(true)
                , false, connection, executionContext);
        if (CollectionUtils.hasValue(attributes)) {
            saveResultDTO.addResult(businessObjectAttrDAO.delete(attributes, false, connection, executionContext));
            childrenMap.put(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, attributes);
        }

        // 6. delete business object group entries
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

    @Override
    public SaveResultDTO deleteCustomAttributes(List<DynamicDTO> boListToDelete, Connection serviceDBConnection,
                                                Connection dataModelDBConnection, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        // setting current operation id  in execution context for logging purpose
        executionContext.setCurrentOperationId(DSPDMConstants.BusinessObjectOperations.DROP_CUSTOM_ATTR_OPR.getId());
        // delete metadata
        rootSaveResultDTO.addResult(delete(boListToDelete, false, serviceDBConnection, executionContext));
        if (rootSaveResultDTO.getDeletedRecordsCount() != boListToDelete.size()) {
            throw new DSPDMException("Unable to delete custom attribute. Please check primary key values. Delete operation aborted.", executionContext.getExecutorLocale(), boListToDelete.size());
        } else {
            // Metadata saved successfully now add column to physical database table
            for (DynamicDTO dynamicDTO : boListToDelete) {
                // deleting default constraint if it is applied. And this is for MSSQL Server only
                if(isMSSQLServerDialect((String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME))){
                    if(StringUtils.hasValue((String) dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DEFAULT))){
                        rootSaveResultDTO.addResult(MetadataStructureChangeDAOImpl.getInstance().dropDefaultConstraintForMSSQLServerOnly(dynamicDTO, dataModelDBConnection, executionContext));
                    }
                }
                rootSaveResultDTO.addResult(MetadataStructureChangeDAOImpl.getInstance().deletePhysicalColumnFromDatabaseTable(dynamicDTO, dataModelDBConnection, executionContext));
            }
            //logging
            trackChangeHistory(rootSaveResultDTO, executionContext.getCurrentOperationId(), boListToDelete,
                    serviceDBConnection, executionContext);
        }
        return rootSaveResultDTO;
    }

    /**
     * It adds unique constraint metadata
     * it adds physical unique constraint on the child table
     *
     * @param busObjAttrUniqueConstraintsToBeCreated
     * @param dataModelDBConnection
     * @param serviceDBConnection
     * @param executionContext
     * @return
     */
    @Override
    public SaveResultDTO addUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeCreated, Connection serviceDBConnection,
            Connection dataModelDBConnection, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        // setting current operation id  in execution context for logging purpose
        executionContext.setCurrentOperationId(DSPDMConstants.BusinessObjectOperations.ADD_UNIQUE_CONSTRAINTS_OPR.getId());
        // 1. create unique constraint metadata
        rootSaveResultDTO.addResult(createUniqueConstraintMetadata_AddUniqueConstraints(busObjAttrUniqueConstraintsToBeCreated, serviceDBConnection, executionContext));
        if (rootSaveResultDTO.isAnyRecordInserted()) {
            // Metadata created successfully now create physical unique constraint in database
            Map<String, List<DynamicDTO>> groupByUniqueConstraintName = CollectionUtils.groupDynamicDTOByPropertyValueIgnoreCase(
                    busObjAttrUniqueConstraintsToBeCreated, DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
            for (List<DynamicDTO> uniqueConstraintsInANamedGroup : groupByUniqueConstraintName.values()) {
                // 2. create unique constraint
                rootSaveResultDTO.addResult(MetadataStructureChangeDAOImpl.getInstance().addPhysicalUniqueConstraintToDatabase(
                        uniqueConstraintsInANamedGroup, dataModelDBConnection, executionContext));
            }
            //logging
            trackChangeHistory(rootSaveResultDTO, executionContext.getCurrentOperationId(), busObjAttrUniqueConstraintsToBeCreated,
                    serviceDBConnection, executionContext);
        }
        return rootSaveResultDTO;
    }

    /**
     * It adds search index metadata
     * it adds physical search index on the child table
     *
     * @param busObjAttrSearchIndexesToBeCreated
     * @param dataModelDBConnection
     * @param serviceDBConnection
     * @param executionContext
     * @return
     */
    @Override
    public SaveResultDTO addSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeCreated, Connection serviceDBConnection,
                                              Connection dataModelDBConnection, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        // setting current operation id  in execution context for logging purpose
        executionContext.setCurrentOperationId(DSPDMConstants.BusinessObjectOperations.ADD_SEARCH_INDEXES_OPR.getId());
        // 1. create search index metadata
        rootSaveResultDTO.addResult(createSearchIndexMetadata_AddSearchIndexes(busObjAttrSearchIndexesToBeCreated, serviceDBConnection, executionContext));
        if (rootSaveResultDTO.isAnyRecordInserted()) {
            // Metadata created successfully now create physical search indexes in database
            Map<String, List<DynamicDTO>> groupBySearchIndexName = CollectionUtils.groupDynamicDTOByPropertyValueIgnoreCase(
                    busObjAttrSearchIndexesToBeCreated, DSPDMConstants.BoAttrName.INDEX_NAME);
            for (List<DynamicDTO> searchIndexesInANamedGroup : groupBySearchIndexName.values()) {
                // 2. create search index
                rootSaveResultDTO.addResult(MetadataStructureChangeDAOImpl.getInstance().addPhysicalSearchIndexToDatabase(
                        searchIndexesInANamedGroup, dataModelDBConnection, executionContext));
            }
            //logging
            trackChangeHistory(rootSaveResultDTO, executionContext.getCurrentOperationId(), busObjAttrSearchIndexesToBeCreated,
                    serviceDBConnection, executionContext);
        }
        return rootSaveResultDTO;
    }

    private static SaveResultDTO createUniqueConstraintMetadata_AddUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeCreated, Connection connection, ExecutionContext executionContext) {

        SaveResultDTO saveResultDTO = new SaveResultDTO();

        if (CollectionUtils.hasValue(busObjAttrUniqueConstraintsToBeCreated)) {

            String boName = null;
            String boAttrName = null;
            // dao
            IDynamicDAO dynamicDAO = null;
            // objects
            DynamicDTO businessObjectAttrDTO = null;

            // now iterate over all the children to be saved of the current child bo name/type
            for (DynamicDTO busObjAttrUniqueConstraintDTO : busObjAttrUniqueConstraintsToBeCreated) {
                boName = (String) busObjAttrUniqueConstraintDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
                boAttrName = (String) busObjAttrUniqueConstraintDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                // read dynamic dao
                dynamicDAO = getDynamicDAO(boName, executionContext);
                // get attribute metadata
                businessObjectAttrDTO = dynamicDAO.readMetadataMap(executionContext).get(boAttrName);
                // !IMPORTANT fill remaining data
                fillBusObjAttrUniqueConstraintMetadata(busObjAttrUniqueConstraintDTO, businessObjectAttrDTO);
            }
            // save all unique constraints using unique constraints dynamic dao
            IDynamicDAOImpl busObjAttrUniqueConstraintDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, executionContext);
            saveResultDTO.addResult(busObjAttrUniqueConstraintDAO.saveOrUpdate(busObjAttrUniqueConstraintsToBeCreated, false, connection, executionContext));
        }
        return saveResultDTO;
    }

    private static SaveResultDTO createSearchIndexMetadata_AddSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeCreated, Connection connection, ExecutionContext executionContext) {

        SaveResultDTO saveResultDTO = new SaveResultDTO();

        if (CollectionUtils.hasValue(busObjAttrSearchIndexesToBeCreated)) {

            String boName = null;
            String boAttrName = null;
            // dao
            IDynamicDAO dynamicDAO = null;
            // objects
            DynamicDTO businessObjectAttrDTO = null;

            // now iterate over all the children to be saved of the current child bo name/type
            for (DynamicDTO busObjAttrSearchIndexDTO : busObjAttrSearchIndexesToBeCreated) {
                boName = (String) busObjAttrSearchIndexDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
                boAttrName = (String) busObjAttrSearchIndexDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                // read dynamic dao
                dynamicDAO = getDynamicDAO(boName, executionContext);
                // get attribute metadata
                businessObjectAttrDTO = dynamicDAO.readMetadataMap(executionContext).get(boAttrName);
                // !IMPORTANT fill remaining data
                fillBusObjAttrSearchIndexMetadata(busObjAttrSearchIndexDTO, businessObjectAttrDTO, executionContext);
            }
            // save all search indexes using search indexes dynamic dao
            IDynamicDAOImpl busObjAttrUniqueConstraintDAO = getDynamicDAOImpl(DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES, executionContext);
            saveResultDTO.addResult(busObjAttrUniqueConstraintDAO.saveOrUpdate(busObjAttrSearchIndexesToBeCreated, false, connection, executionContext));
        }
        return saveResultDTO;
    }

    private static void fillBusObjAttrUniqueConstraintMetadata(DynamicDTO busObjAttrUniqueConstraintDTO,
                                                               DynamicDTO businessObjectAttrDTO) {

        // if entity has a value then set in unique constraint metadata
        if (StringUtils.hasValue((String) businessObjectAttrDTO.get(DSPDMConstants.BoAttrName.ENTITY))) {
            busObjAttrUniqueConstraintDTO.put(DSPDMConstants.BoAttrName.ENTITY, businessObjectAttrDTO.get(DSPDMConstants.BoAttrName.ENTITY));
        }
        // if attribute has a value then set in unique constraint metadata
        if (StringUtils.hasValue((String) businessObjectAttrDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE))) {
            busObjAttrUniqueConstraintDTO.put(DSPDMConstants.BoAttrName.ATTRIBUTE, businessObjectAttrDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE));
        }
        // constraint name if not already provided
        if (StringUtils.isNullOrEmpty((String) busObjAttrUniqueConstraintDTO.get(DSPDMConstants.BoAttrName.CONSTRAINT_NAME))) {
            busObjAttrUniqueConstraintDTO.put(DSPDMConstants.BoAttrName.CONSTRAINT_NAME, "UIX_" + busObjAttrUniqueConstraintDTO.get(DSPDMConstants.BoAttrName.ENTITY) + "_" + busObjAttrUniqueConstraintDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE));
        }

        // set constraint name from unique constraint name
        String constraintName = (String) busObjAttrUniqueConstraintDTO.get(DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
        if (constraintName.contains(" ")) {
            constraintName = constraintName.trim().replaceAll(" ", "_");
        }
        busObjAttrUniqueConstraintDTO.put(DSPDMConstants.BoAttrName.CONSTRAINT_NAME, constraintName);
    }

    private static void fillBusObjAttrSearchIndexMetadata(DynamicDTO busObjAttrSearchIndexDTO, DynamicDTO businessObjectAttrDTO,
                                                          ExecutionContext executionContext) {

        // if entity has a value then set in search index metadata
        if (StringUtils.hasValue((String) businessObjectAttrDTO.get(DSPDMConstants.BoAttrName.ENTITY))) {
            busObjAttrSearchIndexDTO.put(DSPDMConstants.BoAttrName.ENTITY, businessObjectAttrDTO.get(DSPDMConstants.BoAttrName.ENTITY));
        }
        // if attribute has a value then set in search index metadata
        if (StringUtils.hasValue((String) businessObjectAttrDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE))) {
            busObjAttrSearchIndexDTO.put(DSPDMConstants.BoAttrName.ATTRIBUTE, businessObjectAttrDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE));
        }
        // index name if not already provided
        if (StringUtils.isNullOrEmpty((String) busObjAttrSearchIndexDTO.get(DSPDMConstants.BoAttrName.INDEX_NAME))) {
            busObjAttrSearchIndexDTO.put(DSPDMConstants.BoAttrName.INDEX_NAME, "SIX_" + busObjAttrSearchIndexDTO.get(DSPDMConstants.BoAttrName.ENTITY) + "_" + busObjAttrSearchIndexDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE));
        }
        // validating if we've a non-string datatype, there shouldn't be any use_case allowed
        String useCase = (String) busObjAttrSearchIndexDTO.get(DSPDMConstants.BoAttrName.USE_CASE);
        String boName = (String) busObjAttrSearchIndexDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        String boAttrName = (String) busObjAttrSearchIndexDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
        String attributeDatatype = (String) businessObjectAttrDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE);
        if(_isMSSQLServerDialect(boName) && StringUtils.hasValue(useCase)){
            // functions on indices (CREATE INDEX SIX_WELL_TEST_REMARK ON WELL_TEST (UPPER(REMARK))) is not allowed for SQLServer
            throw new DSPDMException("Use case (UPPER/LOWER) is not allowed while adding search index for SQLServer database", executionContext.getExecutorLocale());
        } else if(StringUtils.hasValue(useCase) &&
                (!DSPDMConstants.DataTypes.fromAttributeDataType(attributeDatatype, _isMSSQLServerDialect(boName)).isStringDataType())){
            throw new DSPDMException("Use case (UPPER/LOWER) of search index for attribute '{}' on business object '{}' " +
                    "is only allowed on attribute(s) having string datatype. '{}' of type '{}'.",
                    executionContext.getExecutorLocale(), boAttrName, boName, boAttrName, attributeDatatype);
        }
        // set index name from unique index name
        String indexName = (String) busObjAttrSearchIndexDTO.get(DSPDMConstants.BoAttrName.INDEX_NAME);
        if (indexName.contains(" ")) {
            indexName = indexName.trim().replaceAll(" ", "_");
        }
        busObjAttrSearchIndexDTO.put(DSPDMConstants.BoAttrName.INDEX_NAME, indexName);
    }

    /**
     * @param busObjAttrUniqueConstraintsToBeDropped
     * @param serviceDBConnection
     * @param dataModelDBConnection
     * @param executionContext
     * @return
     * @author Muhammad Imran Ansari
     * @since 13-Mar-2021
     */
    public SaveResultDTO dropUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeDropped, Connection serviceDBConnection,
                                               Connection dataModelDBConnection, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        // setting current operation id  in execution context for logging purpose
        executionContext.setCurrentOperationId(DSPDMConstants.BusinessObjectOperations.DROP_UNIQUE_CONSTRAINTS_OPR.getId());
        // read unique constraints from database on the basis of constraint name
        List<DynamicDTO> finalUniqueConstraintsToBeDropped = readFinalUniqueConstraintsToDropFromDB(busObjAttrUniqueConstraintsToBeDropped, serviceDBConnection, executionContext);
        // delete unique constraints metadata
        rootSaveResultDTO.addResult(delete(finalUniqueConstraintsToBeDropped, false, serviceDBConnection, executionContext));
        if (!rootSaveResultDTO.isAnyRecordDeleted()) {
            throw new DSPDMException("Unable to drop unique constraints. Unique constraints metadata might be already deleted. Please refresh metadata and try again.",
                    executionContext.getExecutorLocale());
        } else {
            // create a set to hold dropped unique constraint names so that if a name exists more than once then the drop db constraint is not invoked on it again
            Set<String> droppedConstraintNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            // Metadata deleted successfully now delete physical unique constraint from database
            for (DynamicDTO uniqueConstraintDTO : finalUniqueConstraintsToBeDropped) {
                String constraintNameToBeDropped = (String) uniqueConstraintDTO.get((DSPDMConstants.BoAttrName.CONSTRAINT_NAME));
                if (!droppedConstraintNames.contains(constraintNameToBeDropped)) {
                    rootSaveResultDTO.addResult(MetadataStructureChangeDAOImpl.getInstance().deletePhysicalUniqueConstraintFromDatabase(uniqueConstraintDTO, dataModelDBConnection, executionContext));
                    droppedConstraintNames.add(constraintNameToBeDropped);
                }
            }
            //logging
            trackChangeHistory(rootSaveResultDTO, executionContext.getCurrentOperationId(), busObjAttrUniqueConstraintsToBeDropped,
                    serviceDBConnection, executionContext);
        }
        return rootSaveResultDTO;
    }

    public SaveResultDTO dropSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeDropped, Connection serviceDBConnection,
                                               Connection dataModelDBConnection, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        // setting current operation id  in execution context for logging purpose
        executionContext.setCurrentOperationId(DSPDMConstants.BusinessObjectOperations.DROP_SEARCH_INDEXES_OPR.getId());
        // read search indexes from database on the basis of index name
        List<DynamicDTO> finalSearchIndexesToBeDropped = readFinalSearchIndexesToDropFromDB(busObjAttrSearchIndexesToBeDropped, serviceDBConnection, executionContext);
        // delete search indexes metadata
        rootSaveResultDTO.addResult(delete(finalSearchIndexesToBeDropped, false, serviceDBConnection, executionContext));
        if (!rootSaveResultDTO.isAnyRecordDeleted()) {
            throw new DSPDMException("Unable to drop search indexes. Search index metadata might be already deleted. Please refresh metadata and try again.",
                    executionContext.getExecutorLocale());
        } else {
            // create a set to hold dropped search indexes names so that if a name exists more than once then the drop db index is not invoked on it again
            Set<String> droppedConstraintNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            // Metadata deleted successfully now delete physical search index from database
            for (DynamicDTO searchIndexDTO : finalSearchIndexesToBeDropped) {
                String indexNameToBeDropped = (String) searchIndexDTO.get((DSPDMConstants.BoAttrName.INDEX_NAME));
                if (!droppedConstraintNames.contains(indexNameToBeDropped)) {
                    rootSaveResultDTO.addResult(MetadataStructureChangeDAOImpl.getInstance().deletePhysicalSearchIndexFromDatabase(searchIndexDTO, dataModelDBConnection, executionContext));
                    droppedConstraintNames.add(indexNameToBeDropped);
                }
            }
            //logging
            trackChangeHistory(rootSaveResultDTO, executionContext.getCurrentOperationId(), busObjAttrSearchIndexesToBeDropped,
                    serviceDBConnection, executionContext);
        }
        return rootSaveResultDTO;
    }

    /**
     * It reads unique constraints from data store on the basis of constraint name
     *
     * @param busObjAttrUniqueConstraintsToBeDropped
     * @param connection
     * @param executionContext
     * @return
     */
    private static List<DynamicDTO> readFinalUniqueConstraintsToDropFromDB(List<DynamicDTO> busObjAttrUniqueConstraintsToBeDropped,
                                                                           Connection connection, ExecutionContext executionContext) {
        // extract all the constraint names from the list
        List<String> constraintNamesToDrop = CollectionUtils.getUpperCaseValuesFromListOfMap(busObjAttrUniqueConstraintsToBeDropped, DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, executionContext);
        // do not call read/count on the current dao but get the dynamic dao for the given business object name and then call read/count on it
        IDynamicDAOImpl dynamicDAOImpl = getDynamicDAOImpl(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, executionContext);
        return dynamicDAOImpl.readLongRangeUsingInClause(businessObjectInfo, null, DSPDMConstants.BoAttrName.CONSTRAINT_NAME, constraintNamesToDrop.toArray(), Operator.IN, connection, executionContext);
    }

    /**
     * It reads search indexes from data store on the basis of index name
     *
     * @param busObjAttrSearchIndexesToBeDropped
     * @param connection
     * @param executionContext
     * @return
     */
    private static List<DynamicDTO> readFinalSearchIndexesToDropFromDB(List<DynamicDTO> busObjAttrSearchIndexesToBeDropped,
                                                                           Connection connection, ExecutionContext executionContext) {
        // extract all the index names from the list
        List<String> indexNamesToDrop = CollectionUtils.getUpperCaseValuesFromListOfMap(busObjAttrSearchIndexesToBeDropped, DSPDMConstants.BoAttrName.INDEX_NAME);
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES, executionContext);
        // do not call read/count on the current dao but get the dynamic dao for the given business object name and then call read/count on it
        IDynamicDAOImpl dynamicDAOImpl = getDynamicDAOImpl(DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES, executionContext);
        return dynamicDAOImpl.readLongRangeUsingInClause(businessObjectInfo, null, DSPDMConstants.BoAttrName.INDEX_NAME, indexNamesToDrop.toArray(), Operator.IN, connection, executionContext);
    }

    protected static void trackChangeHistory(SaveResultDTO saveResultDTO,int userPerformedOprId, List<DynamicDTO> boListToSaveUpdateDelete,
                                             Connection serviceDBConnection, ExecutionContext executionContext){
        // Logging Business Operation
        List<DynamicDTO> busObjAttrChangeHistoryDTOList = executionContext.getBusObjAttrChangeHistoryDTOList();
        if(CollectionUtils.hasValue(busObjAttrChangeHistoryDTOList)){
            // get user performed operation dto to be saved from execution context
            DynamicDTO userOperationDynamicDTO = executionContext.getUserOperationDTO();
            // set effected row count and operation type id
            Integer effectedRowCount = executionContext.getOperationSequenceNumber() - executionContext.getDdlOperationsCount();
            userOperationDynamicDTO.put(DSPDMConstants.BoAttrName.EFFECTED_ROWS_COUNT, effectedRowCount);
            // As introduce business object will put data in BUSINESS OBJECT first, so putting the name accordingly
            userOperationDynamicDTO.put(DSPDMConstants.BoAttrName.BO_NAME, boListToSaveUpdateDelete.get(0).getType());
            userOperationDynamicDTO.put(DSPDMConstants.BoAttrName.R_BUSINESS_OBJECT_OPR_ID, userPerformedOprId);
            // call save api on user performed operation and add response to save result
            IDynamicDAOImpl dynamicDAOForUserPerformedOpr = getDynamicDAOImpl(DSPDMConstants.BoName.USER_PERFORMED_OPR, executionContext);
            saveResultDTO.addOperationResult(dynamicDAOForUserPerformedOpr.saveOrUpdate(Arrays.asList(userOperationDynamicDTO), false,
                    serviceDBConnection, executionContext));
            // now the user performed operation dto has primary key after save is successful on it
            Object userPerformedOperationId = userOperationDynamicDTO.get(DSPDMConstants.BoAttrName.USER_PERFORMED_OPR_ID);
            // set user performed operation id in all its children going to be saved
            CollectionUtils.setPropertyNameAndPropertyValue(busObjAttrChangeHistoryDTOList, DSPDMConstants.BoAttrName.USER_PERFORMED_OPR_ID, userPerformedOperationId);
            // now call save on business object attribute change history table
            IDynamicDAOImpl dynamicDAOForBusObjChangeHistory = getDynamicDAOImpl(DSPDMConstants.BoName.BUS_OBJ_ATTR_CHANGE_HISTORY, executionContext);
            saveResultDTO.addOperationResult(dynamicDAOForBusObjChangeHistory.saveOrUpdate(busObjAttrChangeHistoryDTOList, false,
                    serviceDBConnection, executionContext));
        }
    }

    @Override
    public List<String[]> getAttributeUnitMapping(String boName, Connection connection, ExecutionContext executionContext) {
        return MetadataStructureChangeDAOImpl.getInstance().getAttributesVsUnitMapping(boName, connection, executionContext);
    }

    @Override
    public List<DynamicDTO> restoreBusinessObjectGroupFromHistory(List<DynamicDTO> boList, Connection connection, ExecutionContext executionContext) {
        return MetadataStructureChangeDAOImpl.getInstance().restoreBOGroupFromHistory(boList, connection, executionContext);
    }

    private static String getRelatedBoAttrNameFromRelationships(DynamicDTO busObjRelationshipDynamicDTO,
                                                                List<DynamicDTO> busObjRelationshipDynamicDTOListAlreadySaved, ExecutionContext executionContext) {
        String relatedBoAttrName = null;
        if (!(Boolean) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP)) {
            String parentBoName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_NAME);
            String childBoName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME);
            List<DynamicDTO> pkRelationshipDtoList = MetadataRelationshipUtils.filterRelationshipsForSameParentAndChild(busObjRelationshipDynamicDTOListAlreadySaved, parentBoName, childBoName, true);
            if (pkRelationshipDtoList.size() > 1) {
                // This is a rare case in which one table has two same foreign keys for different attributes in it. Example is WELL PERFORATION BO
                // in which ZONE ID is coming as reference in two columns i.e, TOP_ZONE_ID and BASE_ZONE_ID.
                String boAttrNameAgainstWhichRelatedBoAttrNameIsToBePut = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
                relatedBoAttrName = findBestMatchedRelatedBoAttrName(boAttrNameAgainstWhichRelatedBoAttrNameIsToBePut, pkRelationshipDtoList);

            } else if (pkRelationshipDtoList.size() == 1) {
                relatedBoAttrName = (String) pkRelationshipDtoList.get(0).get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
            } else {
                String childBoAttrName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
                throw new DSPDMException("Cannot add a non primary key relationship for attribute '{}' in child business object '{}' "
                        + "because no primary key relationship already exists for same parent business object '{}'", executionContext.getExecutorLocale(),
                        childBoAttrName, childBoName, parentBoName);
            }
        }
        return relatedBoAttrName;
    }
}
