package com.lgc.dspdm.repo.delegate.common.write;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.metadata.MetadataRelationshipUtils;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAO;
import com.lgc.dspdm.core.dao.dynamic.businessobject.DynamicDAOFactory;
import com.lgc.dspdm.repo.delegate.BusinessDelegateFactory;
import com.lgc.dspdm.repo.delegate.entitytype.EntityTypeDelegateImpl;
import com.lgc.dspdm.repo.delegate.metadata.bosearch.BOSearchDelegateImpl;
import com.lgc.dspdm.repo.delegate.metadata.relationships.read.MetadataRelationshipsDelegateImpl;

import java.util.*;

public class BusinessObjectWriteDelegateImpl implements IBusinessObjectWriteDelegate {
    private static DSPDMLogger logger = new DSPDMLogger(BusinessObjectWriteDelegateImpl.class);

    private static IBusinessObjectWriteDelegate singleton = null;

    private BusinessObjectWriteDelegateImpl() {

    }

    public static IBusinessObjectWriteDelegate getInstance() {
        if (singleton == null) {
            singleton = new BusinessObjectWriteDelegateImpl();
        }
        return singleton;
    }

    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */

    @Override
    public Integer[] getNextFromSequence(String boName, String sequenceName, int count, ExecutionContext executionContext) {
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUSINESS_OBJECT,
                executionContext).getNextFromSequence(boName, sequenceName, count, executionContext);
    }

    @Override
    public Integer[] getNextFromSequenceFromDataModelDB(String boName, String sequenceName, int count, ExecutionContext executionContext) {
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BUSINESS_OBJECT,
                executionContext).getNextFromSequenceFromDataModelDB(boName, sequenceName, count, executionContext);
    }


    @Override
    public SaveResultDTO saveOrUpdate(Map<String, List<DynamicDTO>> mapToSaveOrUpdate, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        String boName = null;
        List<DynamicDTO> boListToSave = null;
        // to be used only in case of save area level or area
        Map<Object, Integer> areaLevelIdsMap = null;
        for (Map.Entry<String, List<DynamicDTO>> entry : mapToSaveOrUpdate.entrySet()) {
            // key is business object name
            boName = entry.getKey();
            // value is the list of dynamic DTOs contains extra key for readback
            boListToSave = entry.getValue();
            if (!boName.equals(DSPDMConstants.BoName.BO_SEARCH)) {
                if (CollectionUtils.hasValue(boListToSave)) {
                    // ENTITY_TYPE validate entity type and entity name
                    EntityTypeDelegateImpl.getInstance().validateREntityTypeForSave(boName, boListToSave, executionContext);
                    if (DSPDMConstants.BoName.R_AREA_LEVEL.trim().equalsIgnoreCase(boName.trim())) {
                        areaLevelIdsMap = BusinessDelegateFactory.getAreaDelegate(executionContext).saveAreaLevel(boName, boListToSave, rootSaveResultDTO, executionContext);
                    } else if (DSPDMConstants.BoName.AREA.equalsIgnoreCase(boName.trim())) {
                        rootSaveResultDTO.addResult(BusinessDelegateFactory.getAreaDelegate(executionContext).saveArea(boName, boListToSave, areaLevelIdsMap, executionContext));
                    } else {
                        rootSaveResultDTO.addResult(DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(boName, executionContext).saveOrUpdate(boListToSave, executionContext));
                    }
                    // save children here
                    rootSaveResultDTO.addResult(saveOrUpdateChildrenOnly(boName, boListToSave, 1, executionContext));
                    // tracking all changes both parent and child
                    if(rootSaveResultDTO.isAnyRecordUpdated() || rootSaveResultDTO.isAnyRecordInserted()){
                        // saving to reporting entity
                        rootSaveResultDTO.addResult(DynamicDAOFactory.getInstance(executionContext).
                                getDynamicDAO(DSPDMConstants.BoName.REPORTING_ENTITY, executionContext)
                                .saveOrUpdate(executionContext.getReportingEntityDTOList(), executionContext));
                        // Logging Business Operation
                        List<DynamicDTO> busObjAttrChangeHistoryDTOList = executionContext.getBusObjAttrChangeHistoryDTOList();
                        if(CollectionUtils.hasValue(busObjAttrChangeHistoryDTOList)){
                            DynamicDTO userOperationDynamicDTO = executionContext.getUserOperationDTO();
                            userOperationDynamicDTO.put(DSPDMConstants.BoAttrName.EFFECTED_ROWS_COUNT,executionContext.getOperationSequenceNumber());
                            // As save request having more than 1 BO Name have very less probability, like only 5-7%, so if we get more than one bo in save request,
                            // we'll be logging only first Bo Name in User Performed Operation Logging i.e., User Performed Opr Table
                            userOperationDynamicDTO.put(DSPDMConstants.BoAttrName.BO_NAME, mapToSaveOrUpdate.keySet().toArray()[0]);
                            Integer busObjOperation = executionContext.getCurrentOperationId();
                            if(busObjOperation == DSPDMConstants.BusinessObjectOperations.SAVE_AND_UPDATE_OPR.getId()){
                                if(rootSaveResultDTO.isAnyRecordUpdated() && !rootSaveResultDTO.isAnyRecordInserted()){
                                    busObjOperation = DSPDMConstants.BusinessObjectOperations.UPDATE_OPR.getId();
                                }else if(!rootSaveResultDTO.isAnyRecordUpdated() && rootSaveResultDTO.isAnyRecordInserted()) {
                                    busObjOperation = DSPDMConstants.BusinessObjectOperations.SAVE_OPR.getId();
                                }
                            }
                            executionContext.getUserOperationDTO().put(DSPDMConstants.BoAttrName.R_BUSINESS_OBJECT_OPR_ID, busObjOperation);
                            rootSaveResultDTO.addOperationResult(DynamicDAOFactory.getInstance(executionContext).
                                    getDynamicDAO(DSPDMConstants.BoName.USER_PERFORMED_OPR, executionContext).
                                    saveOrUpdate(Arrays.asList(userOperationDynamicDTO), executionContext));
                            Object userOperationId = userOperationDynamicDTO.get(DSPDMConstants.BoAttrName.USER_PERFORMED_OPR_ID);
                            CollectionUtils.setPropertyNameAndPropertyValue(busObjAttrChangeHistoryDTOList, DSPDMConstants.BoAttrName.USER_PERFORMED_OPR_ID, userOperationId);
                            rootSaveResultDTO.addOperationResult(DynamicDAOFactory.getInstance(executionContext).
                                    getDynamicDAO(DSPDMConstants.BoName.BUS_OBJ_ATTR_CHANGE_HISTORY, executionContext).
                                    saveOrUpdate(busObjAttrChangeHistoryDTOList, executionContext));
                        }
                        // Save Data in BO_SEARCH
                        if (ConfigProperties.getInstance().use_postgresql_search_db.getBooleanValue()) {
                            rootSaveResultDTO.addSearchResult(BOSearchDelegateImpl.getInstance(executionContext).createSearchIndexForBusinessObjects(boListToSave, boName, executionContext));
                        }
                    }
                    // set data in read back if required
                    // add read back data only in parent not in child because parent already contains child data
                    if(executionContext.isReadBack()) {
                        rootSaveResultDTO.addDataFromReadBack(boName, boListToSave);
                    }
                    // if reporting entity data is updated, return it in read back
                    if(CollectionUtils.hasValue(executionContext.getReportingEntityDTOList())){
                        rootSaveResultDTO.addDataFromReadBack(DSPDMConstants.BoName.REPORTING_ENTITY, executionContext.getReportingEntityDTOList());
                    }
                }
            }
        }
        return rootSaveResultDTO;
    }

    /**
     * When given a list of parent business objects it does not save any parent but it saves only the children records for the given parent records
     *
     * @param parentBoName
     * @param parentBoListAlreadySavedOrUpdated
     * @param currentHierarchyDepth
     * @param executionContext
     * @return
     */
    private SaveResultDTO saveOrUpdateChildrenOnly(String parentBoName, List<DynamicDTO> parentBoListAlreadySavedOrUpdated, int currentHierarchyDepth, ExecutionContext executionContext) {
        int maxAllowedDepth = ConfigProperties.getInstance().max_read_parent_hierarchy_level.getIntegerValue();
        if ((maxAllowedDepth > 0) && (currentHierarchyDepth > maxAllowedDepth)) {
            throw new DSPDMException("Save children details for parent business object '{}' exceeded its max allowed hierarchy level limit '{}'",
                    executionContext.getExecutorLocale(), parentBoName, maxAllowedDepth);
        }
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // map to hold all the children relationships for the current parent bo
        Map<String, List<DynamicDTO>> childRelationshipsMap = null;
        // map to hold all the child business object to be saved for each parent business object
        Map<String, List<DynamicDTO>> childrenMapToSaveOrUpdate = null;
        // list to hold all the child business objects to be saved for a particulat child bo type
        List<DynamicDTO> childrenBoListToSaveOrUpdate = null;
        // save only children data if exists for the given parent list
        // now iterate over all the parents which are already saved.
        for (DynamicDTO parentDynamicDTO : parentBoListAlreadySavedOrUpdated) {
            // check that the current parent bo has any children to be saved
            childrenMapToSaveOrUpdate = (Map<String, List<DynamicDTO>>) parentDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
            if ((childrenMapToSaveOrUpdate != null) && (childrenMapToSaveOrUpdate.size() > 0)) {
                if (childRelationshipsMap == null) {
                    // read all relationships only one time for each call to this method
                    childRelationshipsMap = MetadataRelationshipsDelegateImpl.getInstance().readMetadataChildRelationshipsMap(parentBoName, executionContext);
                }
                // now iterate over all the types of children to be saved for the current parent
                for (String childBoName : childrenMapToSaveOrUpdate.keySet()) {
                    childrenBoListToSaveOrUpdate = childrenMapToSaveOrUpdate.get(childBoName);
                    if (CollectionUtils.hasValue(childrenBoListToSaveOrUpdate)) {
                        // ENTITY_TYPE validate entity type and entity name
                        EntityTypeDelegateImpl.getInstance().validateREntityTypeForSave(childBoName, childrenBoListToSaveOrUpdate, executionContext);
                        if (childRelationshipsMap.containsKey(childBoName)) {
                            List<DynamicDTO> currentChildRelationships = childRelationshipsMap.get(childBoName);
                            // now iterate over all the children to be saved of the current child bo name/type
                            for (DynamicDTO childDynamicDTO : childrenBoListToSaveOrUpdate) {
                                // now iterate over all the relationship defined fields between this parent and child.
                                // If there is a value for parent then copy it to child record also
                                for (DynamicDTO relationshipDTO : currentChildRelationships) {
                                    if (parentDynamicDTO.containsKey(relationshipDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME))) {
                                        Object parentIdOrName = parentDynamicDTO.get(relationshipDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME));
                                        childDynamicDTO.put((String) relationshipDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME), parentIdOrName);
                                    }
                                }
                            }
                            // save all children of current children type
                            saveResultDTO.addResult(DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(childBoName, executionContext).saveOrUpdate(childrenBoListToSaveOrUpdate, executionContext));
                            // Save Data in BO_SEARCH
                            if (saveResultDTO.isAnyRecordUpdated() || saveResultDTO.isAnyRecordInserted()) {
                                if (ConfigProperties.getInstance().use_postgresql_search_db.getBooleanValue()) {
                                    saveResultDTO.addSearchResult(BOSearchDelegateImpl.getInstance(executionContext).createSearchIndexForBusinessObjects(childrenBoListToSaveOrUpdate, childBoName, executionContext));
                                }
                            }

                            // recursive call to save grand children if any grand children with one plus to hierarchy level
                            saveResultDTO.addResult(saveOrUpdateChildrenOnly(childBoName, childrenBoListToSaveOrUpdate, currentHierarchyDepth + 1, executionContext));
                        } else {
                            throw new DSPDMException("Unable to save or update child records. Business object '{}' is not registered as a child of parent business object '{}'",
                                    executionContext.getExecutorLocale(), childBoName, parentBoName);
                        }
                    }
                }
            }
        }
        return saveResultDTO;
    }

    /**
     * main delete method
     *
     * @param mapToDelete
     * @param executionContext
     * @return
     */
    @Override
    public SaveResultDTO delete(Map<String, List<DynamicDTO>> mapToDelete, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        String boName = null;
        List<DynamicDTO> boListToDelete = null;
        // fetch REPORTING ENTITY Data and delete it first if getType() is of type in R_REPORTING_ENTITY_KIND
        List<DynamicDTO> reportingEntityKindDTOsList = DynamicDAOFactory.getInstance(executionContext).
                getDynamicDAO(DSPDMConstants.BoName.R_REPORTING_ENTITY_KIND, executionContext)
                .read(new BusinessObjectInfo(DSPDMConstants.BoName.R_REPORTING_ENTITY_KIND, executionContext)
                        .setReadAllRecords(true),executionContext);
        // create Dynamic DAO for deleting data in REPORTING ENTITY
        IDynamicDAO dynamicDAOForReportingEntity = DynamicDAOFactory.getInstance(executionContext).
                getDynamicDAO(DSPDMConstants.BoName.REPORTING_ENTITY, executionContext);
        for (Map.Entry<String, List<DynamicDTO>> entry : mapToDelete.entrySet()) {
            // key is business object name
            boName = entry.getKey();
            // value is the list of dynamic DTOs
            boListToDelete = entry.getValue();
            if (!boName.equals(DSPDMConstants.BoName.BO_SEARCH)) {
                if (CollectionUtils.hasValue(boListToDelete)) {
                    IDynamicDAO dynamicDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(boName, executionContext);
                    // reading whole boList instead of id only for readBack detailed data
                    // read original copy from db for read back and for logging and change tracking purpose
                    logger.info("Going to read a full copy of the records before deleting them. This is to maintain the read back and also change track");
                    // first delete child records if there are any found in same request
                    if(executionContext.isDeleteCascade()) {
                        boListToDelete = dynamicDAO.readCopy(boListToDelete, executionContext);
                        rootSaveResultDTO.addResult(deleteImplChildrenOnlyWithCascade(boName, boListToDelete, executionContext));
                    }else {
                        // read while retaining the children
                        boListToDelete = dynamicDAO.readCopyRetainChildren(boListToDelete, executionContext);
                        rootSaveResultDTO.addResult(deleteImplChildrenOnly(boName, boListToDelete, 1, executionContext));
                    }
                    String finalBoName = boName;// it is needed to use boName in following predicate
                    Optional<DynamicDTO> matchedDTO = reportingEntityKindDTOsList.stream().filter(dynamicDTOForReportingEntityKind ->
                            finalBoName.equalsIgnoreCase((String) dynamicDTOForReportingEntityKind.get(DSPDMConstants.BoAttrName.REPORTING_ENTITY_KIND))).findFirst();
                    if(matchedDTO.isPresent()){
                        // means current bo is found to be in R_REPORTING_ENTITY_KIND
                        // now we need to delete records in REPORTING ENTITY table as well for boListToDelete
                        DynamicDTO matchedRReportingEntityKindDTO = matchedDTO.get();
                        BusinessObjectInfo businessObjectInfoForReportingEntity = new BusinessObjectInfo(DSPDMConstants.BoName.REPORTING_ENTITY, executionContext);
                        businessObjectInfoForReportingEntity.addFilter(DSPDMConstants.BoAttrName.R_REPORTING_ENTITY_KIND_ID, Operator.EQUALS,
                                matchedRReportingEntityKindDTO.get(DSPDMConstants.BoAttrName.R_REPORTING_ENTITY_KIND_ID));
                        // read records which are to be deleted
                        List<DynamicDTO> reportingEntityListTODelete = dynamicDAOForReportingEntity.readLongRangeUsingInClauseAndExistingFilters(businessObjectInfoForReportingEntity,
                                null,
                                DSPDMConstants.BoAttrName.ASSOCIATED_OBJECT_ID,
                                CollectionUtils.getIntegerValuesFromList(boListToDelete, dynamicDAO.getPrimaryKeyColumnNames().get(0)).toArray()
                                ,Operator.IN, executionContext);
                        // finally delete REPORTING ENTITY records
                        rootSaveResultDTO.addResult(dynamicDAOForReportingEntity.delete(reportingEntityListTODelete, executionContext));
                    }
                    // now delete the records
                    rootSaveResultDTO.addResult(dynamicDAO.delete(boListToDelete, executionContext));
                    // Delete Data in BO_SEARCH
                    if(rootSaveResultDTO.isAnyRecordDeleted()) {
                        // Logging Business Operation
                        List<DynamicDTO> busObjAttrChangeHistoryDTOList = executionContext.getBusObjAttrChangeHistoryDTOList();
                        if (CollectionUtils.hasValue(busObjAttrChangeHistoryDTOList)) {
                            // get user performed operation dto to be saved from execution context
                            DynamicDTO userOperationDynamicDTO = executionContext.getUserOperationDTO();
                            // set effected row count and operation type id
                            userOperationDynamicDTO.put(DSPDMConstants.BoAttrName.EFFECTED_ROWS_COUNT, executionContext.getOperationSequenceNumber());
                            // As delete request having more than 1 BO Name have very less probability, like only 5-7%, so if we get more than one bo in delete request,
                            // we'll be logging only first Bo Name in User Performed Operation Logging i.e., User Performed Opr Table
                            userOperationDynamicDTO.put(DSPDMConstants.BoAttrName.BO_NAME, mapToDelete.keySet().toArray()[0]);
                            userOperationDynamicDTO.put(DSPDMConstants.BoAttrName.R_BUSINESS_OBJECT_OPR_ID, DSPDMConstants.BusinessObjectOperations.DELETE_OPR.getId());
                            // call save api on user performed operation and add response to save result
                            rootSaveResultDTO.addOperationResult(DynamicDAOFactory.getInstance(executionContext)
                                    .getDynamicDAO(DSPDMConstants.BoName.USER_PERFORMED_OPR, executionContext)
                                    .saveOrUpdate(Arrays.asList(userOperationDynamicDTO), executionContext));
                            // now the user performed operation dto has primary key after save is successful on it
                            Object userPerformedOperationId = userOperationDynamicDTO.get(DSPDMConstants.BoAttrName.USER_PERFORMED_OPR_ID);
                            // set user performed operation id in all its children going to be saved
                            CollectionUtils.setPropertyNameAndPropertyValue(busObjAttrChangeHistoryDTOList, DSPDMConstants.BoAttrName.USER_PERFORMED_OPR_ID, userPerformedOperationId);
                            // now call save on business object attribute change history table
                            rootSaveResultDTO.addOperationResult(DynamicDAOFactory.getInstance(executionContext)
                                    .getDynamicDAO(DSPDMConstants.BoName.BUS_OBJ_ATTR_CHANGE_HISTORY, executionContext)
                                    .saveOrUpdate(busObjAttrChangeHistoryDTOList, executionContext));
                        }
                        // delete the deleted records from the search db
                        if (ConfigProperties.getInstance().use_postgresql_search_db.getBooleanValue()) {
                            rootSaveResultDTO.addSearchResult(BOSearchDelegateImpl.getInstance(executionContext).delete(boListToDelete, boName, executionContext));
                        }
                    }
                    if(executionContext.isReadBack()) {
                        rootSaveResultDTO.addDataFromReadBack(boName, boListToDelete);
                    }
                }
            }
        }
        return rootSaveResultDTO;
    }

    /**
     * it will delete only the children records for the given parents
     *
     * @param parentBoName
     * @param parentBoListForDelete
     * @param executionContext
     * @return
     * @Author Muhammad Imran Ansari
     * @since 11-Nov-2020
     */
    private SaveResultDTO deleteImplChildrenOnlyWithCascade(String parentBoName, List<DynamicDTO> parentBoListForDelete, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        List<DynamicDTO> childrenList = new ArrayList<>();
        // map to hold all the children relationships for the current parent bo
        Map<String, List<DynamicDTO>> childRelationshipsMap = MetadataRelationshipsDelegateImpl.getInstance().readMetadataChildRelationshipsMap(parentBoName, executionContext);
        // this business object has child relationships
        if(CollectionUtils.hasValue(childRelationshipsMap)) {
            // iterate over all child relationships to delete all the children records of all types
            for (String childBoName : childRelationshipsMap.keySet()) {
                // get child dao to read child data
                IDynamicDAO childDynamicDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(childBoName, executionContext);
                // find pk relationship if it does not exist then do not throw error just return null
                DynamicDTO bestRelationshipDynamicDTO = findBestRelationshipToDelete(parentBoName, childBoName, childRelationshipsMap, false,executionContext);
                if(bestRelationshipDynamicDTO != null) {
                    // pk relationship found
                    // now read primary key and foreign key attribute names from the relationship dto
                    String parentPKBoAttrName = (String) bestRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME);
                    String childFKBoAttrName = (String) bestRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
                    // collect all parent id together in a unique set
                    Set<Object> parentIds = CollectionUtils.getValuesSetFromList(parentBoListForDelete, parentPKBoAttrName);
                    // now create business object info to read all children records
                    BusinessObjectInfo childBusinessObjectInfo = new BusinessObjectInfo(childBoName, executionContext);
                    // read full record for return data
                    // childBusinessObjectInfo.addColumnsToSelect(childDynamicDAO.getPrimaryKeyColumnNames());
                    List<DynamicDTO> childrenBoListToDelete = childDynamicDAO.readLongRangeUsingInClauseAndExistingFilters(
                            childBusinessObjectInfo, null,childFKBoAttrName, parentIds.toArray(), Operator.IN, executionContext);
                    if(CollectionUtils.hasValue(childrenBoListToDelete)) {
                        // children of this type exist
                        // first delete grand children records
                        // recursive call to delete grand children if any grand children with one plus to hierarchy level
                        saveResultDTO.addResult(deleteImplChildrenOnlyWithCascade(childBoName, childrenBoListToDelete, executionContext));
                        // now delete this children records
                        saveResultDTO.addResult(childDynamicDAO.delete(childrenBoListToDelete, executionContext));
                        // delete Data from BO_SEARCH
                        if (saveResultDTO.isAnyRecordDeleted()) {
                            if (ConfigProperties.getInstance().use_postgresql_search_db.getBooleanValue()) {
                                saveResultDTO.addSearchResult(BOSearchDelegateImpl.getInstance(executionContext).delete(childrenBoListToDelete, childBoName, executionContext));
                            }
                        }
                        // now both children records and grand children records have been deleted
                        // below code is to return the data in tree structure (like in parent/child relationship form)
                        Integer parentPK = null;
                        List<DynamicDTO> childrenListForCurrentParentOnly = null;
                        // map to hold all the child business object to be saved for each parent business object
                        Map<String, Map<String, Object>> childrenMapForAlreadyDeleted = null;
                        for(DynamicDTO parentDynamicDTO : parentBoListForDelete) {
                            parentPK = (Integer) parentDynamicDTO.get(parentPKBoAttrName);
                            // finding children for current parent only
                            childrenListForCurrentParentOnly = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(childrenBoListToDelete, childFKBoAttrName, parentPK);
                            if (CollectionUtils.hasValue(childrenListForCurrentParentOnly)) {
                                // children found, now assigning to respective parent
                                parentDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
                                childrenMapForAlreadyDeleted = (Map<String, Map<String, Object>>) parentDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
                                if (childrenMapForAlreadyDeleted == null) {
                                    // create a new children map
                                    childrenMapForAlreadyDeleted = new LinkedHashMap<>();
                                    // set the newly created map into the dynamic dto with children key
                                    parentDynamicDTO.put(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY, childrenMapForAlreadyDeleted);
                                }
                                Map<String, Object> currentChildRecordsMap = new HashMap<>();
                                currentChildRecordsMap.put(DSPDMConstants.DSPDM_REQUEST.LANGUAGE_KEY, "en");
                                currentChildRecordsMap.put(DSPDMConstants.DSPDM_REQUEST.TIMEZONE_KEY, "GMT+08:00");
                                currentChildRecordsMap.put(DSPDMConstants.DSPDM_REQUEST.DATA_KEY, Arrays.asList(childrenListForCurrentParentOnly));
                                childrenMapForAlreadyDeleted.put(childBoName, currentChildRecordsMap);
                            }
                        }
                    }
                }
            }
        }
        return saveResultDTO;
    }

    /**
     * it will delete only the children records for the given parents
     *
     * @param parentBoName
     * @param parentBoListForDelete
     * @param currentHierarchyDepth
     * @param executionContext
     * @return
     * @Author Muhammad Imran Ansari
     * @since 11-Nov-2020
     */
    private SaveResultDTO deleteImplChildrenOnly(String parentBoName, List<DynamicDTO> parentBoListForDelete, int currentHierarchyDepth, ExecutionContext executionContext) {

        int maxAllowedDepth = ConfigProperties.getInstance().max_read_parent_hierarchy_level.getIntegerValue();
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // map to hold all the children relationships for the current parent bo
        Map<String, List<DynamicDTO>> childRelationshipsMap = MetadataRelationshipsDelegateImpl.getInstance().readMetadataChildRelationshipsMap(parentBoName, executionContext);
        // create a temporary local map of best relationship with a particular child so that same relationship can be used for multiple iterations
        Map<String, DynamicDTO> bestChildRelationshipMap = new HashMap<>();
        // map to hold all the child business object to be deleted for each parent business object
        Map<String, List<DynamicDTO>> childrenMapToDelete = null;
        // list to hold all the child business objects to be deleted for a particular child bo type
        List<DynamicDTO> childrenBoListToDelete = null;
        // delete only children data if exists for the given parent list
        // now iterate over all the parents which will be deleted externally not here after deleting child records here
        for (DynamicDTO parentDynamicDTO : parentBoListForDelete) {
            // check that the current parent bo has any children to be deleted
            childrenMapToDelete = (Map<String, List<DynamicDTO>>) parentDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
            if ((childrenMapToDelete != null) && (childrenMapToDelete.size() > 0)) {
                if ((maxAllowedDepth > 0) && (currentHierarchyDepth > maxAllowedDepth)) {
                    throw new DSPDMException("Delete children details for parent business object '{}' exceeded its max allowed hierarchy level limit '{}'",
                            executionContext.getExecutorLocale(), parentBoName, maxAllowedDepth);
                }
                // now iterate over all the types of children to be deleted for the current parent
                for (Map.Entry<String, List<DynamicDTO>> entry : childrenMapToDelete.entrySet()) {
                    String childBoName = entry.getKey();
                    childrenBoListToDelete = entry.getValue();
                    if (CollectionUtils.hasValue(childrenBoListToDelete)) {
                        DynamicDTO bestRelationshipDynamicDTO = bestChildRelationshipMap.get(childBoName);
                        if (bestRelationshipDynamicDTO == null) {
                            // send verify true to throw error in case pk relationship is not found
                            bestRelationshipDynamicDTO = findBestRelationshipToDelete(parentBoName, childBoName, childRelationshipsMap, true, executionContext);
                            // put in the map for future use in upcoming iterations
                            bestChildRelationshipMap.put(childBoName, bestRelationshipDynamicDTO);
                        }
                        IDynamicDAO childDynamicDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(childBoName, executionContext);
                        verifyChildrenBeforeDelete(parentBoName, childBoName, bestRelationshipDynamicDTO, parentDynamicDTO, childrenBoListToDelete,
                                childDynamicDAO, executionContext);
                        // read original copy from db for read back and for logging and change tracking purpose
                        // read while retaining the children
                        logger.info("Going to read a full copy of the records before deleting them. This is to maintain the read back and also change track");
                        childrenBoListToDelete = childDynamicDAO.readCopyRetainChildren(childrenBoListToDelete, executionContext);
                        // put the original read from db copy to map to change the existing list inside the children map
                        childrenMapToDelete.put(childBoName, childrenBoListToDelete);
                        // all children belongs to this parent is verified
                        // first delete grand children records
                        // recursive call to delete grand children if any grand children with one plus to hierarchy level
                        saveResultDTO.addResult(deleteImplChildrenOnly(childBoName, childrenBoListToDelete, currentHierarchyDepth + 1, executionContext));
                        // now delete this children records
                        saveResultDTO.addResult(childDynamicDAO.delete(childrenBoListToDelete, executionContext));

                        // delete Data from BO_SEARCH
                        if (saveResultDTO.isAnyRecordDeleted()) {
                            if (ConfigProperties.getInstance().use_postgresql_search_db.getBooleanValue()) {
                                saveResultDTO.addSearchResult(BOSearchDelegateImpl.getInstance(executionContext).delete(childrenBoListToDelete, childBoName, executionContext));
                            }
                        }
                    }
                }
            }
        }
        return saveResultDTO;
    }

    /**
     * verifies that the given list of children really belongs to the given parent
     *
     * @param parentBoName
     * @param childBoName
     * @param bestRelationshipDynamicDTO
     * @param parentDynamicDTO
     * @param childrenBoListToDelete
     * @param childDynamicDAO
     * @param executionContext
     */
    private static void verifyChildrenBeforeDelete(String parentBoName, String childBoName, DynamicDTO bestRelationshipDynamicDTO,
                                                   DynamicDTO parentDynamicDTO, List<DynamicDTO> childrenBoListToDelete,
                                                   IDynamicDAO childDynamicDAO, ExecutionContext executionContext) {
        String parentPKBoAttrName = (String) bestRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME);
        String childFKBoAttrName = (String) bestRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
        Object parentId = parentDynamicDTO.get(parentPKBoAttrName);
        String childPKBoAttrName = childDynamicDAO.getPrimaryKeyColumnNames().get(0);
        Set<Object> childrenIds = CollectionUtils.getValuesSetFromList(childrenBoListToDelete, childPKBoAttrName);
        BusinessObjectInfo childBusinessObjectInfo = new BusinessObjectInfo(childBoName, executionContext);
        childBusinessObjectInfo.addColumnsToSelect(childPKBoAttrName);
        childBusinessObjectInfo.addFilter(childFKBoAttrName, parentId);
        List<DynamicDTO> childrenBoListAfterRead = childDynamicDAO.readLongRangeUsingInClauseAndExistingFilters(
                childBusinessObjectInfo, null, childPKBoAttrName, childrenIds.toArray(), Operator.IN, executionContext);

        if (childrenBoListAfterRead.size() != childrenIds.size()) {
            Map<Object, DynamicDTO> childrenBoPKMapAfterRead = CollectionUtils.prepareMapFromValuesOfKey(childrenBoListAfterRead, childPKBoAttrName);
            for (Object childId : childrenIds) {
                if (!(childrenBoPKMapAfterRead.containsKey(childId))) {
                    throw new DSPDMException("Cannot delete child business object of type '{}'." +
                            " Given child business object with primary key value '{}' is not registered " +
                            "as a child of parent business object of type '{}' with primary key value '{}'",
                            executionContext.getExecutorLocale(), childBoName, childId, parentBoName, parentId);
                }
            }
        }
    }

    /**
     * make sure that primary key relationship exists between then two parent and child.
     * If the pk relationship is not found and verify is false then null is returned.
     * If verify is true then exception is thrown in case pk relationship is not found.
     *
     * @param parentBoName
     * @param childBoName
     * @param childRelationshipsMap
     * @param verify
     * @param executionContext
     * @return
     */
    private static DynamicDTO findBestRelationshipToDelete(String parentBoName, String childBoName, Map<String, List<DynamicDTO>> childRelationshipsMap, boolean verify, ExecutionContext executionContext) {
        DynamicDTO bestRelationshipDynamicDTO = null;
        if (!(childRelationshipsMap.containsKey(childBoName))) {
            if (verify) {
                throw new DSPDMException("Cannot delete child records. Business object '{}' is not registered as a child of parent business object '{}'",
                        executionContext.getExecutorLocale(), childBoName, parentBoName);
            }
        } else {
            // get all relationships for current child
            List<DynamicDTO> currentParentAndChildRelationshipList = childRelationshipsMap.get(childBoName);
            Map<String, List<DynamicDTO>> currentParentAndChildRelationshipsByRelationshipName = CollectionUtils.groupDynamicDTOByPropertyValueIgnoreCase(
                    currentParentAndChildRelationshipList, DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME);
            // now iterate over all the children to be deleted of the current child bo name/type
            List<DynamicDTO> bestRelationshipWithCurrentChild = MetadataRelationshipUtils.findBestRelationshipWithCurrentChild(currentParentAndChildRelationshipsByRelationshipName, childBoName);
            if (bestRelationshipWithCurrentChild.size() != 1) {
                if (verify) {
                    throw new DSPDMException("Cannot delete children business object of type '{}'. No primary key relationship exists with parent business object '{}'",
                            executionContext.getExecutorLocale(), childBoName, parentBoName);
                }
            } else {
                bestRelationshipDynamicDTO = bestRelationshipWithCurrentChild.get(0);
                if (Boolean.FALSE.equals(bestRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP))) {
                    if (verify) {
                        throw new DSPDMException("Cannot delete children business object of type '{}'. No primary key relationship exists with parent business object '{}'",
                                executionContext.getExecutorLocale(), childBoName, parentBoName);
                    }
                }
            }
        }
        return bestRelationshipDynamicDTO;
    }

    @Override
    public SaveResultDTO softDelete(Map<String, List<DynamicDTO>> mapToDelete, ExecutionContext executionContext) {
        SaveResultDTO rootSaveResultDTO = new SaveResultDTO();
        String boName = null;
        List<DynamicDTO> boListToDelete = null;
        for (Map.Entry<String, List<DynamicDTO>> entry : mapToDelete.entrySet()) {
            // key is business object name
            boName = entry.getKey();
            // value is the list of dynamic DTOs
            boListToDelete = entry.getValue();
            if (!boName.equals(DSPDMConstants.BoName.BO_SEARCH)) {
                if (CollectionUtils.hasValue(boListToDelete)) {
                    rootSaveResultDTO.addResult(DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(boName, executionContext).softDelete(boListToDelete, executionContext));

                    // Delete Data in BO_SEARCH
                    if (rootSaveResultDTO.isAnyRecordDeleted() || rootSaveResultDTO.isAnyRecordUpdated()) {
                        if (ConfigProperties.getInstance().use_postgresql_search_db.getBooleanValue()) {
                            rootSaveResultDTO.addSearchResult(BOSearchDelegateImpl.getInstance(executionContext).softDelete(boListToDelete, boName, executionContext));
                        }
                    }
                }
            }
        }
        return rootSaveResultDTO;
    }
}
