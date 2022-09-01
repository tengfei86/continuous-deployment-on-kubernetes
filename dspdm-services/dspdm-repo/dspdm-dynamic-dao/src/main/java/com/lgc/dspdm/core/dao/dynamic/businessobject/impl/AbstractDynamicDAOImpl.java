package com.lgc.dspdm.core.dao.dynamic.businessobject.impl;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectAttributeConstraintsDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectAttributeDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.*;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAO;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAOImpl;
import com.lgc.dspdm.core.dao.dynamic.businessobject.DynamicDAOFactory;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Class to hold common code for utility methods or base methods.
 * These methods will not be public and can only be invoked from the child classes.
 * This class will not contain any public business method
 *
 * @author Muhammad Imran Ansari
 * @date 27-August-2019
 */
public abstract class AbstractDynamicDAOImpl extends AbstractDynamicDAOImplForReverseEngineerMetadata implements IDynamicDAOImpl {
    private static DSPDMLogger logger = new DSPDMLogger(AbstractDynamicDAOImpl.class);

    protected AbstractDynamicDAOImpl(BusinessObjectDTO businessObjectDTO, ExecutionContext executionContext) {
        super(businessObjectDTO, executionContext);
    }

    /* ************************************************************** */
    /* ************** BUSINESS METHOD SAVE OR UPDATE **************** */
    /* ************************************************************** */
    @Override
    public SaveResultDTO saveOrUpdate(List<DynamicDTO> businessObjectsToSave, boolean hasSQLExpression, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        if (CollectionUtils.hasValue(businessObjectsToSave)) {
            List<DynamicDTO> saveList = new ArrayList<>();
            List<DynamicDTO> updateList = new ArrayList<>();

            for (DynamicDTO dynamicDTO : businessObjectsToSave) {
                if (dynamicDTO != null) {
                    // these dtos are coming to save and they might not have primary key column names inside
                    // you must set primary key column names from dao before getting primary key from the object
                    dynamicDTO.setPrimaryKeyColumnNames(this.getPrimaryKeyColumnNames());
                    if (dynamicDTO.getId() == null) {
                        // candidate for insert in db
                        saveList.add(dynamicDTO);
                    } else {
                        // candidate for update in db
                        updateList.add(dynamicDTO);
                    }
                }
            }
            // INSERT
            if (saveList.size() > 0) {
                // perform metadata based validations before update
                performValidationsBeforeSave(saveList, connection, executionContext);
                // call underlying insert api on all records to be inserted
                saveResultDTO.addResult(insertImpl(saveList, hasSQLExpression, connection, executionContext));
                // syncing data in reporting table if getType() is of type available in R_REPORTING_ENTITY_KIND
                List<DynamicDTO> reportingEntityKindDTOsList = executionContext.getrReportingEntityKindDTOList();
                // execution context will have r_reporting_entity_kind_dto_list only in case of {{URL}}/save call.
                if(CollectionUtils.hasValue(reportingEntityKindDTOsList)){
                    Optional<DynamicDTO> matchedDTO = reportingEntityKindDTOsList.stream().filter(dynamicDTOForProductVolumeSummary ->
                            getType().equalsIgnoreCase((String) dynamicDTOForProductVolumeSummary.get(DSPDMConstants.BoAttrName.REPORTING_ENTITY_KIND))).findFirst();
                    if(matchedDTO.isPresent()){
                        DynamicDTO matchedRReportingEntityKindDTO = matchedDTO.get();
                        // means current type is in R_REPORTING_ENTITY_KIND, so we'll need to insert and sync above newly saved records in REPORTING_ENTITY
                        for(DynamicDTO saveDynamicDTO: saveList){
                            DynamicDTO reportingEntityDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.REPORTING_ENTITY,
                                    getDynamicDAO(DSPDMConstants.BoName.REPORTING_ENTITY, executionContext).getPrimaryKeyColumnNames(), executionContext);
                            reportingEntityDynamicDTO.put(DSPDMConstants.BoAttrName.R_REPORTING_ENTITY_KIND_ID,
                                    matchedRReportingEntityKindDTO.get(DSPDMConstants.BoAttrName.R_REPORTING_ENTITY_KIND_ID));
                            reportingEntityDynamicDTO.put(DSPDMConstants.BoAttrName.ASSOCIATED_OBJECT_ID, saveDynamicDTO.getId().getPK()[0]);
                            reportingEntityDynamicDTO.put(DSPDMConstants.BoAttrName.ASSOCIATED_OBJECT_NAME, "TO_BE_DELETED");
                            reportingEntityDynamicDTO.put(DSPDMConstants.BoAttrName.IS_ACTIVE, saveDynamicDTO.get(DSPDMConstants.BoAttrName.IS_ACTIVE));
                            reportingEntityDynamicDTO.put(DSPDMConstants.BoAttrName.REMARK, saveDynamicDTO.get(DSPDMConstants.BoAttrName.REMARK));
                            reportingEntityDynamicDTO.put(DSPDMConstants.BoAttrName.ROW_CREATED_BY, saveDynamicDTO.get(DSPDMConstants.BoAttrName.ROW_CREATED_BY));
                            reportingEntityDynamicDTO.put(DSPDMConstants.BoAttrName.ROW_CHANGED_BY, saveDynamicDTO.get(DSPDMConstants.BoAttrName.ROW_CHANGED_BY));
                            // adding into the execution context and will be saved in db later
                            executionContext.addDTOToReportingEntityDTOList(reportingEntityDynamicDTO);
                        }
                    }
                }
            }

            // UPDATE
            if (updateList.size() > 0) {
                boolean readBeforeUpdate = ConfigProperties.getInstance().read_before_update.getBooleanValue();
                boolean doTinyUpdate = ConfigProperties.getInstance().do_tiny_update.getBooleanValue();
                // perform metadata based validations before update
                performValidationsBeforeUpdate(updateList, doTinyUpdate, connection, executionContext);
                // do not read before update for search tables
                if (isSearchDAO(getType())) {
                    readBeforeUpdate = false;
                    doTinyUpdate = false;
                }
                // call underlying update api on all records to be updated
                saveResultDTO.addResult(updateImpl(updateList, readBeforeUpdate, doTinyUpdate, hasSQLExpression, connection,
                        executionContext));
            }
        }
        return saveResultDTO;
    }

    private void performValidationsBeforeSave(List<DynamicDTO> saveList, Connection connection, ExecutionContext executionContext) {
        // name to id conversion is not required in save or update process
        // performNameToIDBeforeSaveOrUpdate(saveList, connection, executionContext);
        verifyMandatoryFieldsForSave(saveList, connection, executionContext);
        verifyUniqueConstraintsForSave(saveList, connection, executionContext);
    }

    private void performValidationsBeforeUpdate(List<DynamicDTO> updateList, boolean doTinyUpdate, Connection connection, ExecutionContext executionContext) {
        // name to id conversion is not required in save or update process
        // performNameToIDBeforeSaveOrUpdate(updateList, connection, executionContext);
        // perform mandatory validations only if the tiny update flag is not present or it is false
        verifyMandatoryFieldsForUpdate(updateList, doTinyUpdate, connection, executionContext);
        verifyUniqueConstraintsForUpdate(updateList, connection, executionContext);
    }

//    private void performNameToIDBeforeSaveOrUpdate(List<DynamicDTO> saveOrUpdateList, Connection connection, ExecutionContext executionContext) {
//        List<BusinessObjectAttributeDTO> boAttrDTOList = getBusinessObjectAttributeDTOS();
//        //1. Find all related_bo_attr_name is not null
//        // put it into Map(Key:relatedBoAttrName, Value: all related attrs)
//        HashMap<String, List<BusinessObjectAttributeDTO>> relatedAttrMap = new HashMap<>();
//        for (BusinessObjectAttributeDTO boAttrDTO : boAttrDTOList) {
//            String relatedBoAttrName = boAttrDTO.getRelatedBoAttrName();
//            if (StringUtils.hasValue(relatedBoAttrName)) {
//                List<BusinessObjectAttributeDTO> currentRelatedBoAttList = new ArrayList<>();
//                List<BusinessObjectAttributeDTO> preRelatedBoAttList = relatedAttrMap.putIfAbsent(relatedBoAttrName, currentRelatedBoAttList);
//                if (preRelatedBoAttList != null) {
//                    preRelatedBoAttList.add(boAttrDTO);
//                } else {
//                    currentRelatedBoAttList.add(boAttrDTO);
//                }
//            }
//        }
//        if (relatedAttrMap.size() == 0) {
//            return;
//        }
//        //2.loop relatedIDNameMap: read data from bus_obj_relationship：where parent_bo_name= {reference_bo_name} and child_bo_name = {current bo}
//        //              and (child_bo_attr_name = {related_bo_attr_name} or child_bo_attr_name = { all related attrs})
//        // put it into Map(Key:relatedBoAttrName, Value: all relationship)
//        HashMap<String, List<BusinessObjectRelationshipDTO>> relationshipMap = new HashMap<>();
//        List<BusinessObjectRelationshipDTO> businessObjectRelationshipDTOS = getBusinessObjectRelationshipDTOS();
//        for (Map.Entry<String, List<BusinessObjectAttributeDTO>> entry : relatedAttrMap.entrySet()) {
//            String relatedBoAttrName = entry.getKey();
//            List<BusinessObjectAttributeDTO> relatedAttrs = entry.getValue();
//            String parent_bo_name = relatedAttrs.get(0).getReferenceBOName();//all related attrs has same reference_bo_name
//            String child_bo_name = getType();
//            for (BusinessObjectRelationshipDTO relationshipDTO : businessObjectRelationshipDTOS) {
//                if ((relationshipDTO.getParentBoName().equalsIgnoreCase(parent_bo_name)) && (relationshipDTO.getChildBoName().equalsIgnoreCase(child_bo_name))) {
//                    if ((relationshipDTO.getChildBoAttrName().equals(relatedBoAttrName)) || relatedAttrs.stream().anyMatch(p -> relationshipDTO.getChildBoAttrName().equals(p.getBoAttrName()))) {
//                        List<BusinessObjectRelationshipDTO> currentRelationshipList = new ArrayList<>();
//                        List<BusinessObjectRelationshipDTO> preRelationshipList = relationshipMap.putIfAbsent(relatedBoAttrName, currentRelationshipList);
//                        if (preRelationshipList != null) {
//                            preRelationshipList.add(relationshipDTO);
//                        } else {
//                            currentRelationshipList.add(relationshipDTO);
//                        }
//                    }
//                }
//            }
//        }
//        if (relationshipMap.size() == 0) {
//            return;
//        }
//        //3. update id by name
//        String selectColumn = null;
//        List<BusinessObjectRelationshipDTO> whereColumnList = null;
//        for (DynamicDTO dynamicDTO : saveOrUpdateList) {
//            //loop relationshipMap
//            for (Map.Entry<String, List<BusinessObjectRelationshipDTO>> entry : relationshipMap.entrySet()) {
//                String relatedBoAttrName = entry.getKey();
//                if (!dynamicDTO.containsKey(relatedBoAttrName) || dynamicDTO.get(relatedBoAttrName) == null) {// id is null
//                    //read data from parent_bo_name： select PK.parent_bo_attr_name where non_PK.parent_bo_attr_name = all name values
//                    selectColumn = null;
//                    whereColumnList = new ArrayList<>();
//                    for (BusinessObjectRelationshipDTO relationshipDTO : entry.getValue()) {
//                        if ((relationshipDTO.getPrimaryKeyRelationship().equals(Boolean.TRUE))) {
//                            selectColumn = relationshipDTO.getParentBoAttrName();
//                        } else {
//                            whereColumnList.add(relationshipDTO);
//                        }
//                    }
//                    if (selectColumn != null && whereColumnList.size() > 0) {
//                        String parentBoName = whereColumnList.get(0).getParentBoName();
//                        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(parentBoName, executionContext);
//                        businessObjectInfo.addColumnsToSelect(selectColumn);
//                        boolean allNonPKHasValue = true;
//                        for (BusinessObjectRelationshipDTO whereColumn : whereColumnList) {
//                            //read value from child bo
//                            Object childAttrValue = dynamicDTO.get(whereColumn.getChildBoAttrName());
//                            //if one of them is null, we can't read the unique id, so don't perform name to id
//                            if (childAttrValue == null) {
//                                allNonPKHasValue = false;
//                                break;
//                            }
//                            //filter in the parent bo
//                            businessObjectInfo.addFilter(whereColumn.getParentBoAttrName(), Operator.EQUALS, childAttrValue);
//                        }
//                        if (allNonPKHasValue) {
//                            List<DynamicDTO> resultList = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(parentBoName, executionContext).read(businessObjectInfo, executionContext);
//                            if (resultList != null && resultList.size() > 0) {
//                                dynamicDTO.put(relatedBoAttrName, resultList.get(0).get(selectColumn));
//                            }
//                        }
//                    }
//                }
//            }
//        }
//    }

    private void verifyMandatoryFieldsForSave(List<DynamicDTO> saveList, Connection connection, ExecutionContext executionContext) {
        List<BusinessObjectAttributeDTO> mandatoryColumns = getMandatoryColumns();
        // clone mandatoryColumns list so that we do not change in original metadata copy
        mandatoryColumns = new ArrayList<>(mandatoryColumns);
        for (int i = 0; i < mandatoryColumns.size(); ) {
            BusinessObjectAttributeDTO metadataDTO = mandatoryColumns.get(i);
            // do just mandatory check on all fields excluding primary keys
            if ((metadataDTO.getPrimaryKey()) || (metadataDTO.isCreateAuditField()) || (metadataDTO.isUpdateAuditField())) {
                mandatoryColumns.remove(i);
                // do not increment index
                continue;
            }
            i++;
        }
        int recordIndex = 1;
        for (DynamicDTO dynamicDTO : saveList) {
            for (BusinessObjectAttributeDTO mandatoryFieldDTO : mandatoryColumns) {
                // do just mandatory check on all fields excluding auto generated fields 
                verifyValueExists("save", dynamicDTO.get(mandatoryFieldDTO.getBoAttrName()), mandatoryFieldDTO.getBoDisplayName(),
                        mandatoryFieldDTO.getAttributeDisplayName(), recordIndex, executionContext);
            }
            recordIndex++;
        }
    }

    private void verifyUniqueConstraintsForSave(List<DynamicDTO> saveList, Connection connection, ExecutionContext executionContext) {
        List<BusinessObjectAttributeConstraintsDTO> uniqueConstraintsDTOList = getBusinessObjectAttributeConstraintsDTOS();
        if (CollectionUtils.hasValue(uniqueConstraintsDTOList)) {
            Map<String, List<BusinessObjectAttributeConstraintsDTO>> mapConstraintsDTO =
                    uniqueConstraintsDTOList.stream().collect(Collectors.groupingBy(BusinessObjectAttributeConstraintsDTO::getConstraintName)
                    );

            for (Map.Entry<String, List<BusinessObjectAttributeConstraintsDTO>> entry : mapConstraintsDTO.entrySet()) {
                List<BusinessObjectAttributeConstraintsDTO> uniqueConstraints = entry.getValue();
                if (CollectionUtils.hasValue(uniqueConstraints)) {
                    BusinessObjectAttributeConstraintsDTO constraintsDTO = uniqueConstraints.get(0);
                    if (Boolean.TRUE.equals(constraintsDTO.getVerify())) {
                        if (uniqueConstraints.size() == 1) {
                            // simple unique constraint case use IN operator
                            verifySimpleUniqueConstraintForSave(saveList, constraintsDTO, connection, executionContext);
                        } else {
                            // composite unique constraint
                            verifyCompositeUniqueConstraintsForSave(saveList, entry.getKey(), uniqueConstraints, connection, executionContext);
                        }
                    }
                }
            }
        }
    }

    private void verifySimpleUniqueConstraintForSave(List<DynamicDTO> saveList, BusinessObjectAttributeConstraintsDTO constraintsDTO, Connection connection, ExecutionContext executionContext) {
        // simple unique constraint case use IN operator
        BusinessObjectAttributeDTO metadataDTO = getBoAttributeNamesMetadataMap().get(constraintsDTO.getBoAttrName());
        List<Object> uniqueColumnValuesToBeSaved = CollectionUtils.getValuesFromListOfMap(saveList, constraintsDTO.getBoAttrName());
        Object[] uniqueValuesToBeSaved = uniqueColumnValuesToBeSaved.toArray();
        // create business object info to read data from database
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(getType(), executionContext);
        // add unique column to the select list
        businessObjectInfo.addColumnsToSelect(constraintsDTO.getBoAttrName());
        // must read read all records to true otherwise full data will not come
        businessObjectInfo.setReadAllRecords(true);
        List<DynamicDTO> uniqueBOList = readLongRangeUsingInClause(businessObjectInfo, null, constraintsDTO.getBoAttrName(), uniqueValuesToBeSaved, Operator.IN, connection, executionContext);
        if (CollectionUtils.hasValue(uniqueBOList)) {
            if (metadataDTO.getJavaDataType() == java.lang.String.class) {
                // perform case insensitive check using upper case
                List<String> uniqueColumnValuesAfterReadFromDB = CollectionUtils.getUpperCaseValuesFromListOfMap(uniqueBOList, constraintsDTO.getBoAttrName());
                for (Object uniqueValueToBeSaved : uniqueValuesToBeSaved) {
                    if (uniqueColumnValuesAfterReadFromDB.contains(((String) uniqueValueToBeSaved).toUpperCase())) {
                        throwExceptionOnSimpleUniqueConstraintFailure("save", metadataDTO.getAttributeDisplayName(), (String) uniqueValueToBeSaved, executionContext);
                    }
                }
            } else {
                List<Object> uniqueColumnValuesAfterReadFromDB = CollectionUtils.getValuesFromListOfMap(uniqueBOList, constraintsDTO.getBoAttrName());
                for (Object uniqueValueToBeSaved : uniqueValuesToBeSaved) {
                    if (uniqueColumnValuesAfterReadFromDB.contains(uniqueValueToBeSaved)) {
                        throwExceptionOnSimpleUniqueConstraintFailure("save", metadataDTO.getAttributeDisplayName(), uniqueValueToBeSaved.toString(), executionContext);
                    }
                }
            }
        }
    }

    private void verifyCompositeUniqueConstraintsForSave(List<DynamicDTO> saveList, String constraintName, List<BusinessObjectAttributeConstraintsDTO> uniqueConstraints, Connection connection, ExecutionContext executionContext) {
        // composite unique constraint
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(getType(), executionContext);
        for (DynamicDTO dynamicDTO : saveList) {
            // clear all existing filters
            if ((businessObjectInfo.getFilterList() != null) && (businessObjectInfo.getFilterList().getFilters() != null)) {
                businessObjectInfo.getFilterList().getFilters().clear();
            }
            for (BusinessObjectAttributeConstraintsDTO constraintsDTO : uniqueConstraints) {
                businessObjectInfo.addFilter(constraintsDTO.getBoAttrName(), dynamicDTO.get(constraintsDTO.getBoAttrName()));
            }
            businessObjectInfo.setReadFirst(true);
            // Read count from database
            logger.info("Going to verify that unique values combination does not already exist.");
            int count = count(businessObjectInfo, false, connection, executionContext);
            if (count > 0) {
                Map<String, BusinessObjectAttributeDTO> boAttributeNamesMetadataMap = getBoAttributeNamesMetadataMap();
                List<Object> fields = new ArrayList<>(uniqueConstraints.size());
                List<Object> values = new ArrayList<>(uniqueConstraints.size());
                for (BusinessObjectAttributeConstraintsDTO constraintsDTO : uniqueConstraints) {
                    fields.add(boAttributeNamesMetadataMap.get(constraintsDTO.getBoAttrName()).getAttributeDisplayName());
                    values.add(dynamicDTO.get(constraintsDTO.getBoAttrName()));
                }
                throwExceptionOnCompositeUniqueConstraintFailure("save", constraintName, fields, values, executionContext);
            }
        }
    }
    private void verifyMandatoryFieldsForUpdate(List<DynamicDTO> updateList, boolean doTinyUpdate, Connection connection, ExecutionContext executionContext) {

        List<BusinessObjectAttributeDTO> mandatoryColumns = getMandatoryColumns();
        // clone mandatoryColumns list so that we do not change in original metadata copy
        mandatoryColumns = new ArrayList<>(mandatoryColumns);
        for (int i = 0; i < mandatoryColumns.size(); ) {
            BusinessObjectAttributeDTO metadataDTO = mandatoryColumns.get(i);
            // do just mandatory check on all fields excluding primary keys
            if ((metadataDTO.getPrimaryKey()) || (metadataDTO.isCreateAuditField()) || (metadataDTO.isUpdateAuditField())) {
                mandatoryColumns.remove(i);
                // do not increment index
                continue;
            }
            i++;
        }
        int recordIndex = 1;
        for (DynamicDTO dynamicDTO : updateList) {
            for (BusinessObjectAttributeDTO mandatoryFieldDTO : mandatoryColumns) {
                // do just check that if a field is mandatory (already non-null in db) and we are going to update it with null 
                if (!doTinyUpdate) {
                    // verify all mandatory fields
                    verifyValueExists("update", dynamicDTO.get(mandatoryFieldDTO.getBoAttrName()), mandatoryFieldDTO.getBoDisplayName(),
                            mandatoryFieldDTO.getAttributeDisplayName(), recordIndex, executionContext);
                } else {
                    // verify only those mandatory fields which are present in the dto
                    if (dynamicDTO.containsKey(mandatoryFieldDTO.getBoAttrName())) {
                        verifyValueExists("update", dynamicDTO.get(mandatoryFieldDTO.getBoAttrName()), mandatoryFieldDTO.getBoDisplayName(),
                                mandatoryFieldDTO.getAttributeDisplayName(), recordIndex, executionContext);
                    }
                }
            }
            recordIndex++;
        }
    }

    private void verifyUniqueConstraintsForUpdate(List<DynamicDTO> updateList, Connection connection, ExecutionContext executionContext) {

        List<BusinessObjectAttributeConstraintsDTO> uniqueConstraintsDTOList = getBusinessObjectAttributeConstraintsDTOS();
        if (CollectionUtils.hasValue(uniqueConstraintsDTOList)) {
            Map<String, List<BusinessObjectAttributeConstraintsDTO>> mapConstraintsDTO =
                    uniqueConstraintsDTOList.stream().collect(Collectors.groupingBy(BusinessObjectAttributeConstraintsDTO::getConstraintName)
                    );

            for (Map.Entry<String, List<BusinessObjectAttributeConstraintsDTO>> entry : mapConstraintsDTO.entrySet()) {
                List<BusinessObjectAttributeConstraintsDTO> uniqueConstraints = entry.getValue();
                if (CollectionUtils.hasValue(uniqueConstraints)) {
                    BusinessObjectAttributeConstraintsDTO constraintsDTO = uniqueConstraints.get(0);
                    if (Boolean.TRUE.equals(constraintsDTO.getVerify())) {
                        if (uniqueConstraints.size() == 1) {
                            // simple unique constraint case use IN operator
                            verifySimpleUniqueConstraintForUpdate(updateList, constraintsDTO, connection, executionContext);
                        } else {
                            // composite unique constraint
                            verifyCompositeUniqueConstraintsForUpdate(updateList, entry.getKey(), uniqueConstraints, connection, executionContext);
                        }
                    }
                }
            }
        }
    }

    private void verifySimpleUniqueConstraintForUpdate(List<DynamicDTO> updateList, BusinessObjectAttributeConstraintsDTO constraintsDTO, Connection connection, ExecutionContext executionContext) {
        // simple unique constraint case use IN operator
        BusinessObjectAttributeDTO metadataDTO = getBoAttributeNamesMetadataMap().get(constraintsDTO.getBoAttrName());
        List<String> primaryKeyColumnNames = getPrimaryKeyColumnNames();
        List<Object> uniqueColumnValuesToBeSaved = CollectionUtils.getValuesFromListOfMap(updateList, constraintsDTO.getBoAttrName());
        Object[] uniqueValuesToBeSaved = uniqueColumnValuesToBeSaved.toArray();
        // create business object info to read data from database
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(getType(), executionContext);
        // add unique column to the select list
        businessObjectInfo.addColumnsToSelect(constraintsDTO.getBoAttrName());
        // add primary key column name to select
        businessObjectInfo.addColumnsToSelect(primaryKeyColumnNames);
        // must read read all records to true otherwise full data will not come
        businessObjectInfo.setRecordsPerPage(uniqueValuesToBeSaved.length, executionContext);
        // one in clause does not support more than 256 values
        logger.info("Going to read only unique column and primary key columns from db before update to verify simple unique constraints for BO : {}", getType());
        List<DynamicDTO> uniqueBOList = readLongRangeUsingInClause(businessObjectInfo, null, constraintsDTO.getBoAttrName(), uniqueValuesToBeSaved, Operator.IN, connection, executionContext);
        if (CollectionUtils.hasValue(uniqueBOList)) {
            boolean throwUniqueConstraintError = false;
            if (metadataDTO.getJavaDataType() == java.lang.String.class) {
                // perform case insensitive check using upper case
                List<String> uniqueColumnValuesAfterReadFromDB = CollectionUtils.getUpperCaseValuesFromListOfMap(uniqueBOList, constraintsDTO.getBoAttrName());
                String uniqueValue = null;
                int index = -1;
                for (DynamicDTO dynamicDTOToSave : updateList) {
                    // use upper case
                    uniqueValue = ((String) dynamicDTOToSave.get(constraintsDTO.getBoAttrName())).toUpperCase();
                    index = uniqueColumnValuesAfterReadFromDB.indexOf(uniqueValue);
                    if (index >= 0) {
                        DynamicDTO dynamicDTOFromRead = uniqueBOList.get(index);
                        for (String pkBoAttrName : primaryKeyColumnNames) {
                            if (!(Objects.equals(dynamicDTOFromRead.get(pkBoAttrName), dynamicDTOToSave.get(pkBoAttrName)))) {
                                throwExceptionOnSimpleUniqueConstraintFailure("update", metadataDTO.getAttributeDisplayName(), uniqueValue, executionContext);
                            }
                        }
                        // same record is found now check other record and use last index of
                        if (index != uniqueColumnValuesAfterReadFromDB.lastIndexOf(uniqueValue)) {
                            // another record found
                            throwExceptionOnSimpleUniqueConstraintFailure("update", metadataDTO.getAttributeDisplayName(), uniqueValue.toString(), executionContext);
                        }
                    }
                }
            } else {
                List<Object> uniqueColumnValuesAfterReadFromDB = CollectionUtils.getValuesFromListOfMap(uniqueBOList, constraintsDTO.getBoAttrName());
                Object uniqueValue = null;
                int index = -1;
                for (DynamicDTO dynamicDTOToSave : updateList) {
                    // use upper case
                    uniqueValue = dynamicDTOToSave.get(constraintsDTO.getBoAttrName());
                    index = uniqueColumnValuesAfterReadFromDB.indexOf(uniqueValue);
                    if (index >= 0) {
                        // get whole object at same index from list which is read from database
                        DynamicDTO dynamicDTOFromRead = uniqueBOList.get(index);
                        for (String pkBoAttrName : primaryKeyColumnNames) {
                            if (!(Objects.equals(dynamicDTOFromRead.get(pkBoAttrName), dynamicDTOToSave.get(pkBoAttrName)))) {
                                throwExceptionOnSimpleUniqueConstraintFailure("update", metadataDTO.getAttributeDisplayName(), uniqueValue.toString(), executionContext);
                            }
                        }
                        // same record is found now check other record and use last index of
                        if (index != uniqueColumnValuesAfterReadFromDB.lastIndexOf(uniqueValue)) {
                            // another record found
                            throwExceptionOnSimpleUniqueConstraintFailure("update", metadataDTO.getAttributeDisplayName(), uniqueValue.toString(), executionContext);
                        }
                    }
                }
            }
        }
    }

    private void verifyCompositeUniqueConstraintsForUpdate(List<DynamicDTO> updateList, String constraintName, List<BusinessObjectAttributeConstraintsDTO> uniqueConstraints, Connection connection, ExecutionContext executionContext) {
        // composite unique constraint
        List<String> primaryKeyColumnNames = getPrimaryKeyColumnNames();
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(getType(), executionContext);
        for (DynamicDTO dynamicDTO : updateList) {
            // clear all existing filters
            if ((businessObjectInfo.getFilterList() != null) && (businessObjectInfo.getFilterList().getFilters() != null)) {
                businessObjectInfo.getFilterList().getFilters().clear();
            }
            for (BusinessObjectAttributeConstraintsDTO constraintsDTO : uniqueConstraints) {
                businessObjectInfo.addFilter(constraintsDTO.getBoAttrName(), dynamicDTO.get(constraintsDTO.getBoAttrName()));
            }
            // add condition where id not equal to current record id
            for (String pkBoAttrName : primaryKeyColumnNames) {
                businessObjectInfo.addFilter(pkBoAttrName, Operator.NOT_EQUALS, dynamicDTO.get(pkBoAttrName));
            }
            businessObjectInfo.setReadFirst(true);
            // Read count from database
            logger.info("Going to read count from db before update to verify composite unique constraints so that the same unique values do not already exist in db for some other primary key values for BO : {}", getType());
            int count = count(businessObjectInfo, false, connection, executionContext);
            if (count > 0) {
                Map<String, BusinessObjectAttributeDTO> boAttributeNamesMetadataMap = getBoAttributeNamesMetadataMap();
                List<Object> fields = new ArrayList<>(uniqueConstraints.size());
                List<Object> values = new ArrayList<>(uniqueConstraints.size());
                for (BusinessObjectAttributeConstraintsDTO constraintsDTO : uniqueConstraints) {
                    fields.add(boAttributeNamesMetadataMap.get(constraintsDTO.getBoAttrName()).getAttributeDisplayName());
                    values.add(dynamicDTO.get(constraintsDTO.getBoAttrName()));
                }
                throwExceptionOnCompositeUniqueConstraintFailure("update", constraintName, fields, values, executionContext);
            }
        }
    }

    private void verifyValueExists(String operationName, Object value, String boDisplayName, String boAttrDisplayName, int recordIndex, ExecutionContext executionContext) {
        if (value == null) {
            Object[] notNullViolationSQLState = SQLState.getNotNullViolationSQLState();
            throw new DSPDMException((String) notNullViolationSQLState[0], (Integer) notNullViolationSQLState[1], "Cannot perform {} operation on business object '{}', field '{}' for record at index '{} ' must have a value",
                    executionContext.getExecutorLocale(), operationName, boDisplayName, boAttrDisplayName, recordIndex);
        } else if (value instanceof String) {
            if (((String) value).trim().length() == 0) {
                Object[] zeroLengthStringSQLState = SQLState.getZeroLengthStringSQLState();
                throw new DSPDMException((String) zeroLengthStringSQLState[0], (Integer) zeroLengthStringSQLState[1], "Cannot perform {} operation on business object '{}', field '{}' for record at index '{} ' must have a value",
                        executionContext.getExecutorLocale(), operationName, boDisplayName, boAttrDisplayName, recordIndex);
            }
        }
    }

    private void throwExceptionOnSimpleUniqueConstraintFailure(String operationName, String attributeDisplayName, String uniqueValue, ExecutionContext executionContext) {
        Object[] uniqueConstraintsViolatedSQLState = SQLState.getUniqueConstraintsViolatedSQLState();
        throw new DSPDMException((String) uniqueConstraintsViolatedSQLState[0], (Integer) uniqueConstraintsViolatedSQLState[1], "Cannot perform {} operation on business object '{}'. Value '{}' for attribute '{}' already exists",
                executionContext.getExecutorLocale(), operationName, getType(), uniqueValue, attributeDisplayName);
    }

    private void throwExceptionOnCompositeUniqueConstraintFailure(String operationName, String constraintName, List<Object> fields, List<Object> values, ExecutionContext executionContext) {
        Object[] uniqueConstraintsViolatedSQLState = SQLState.getUniqueConstraintsViolatedSQLState();
        throw new DSPDMException((String) uniqueConstraintsViolatedSQLState[0], (Integer) uniqueConstraintsViolatedSQLState[1], "Cannot perform {} operation on business object '{}', unique constraint '{}' failed, fields '{}' does not meet unique constraint criteria. Values '{}' already exists",
                executionContext.getExecutorLocale(), operationName, getType(), constraintName, CollectionUtils.getCommaSeparated(fields), CollectionUtils.getCommaSeparated(values));
    }

    /* ****************************************************** */
    /* ************** BUSINESS METHOD DELETE **************** */
    /* ****************************************************** */

    @Override
    public SaveResultDTO delete(List<DynamicDTO> businessObjectsToDelete, boolean hasSQLExpression, Connection connection,
                                ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        if (CollectionUtils.hasValue(businessObjectsToDelete)) {
            // call underlying API to perform delete
            saveResultDTO.addResult(deleteImpl(businessObjectsToDelete, hasSQLExpression, connection, executionContext));
            // tracking change history
            if (isChangeHistoryTrackEnabledForBoName(this.getType(), executionContext)) {
                for (DynamicDTO dynamicDTO : businessObjectsToDelete) {
                    boolean first = true;
                    Timestamp timestamp = null;
                    for (Map.Entry<String, Object> entry : dynamicDTO.entrySet()) {
                        String columnName = entry.getKey();
                        Object columnValue = entry.getValue();
                        // not tracking null values
                        if (columnValue == null) {
                            continue;
                        }
                        //ignoring primary key column name. not needed to be logged
                        if ((CollectionUtils.hasValue(this.getPrimaryKeyColumnNames())
                                && (CollectionUtils.containsIgnoreCase(this.getPrimaryKeyColumnNames(), columnName)))) {
                            continue;
                        }
                        if (DSPDMConstants.BoAttrName.isAuditField(columnName)) {
                            continue;
                        }
                        if(first) {
                            first = false;
                            // only increment for only one whole dto.
                            executionContext.incrementOperationSequenceNumber();
                            // use same timestamp for all attributes changed under a same dto
                            timestamp = DateTimeUtils.getCurrentTimestampUTC();
                        }
                        addChangeHistoryDTOToExecutionContext(this.getType(),
                                (Integer) dynamicDTO.getId().getPK()[0], // primary key
                                columnName,
                                columnValue,
                                null, // since its a delete operation. we don't have new value so setting it to null
                                timestamp,
                                DSPDMConstants.BusinessObjectOperations.DELETE_OPR.getId(),
                                executionContext);
                    }
                }
            }
        }
        return saveResultDTO;
    }
    
	@Override
	public SaveResultDTO deleteAll(Connection connection, ExecutionContext executionContext) {
		// call underlying API to perform delete all records
		return deleteAllImpl(connection, executionContext);
	}
    
    @Override
    public SaveResultDTO softDelete(List<DynamicDTO> businessObjectsToDelete, boolean hasSQLExpression, Connection connection,
                                    ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        if (CollectionUtils.hasValue(businessObjectsToDelete)) {
            // call underlying API to perform delete
            saveResultDTO.addResult(softDeleteImpl(businessObjectsToDelete, hasSQLExpression, connection, executionContext));
        }
        return saveResultDTO;
    }

    @Override
    public Integer[] getNextFromSequence(String boName, String sequenceName, int count, Connection connection, ExecutionContext executionContext) {
        return getNextFromSequenceImpl(boName, sequenceName, count, connection, executionContext);
    }
}
