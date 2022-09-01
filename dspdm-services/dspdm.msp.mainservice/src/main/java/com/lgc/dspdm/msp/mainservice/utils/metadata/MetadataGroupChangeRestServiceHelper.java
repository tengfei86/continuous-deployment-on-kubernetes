package com.lgc.dspdm.msp.mainservice.utils.metadata;

import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateFunction;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.*;
import com.lgc.dspdm.service.common.dynamic.read.IDynamicReadService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * It is a Helper class to validate the json request syntax and make sure that
 * the request is properly structured and fulfills the criteria to create or drop a business object group
 *
 * @author Qinghua Ma
 * @since 25-June-2021
 */
public class MetadataGroupChangeRestServiceHelper {

    /**
     * to validate the json request syntax and make sure that the request is properly structured and fulfills the criteria to create a business object group
     *
     * @param busObjGroupsToBeCreated
     * @param dynamicReadService
     * @param executionContext
     */
    public static void validateAddBusinessObjectGroups(List<DynamicDTO> busObjGroupsToBeCreated, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        if (CollectionUtils.hasValue(busObjGroupsToBeCreated)) {

            List<DynamicDTO> rGroupCategories = dynamicReadService.read(DSPDMConstants.BoName.R_GROUP_CATEGORY, executionContext);
            if (CollectionUtils.isNullOrEmpty(rGroupCategories)) {
                throw new DSPDMException("Cannot add business object group. No data found for business object '{}'.",
                        executionContext.getExecutorLocale(), DSPDMConstants.BoName.R_GROUP_CATEGORY);
            }
            Map<String, DynamicDTO> categoryNameMap = CollectionUtils.prepareIgnoreCaseMapFromStringValuesOfKey(rGroupCategories, DSPDMConstants.BoAttrName.GROUP_CATEGORY_NAME);
            String groupCategoryName = null;
            String boName = null;
            DynamicDTO rGroupCategoryDTO = null;
            Map<String, Integer> sequenceNoMap = new HashMap<>();
            for (DynamicDTO busObjGroupToBeCreated : busObjGroupsToBeCreated) {
                groupCategoryName = (String) busObjGroupToBeCreated.get(DSPDMConstants.BoAttrName.GROUP_CATEGORY_NAME);
                boName = (String) busObjGroupToBeCreated.get(DSPDMConstants.BoAttrName.BO_NAME);
                // validate mandatory attributes to define a business object group
                if (StringUtils.isNullOrEmpty(groupCategoryName)) {
                    throw new DSPDMException("'{}' is mandatory to add a business object group", executionContext.getExecutorLocale(), DSPDMConstants.BoAttrName.GROUP_CATEGORY_NAME);
                }
                if (StringUtils.isNullOrEmpty(boName)) {
                    throw new DSPDMException("'{}' is mandatory to add a business object group", executionContext.getExecutorLocale(), DSPDMConstants.BoAttrName.BO_NAME);
                }
                // validate that groupCategoryName exist in group category
                rGroupCategoryDTO = categoryNameMap.get(groupCategoryName);
                if (rGroupCategoryDTO == null) {
                    throw new DSPDMException("Cannot add business object group. Provided value '{}' for attribute '{}' does not exist in business object '{}'.",
                            executionContext.getExecutorLocale(), groupCategoryName, DSPDMConstants.BoAttrName.GROUP_CATEGORY_NAME, DSPDMConstants.BoName.R_GROUP_CATEGORY);
                } else {
                    // Now change the category name from which we read from db to fix any case of type errors
                    groupCategoryName = (String) rGroupCategoryDTO.get(DSPDMConstants.BoAttrName.GROUP_CATEGORY_NAME);
                    busObjGroupToBeCreated.put(DSPDMConstants.BoAttrName.GROUP_CATEGORY_NAME, groupCategoryName);
                    busObjGroupToBeCreated.put(DSPDMConstants.BoAttrName.R_GROUP_CATEGORY_ID, rGroupCategoryDTO.get(DSPDMConstants.BoAttrName.R_GROUP_CATEGORY_ID));
                }
                // validate that BO_NAME exist in business object
                DynamicDTO businessObjectDTO = dynamicReadService.readMetadataBusinessObject(boName, executionContext);
                if (businessObjectDTO == null) {
                    throw new DSPDMException("Cannot add business object group. Bo name '{}' does not exist in business object.", executionContext.getExecutorLocale(), boName);
                } else {
                    // Now change the bo name from which we read from db to fix any case of type errors
                    boName = (String) businessObjectDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
                    busObjGroupToBeCreated.put(DSPDMConstants.BoAttrName.BO_NAME, boName);
                    busObjGroupToBeCreated.put(DSPDMConstants.BoAttrName.BUSINESS_OBJECT_ID, businessObjectDTO.get(DSPDMConstants.BoAttrName.BUSINESS_OBJECT_ID));
                    busObjGroupToBeCreated.put(DSPDMConstants.BoAttrName.DISPLAY_NAME, businessObjectDTO.get(DSPDMConstants.BoAttrName.BO_DISPLAY_NAME));
                    busObjGroupToBeCreated.put(DSPDMConstants.BoAttrName.IS_ACTIVE, businessObjectDTO.get(DSPDMConstants.BoAttrName.IS_ACTIVE));
                }
                // validate that BO_NAME not already exist in business object group
                BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BUSINESS_OBJECT_GROUP, executionContext);
                businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.BO_NAME, boName);
                // if primary key provided the exclude it
                Integer businessObjectGroupId = (Integer) busObjGroupToBeCreated.get(DSPDMConstants.BoAttrName.BUSINESS_OBJECT_GROUP_ID);
                if (businessObjectGroupId != null) {
                    // in case of update exclude current record primary key
                    businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.BUSINESS_OBJECT_GROUP_ID, Operator.NOT_EQUALS, businessObjectGroupId);
                }
                List<DynamicDTO> listFromDB = dynamicReadService.readSimple(businessObjectInfo.setReadFirst(true), executionContext);
                if (CollectionUtils.hasValue(listFromDB)) {
                    String categoryNameFromDB = (String) listFromDB.get(0).get(DSPDMConstants.BoAttrName.GROUP_CATEGORY_NAME);
                    if (businessObjectGroupId == null) {
                        throw new DSPDMException("Cannot add business object group. Bo name '{}' already belongs to group {}.", executionContext.getExecutorLocale(), boName, categoryNameFromDB);
                    } else {
                        throw new DSPDMException("Cannot update business object group. Bo name '{}' already belongs to group {}.", executionContext.getExecutorLocale(), boName, categoryNameFromDB);
                    }
                }

                // now set sequence number of the business object going to be added in the current group.
                Integer nextSequenceNumber = null;
                if (sequenceNoMap.containsKey(groupCategoryName)) {
                    Integer lastSequenceNumber = sequenceNoMap.get(groupCategoryName);
                    nextSequenceNumber = lastSequenceNumber + 1;
                } else {
                    Integer lastSequenceNumber = readMaxSequenceNumberForCategoryFromDB(groupCategoryName, businessObjectGroupId, dynamicReadService, executionContext);
                    nextSequenceNumber = lastSequenceNumber + 1;
                }
                // now set sequence number to existing max sequence number + 1
                busObjGroupToBeCreated.put(DSPDMConstants.BoAttrName.SEQUENCE_NUM, nextSequenceNumber);
                // put sequence number in map for subsequent bo names coming in same group category name
                sequenceNoMap.put(groupCategoryName, nextSequenceNumber);
            }
        }
    }

    /**
     * Returns max sequence number already existing for a category group
     *
     * @param groupCategoryName
     * @param businessObjectGroupId
     * @param dynamicReadService
     * @param executionContext
     * @return
     */
    public static int readMaxSequenceNumberForCategoryFromDB(String groupCategoryName, Integer businessObjectGroupId, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        Integer maxSequenceNumber = 0;
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BUSINESS_OBJECT_GROUP, executionContext);
        businessObjectInfo.addAggregateColumnToSelect(DSPDMConstants.BoAttrName.SEQUENCE_NUM, AggregateFunction.MAX, "max_sequence_number");
        businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.GROUP_CATEGORY_NAME, groupCategoryName);
        if (businessObjectGroupId != null) {
            // in case of update exclude current record primary key
            businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.BUSINESS_OBJECT_GROUP_ID, Operator.NOT_EQUALS, businessObjectGroupId);
        }
        businessObjectInfo.setReadFirst(true);
        List<DynamicDTO> list = dynamicReadService.readSimple(businessObjectInfo, executionContext);
        if (CollectionUtils.hasValue(list)) {
            DynamicDTO dynamicDTO = list.get(0);
            Integer max_sequence_number = NumberUtils.convertToInteger(dynamicDTO.get("max_sequence_number"), executionContext);
            if (max_sequence_number != null) {
                maxSequenceNumber = max_sequence_number;
            }
        }
        return maxSequenceNumber;
    }
}
