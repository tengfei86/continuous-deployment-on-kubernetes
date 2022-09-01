package com.lgc.dspdm.msp.mainservice.utils.metadata;

import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.core.common.util.metadata.MetadataSearchIndexUtils;
import com.lgc.dspdm.service.common.dynamic.read.IDynamicReadService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * It is a Helper class to validate the json request syntax and make sure that
 * the request is properly structured and fulfills the criteria to create or drop a search index
 *
 * @author Muhammad Suleman Tanveer
 * @since 24-Dec-2021
 */
public class MetadataSearchIndexChangeRestServiceHelper {

    /**
     * to validate the json request syntax and make sure that the request is properly structured and fulfills the criteria to create a search index
     *
     * @param busObjAttrSearchIndexesToBeCreated
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Suleman Tanveer
     * @since 24-Dec-2021
     */
    public static void validateAddSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeCreated, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {

        String boName = null;
        String boAttrName = null;
        String indexName = null;
        String useCase = null;
        for (DynamicDTO busObjAttrSearchIndexDTOToBeCreated : busObjAttrSearchIndexesToBeCreated) {
            boName = (String) busObjAttrSearchIndexDTOToBeCreated.get(DSPDMConstants.BoAttrName.BO_NAME);
            boAttrName = (String) busObjAttrSearchIndexDTOToBeCreated.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
            indexName = (String) busObjAttrSearchIndexDTOToBeCreated.get(DSPDMConstants.BoAttrName.INDEX_NAME);
            useCase = (String) busObjAttrSearchIndexDTOToBeCreated.get(DSPDMConstants.BoAttrName.USE_CASE);
            // validate mandatory attributes to define a search index
            if (StringUtils.isNullOrEmpty(boName)) {
                throw new DSPDMException("'{}' is mandatory to add a search index", executionContext.getExecutorLocale(), DSPDMConstants.BoAttrName.BO_NAME);
            }
            if (StringUtils.isNullOrEmpty(boAttrName)) {
                throw new DSPDMException("'{}' is mandatory to add a Search index", executionContext.getExecutorLocale(), DSPDMConstants.BoAttrName.BO_ATTR_NAME);
            }
            // check if search index name is provided then its length should not be more than 200
            if ((StringUtils.hasValue(indexName)) && (indexName.length() > 200)) {
                throw new DSPDMException("'{}' length cannot be greater than 200", executionContext.getExecutorLocale(), DSPDMConstants.BoAttrName.INDEX_NAME);
            }
            //make sure primary key is not provided in any of the search index object
            if (busObjAttrSearchIndexDTOToBeCreated.get(DSPDMConstants.BoAttrName.BUS_OBJ_ATTR_SEARCH_INDEX_ID) != null) {
                throw new DSPDMException("Cannot add search index '{}'. Primary key '{}' must not be provided.",
                        executionContext.getExecutorLocale(), indexName, DSPDMConstants.BoAttrName.BUS_OBJ_ATTR_SEARCH_INDEX_ID);
            }

            // validate that business object and business object attribute exists and it is not a primary key
            DynamicDTO boAttrNameDTO = dynamicReadService.readMetadataMapForBOName(boName, executionContext).get(boAttrName);
            if (boAttrNameDTO == null) {
                throw new DSPDMException("Cannot add search index '{}'. Business object attribute '{}' does not exist in business object '{}'.",
                        executionContext.getExecutorLocale(), indexName, boAttrName, boName);
            } else if (Boolean.TRUE.equals(boAttrNameDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY))) {
                throw new DSPDMException("Cannot add search index '{}' for business object '{}'. Primary key attribute '{}' "
                        + "cannot be involved in any search index definition.",
                        executionContext.getExecutorLocale(), indexName, boName, boAttrName);
            }

            // validate search index name not already exists
            if (StringUtils.hasValue(indexName)) {
                validateSearchIndexName_AddSearchIndex(indexName, dynamicReadService, executionContext);
            }

            // validate if useCase has a value, It must be UPPER or LOWER onl, else throw error
            if (StringUtils.hasValue(useCase)) {
                if ((!useCase.equalsIgnoreCase(DSPDMConstants.SEARCH_INDEX_USE_CASE.UPPER))
                        && (!useCase.equalsIgnoreCase(DSPDMConstants.SEARCH_INDEX_USE_CASE.LOWER))) {
                    throw new DSPDMException("Invalid value for USE_CASE. It can only be '{}' or '{}'.", executionContext.getExecutorLocale(),
                            DSPDMConstants.SEARCH_INDEX_USE_CASE.UPPER, DSPDMConstants.SEARCH_INDEX_USE_CASE.LOWER);
                }
            }
        }
        // now verify no search index with same attribute set already exists
        verifySearchIndexNotAlreadyExists(busObjAttrSearchIndexesToBeCreated, dynamicReadService, executionContext);
    }

    /**
     * verifies that a search index with same name does not already exist
     *
     * @param indexName
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Suleman Tanveer
     * @since 24-Dec-2021
     */
    private static void validateSearchIndexName_AddSearchIndex(String indexName, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        // index name should not have special characters
        if (!(StringUtils.isAlphaNumericOrWhitespaceUnderScore(indexName))) {
            throw new DSPDMException("Cannot add search index. Index name '{}' cannot have special characters.",
                    executionContext.getExecutorLocale(), indexName);
        }
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES, executionContext);
        businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.INDEX_NAME, indexName);
        if (dynamicReadService.count(businessObjectInfo, executionContext) > 0) {
            throw new DSPDMException("Cannot add search index. A search index with name '{}' already exists.",
                    executionContext.getExecutorLocale(), indexName);
        }
    }

    /**
     * verify search index not already exist for same set of attributes and business objects
     *
     * @param busObjAttrSearchIndexesToBeCreated
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Suleman Tanveer
     * @since 24-Dec-2021
     */
    private static void verifySearchIndexNotAlreadyExists(List<DynamicDTO> busObjAttrSearchIndexesToBeCreated, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {

        List<DynamicDTO> simpleSearchIndexesToBeCreated = new ArrayList<>(busObjAttrSearchIndexesToBeCreated.size());
        List<DynamicDTO> compositeSearchIndexesToBeCreated = new ArrayList<>(busObjAttrSearchIndexesToBeCreated.size());
        for (DynamicDTO dynamicDTO : busObjAttrSearchIndexesToBeCreated) {
            if (StringUtils.isNullOrEmpty((String) dynamicDTO.get(DSPDMConstants.BoAttrName.INDEX_NAME))) {
                // add to simple as no name is provided
                simpleSearchIndexesToBeCreated.add(dynamicDTO);
            } else {
                // add to composite by default as a name is provided
                compositeSearchIndexesToBeCreated.add(dynamicDTO);
            }
        }
        // process all composite search indexes. First group by name and if a group size is one then it means its is a simple search index
        if (CollectionUtils.hasValue(compositeSearchIndexesToBeCreated)) {
            Map<String, List<DynamicDTO>> groupBySearchIndexName = CollectionUtils.groupDynamicDTOByPropertyValueIgnoreCase(compositeSearchIndexesToBeCreated, DSPDMConstants.BoAttrName.INDEX_NAME);
            for (Map.Entry<String, List<DynamicDTO>> entry : groupBySearchIndexName.entrySet()) {
                compositeSearchIndexesToBeCreated = entry.getValue();
                if (compositeSearchIndexesToBeCreated.size() == 1) {
                    simpleSearchIndexesToBeCreated.add(compositeSearchIndexesToBeCreated.get(0));
                } else {
                    // 1. verify composite search index not already exists for same attributes
                    verifyCompositeSearchIndexNotAlreadyExists(compositeSearchIndexesToBeCreated, dynamicReadService, executionContext);
                    // 2. make sure duplicate/repeated attribute does not exist in a composite search index
                    verifyNoCompositeSearchIndexHasRepeatedAttribute(compositeSearchIndexesToBeCreated, executionContext);
                }
            }
        }
        // process all simple search indexes individually and independently
        if (CollectionUtils.hasValue(simpleSearchIndexesToBeCreated)) {
            TreeSet<String> alreadyVerifiedSimpleSearchIndexes = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
            // if objects in this group has name then it is unique in the group
            // this group has no search index name it means all objects are independent and must be verified individually/independently
            for (DynamicDTO simpleSearchIndexToBeCreated : simpleSearchIndexesToBeCreated) {
                String boName = (String) simpleSearchIndexToBeCreated.get(DSPDMConstants.BoAttrName.BO_NAME);
                String boAttrName = (String) simpleSearchIndexToBeCreated.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                String uniqueName = boName + "_" + boAttrName;
                if (alreadyVerifiedSimpleSearchIndexes.contains(uniqueName)) {
                    String indexName = (String) simpleSearchIndexToBeCreated.get(DSPDMConstants.BoAttrName.INDEX_NAME);
                    throw new DSPDMException("Cannot add search index '{}'. The business object attribute '{}' already has a simple search index.",
                            executionContext.getExecutorLocale(), indexName, boAttrName);
                } else {
                    // verify search index not already exists for attributes and bo name
                    verifySimpleSearchIndexesNotAlreadyExists(simpleSearchIndexToBeCreated, dynamicReadService, executionContext);
                }
                alreadyVerifiedSimpleSearchIndexes.add(uniqueName);
            }
        }
    }

    /**
     * verify that simple search indexes not already exists for same attribute and business object
     *
     * @param simpleSearchIndex
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Suleman Tanveer
     * @since 24-Dec-2021
     */
    private static void verifySimpleSearchIndexesNotAlreadyExists(DynamicDTO simpleSearchIndex, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        String boName = (String) simpleSearchIndex.get(DSPDMConstants.BoAttrName.BO_NAME);
        String boAttrName = (String) simpleSearchIndex.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
        // check all old already existing search indexes for same business object name
        // read from cache
        List<DynamicDTO> allExistingSearchIndexesForBoName = dynamicReadService.readMetadataSearchIndexesForBOName(boName, executionContext);
        if ((CollectionUtils.hasValue(allExistingSearchIndexesForBoName)) && (StringUtils.hasValue(boAttrName))) {
            // call a utility method to check that the given combination already exists or not
            if (MetadataSearchIndexUtils.searchIndexExistsForAttributes(allExistingSearchIndexesForBoName, boAttrName)) {
                String indexName = (String) simpleSearchIndex.get(DSPDMConstants.BoAttrName.INDEX_NAME);
                throw new DSPDMException("Cannot add search index '{}'. A search index already exists for business object attribute '{}'.",
                        executionContext.getExecutorLocale(), indexName, CollectionUtils.getCommaSeparated(boAttrName));
            }
        }
    }

    /**
     * verify composite search index not already exists for same attributes
     *
     * @param compositeSearchIndexes
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 03-Mar-2021
     */
    private static void verifyCompositeSearchIndexNotAlreadyExists(List<DynamicDTO> compositeSearchIndexes, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        DynamicDTO firstBusObjAttrSearchIndexDTO = compositeSearchIndexes.get(0);
        String boName = (String) firstBusObjAttrSearchIndexDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        List<String> boAttrNames = CollectionUtils.getUpperCaseValuesFromListOfMap(compositeSearchIndexes, DSPDMConstants.BoAttrName.BO_ATTR_NAME);
        // check all old already existing search index for same business object name
        // read from cache
        List<DynamicDTO> allExistingSearchIndexesForBoName = dynamicReadService.readMetadataSearchIndexesForBOName(boName, executionContext);
        if ((CollectionUtils.hasValue(allExistingSearchIndexesForBoName)) && (CollectionUtils.hasValue(boAttrNames))) {
            // call a utility method to check that the given combination already exists or not
            if (MetadataSearchIndexUtils.searchIndexExistsForAttributes(allExistingSearchIndexesForBoName, boAttrNames)) {
                String indexName = (String) firstBusObjAttrSearchIndexDTO.get(DSPDMConstants.BoAttrName.INDEX_NAME);
                throw new DSPDMException("Cannot add search index '{}'. A search index already exists for business object attributes '{}'.",
                        executionContext.getExecutorLocale(), indexName, CollectionUtils.getCommaSeparated(boAttrNames));
            }
        }
    }

    /**
     * Makes sure that all the attributes involved in a composite search indexes are different from each other
     *
     * @param searchIndexesInANamedGroup
     * @param executionContext
     * @author Muhammad Suleman Tanveer
     * @since 24-Dec-2021
     */
    private static void verifyNoCompositeSearchIndexHasRepeatedAttribute(List<DynamicDTO> searchIndexesInANamedGroup, ExecutionContext executionContext) {
        // now verify no two search indexes dto objects have same attribute name under same search index name
        List<String> boAttrNames = CollectionUtils.getUpperCaseValuesFromListOfMap(searchIndexesInANamedGroup, DSPDMConstants.BoAttrName.BO_ATTR_NAME);
        // means that all the attributes in a composite search index must be different from each other
        TreeSet<String> boAttrNamesSet = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
        boAttrNamesSet.addAll(boAttrNames);
        if (boAttrNamesSet.size() != searchIndexesInANamedGroup.size()) {
            String indexName = (String) searchIndexesInANamedGroup.get(0).get(DSPDMConstants.BoAttrName.INDEX_NAME);
            throw new DSPDMException("Cannot add search index '{}'. A business object attribute has been used more than once.",
                    executionContext.getExecutorLocale(), indexName);
        }
    }

    /**
     * to validate the json request syntax and make sure that the request is properly structured and fulfills the criteria to drop a search index
     *
     * @param busObjAttrSearchIndexesToBeDropped
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Suleman Tanveer
     * @since 28-Dec-2021
     */
    public static void validateDropSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeDropped,
                                                     IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        String indexName = null;
        for (DynamicDTO busObjAttrSearchIndexDTOToBeDropped : busObjAttrSearchIndexesToBeDropped) {
            indexName = (String) busObjAttrSearchIndexDTOToBeDropped.get(DSPDMConstants.BoAttrName.INDEX_NAME);
            // index name is mandatory to drop a unique constraint
            if (StringUtils.isNullOrEmpty(indexName)) {
                throw new DSPDMException("Cannot drop search index. '{}' is mandatory to drop a search index.",
                        executionContext.getExecutorLocale(), indexName, DSPDMConstants.BoAttrName.INDEX_NAME);
            }
            // check if search index name is provided then its length should not be more than 200
            if (indexName.length() > 200) {
                throw new DSPDMException("'{}' length cannot be greater than 200", executionContext.getExecutorLocale(), DSPDMConstants.BoAttrName.INDEX_NAME);
            }
            // index name should not have special characters
            if (!(StringUtils.isAlphaNumericOrWhitespaceUnderScore(indexName))) {
                throw new DSPDMException("Cannot drop search indexes. Index name '{}' cannot have special characters.",
                        executionContext.getExecutorLocale(), indexName);
            }
            //make sure primary key is not provided in any of the search index object
            if (busObjAttrSearchIndexDTOToBeDropped.get(DSPDMConstants.BoAttrName.BUS_OBJ_ATTR_SEARCH_INDEX_ID) != null) {
                throw new DSPDMException("Cannot drop search index '{}'. Primary key '{}' must not be provided.",
                        executionContext.getExecutorLocale(), indexName, DSPDMConstants.BoAttrName.BUS_OBJ_ATTR_SEARCH_INDEX_ID);
            }
        }
        // now verify no search index name has been used more than once
        verifySearchIndexNameIsNotRepeated_DropSearchIndexes(busObjAttrSearchIndexesToBeDropped, executionContext);

        // validate search index name must already exist
        validateSearchIndexName_DropSearchIndexes(busObjAttrSearchIndexesToBeDropped, dynamicReadService, executionContext);
    }

    /**
     * verifies that a search index must already exist
     *
     * @param busObjAttrSearchIndexesToBeDropped
     * @param dynamicReadService
     * @param executionContext
     *
     * @author Muhammad Suleman Tanveer
     * @since 28-Dec-2021
     */
    private static void validateSearchIndexName_DropSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeDropped, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        // extract all the index names from the list
        List<String> indexNamesToDrop = CollectionUtils.getUpperCaseValuesFromListOfMap(busObjAttrSearchIndexesToBeDropped, DSPDMConstants.BoAttrName.INDEX_NAME);
        // now read index name from db
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BUS_OBJ_ATTR_SEARCH_INDEXES, executionContext);
        // set read with distinct
        businessObjectInfo.setReadWithDistinct(true);
        // just read index name
        businessObjectInfo.addColumnsToSelect(DSPDMConstants.BoAttrName.INDEX_NAME);
        List<DynamicDTO> busObjAttrSearchIndexesReadFromDB = dynamicReadService.readLongRangeUsingInClause(businessObjectInfo, null, DSPDMConstants.BoAttrName.INDEX_NAME, indexNamesToDrop.toArray(), Operator.IN, executionContext);
        List<String> indexNamesFromDB = CollectionUtils.getUpperCaseValuesFromListOfMap(busObjAttrSearchIndexesReadFromDB, DSPDMConstants.BoAttrName.INDEX_NAME);
        if (indexNamesToDrop.size() != indexNamesFromDB.size()) {
            for (String indexName : indexNamesToDrop) {
                if (!indexNamesFromDB.contains(indexName)) {
                    throw new DSPDMException("Cannot drop search indexes. No existing search index found with name '{}'.",
                            executionContext.getExecutorLocale(), indexName);
                }
            }
        }
    }

    /**
     * Makes sure that no search index name is coming more than once
     *
     * @param busObjAttrSearchIndexesToBeDropped
     * @param executionContext
     * @author Muhammad Suleman Tanveer
     * @since 28-Dec-2021
     */
    private static void verifySearchIndexNameIsNotRepeated_DropSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeDropped, ExecutionContext executionContext) {
        // now verify that no index name is coming more than once
        List<String> indexNames = CollectionUtils.getUpperCaseValuesFromListOfMap(busObjAttrSearchIndexesToBeDropped, DSPDMConstants.BoAttrName.INDEX_NAME);
        // means that all the index names must be different from each other
        TreeSet<String> indexNamesSet = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
        indexNamesSet.addAll(indexNames);
        if (indexNamesSet.size() != busObjAttrSearchIndexesToBeDropped.size()) {
            throw new DSPDMException("Cannot drop search indexes. An index name has been used more than once.",
                    executionContext.getExecutorLocale());
        }
    }
}
