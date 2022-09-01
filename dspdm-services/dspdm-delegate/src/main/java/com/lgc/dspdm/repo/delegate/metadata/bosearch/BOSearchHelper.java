package com.lgc.dspdm.repo.delegate.metadata.bosearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.CriteriaFilter;
import com.lgc.dspdm.core.common.data.criteria.FilterGroup;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.join.BaseJoinClause;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.ObjectUtils;
import com.lgc.dspdm.core.dao.dynamic.businessobject.DynamicDAOFactory;

import java.util.*;

/**
 * @author rao.alikhan
 * @since 01-Apr-2020
 */
public class BOSearchHelper {
    private static BOSearchHelper singleton = null;
    private ObjectMapper objectMapper = null;
    private final DSPDMLogger logger = new DSPDMLogger(BOSearchHelper.class);

    private BOSearchHelper(ExecutionContext executionContext) {
        objectMapper = new ObjectMapper();
        SimpleModule simpleModule = new SimpleModule();
        // For BO Search Dynamic DTO JSON Object
        simpleModule.addSerializer(DynamicDTO.class, new BOSearchObjectSerializer());
        // For BO Search Values JSON Array
        simpleModule.addSerializer(BOSearchValuesList.class, new BOSearchValuesListSerializer());
        objectMapper.registerModule(simpleModule);
    }

    public static BOSearchHelper getInstance(ExecutionContext executionContext) {
        if (singleton == null) {
            singleton = new BOSearchHelper(executionContext);
        }
        return singleton;
    }

    /**
     * Will filter business objects list like which one is inserted and which one is updated
     *
     * @param boListToDivide Business Object List
     * @return Will return map which contains both inserted and updated records in a separate index
     * @author rao.alikhan
     * @since 01-Apr-2020
     */
    public Map<DSPDMConstants.DBOperation, List<DynamicDTO>> getSeparateSaveAndUpdateListsMap(List<DynamicDTO> boListToDivide) {
        Map<DSPDMConstants.DBOperation, List<DynamicDTO>> saveOrUpdateMap = new HashMap<>(2);
        List<DynamicDTO> insertBusinessObjectList = new ArrayList<>(boListToDivide.size());
        List<DynamicDTO> updateBusinessObjectList = new ArrayList<>(boListToDivide.size());
        for (DynamicDTO dynamicDTO : boListToDivide) {
            if (dynamicDTO.isInserted()) {
                insertBusinessObjectList.add(dynamicDTO);
            } else if (dynamicDTO.isUpdated()) {
                updateBusinessObjectList.add(dynamicDTO);
            }
        }
        if (insertBusinessObjectList.size() > 0) {
            saveOrUpdateMap.put(DSPDMConstants.DBOperation.CREATE, insertBusinessObjectList);
        }
        if (updateBusinessObjectList.size() > 0) {
            saveOrUpdateMap.put(DSPDMConstants.DBOperation.UPDATE, updateBusinessObjectList);
        }
        return saveOrUpdateMap;
    }

    /**
     * Reads the dynamic dto business object list based from database on result set found in BO_SEARCH
     *
     * @param businessObjectInfo
     * @param boSearchResultsList
     * @param primaryKeyColumnNames
     * @param executionContext
     * @return
     * @author rao.alikhan
     * @since 06-Apr-2020
     */
    public List<DynamicDTO> readBusinessObjectListUsingBoSearchResultsList(BusinessObjectInfo businessObjectInfo,
                                                                           BaseJoinClause joinClauseToApplyFilter,
                                                                           List<DynamicDTO> boSearchResultsList,
                                                                           String[] primaryKeyColumnNames,
                                                                           ExecutionContext executionContext) {
        List<DynamicDTO> businessObjectList = null;
        if (CollectionUtils.hasValue(primaryKeyColumnNames)) {
            if (primaryKeyColumnNames.length > 1) {
                // Handle composite key case for business object
                businessObjectList = readBusinessObjectListForCompositePrimaryKey(businessObjectInfo, joinClauseToApplyFilter, boSearchResultsList, primaryKeyColumnNames, executionContext);
            } else {
                // Handle single primary case for business object
                businessObjectList = readBusinessObjectListForSinglePrimaryKey(businessObjectInfo, joinClauseToApplyFilter, boSearchResultsList, primaryKeyColumnNames[0], executionContext);
            }
        } else {
            throw new DSPDMException("Unable to perform search. No primary key defined for business object ", executionContext.getExecutorLocale(), businessObjectInfo.getBusinessObjectType());
        }
        return businessObjectList;
    }

    /**
     * Create Json from dynamicDTO for ordering purpose
     *
     * @param businessObject   Business object which contains values to be converted in JSON
     * @param executionContext Execution context
     * @return Will return values in JSON form
     * @author rao.alikhan
     * @since 01-Apr-2020
     */
    private String convertToObjectJSON(DynamicDTO businessObject, ExecutionContext executionContext) {
        String json = null;
        try {
            json = objectMapper.writeValueAsString(businessObject);
        } catch (Throwable ex) {
            DSPDMException.throwException(ex, executionContext);
        }
        return json;
    }

    /**
     * Create Json from dynamicDTO values for search purpose
     *
     * @param businessObject   Business object which contains values to be converted in JSON
     * @param executionContext Execution context
     * @return Will return values in JSON form
     * @author rao.alikhan
     * @since 01-Apr-2020
     */
    private String convertToSearchValuesListJSON(DynamicDTO businessObject, ExecutionContext executionContext) {
        String json = null;
        try {
            json = objectMapper.writeValueAsString(new BOSearchValuesList(businessObject.values(), executionContext));
        } catch (Throwable ex) {
            DSPDMException.throwException(ex, executionContext);
        }
        return json;
    }


    /**
     * returns a newly create BO_Search dynamic dto
     *
     * @param boName
     * @param pkBoAttrNames
     * @param pkBoAttrVales
     * @param businessObject
     * @param executionContext
     * @return
     * @author rao.alikhan
     * @since 01-Apr-2020
     */
    private DynamicDTO createBOSearchDTO(String boName, String pkBoAttrNames, String pkBoAttrVales, DynamicDTO businessObject, ExecutionContext executionContext) {
        DynamicDTO boSearchDynamicDTO = new DynamicDTO(DSPDMConstants.BoName.BO_SEARCH, Arrays.asList(DSPDMConstants.BoAttrName.BO_SEARCH_ID), executionContext);
        // make json
        boSearchDynamicDTO.put(DSPDMConstants.BoAttrName.PK_BO_ATTR_NAMES, pkBoAttrNames);
        boSearchDynamicDTO.put(DSPDMConstants.BoAttrName.PK_BO_ATTR_VALUES, pkBoAttrVales);
        boSearchDynamicDTO.put(DSPDMConstants.BoAttrName.BO_NAME, boName);
        boSearchDynamicDTO.put(DSPDMConstants.BoAttrName.SEARCH_JSONB, convertToSearchValuesListJSON(businessObject, executionContext));
        boSearchDynamicDTO.put(DSPDMConstants.BoAttrName.OBJECT_JSONB, convertToObjectJSON(businessObject, executionContext));
        return boSearchDynamicDTO;
    }

    /**
     * read existing bo search records
     *
     * @param businessObjectList
     * @param executionContext
     * @return
     */
    private List<DynamicDTO> readExistingBoSearchRecordsForList(List<DynamicDTO> businessObjectList, DSPDMConstants.DBOperation dbOperation, ExecutionContext executionContext) {
        Set<String> primaryKeyValues = new LinkedHashSet<>(businessObjectList.size());
        String[] getPrimaryKeyNamesAndValues = null;
        for (DynamicDTO businessObjectDTO : businessObjectList) {
            getPrimaryKeyNamesAndValues = getPrimaryKeyNamesAndValuesFromBusinessObject(businessObjectDTO, DSPDMConstants.Separator.BO_SEARCH_ATTR_SEPARATOR, executionContext);
            if(getPrimaryKeyNamesAndValues != null) {
                // add to the set of primary keys to be used to read from database
                primaryKeyValues.add(getPrimaryKeyNamesAndValues[1]);
                // add this primary key value to the business object dto to be used in future
                businessObjectDTO.put(DSPDMConstants.BoAttrName.PK_BO_ATTR_NAMES, getPrimaryKeyNamesAndValues[0]);
                businessObjectDTO.put(DSPDMConstants.BoAttrName.PK_BO_ATTR_VALUES, getPrimaryKeyNamesAndValues[1]);
            }
        }

        if ((primaryKeyValues.size() == 0)
                || (executionContext.isIndexAll())
                || (dbOperation == DSPDMConstants.DBOperation.CREATE)
                && (!(ConfigProperties.getInstance().read_existing_search_record_index_before_insert_new.getBooleanValue()))) {
            return new ArrayList<>(0);
        } else {
            String boName = businessObjectList.get(0).getType();
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BO_SEARCH, executionContext);
            businessObjectInfo.addColumnsToSelect(
                    DSPDMConstants.BoAttrName.BO_NAME,
                    DSPDMConstants.BoAttrName.BO_SEARCH_ID,
                    DSPDMConstants.BoAttrName.PK_BO_ATTR_NAMES,
                    DSPDMConstants.BoAttrName.PK_BO_ATTR_VALUES,
                    DSPDMConstants.BoAttrName.ROW_CREATED_DATE,
                    DSPDMConstants.BoAttrName.ROW_CREATED_BY);
            businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.BO_NAME, Operator.EQUALS, boName);
            businessObjectInfo.setRecordsPerPage(primaryKeyValues.size(), executionContext);
            // going to read from db
            logger.info("Going to read BO_SEARCH records from db because they will be updated against as data model records has already been updated for BO : {}", boName);
            return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BO_SEARCH, executionContext)
                    .readLongRangeUsingInClauseAndExistingFilters(businessObjectInfo, null,
                            DSPDMConstants.BoAttrName.PK_BO_ATTR_VALUES, primaryKeyValues.toArray(), Operator.IN, executionContext);
        }
    }

    /**
     * Will get the update list for boSearch
     *
     * @param businessObjectList Business Object list from database
     * @param executionContext
     * @return Will return list of dynamic DTO which includes only those records which will update only in database
     * @author rao.alikhan
     * @since 01-Apr-2020
     */
    public List<DynamicDTO> getBoSearchDynamicDTOListForInsertOrUpdateOrDelete(List<DynamicDTO> businessObjectList, DSPDMConstants.DBOperation dbOperation, ExecutionContext executionContext) {
        List<DynamicDTO> boSearchDynamicDTOListForInsertOrUpdateOrDelete = readExistingBoSearchRecordsForList(businessObjectList, dbOperation, executionContext);
        // search records are already read from database.
        // now update the json in all searched DTOs
        Map<String, List<DynamicDTO>> primaryKeyValueMapFromBOSearch = CollectionUtils.groupDynamicDTOByPropertyValue(boSearchDynamicDTOListForInsertOrUpdateOrDelete, DSPDMConstants.BoAttrName.PK_BO_ATTR_VALUES);
        String primaryKeyColumnName = null;
        String primaryKeyColumnValue = null;
        for (DynamicDTO businessObjectDTO : businessObjectList) {
            // use remove below instead of get because they were added temporarily and should not go up in read back
            primaryKeyColumnName = (String) businessObjectDTO.remove(DSPDMConstants.BoAttrName.PK_BO_ATTR_NAMES);
            primaryKeyColumnValue = (String) businessObjectDTO.remove(DSPDMConstants.BoAttrName.PK_BO_ATTR_VALUES);
            if ((primaryKeyColumnName != null) && (primaryKeyColumnValue != null)) {
                List<DynamicDTO> existingBOSearchDTOList = primaryKeyValueMapFromBOSearch.get(primaryKeyColumnValue);
                if (existingBOSearchDTOList != null) {
                    for (DynamicDTO boSearchDTO : existingBOSearchDTOList) {
                        boSearchDTO.put(DSPDMConstants.BoAttrName.SEARCH_JSONB, convertToSearchValuesListJSON(businessObjectDTO, executionContext));
                        boSearchDTO.put(DSPDMConstants.BoAttrName.OBJECT_JSONB, convertToObjectJSON(businessObjectDTO, executionContext));
                    }
                } else if ((DSPDMConstants.DBOperation.CREATE == dbOperation) || (DSPDMConstants.DBOperation.UPDATE == dbOperation)) {
                    boSearchDynamicDTOListForInsertOrUpdateOrDelete.add(createBOSearchDTO(businessObjectDTO.getType(), primaryKeyColumnName, primaryKeyColumnValue, businessObjectDTO, executionContext));
                }
            }
        }
        return boSearchDynamicDTOListForInsertOrUpdateOrDelete;
    }

    /**
     * This will create a map with business object primary keys and its values for the given business object
     *
     * @param businessObject Business Object which has the information of its data and its primary keys
     * @param separator      Separator is needed to separate composite keys
     * @return It will return map which has primary keys and values for BO_SEARCH
     * @author rao.alikhan
     * @since 01-Apr-2020
     */
    private String[] getPrimaryKeyNamesAndValuesFromBusinessObject(DynamicDTO businessObject, String separator, ExecutionContext executionContext) {
        String boSearchAttrNames = null;
        String boSearchAttrValues = null;
        if (businessObject.getPrimaryKeyColumnNames().size() > 1) {
            // Get keys string for composite keys
            boSearchAttrNames = CollectionUtils.joinWith(separator, businessObject.getPrimaryKeyColumnNames().toArray());
            // Get values for composite primary keys
            boSearchAttrValues = CollectionUtils.joinWith(separator, CollectionUtils.getMapValuesForKeys(businessObject, businessObject.getPrimaryKeyColumnNames(), executionContext));
        } else if (businessObject.getPrimaryKeyColumnNames().size() == 1) {
            boSearchAttrNames = businessObject.getPrimaryKeyColumnNames().get(0);
            boSearchAttrValues = String.valueOf(businessObject.get(businessObject.getPrimaryKeyColumnNames().get(0)));
        } else {
            return null;
        }
        return new String[]{boSearchAttrNames, boSearchAttrValues};
    }

    /**
     * Will get business object list for single primary key
     *
     * @param businessObjectInfo
     * @param boSearchList
     * @param primaryKeyColumnName
     * @param executionContext
     * @return
     * @author rao.alikhan
     * @since 06-Apr-2020
     */
    private List<DynamicDTO> readBusinessObjectListForSinglePrimaryKey(BusinessObjectInfo businessObjectInfo,
                                                                       BaseJoinClause joinClauseToApplyFilter,
                                                                       List<DynamicDTO> boSearchList,
                                                                       String primaryKeyColumnName,
                                                                       ExecutionContext executionContext) {
        Set<Integer> primaryKeyValues = CollectionUtils.getIntegerValuesFromList(boSearchList, DSPDMConstants.BoAttrName.PK_BO_ATTR_VALUES);
        if (businessObjectInfo.isReadAllRecords()) {
            businessObjectInfo.addPagesToRead(executionContext, primaryKeyValues.size(), 1);
        }
        List<DynamicDTO> businessObjectList = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(businessObjectInfo.getBusinessObjectType(), executionContext)
                    .readLongRangeUsingInClauseAndExistingFilters(businessObjectInfo, joinClauseToApplyFilter, primaryKeyColumnName, primaryKeyValues.toArray(), Operator.IN, executionContext);
        return businessObjectList;
    }

    /**
     * Will get business object list for composite primary key
     *
     * @param boSearchList            BO Search list from database
     * @param joinClauseToApplyFilter where to apply conditions
     * @param primaryKeyColumnNames   Primary Column Names
     * @param executionContext        Execution Context
     * @return Will return business object list based on the criteria selected from BO_SEARCH
     * @author rao.alikhan
     * @since 06-Apr-2020
     */
    private List<DynamicDTO> readBusinessObjectListForCompositePrimaryKey(BusinessObjectInfo businessObjectInfo,
                                                                          BaseJoinClause joinClauseToApplyFilter,
                                                                          List<DynamicDTO> boSearchList,
                                                                          String[] primaryKeyColumnNames,
                                                                          ExecutionContext executionContext) {
        List<FilterGroup> filterGroups = new ArrayList<>();
        for (DynamicDTO boSearchDynamicDTO : boSearchList) {
            String pkBOAttrValues = (String) boSearchDynamicDTO.get(DSPDMConstants.BoAttrName.PK_BO_ATTR_VALUES);
            String[] primaryKeyColumnValues = pkBOAttrValues.split(DSPDMConstants.Separator.BO_SEARCH_ATTR_SEPARATOR);
            if ((primaryKeyColumnValues != null) && (primaryKeyColumnValues.length == primaryKeyColumnNames.length)) {
                CriteriaFilter[] criteriaFilters = new CriteriaFilter[primaryKeyColumnNames.length];
                for (int i = 0; i < primaryKeyColumnNames.length; i++) {
                    criteriaFilters[i] = new CriteriaFilter(primaryKeyColumnNames[i], Operator.EQUALS, new Object[]{primaryKeyColumnValues[i].trim()});
                }
                filterGroups.add(new FilterGroup(Operator.OR, Operator.AND, criteriaFilters));
            } else {
                throw new DSPDMException("BO_Search record for business object {} has invalid values for primary key columns.", executionContext.getExecutorLocale(), businessObjectInfo.getBusinessObjectType());
            }
        }
        BusinessObjectInfo newBusinessObjectInfo = ObjectUtils.deepCopy(businessObjectInfo, executionContext);
        if ((newBusinessObjectInfo.isJoiningRead()) && (joinClauseToApplyFilter != null)) {
            // find similar join clause on the new cloned business object info
            BaseJoinClause newJoinClauseToApplyFilter = newBusinessObjectInfo.findFirstJoinClauseForJoinAlias(joinClauseToApplyFilter.getJoinAlias());
            newJoinClauseToApplyFilter.addFilterGroup(filterGroups);
        } else {
            newBusinessObjectInfo.addFilterGroup(new FilterGroup(Operator.AND, Operator.OR, null, filterGroups.toArray(new FilterGroup[0])));
        }
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(newBusinessObjectInfo.getBusinessObjectType(), executionContext).read(businessObjectInfo, executionContext);
    }
}
