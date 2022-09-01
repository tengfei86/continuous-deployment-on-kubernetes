package com.lgc.dspdm.msp.mainservice.utils;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.*;
import com.lgc.dspdm.service.common.dynamic.read.IDynamicReadService;
import com.lgc.dspdm.msp.mainservice.model.BOQuery;
import com.lgc.dspdm.msp.mainservice.model.CriteriaFilter;
import com.lgc.dspdm.msp.mainservice.model.HierarchyObjectInfo;
import com.lgc.dspdm.msp.mainservice.model.HierarchyStruct;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * This class represents the conversion of Hierarchy table data into HierarchyObjectInfo structure objects,
 * Used for converting HierarchyResult objects.
 *
 * @author Bo Liang
 * @createDate: 2020/06/02
 * @description: Hierarchy Builder
 */
public class HierarchyBuilder {
    /**
     * The role of this method is: map Hierarchy table data in the database to the object HierarchyObjectInfo collection
     * List<HierarchyObjectInfo>: Hierarchy table data conversion result
     *
     * @param entity:             The filter conditions of the hierarchy table.
     *                            If the entity.getBoName() is "ALL", query all hierarchy data,
     *                            Otherwise, it corresponds to the hierarchy data
     * @param dynamicReadService: Database operation interface
     * @param executionContext:   Log information record
     * @return: return HierarchyObjectInfo data
     */
    public static List<HierarchyObjectInfo> build(BOQuery entity, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        List<HierarchyObjectInfo> hierarchyObjectInfoList = new ArrayList<>();
        try {
            // SET SHOW SQL STATS
            if (ConfigProperties.getInstance().allow_sql_stats_collection.getBooleanValue()) {
                Try.apply(() -> executionContext.setCollectSQLStats(entity.getShowSQLStats().booleanValue()));
                // SET COLLECT SQL SCRIPT
                CollectSQLScriptOptions option = CollectSQLScriptOptions.getByValue(entity.getCollectSQLScript());
                if(option != null) {
                    executionContext.setCollectSQLScriptOptions(option);
                } else {
                    throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                            "Invalid value '{}' given for param '{}'. {}", executionContext.getExecutorLocale(), entity.getCollectSQLScript(),
                            DSPDMConstants.DSPDM_REQUEST.COLLECT_SQL_SCRIPT_KEY, CollectSQLScriptOptions.description);
                }
            }

            // SET WRITE NULL VALUES
            Try.apply(() -> executionContext.setWriteNullValues(entity.getWriteNullValues().booleanValue()));

            // Dynamic loading field table name
            HierarchyStruct hierarchyTable = new HierarchyStruct();
            // Get the hierarchy table data
            List<DynamicDTO> hierarchyData = getHierarchyTableData(entity.getBoName(),hierarchyTable, dynamicReadService, executionContext);

            if (hierarchyData != null) {
                /*
                Return a unique value set based on hierarchy_name. That is, remove duplicate hierarchy_name value sets
                For example: hierarchyData data volume: 9 row [FIELD-WELL: 2 row; FIELD-COMPLETION: 4 row; SURFACE-NETWORK: 3 row].
                The returned hierarchyData value is 3 rows: [FIELD-WELL; FIELD-COMPLETION; SURFACE-NETWORK]
                 */
                List<DynamicDTO> hierarchyNameUniqueData = hierarchyData.stream().distinct().collect(Collectors.collectingAndThen(
                        Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(o -> o.get(hierarchyTable.getHierarchyName()).toString()))), ArrayList::new));

                // Level data contained in hierarchyName
                List<DynamicDTO> singleHierarchyData = null;

                // The type and ID field of the parent level, used when recursively combining trees
                String parentColumnID = null;
                String parentLevelType = null;

                for (DynamicDTO hierarchyNameUnique : hierarchyNameUniqueData) {
                    // According to hierarchyName to obtain the included Level dataAccording to hierarchyName to obtain the included Level data
                    singleHierarchyData = hierarchyData.stream().filter(p -> p.get(hierarchyTable.getHierarchyName()).equals(hierarchyNameUnique.get(hierarchyTable.getHierarchyName()))).collect(Collectors.toList());
                    parentColumnID = null;
                    parentLevelType = null;

                    HierarchyObjectInfo hierarchyObjectInfo = new HierarchyObjectInfo();
                    hierarchyObjectInfo.setHierarchyName(hierarchyNameUnique.get(hierarchyTable.getHierarchyName()).toString());
                    hierarchyObjectInfo.setLevelMinIndex(singleHierarchyData.size());

                    for (DynamicDTO singleHierarchy : singleHierarchyData) {
                        //Convert hierarchy table data to HierarchyObjectInfo
                        HierarchyStruct hierarchyStruct = new HierarchyStruct();
                        hierarchyStruct.setLevelType(singleHierarchy.get(hierarchyTable.getLevelType()).toString());
                        hierarchyStruct.setBoName(singleHierarchy.get(hierarchyTable.getBoName()).toString());
                        hierarchyStruct.setColumnID(singleHierarchy.get(hierarchyTable.getColumnID()).toString());
                        hierarchyStruct.setColumnName(singleHierarchy.get(hierarchyTable.getColumnName()).toString());
                        hierarchyStruct.setLevelIndex(NumberUtils.convertToInteger(singleHierarchy.get(hierarchyTable.getHierarchyIndexName()), executionContext));
                        hierarchyStruct.setParentLevelType(parentLevelType);
                        hierarchyStruct.setTempParentColumnID(parentColumnID);

                        if (singleHierarchy.get(hierarchyTable.getParentColumnID()) != null) {
                            hierarchyStruct.setParentColumnID(singleHierarchy.get(hierarchyTable.getParentColumnID()).toString());
                            hierarchyStruct.setParentColumnName(singleHierarchy.get(hierarchyTable.getParentColumnName()).toString());
                            if (singleHierarchy.get(hierarchyTable.getRootValue()) != null) {
                                hierarchyStruct.setRootValue(singleHierarchy.get(hierarchyTable.getRootValue()).toString());
                            }
                        }
                        if (singleHierarchy.get(hierarchyTable.getRelationColumnID()) != null) {
                            hierarchyStruct.setRelationColumnID(singleHierarchy.get(hierarchyTable.getRelationColumnID()).toString());
                            hierarchyStruct.setRelationColumnName(singleHierarchy.get(hierarchyTable.getRelationColumnName()).toString());
                        }

                        /*
                        Call method [appendCriteriaFiltersToHierarchy], this method has two purposes:
                        1. Assign CriteriaFilters in the BOQuery object to hierarchy,
                        2. And determine whether the BOQuery object exists, return true, otherwise false
                            If BOQuery exists, record the levelIndex value,
                            Use it when converting HierarchyResult.
                         */
                        if (appendCriteriaFiltersToHierarchy(entity, hierarchyStruct, executionContext)) {
                            //The minimum hierarchyIndex value corresponding to BOQuery, used when the BO data is converted into the hierarchy structure
                            if (hierarchyObjectInfo.getLevelMinIndex() >= hierarchyStruct.getLevelIndex()) {
                                hierarchyObjectInfo.setLevelMinIndex(hierarchyStruct.getLevelIndex());
                            }
                            //The maximum hierarchyIndex value corresponding to BOQuery, used when the BO data is converted into the hierarchy structure
                            if (hierarchyObjectInfo.getLevelMaxIndex() <= hierarchyStruct.getLevelIndex()) {
                                hierarchyObjectInfo.setLevelMaxIndex(hierarchyStruct.getLevelIndex());
                            }
                        } else {
                            hierarchyStruct.setHierarchyHide(true);
                        }
                        hierarchyObjectInfo.setChildHierarchy(hierarchyStruct);

                        parentLevelType = singleHierarchy.get(hierarchyTable.getLevelType()).toString();
                        parentColumnID = singleHierarchy.get(hierarchyTable.getColumnID()).toString();
                    }
                    hierarchyObjectInfoList.add(hierarchyObjectInfo);
                }
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return hierarchyObjectInfoList;
    }

    /**
     * This method is to get the Hierarchy table data in the database and return the result
     *
     * @param hierarchyName:      The filter conditions of the hierarchy table.
     *                            If hierarchyName is "ALL", query all hierarchy data,
     *                            Otherwise, it corresponds to the hierarchy data
     * @param dynamicReadService:
     * @param executionContext:   Log information recordS
     * @return: get hierarchy DynamicDTO data
     */
    private static List<DynamicDTO> getHierarchyTableData(String hierarchyName,HierarchyStruct hierarchyTable, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        List<DynamicDTO> dataList = null;
        try {
            /*
            Dynamically obtain HIERARCHY's Attribute from the business_object_attr table,
            And load the object hierarchyTable.
            For example: boName="BO_NAME"; columnID="COLUMN_ID";
             */
            BusinessObjectInfo boquery = new BusinessObjectInfo("BUSINESS OBJECT ATTR", executionContext);
            boquery.setReadAllRecords(true);
            boquery.addFilter("bo_name", Operator.EQUALS, "HIERARCHY");
            dataList = dynamicReadService.readSimple(boquery, executionContext);
            hierarchyTable.setHierarchyColumnName(dataList);

            // Get hierarchy table data
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo("HIERARCHY", executionContext);
            businessObjectInfo.setReadAllRecords(true);

            businessObjectInfo.addOrderByAsc(hierarchyTable.getHierarchyName());
            businessObjectInfo.addOrderByAsc(hierarchyTable.getHierarchyIndexName());

            /*
            If hierarchyName is equal to "ALL", do not enter filter criteria
             */
            if (!hierarchyName.toUpperCase(executionContext.getExecutorLocale()).equals("ALL")) {
                businessObjectInfo.addFilter(hierarchyTable.getHierarchyName(), Operator.EQUALS, hierarchyName);
            }
            dataList = dynamicReadService.readSimple(businessObjectInfo, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return dataList;
    }

    /**
     * this method has two purposes:
     * 1. Assign CriteriaFilters in the BOQuery object to hierarchy,
     * 2. And determine whether the BOQuery object exists, return true, otherwise false
     *
     * @param boQuery:          BOQuery
     * @param hierarchyStruct:  HierarchyStruct
     * @param executionContext: Log information recordS
     * @return: get hierarchy DynamicDTO data
     */
    private static boolean appendCriteriaFiltersToHierarchy(BOQuery boQuery, HierarchyStruct hierarchyStruct, ExecutionContext executionContext) {
        // There is a default BOQuery, so the default value is set to true;
        // If no entity.getReadChildBO() value is entered, it means that all child nodes exist.
        boolean existBOQuery = true;
        try {
            if (boQuery.getReadChildBO() != null) {
                List<BOQuery> tempQuery = boQuery.getReadChildBO().stream().filter(p -> p.getBoName().equals(hierarchyStruct.getBoName())).collect(Collectors.toList());
                if (tempQuery.size() > 0) {
                    // CriteriaFilters in BOQuery is not empty, assign value to Hierarchy
                    for (BOQuery childQueryen : tempQuery) {
                        if (CollectionUtils.hasValue(childQueryen.getCriteriaFilters())) {
                            for (CriteriaFilter criteriaFilter : childQueryen.getCriteriaFilters()) {
                                if (criteriaFilter.getOperator() == Operator.JSONB_FIND_LIKE
                                        || criteriaFilter.getOperator() == Operator.JSONB_FIND_EXACT) {
                                    Object[] values = new Object[criteriaFilter.getValues().length];
                                    for (int i = 0; i < criteriaFilter.getValues().length; i++) {
                                        values[i] = String.valueOf(criteriaFilter.getValues()[i])
                                                .toLowerCase();
                                    }
                                    hierarchyStruct.setFilter(criteriaFilter.getBoAttrName(),
                                            criteriaFilter.getOperator(), values);
                                } else {
                                    hierarchyStruct.setFilter(criteriaFilter.getBoAttrName(),
                                            criteriaFilter.getOperator(), criteriaFilter.getValues());
                                }
                            }
                        }
                        break;
                    }
                } else {
                    // BOQuery object does not exist, false
                    existBOQuery = false;
                }
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return existBOQuery;
    }
}
