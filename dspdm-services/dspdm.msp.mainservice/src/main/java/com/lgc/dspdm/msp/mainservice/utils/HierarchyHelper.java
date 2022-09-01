package com.lgc.dspdm.msp.mainservice.utils;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.service.common.dynamic.read.IDynamicReadService;
import com.lgc.dspdm.msp.mainservice.model.HierarchyObjectInfo;
import com.lgc.dspdm.msp.mainservice.model.HierarchyResult;
import com.lgc.dspdm.msp.mainservice.model.HierarchyStruct;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class HierarchyHelper {
    /**
     * This method is to convert Hierarchy's table data into Hierarchy tree structure.
     * For example: Hierarchy table definition data is: FIELD-WELL: [FIELD; WELL]
     * Hierarchy returns tree data [infinite recursive tree]
     * +FIELD [Hierarchy]
     * -WELL [Hierarchy]
     *
     * @param executionContext: Log information record
     * @return: Hierarchy Data convert Hierarchy Struct
     */
    public static List<HierarchyResult> convertHierarchyDataToHierarchyStruct(HierarchyObjectInfo hierarchyObjectInfo, ExecutionContext executionContext) {
        List<HierarchyResult> hierarchyResultList = null;
        try {
            if (hierarchyObjectInfo.getChildHierarchy() == null) {
                hierarchyResultList = new ArrayList<>();
            } else {
                /*
                HierarchyStruct data of the root node, representing the first level data in the tree
                Use it when combining tree structures.
                 */
                List<HierarchyStruct> rootHierarchyStruct = new ArrayList<>();
                /*
                Tree node HierarchyStruct data, data of all child nodes in the tree,
                Use it when combining tree structures.
                 */
                List<HierarchyStruct> bodyHierarchyStruct = new ArrayList<>();

                for (HierarchyStruct hierarchyStruct : hierarchyObjectInfo.getChildHierarchy()) {
                    HierarchyStruct tempHierarchyStruct = new HierarchyStruct();
                    tempHierarchyStruct.setLevelType(hierarchyStruct.getLevelType());
                    tempHierarchyStruct.setBoName(hierarchyStruct.getBoName());
                    tempHierarchyStruct.setColumnID(hierarchyStruct.getColumnID());
                    tempHierarchyStruct.setColumnName(hierarchyStruct.getColumnName());
                    tempHierarchyStruct.setTempParentColumnID(hierarchyStruct.getTempParentColumnID());
                    tempHierarchyStruct.setParentLevelType(hierarchyStruct.getParentLevelType());

                    if (rootHierarchyStruct.size() == 0) {
                        rootHierarchyStruct.add(tempHierarchyStruct);
                    } else {
                        bodyHierarchyStruct.add(tempHierarchyStruct);
                    }
                }
                hierarchyResultList = getHierarchyTree(rootHierarchyStruct, bodyHierarchyStruct, executionContext);
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return hierarchyResultList;
    }

    /**
     * This method is to convert LevelType's table data into Hierarchy tree structure.
     * For example: Hierarchy table definition data is: FIELD-WELL: [FIELD; WELL]
     * Hierarchy returns tree data [infinite recursive tree]
     * +FIELD [LevelType data]
     * -WELL [LevelType data]
     *
     * @param executionContext: Log information record
     * @return: BO Data convert Hierarchy Struct
     */
    public static List<HierarchyResult> convertBODataToHierarchyStruct(HierarchyObjectInfo hierarchyObjectInfo, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        List<HierarchyResult> hierarchyResultList = null;
        try {
            if (hierarchyObjectInfo.getChildHierarchy() == null) {
                hierarchyResultList = new ArrayList<>();
            } else {
                /*
                HierarchyStruct data of the root node, representing the first level data in the tree
                Use it when combining tree structures.
                 */
                List<HierarchyStruct> rootHierarchyStruct = new ArrayList<>();
                /*
                Tree node HierarchyStruct data, data of all child nodes in the tree,
                Use it when combining tree structures.
                 */
                List<HierarchyStruct> bodyHierarchyStruct = new ArrayList<>();

                List<DynamicDTO> dataList = null;
                List<DynamicDTO> equalRootValueList = null;
                List<DynamicDTO> notEqualRootValueList = null;

                // The main function of this attribute is: whether the current data is appended to rootHierarchyStruct or bodyHierarchyStruct
                boolean isAppendRootData = true;

                for (HierarchyStruct hierarchyStruct : hierarchyObjectInfo.getChildHierarchy()) {
                    if (hierarchyStruct.getLevelIndex() >= hierarchyObjectInfo.getLevelMinIndex()
                            && hierarchyStruct.getLevelIndex() <= hierarchyObjectInfo.getLevelMaxIndex()) {

                        // Get the data of LevelType [BO Name]
                        dataList = getLevelTypeData(hierarchyStruct, dynamicReadService, executionContext);

                        if (dataList != null) {
                            if (hierarchyStruct.getParentColumnID() != null) {
                                /*
                                When the parent table data contains Parent and Relation fields.
                                We need to complete the data combination in two steps:
                                  1. Query the data equal to the Root_Value value as the root node value of the current Level;
                                  2. Query data that is not equal to the Root_Value value, as the child node value of the current Level;
                                 */
                                if (hierarchyStruct.getRootValue().equals("null")) {
                                    equalRootValueList = dataList.stream().filter(p -> p.get(hierarchyStruct.getParentColumnID()) == null).collect(Collectors.toList());
                                    notEqualRootValueList = dataList.stream().filter(p -> p.get(hierarchyStruct.getParentColumnID()) != null).collect(Collectors.toList());
                                } else {
                                    equalRootValueList = dataList.stream().filter(p -> p.get(hierarchyStruct.getParentColumnID()).equals(hierarchyStruct.getRootValue())).collect(Collectors.toList());
                                    notEqualRootValueList = dataList.stream().filter(p -> !p.get(hierarchyStruct.getParentColumnID()).equals(hierarchyStruct.getRootValue())).collect(Collectors.toList());
                                }

                                if (hierarchyStruct.getRelationColumnID() != null) {
                                    for (DynamicDTO data : equalRootValueList) {
                                        HierarchyStruct tempHierarchyStruct = getDynamicDTOToHierarchyStruct(hierarchyStruct, data, executionContext);
                                        // The field mapped by the root node is RelationID
                                        tempHierarchyStruct.setParentColumnID((data.get(hierarchyStruct.getRelationColumnID()) == null) ? null : data.get(hierarchyStruct.getRelationColumnID()).toString());
                                        tempHierarchyStruct.setTempParentColumnID((data.get(hierarchyStruct.getRelationColumnID()) == null) ? null : data.get(hierarchyStruct.getRelationColumnID()).toString());
                                        // The parent type of the root node mapping is ParentType
                                        tempHierarchyStruct.setParentLevelType(hierarchyStruct.getParentLevelType());

                                        appendHierarchyStructToList(tempHierarchyStruct, rootHierarchyStruct, bodyHierarchyStruct, isAppendRootData, executionContext);
                                    }
                                } else {
                                    for (DynamicDTO data : equalRootValueList) {
                                        HierarchyStruct tempHierarchyStruct = getDynamicDTOToHierarchyStruct(hierarchyStruct, data, executionContext);
                                        // The field mapped by the root node is ParentID
                                        tempHierarchyStruct.setParentColumnID((data.get(hierarchyStruct.getParentColumnID()) == null) ? null : data.get(hierarchyStruct.getParentColumnID()).toString());
                                        tempHierarchyStruct.setTempParentColumnID((data.get(hierarchyStruct.getParentColumnID()) == null) ? null : data.get(hierarchyStruct.getParentColumnID()).toString());
                                        // The parent type of the root node mapping is ParentType
                                        tempHierarchyStruct.setParentLevelType(hierarchyStruct.getParentLevelType());

                                        appendHierarchyStructToList(tempHierarchyStruct, rootHierarchyStruct, bodyHierarchyStruct, isAppendRootData, executionContext);
                                    }
                                }
                                isAppendRootData = false;

                                for (DynamicDTO data : notEqualRootValueList) {
                                    HierarchyStruct tempHierarchyStruct = getDynamicDTOToHierarchyStruct(hierarchyStruct, data, executionContext);
                                    // The field mapped by the root node is ParentID
                                    tempHierarchyStruct.setParentColumnID((data.get(hierarchyStruct.getParentColumnID()) == null) ? null : data.get(hierarchyStruct.getParentColumnID()).toString());
                                    tempHierarchyStruct.setTempParentColumnID((data.get(hierarchyStruct.getParentColumnID()) == null) ? null : data.get(hierarchyStruct.getParentColumnID()).toString());
                                    // The parent type of node mapping is LevelType
                                    tempHierarchyStruct.setParentLevelType(hierarchyStruct.getLevelType());

                                    appendHierarchyStructToList(tempHierarchyStruct, rootHierarchyStruct, bodyHierarchyStruct, isAppendRootData, executionContext);
                                }
                            } else {
                                if (hierarchyStruct.getRelationColumnID() != null) {
                                    for (DynamicDTO data : dataList) {
                                        HierarchyStruct tempHierarchyStruct = getDynamicDTOToHierarchyStruct(hierarchyStruct, data, executionContext);
                                        // The field mapped by the root node is RelationID
                                        tempHierarchyStruct.setParentColumnID((data.get(hierarchyStruct.getRelationColumnID()) == null) ? null : data.get(hierarchyStruct.getRelationColumnID()).toString());
                                        tempHierarchyStruct.setTempParentColumnID((data.get(hierarchyStruct.getRelationColumnID()) == null) ? null : data.get(hierarchyStruct.getRelationColumnID()).toString());
                                        tempHierarchyStruct.setParentLevelType(hierarchyStruct.getParentLevelType());

                                        appendHierarchyStructToList(tempHierarchyStruct, rootHierarchyStruct, bodyHierarchyStruct, isAppendRootData, executionContext);
                                    }
                                } else {
                                    for (DynamicDTO data : dataList) {
                                        HierarchyStruct tempHierarchyStruct = getDynamicDTOToHierarchyStruct(hierarchyStruct, data, executionContext);

                                        appendHierarchyStructToList(tempHierarchyStruct, rootHierarchyStruct, bodyHierarchyStruct, isAppendRootData, executionContext);
                                    }
                                }
                                isAppendRootData = false;
                            }
                        }
                    }
                }
                hierarchyResultList = getHierarchyTree(rootHierarchyStruct, bodyHierarchyStruct, executionContext);
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return hierarchyResultList;
    }

    /**
     * This method is to append data to rootHierarchyStruct or bodyHierarchyStruct.
     * Because the assignment operation is used multiple times in the method "convertBODataToHierarchyStruct",
     * Therefore, implement its common method
     *
     * @param tempHierarchyStruct: HierarchyStruct data to be inserted into rootHierarchyStruct or bodyHierarchyStruct
     * @param rootHierarchyStruct: Root node data collection
     * @param bodyHierarchyStruct: Child node data collection
     * @param isAppendRootData:    Need to insert rootHierarchyStruct or bodyHierarchyStruct, true insert rootHierarchyStruct, false insert bodyHierarchyStruct.
     * @param executionContext:
     */
    private static void appendHierarchyStructToList(HierarchyStruct tempHierarchyStruct, List<HierarchyStruct> rootHierarchyStruct, List<HierarchyStruct> bodyHierarchyStruct, boolean isAppendRootData, ExecutionContext executionContext) {
        try {
            if (isAppendRootData) {
                rootHierarchyStruct.add(tempHierarchyStruct);
            } else {
                bodyHierarchyStruct.add(tempHierarchyStruct);
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
    }

    /**
     * This method instantiates and assigns a new HierarchyStruct.
     * Because the assignment operation is used multiple times in the method "convertBODataToHierarchyStruct",
     * Therefore, implement its common method
     *
     * @param hierarchyStruct:  HierarchyStruct Data
     * @param data:             LevelType Data
     * @param executionContext: Log information recordS
     * @return: get HierarchyStruct data
     */
    private static HierarchyStruct getDynamicDTOToHierarchyStruct(HierarchyStruct hierarchyStruct, DynamicDTO data, ExecutionContext executionContext) {
        HierarchyStruct tempHierarchyStruct = new HierarchyStruct();
        try {
            tempHierarchyStruct.setLevelType(hierarchyStruct.getLevelType());
            tempHierarchyStruct.setBoName(hierarchyStruct.getBoName());
            tempHierarchyStruct.setColumnID((data.get(hierarchyStruct.getColumnID()) == null) ? null : data.get(hierarchyStruct.getColumnID()).toString());
            tempHierarchyStruct.setColumnName((data.get(hierarchyStruct.getColumnName()) == null) ? null : data.get(hierarchyStruct.getColumnName()).toString());
            tempHierarchyStruct.setHierarchyHide(hierarchyStruct.getHierarchyHide());
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return tempHierarchyStruct;
    }

    /**
     * This method is to get the LevelType table data in the database and return the result
     *
     * @param hierarchyStruct:    The filter conditions of the hierarchy table.
     * @param dynamicReadService:
     * @param executionContext:   Log information recordS
     * @return: get LevelType DynamicDTO data
     */
    private static List<DynamicDTO> getLevelTypeData(HierarchyStruct hierarchyStruct, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        List<DynamicDTO> dataList = null;
        try {
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(hierarchyStruct.getBoName(),
                    executionContext);
            businessObjectInfo.setRecordsPerPage(ConfigProperties.getInstance().max_records_to_read.getIntegerValue(),executionContext);
            businessObjectInfo.addColumnsToSelect(hierarchyStruct.getColumnID());
            businessObjectInfo.addColumnsToSelect(hierarchyStruct.getColumnName());
            // Add Relation field to be queried
            if (hierarchyStruct.getRelationColumnID() != null) {
                businessObjectInfo.addColumnsToSelect(hierarchyStruct.getRelationColumnID());
                businessObjectInfo.addColumnsToSelect(hierarchyStruct.getRelationColumnName());
            }
            // Add Parent field to be queried
            if (hierarchyStruct.getParentColumnID() != null) {
                businessObjectInfo.addColumnsToSelect(hierarchyStruct.getParentColumnID());
                businessObjectInfo.addColumnsToSelect(hierarchyStruct.getParentColumnName());
            }
            // Add the filter condition to be queried
            if (CollectionUtils.hasValue(hierarchyStruct.getFilters())) {
                for (com.lgc.dspdm.core.common.data.criteria.CriteriaFilter criteriaFilter : hierarchyStruct.getFilters()) {
                    if (criteriaFilter.getOperator() == Operator.JSONB_FIND_LIKE
                            || criteriaFilter.getOperator() == Operator.JSONB_FIND_EXACT) {
                        Object[] values = new Object[criteriaFilter.getValues().length];
                        for (int i = 0; i < criteriaFilter.getValues().length; i++) {
                            values[i] = String.valueOf(criteriaFilter.getValues()[i])
                                    .toLowerCase();
                        }
                        businessObjectInfo.addFilter(criteriaFilter.getColumnName(),
                                criteriaFilter.getOperator(), values);
                    } else {
                        businessObjectInfo.addFilter(criteriaFilter.getColumnName(),
                                criteriaFilter.getOperator(), criteriaFilter.getValues());
                    }
                }
            }
            dataList = dynamicReadService.readSimple(businessObjectInfo, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return dataList;
    }

    /**
     * The role of this method (recursive combination tree):
     * 1. If rootHierarchyStruct.Size()>0 and bodyHierarchyStruct.Size()==0, return rootHierarchyStruct combination data;
     * 2. If rootHierarchyStruct.Size()>0 and bodyHierarchyStruct.Size()>0, then implement the tree recursively
     *
     * @param rootHierarchyStruct: Root node data collection
     * @param bodyHierarchyStruct: Child node data collection
     * @param executionContext:    Log information recordS
     * @return: HierarchyResult tree
     */
    private static List<HierarchyResult> getHierarchyTree
    (List<HierarchyStruct> rootHierarchyStruct, List<HierarchyStruct> bodyHierarchyStruct, ExecutionContext executionContext) {
        List<HierarchyResult> hierarchyResultList = new ArrayList<>();
        try {
            if (rootHierarchyStruct != null && !rootHierarchyStruct.isEmpty()) {
                if (bodyHierarchyStruct != null && !bodyHierarchyStruct.isEmpty()) {
                    for (HierarchyStruct root : rootHierarchyStruct) {
                        HierarchyResult hierarchyResult = new HierarchyResult();
                        hierarchyResult.setBoName(root.getBoName());
                        hierarchyResult.setLevelID(root.getColumnID());
                        hierarchyResult.setLevelName(root.getColumnName());
                        hierarchyResult.setParentLevelID(root.getParentColumnID());
                        hierarchyResult.setLevelType(root.getLevelType());
                        hierarchyResult.setParentLevelType(root.getParentLevelType());
                        recursiveCombinationTree(hierarchyResult, null, bodyHierarchyStruct, executionContext);
                        hierarchyResultList.add(hierarchyResult);
                    }
                } else {
                    for (HierarchyStruct root : rootHierarchyStruct) {
                        HierarchyResult hierarchyResult = new HierarchyResult();
                        hierarchyResult.setBoName(root.getBoName());
                        hierarchyResult.setLevelID(root.getColumnID());
                        hierarchyResult.setLevelName(root.getColumnName());
                        hierarchyResult.setParentLevelID(root.getParentColumnID());
                        hierarchyResult.setLevelType(root.getLevelType());
                        hierarchyResult.setParentLevelType(root.getParentLevelType());
                        hierarchyResultList.add(hierarchyResult);
                    }
                }
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return hierarchyResultList;
    }

    /**
     * This method implements a tree for recursive composition
     *
     * @param hierarchyResult:     Hierarchy data of the parent node
     * @param hideHierarchyResult: Hide Hierarchy data, if the layer Hide is true, the corresponding value is null
     * @param bodyHierarchyStruct: Child node data collection,Data that needs to be anchored to the node
     * @param executionContext:    Log information recordS
     */
    private static void recursiveCombinationTree(HierarchyResult hierarchyResult, HierarchyResult hideHierarchyResult, List<HierarchyStruct> bodyHierarchyStruct, ExecutionContext executionContext) {
        try {
            for (HierarchyStruct body : bodyHierarchyStruct) {
                if (body.getHierarchyHide()) {
                    /*
                    This logic is to hide LevelType data, because BOQuery data is cross-layer data. Hidden data we don’t need to add to the node.
                    For example: Hierarchy [FIELD-COMPLETION] contains level [FIELD; WELL; WELLBORE; WELL COMPLETION];
                    When the incoming level is [FIELD; WELL COMPLETION], level [WELL; WELLBORE] data hiding.
                    Can't skip the middle layer, because layer association is needed
                     */
                    if (hideHierarchyResult != null) {
                        if (body.getParentLevelType().equals(hideHierarchyResult.getLevelType()) && body.getTempParentColumnID().equals(hideHierarchyResult.getLevelID())) {
                            HierarchyResult tempHierarchyResult = new HierarchyResult();
                            tempHierarchyResult.setBoName(body.getBoName());
                            tempHierarchyResult.setLevelID(body.getColumnID());
                            tempHierarchyResult.setLevelName(body.getColumnName());
                            tempHierarchyResult.setParentLevelID(body.getParentColumnID());
                            tempHierarchyResult.setLevelType(body.getLevelType());
                            tempHierarchyResult.setParentLevelType(body.getParentLevelType());
                            recursiveCombinationTree(hierarchyResult, tempHierarchyResult, bodyHierarchyStruct, executionContext);
                        }
                    } else {
                        if (body.getParentLevelType().equals(hierarchyResult.getLevelType()) && body.getTempParentColumnID().equals(hierarchyResult.getLevelID())) {
                            HierarchyResult tempHierarchyResult = new HierarchyResult();
                            tempHierarchyResult.setBoName(body.getBoName());
                            tempHierarchyResult.setLevelID(body.getColumnID());
                            tempHierarchyResult.setLevelName(body.getColumnName());
                            tempHierarchyResult.setParentLevelID(body.getParentColumnID());
                            tempHierarchyResult.setLevelType(body.getLevelType());
                            tempHierarchyResult.setParentLevelType(body.getParentLevelType());
                            recursiveCombinationTree(hierarchyResult, tempHierarchyResult, bodyHierarchyStruct, executionContext);
                        }
                    }
                } else {
                    /*
                    This logic is a combination tree of LevelType data
                     */
                    if (hideHierarchyResult != null) {
                        if (body.getParentLevelType().equals(hideHierarchyResult.getLevelType()) && body.getTempParentColumnID().equals(hideHierarchyResult.getLevelID())) {
                            HierarchyResult tempHierarchyResult = new HierarchyResult();
                            tempHierarchyResult.setBoName(body.getBoName());
                            tempHierarchyResult.setLevelID(body.getColumnID());
                            tempHierarchyResult.setLevelName(body.getColumnName());
                            tempHierarchyResult.setParentLevelID(body.getParentColumnID());
                            tempHierarchyResult.setLevelType(body.getLevelType());
                            tempHierarchyResult.setParentLevelType(body.getParentLevelType());
                            recursiveCombinationTree(tempHierarchyResult, null, bodyHierarchyStruct, executionContext);
                            hierarchyResult.setChildHierarchy(tempHierarchyResult);
                        }
                    } else {
                        if (body.getParentLevelType().equals(hierarchyResult.getLevelType()) && body.getTempParentColumnID().equals(hierarchyResult.getLevelID())) {
                            HierarchyResult tempHierarchyResult = new HierarchyResult();
                            tempHierarchyResult.setBoName(body.getBoName());
                            tempHierarchyResult.setLevelID(body.getColumnID());
                            tempHierarchyResult.setLevelName(body.getColumnName());
                            tempHierarchyResult.setParentLevelID(body.getParentColumnID());
                            tempHierarchyResult.setLevelType(body.getLevelType());
                            tempHierarchyResult.setParentLevelType(body.getParentLevelType());
                            recursiveCombinationTree(tempHierarchyResult, null, bodyHierarchyStruct, executionContext);
                            hierarchyResult.setChildHierarchy(tempHierarchyResult);
                        }
                    }
                }
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
    }
}
