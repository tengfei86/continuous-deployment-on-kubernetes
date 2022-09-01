package com.lgc.dspdm.service.utils.excel;

import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.service.common.dynamic.read.IDynamicReadService;

import java.util.*;
import java.util.stream.Collectors;

import static com.lgc.dspdm.service.utils.excel.ExcelImportUtil.*;

/**
 * Class is specially for name to id conversion or lookup using reference data and using unique constraints.
 * The code in this class was already in the import util class. As this code is complex thats why it is moved to a separate class
 *
 * @author Muhammad Imran Ansari
 * @since 12-Aug-2020
 */
public class NameToIdConversionUtils {

    private static DSPDMLogger logger = new DSPDMLogger(NameToIdConversionUtils.class);

    /**
     * Lookups id fields for each of the name fields coming from excel file if and only if
     * 1. If the name field is registered as a unique column in the unique columns constraints metadata table
     * 2. Or the name field is registered as a reference field in the metadata
     *
     * @param boName                      bo name of the excel sheet data being parsed
     * @param metadataByboAttrName        metadata for the bo name
     * @param colIndexToBoAttrNameMapping key is column index and value is bo attr name
     * @param columnToRowsDataMap         key is bo attr name and value is all rows data for this bo attr name
     * @param readOldValuesFromDB
     * @param dynamicReadService
     * @param executionContext
     * @Author Yuan Tian, Muhammad Imran Ansari
     * @since 11-Nov-2019
     */
    public static void processNameToIdConversion(String boName, Map<String, DynamicDTO> metadataByboAttrName, Map<Integer,
                                                 String> colIndexToBoAttrNameMapping, Map<String, Map<Integer, Object>> columnToRowsDataMap,
                                                 List<DynamicDTO> readOldValuesFromDB, List<String> primaryKeyColumnNames,
                                                 Set<String> idBoAttrNamesAlreadyConverted, Map<Integer, DynamicDTO> invalidRecordsByRowNumber,
                                                 IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        long startTime = System.currentTimeMillis();
        final List<String> boAttrNamesFromExcel = new ArrayList<>(colIndexToBoAttrNameMapping.values());

        // 1. First make sure that any id column should not already exist in excel file
        verifyIdColumnsDoNotAlreadyExist(boName, metadataByboAttrName, boAttrNamesFromExcel, primaryKeyColumnNames, dynamicReadService, executionContext);

        // 2. process name to id conversion for all the reference column
        processIdConversionUsingRelationships(boName, metadataByboAttrName, boAttrNamesFromExcel, columnToRowsDataMap,
                primaryKeyColumnNames, idBoAttrNamesAlreadyConverted, invalidRecordsByRowNumber, dynamicReadService, executionContext);
//        processIdConversionForReferenceColumn(boName, metadataByboAttrName, boAttrNamesFromExcel, columnToRowsDataMap,
//                primaryKeyColumnNames, invalidRecordsByRowNumber, dynamicReadService, executionContext);

        if(columnToRowsDataMap.size() > boAttrNamesFromExcel.size()) {
            for (String newColumn : columnToRowsDataMap.keySet()) {
                if(!boAttrNamesFromExcel.contains((newColumn))){
                    boAttrNamesFromExcel.add(newColumn);
                }
            }
        }
        // 3. process primary key lookup using unique constraints
        if(!idBoAttrNamesAlreadyConverted.containsAll(primaryKeyColumnNames)) {
            processIdConversionUsingUniqueConstraints(boName, metadataByboAttrName, boAttrNamesFromExcel, columnToRowsDataMap,
                    readOldValuesFromDB, primaryKeyColumnNames, idBoAttrNamesAlreadyConverted, invalidRecordsByRowNumber, dynamicReadService, executionContext);
        }
        logger.info("Total time taken by Name to Id Conversion Process for bo name {} : {} millis", boName, (System.currentTimeMillis() - startTime));
    }

    private static void verifyIdColumnsDoNotAlreadyExist(String boName, Map<String, DynamicDTO> metadataByboAttrName, List<String> boAttrNamesFromExcel,
                                                         List<String> primaryKeyColumnNames, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        // 1. check primary key
        for (String pkBoAttrName : primaryKeyColumnNames) {
            if (CollectionUtils.containsIgnoreCase(boAttrNamesFromExcel, pkBoAttrName)) {
                String boAttrDisplayName = (String) metadataByboAttrName.get(pkBoAttrName).get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME);
                throw new DSPDMException("Cannot convert from Name to Id. Identifier '{}' already exists in excel file for business object '{}'",
                        executionContext.getExecutorLocale(), boAttrDisplayName, boName);
            }
        }

        // 2. check foreign key id does not exist already, all foreign keys will be looked up
//        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, executionContext);
//        businessObjectInfo.addColumnsToSelect(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
//        businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.CHILD_BO_NAME, boName);
//        businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, true);
//        // read all relationships
//        businessObjectInfo.setReadAllRecords(true);
//        List<DynamicDTO> relationships = dynamicReadService.readSimple(businessObjectInfo, executionContext);
//        String fkBoAttrName = null;
//        for (DynamicDTO relationshipDTO : relationships) {
//            fkBoAttrName = (String) relationshipDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
//            if (CollectionUtils.containsIgnoreCase(boAttrNamesFromExcel, fkBoAttrName)) {
//                String boAttrDisplayName = (String) metadataByboAttrName.get(fkBoAttrName).get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME);
//                throw new DSPDMException("Cannot convert from Name to Id. Identifier '{}' already exists in excel file for business object '{}'",
//                        executionContext.getExecutorLocale(), boAttrDisplayName, boName);
//            }
//        }
    }

    /**
     * Process name to id conversion only for reference fields
     *
     * @param metadataByboAttrName
     * @param columnToRowsDataMap
     * @param dynamicReadService
     * @param executionContext
     * @Author Yuan Tian, Muhammad Imran Ansari
     * @since 11-Nov-2019
     */
    private static void processIdConversionForReferenceColumn(String boName, Map<String, DynamicDTO> metadataByboAttrName,
                                                              List<String> boAttrNamesFromExcel,
                                                              Map<String, Map<Integer, Object>> columnToRowsDataMap,
                                                              List<String> primaryKeyColumnNames,
                                                              Set<String> idBoAttrNamesAlreadyConverted,
                                                              Map<Integer, DynamicDTO> invalidRecordsByRowNumber,
                                                              IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        // iterate on all metadata and process only reference data
        for (DynamicDTO metadataDTO : metadataByboAttrName.values()) {
            // first check reference indicator
            if (metadataDTO.get(DSPDMConstants.BoAttrName.IS_REFERENCE_IND).equals(true)) {
                String boAttrName = (String) metadataDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                String refBoName = (String) metadataDTO.get(DSPDMConstants.BoAttrName.REFERENCE_BO_NAME);
                String refBoAttrValue = (String) metadataDTO.get(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_VALUE);
                String refBoAttrLabel = (String) metadataDTO.get(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_LABEL);
                String boAttrDisplayName = (String) metadataDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME);
                if ((StringUtils.hasValue(refBoName)) && (StringUtils.hasValue(refBoAttrValue)) && (StringUtils.hasValue(refBoAttrLabel))) {
                    // SKIP REFERENCE ITSELF to same bo name and same attribute like in case of area
                    if ((refBoName.equals(boName)) && (refBoAttrValue.equals(boAttrName))) {
                        continue;
                    } else {
                        // name to id conversion is only possible when refBoAttrValue is different than refBoAttrLabel
                        if (!refBoAttrValue.equals(refBoAttrLabel)) {
                            // now we will lookup the value of this reference column boAttrName
                            // first make sure this reference column which we are going to lookup does not already exist in the excel file
                            if (!(CollectionUtils.containsIgnoreCase(boAttrNamesFromExcel, boAttrName))) {
                                // now make sure that the label exists in excel file
                                if (CollectionUtils.containsIgnoreCase(boAttrNamesFromExcel, refBoAttrLabel)) {
                                    // now lookup boAttrName corresponding id metadata field
                                    // child id attribute name whose values are to be looked up
                                    String childBoAttrNameForIdToLookup = boAttrName; // Example: WELL_ID (as foreign key from wellbore table)
                                    // child name attribute whose values are to be read from excel file
                                    String childBoAttrNameForNameAttributeToReadFromExcel = boAttrName; // Example: WELL_UWI  (as foreign key from wellbore table)
                                    // child name attribute display name to report an error in case the values read from excel is not found in db for parent business object
                                    String childBoAttrDisplayNameForNameAttributeToReportError = boAttrDisplayName; // Example: WELL UWI
                                    String parentBoName = refBoName;
                                    // parent id attribute name whose values are to be read from database for parent table
                                    String parentBoAttrNameForIdAttribute = refBoAttrValue; // Example: WELL_ID  (as primary key from well table)
                                    // parent name attribute name whose values are same as child name attribute and for these values we will read parent id values.
                                    String parentBoAttrNameForNameAttribute = refBoAttrLabel; // Example: UWI  (as unique key from well table)
                                    // call the service
                                    if(idBoAttrNamesAlreadyConverted.contains(childBoAttrNameForIdToLookup)) {
                                        continue;
                                    }
                                    injectParentIdUsingSingleParentNameAttribute(boName, childBoAttrNameForIdToLookup,
                                            childBoAttrNameForNameAttributeToReadFromExcel, childBoAttrDisplayNameForNameAttributeToReportError,
                                            parentBoName, parentBoAttrNameForIdAttribute, parentBoAttrNameForNameAttribute, columnToRowsDataMap,
                                            primaryKeyColumnNames, idBoAttrNamesAlreadyConverted, invalidRecordsByRowNumber, dynamicReadService, executionContext);

                                } else if (refBoAttrLabel.contains(",")) {
                                    // composite reference column case
                                    String[] refBoAttrLabels = refBoAttrLabel.split(",");
                                    List<String> uniqueBoAttrNames = new ArrayList<>(refBoAttrLabels.length);
                                    for (String uniqueBoAttrName : refBoAttrLabels) {
                                        uniqueBoAttrNames.add(uniqueBoAttrName.trim());
                                    }
                                    // now read data from the db using this composite reference columns.
                                    // consider combination of this composite reference columns as composite unique constraint
                                    // process name to id conversion using composite for reference bo names and its columns
                                    Map<String, DynamicDTO> referenceBoMetadataMap = dynamicReadService.readMetadataMapForBOName(refBoName, executionContext);
                                    // create reference columns names to lookup
                                    List<String> referenceBoPrimaryKeyColumnNames = Arrays.asList(refBoAttrValue);
                                    // call the business
                                    String currentBoAttrDisplayNameBeingLookedUp = (String) metadataDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME);
                                    Map<String, String> parentPKToChildPKBoAttrNameMap = null;
                                    if (!refBoAttrValue.equals(boAttrName)) {
                                        // it means that foreign key name in child table is different than the primary key name in the parent table.
                                        // we are in child table therefore we need to change the foreign key name according to the child table.
                                        parentPKToChildPKBoAttrNameMap = new LinkedHashMap<>(1);
                                        parentPKToChildPKBoAttrNameMap.put(refBoAttrValue, boAttrName);
                                    }
                                    lookupForeignKeyFromCompositeUniqueConstraint(boName, parentPKToChildPKBoAttrNameMap,
                                            Arrays.asList(currentBoAttrDisplayNameBeingLookedUp), refBoName,
                                            referenceBoMetadataMap, uniqueBoAttrNames, columnToRowsDataMap,
                                            null, primaryKeyColumnNames, referenceBoPrimaryKeyColumnNames,
                                            idBoAttrNamesAlreadyConverted, invalidRecordsByRowNumber, dynamicReadService, executionContext);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * This method injects the id values after lookup into the given excel sheet data
     *
     * @param boName                                              current or child bo name
     * @param childBoAttrNameForIdToLookup
     * @param childBoAttrNameForNameAttributeToReadFromExcel
     * @param childBoAttrDisplayNameForNameAttributeToReportError
     * @param parentBoName
     * @param parentBoAttrNameForIdAttribute
     * @param parentBoAttrNameForNameAttribute
     * @param columnToRowsDataMap                                 excel sheet data so that id column can be added
     * @param primaryKeyColumnNames
     * @param idBoAttrNamesAlreadyConverted
     * @param invalidRecordsByRowNumber
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Imran Ansari
     */
    private static void injectParentIdUsingSingleParentNameAttribute(String boName, String childBoAttrNameForIdToLookup,
                                                                     String childBoAttrNameForNameAttributeToReadFromExcel,
                                                                     String childBoAttrDisplayNameForNameAttributeToReportError,
                                                                     String parentBoName,
                                                                     String parentBoAttrNameForIdAttribute,
                                                                     String parentBoAttrNameForNameAttribute,
                                                                     Map<String, Map<Integer, Object>> columnToRowsDataMap,
                                                                     List<String> primaryKeyColumnNames,
                                                                     Set<String> idBoAttrNamesAlreadyConverted,
                                                                     Map<Integer, DynamicDTO> invalidRecordsByRowNumber,
                                                                     IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        // continue reference field id lookup calculations
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(parentBoName, executionContext);
        businessObjectInfo.addColumnsToSelect(parentBoAttrNameForIdAttribute, parentBoAttrNameForNameAttribute);

        // must put read all records to true. otherwise only one page (20) records will be read
        businessObjectInfo.setReadAllRecords(true);
        // for current boAttrName/column row index is key and cell value is the value after read
        Map<Integer, Object> rowIndexToCellValueMappingForCurrentBoAttrNameForName = columnToRowsDataMap.get(childBoAttrNameForNameAttributeToReadFromExcel);
        // make sure that the column exists in excel file
        if (rowIndexToCellValueMappingForCurrentBoAttrNameForName != null) {
            // use hash set to put only unique values in the SQL IN clause
            Object[] uniqueReferenceNames = new LinkedHashSet<Object>(rowIndexToCellValueMappingForCurrentBoAttrNameForName.values()).toArray();
            List<DynamicDTO> referenceBOList = null;
            if (CollectionUtils.hasValue(uniqueReferenceNames)) {
                if ((uniqueReferenceNames.length == 1) && (uniqueReferenceNames[0] == null)) {
                    // it means that whole column is empty in excel and all reference names are null and this one null value came through unique set creation
                    // do not issue an sql and do not read data.
                    // this is to fix this SQL :  WHERE (WELL_TYPE IN (null))
                    referenceBOList = new ArrayList<>(0);
                } else {
                    // split array in case it is greater than 256 entries
                    referenceBOList = dynamicReadService.readLongRangeUsingInClause(businessObjectInfo, null,
                            parentBoAttrNameForNameAttribute, uniqueReferenceNames, Operator.IN, executionContext);
                }
            } else {
                referenceBOList = new ArrayList<>(0);
            }
            // reference data has been read now
            Map<Object, Object> refNameToIdMap = new LinkedHashMap<>(referenceBOList.size());
            referenceBOList.forEach(refDynamicDTO -> {
                refNameToIdMap.put(refDynamicDTO.get(parentBoAttrNameForNameAttribute), refDynamicDTO.get(parentBoAttrNameForIdAttribute));
            });
            Map<Integer, Object> rowIdToLookupIdMap = new LinkedHashMap<>();
            for (Map.Entry<Integer, Object> rowIndexToCellValueEntry : rowIndexToCellValueMappingForCurrentBoAttrNameForName.entrySet()) {
                if (rowIndexToCellValueEntry.getValue() != null) {
                    Object refName = rowIndexToCellValueEntry.getValue();
                    Object refId = refNameToIdMap.get(refName);
                    if (refId == null) {
                        addInvalidRecordForReferenceIdNotFound(boName, childBoAttrNameForNameAttributeToReadFromExcel, childBoAttrDisplayNameForNameAttributeToReportError, refName, parentBoName, parentBoAttrNameForNameAttribute, rowIndexToCellValueEntry.getKey(), primaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
                    }
                    // must clear or overwrite existing value
                    rowIdToLookupIdMap.put(rowIndexToCellValueEntry.getKey(), refId);
                }
            }
            // inject all rows map for this newly added column
            columnToRowsDataMap.put(childBoAttrNameForIdToLookup, rowIdToLookupIdMap);
            // add to already converted so that we do not convert it again
            idBoAttrNamesAlreadyConverted.add(childBoAttrNameForIdToLookup);
        }
    }

    /**
     * main method to id conversion on the basis of unique constraints either simple or composite
     *
     * @param boName
     * @param metadataByboAttrName
     * @param boAttrNamesFromExcel
     * @param columnToRowsDataMap
     * @param readOldValuesFromDB
     * @param primaryKeyColumnNames
     * @param idBoAttrNamesAlreadyConverted
     * @param invalidRecordsByRowNumber
     * @param dynamicReadService
     * @param executionContext
     */
    private static void processIdConversionUsingUniqueConstraints(String boName, Map<String, DynamicDTO> metadataByboAttrName,
                                                                  List<String> boAttrNamesFromExcel,
                                                                  Map<String, Map<Integer, Object>> columnToRowsDataMap,
                                                                  List<DynamicDTO> readOldValuesFromDB,
                                                                  List<String> primaryKeyColumnNames,
                                                                  Set<String> idBoAttrNamesAlreadyConverted,
                                                                  Map<Integer, DynamicDTO> invalidRecordsByRowNumber,
                                                                  IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        // process primary key lookup using unique constraints
        boolean uniqueConstraintsBasedIdLookupDone = false;
        List<DynamicDTO> metadataConstraintsForBOName = dynamicReadService.readMetadataConstraintsForBOName(boName, executionContext);

        if (CollectionUtils.hasValue(metadataConstraintsForBOName)) {
            Map<String, List<DynamicDTO>> mapConstraintsDTO = metadataConstraintsForBOName.stream().collect(
                    Collectors.groupingBy(
                            dynamicDTO -> (String) dynamicDTO.get(DSPDMConstants.BoAttrName.CONSTRAINT_NAME)));
            // now start processing simple (one column based) unique constraints first
            for (Map.Entry<String, List<DynamicDTO>> entry : mapConstraintsDTO.entrySet()) {
                List<DynamicDTO> uniqueConstraints = entry.getValue();
                if (uniqueConstraints.size() == 1) {
                    // first proceed with simple unique constraints
                    DynamicDTO constraintsDTO = uniqueConstraints.get(0);
                    String boAttrNameFromUniqueConstraints = (String) constraintsDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                    // compare with ignore case, that bo attr name exists in excel file
                    if (CollectionUtils.containsIgnoreCase(boAttrNamesFromExcel, boAttrNameFromUniqueConstraints)) {
                        lookupPrimaryKeyFromSimpleUniqueConstraint(boName, boAttrNameFromUniqueConstraints,
                                metadataByboAttrName, columnToRowsDataMap, readOldValuesFromDB, primaryKeyColumnNames,
                                idBoAttrNamesAlreadyConverted, invalidRecordsByRowNumber, dynamicReadService, executionContext);
                        // do not do again unique column processing for other unique columns
                        uniqueConstraintsBasedIdLookupDone = true;
                        break;
                    }
                }
            }
            // continue with composite unique constraints lookup if simple did not work
            if (!uniqueConstraintsBasedIdLookupDone) {
                // simple unique constraint is not found. Now Try to lookup id using composite unique constraint
                for (Map.Entry<String, List<DynamicDTO>> entry : mapConstraintsDTO.entrySet()) {
                    List<DynamicDTO> uniqueConstraints = entry.getValue();
                    if (uniqueConstraints.size() > 1) {
                        // now proceed with composite unique constraints only
                        List<String> boAttrNamesFromUniqueConstraints = CollectionUtils.getStringValuesFromList(uniqueConstraints, DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                        // now first verify that all the attributes mentioned in unique constraints are coming from excel file and no one is missing
                        boolean allUniqueConstraintColumnsFoundInExcel = true;
                        for (String boAttrNameFromUniqueConstraints : boAttrNamesFromUniqueConstraints) {
                            if (!(CollectionUtils.containsIgnoreCase(boAttrNamesFromExcel, boAttrNameFromUniqueConstraints))) {
                                // one unique constraint column is missing in excel therefore we cannot conclude id by using this unique constraint
                                // this composite unique constraint will not work because of of its attribute does not exist in excel
                                // continue for next composite unique constraint in the list
                                allUniqueConstraintColumnsFoundInExcel = false;
                            }
                        }
                        if (allUniqueConstraintColumnsFoundInExcel) {
                            // all the columns mentioned in the unique constraint metadata definition are in excel file so we can proceed further
                            lookupPrimaryKeyFromCompositeUniqueConstraint(
                                    boName, metadataByboAttrName, boAttrNamesFromUniqueConstraints, columnToRowsDataMap,
                                    readOldValuesFromDB, primaryKeyColumnNames, idBoAttrNamesAlreadyConverted,
                                    invalidRecordsByRowNumber, dynamicReadService, executionContext);
                            break;
                        } else {
                            logger.info("Name to id conversion is not possible using unique constraints because all the columns mentioned in unique constraint does not exist in excel file");
                        }
                    }
                }
            }
        }
    }

    /**
     * Process name to id conversion only for primary key lookup using single unique constraint
     *
     * @param boName
     * @param uniqueBoAttrName
     * @param metadataByboAttrName
     * @param columnToRowsDataMap
     * @param readOldValuesFromDB
     * @param primaryKeyColumnNames
     * @param invalidRecordsByRowNumber
     * @param dynamicReadService
     * @param executionContext
     */
    private static void lookupPrimaryKeyFromSimpleUniqueConstraint(String boName, String uniqueBoAttrName,
                                                                   Map<String, DynamicDTO> metadataByboAttrName,
                                                                   Map<String, Map<Integer, Object>> columnToRowsDataMap,
                                                                   List<DynamicDTO> readOldValuesFromDB,
                                                                   List<String> primaryKeyColumnNames,
                                                                   Set<String> idBoAttrNamesAlreadyConverted,
                                                                   Map<Integer, DynamicDTO> invalidRecordsByRowNumber,
                                                                   IDynamicReadService dynamicReadService,
                                                                   ExecutionContext executionContext) {
        String uniqueBoAttrDisplayName = (String) metadataByboAttrName.get(uniqueBoAttrName).get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME);

        processIdConversionFromSingleUniqueColumn(true, boName, uniqueBoAttrName,
                uniqueBoAttrDisplayName, metadataByboAttrName, columnToRowsDataMap, readOldValuesFromDB, primaryKeyColumnNames,
                idBoAttrNamesAlreadyConverted, invalidRecordsByRowNumber, dynamicReadService, executionContext);
    }

    /**
     * Process name to id conversion only for primary key lookup.
     *
     * @param boName
     * @param metadataMap
     * @param uniqueBoAttrNames
     * @param columnToRowsDataMap
     * @param readOldValuesFromDB
     * @param primaryKeyColumnNames
     * @param idBoAttrNamesAlreadyConverted
     * @param invalidRecordsByRowNumber
     * @param dynamicReadService
     * @param executionContext
     */
    private static void lookupPrimaryKeyFromCompositeUniqueConstraint(String boName, Map<String, DynamicDTO> metadataMap,
                                                                      List<String> uniqueBoAttrNames,
                                                                      Map<String, Map<Integer, Object>> columnToRowsDataMap,
                                                                      List<DynamicDTO> readOldValuesFromDB,
                                                                      List<String> primaryKeyColumnNames,
                                                                      Set<String> idBoAttrNamesAlreadyConverted,
                                                                      Map<Integer, DynamicDTO> invalidRecordsByRowNumber,
                                                                      IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        List<String> uniqueBoAttrDisplayNames = new ArrayList<>(uniqueBoAttrNames.size());
        for (String uniqueBoAttrName : uniqueBoAttrNames) {
            uniqueBoAttrDisplayNames.add((String) metadataMap.get(uniqueBoAttrName).get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME));
        }
        processIdConversionFromCompositeUniqueConstraint(
                true, boName, null, primaryKeyColumnNames, boName, metadataMap,
                uniqueBoAttrNames, uniqueBoAttrDisplayNames, columnToRowsDataMap,
                readOldValuesFromDB, primaryKeyColumnNames, primaryKeyColumnNames,
                idBoAttrNamesAlreadyConverted, invalidRecordsByRowNumber, dynamicReadService, executionContext);
    }

    /**
     * Process name to id conversion only for foreign key lookup.
     *
     * @param currentBoName
     * @param referenceBoName
     * @param referenceBoMetadataMap
     * @param uniqueReferenceBoAttrNames
     * @param columnToRowsDataMap
     * @param readOldValuesFromDB
     * @param currentBoPrimaryKeyColumnNames
     * @param referenceBoPrimaryKeyColumnNames
     * @param idBoAttrNamesAlreadyConverted
     * @param invalidRecordsByRowNumber
     * @param dynamicReadService
     * @param executionContext
     */
    private static void lookupForeignKeyFromCompositeUniqueConstraint(String currentBoName, Map<String, String> parentPKToChildPKBoAttrNameMap,
                                                                      List<String> currentBoAttrDisplayNamesBeingLookedUp,
                                                                      String referenceBoName,
                                                                      Map<String, DynamicDTO> referenceBoMetadataMap,
                                                                      List<String> uniqueReferenceBoAttrNames,
                                                                      Map<String, Map<Integer, Object>> columnToRowsDataMap,
                                                                      List<DynamicDTO> readOldValuesFromDB,
                                                                      List<String> currentBoPrimaryKeyColumnNames,
                                                                      List<String> referenceBoPrimaryKeyColumnNames,
                                                                      Set<String> idBoAttrNamesAlreadyConverted,
                                                                      Map<Integer, DynamicDTO> invalidRecordsByRowNumber,
                                                                      IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        List<String> uniqueReferenceBoAttrDisplayNames = new ArrayList<>(uniqueReferenceBoAttrNames.size());
        for (String uniqueBoAttrName : uniqueReferenceBoAttrNames) {
            uniqueReferenceBoAttrDisplayNames.add((String) referenceBoMetadataMap.get(uniqueBoAttrName).get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME));
        }
        // call the business
        processIdConversionFromCompositeUniqueConstraint(
                false, currentBoName, parentPKToChildPKBoAttrNameMap,
                currentBoAttrDisplayNamesBeingLookedUp, referenceBoName, referenceBoMetadataMap,
                uniqueReferenceBoAttrNames, uniqueReferenceBoAttrDisplayNames, columnToRowsDataMap,
                readOldValuesFromDB, currentBoPrimaryKeyColumnNames, referenceBoPrimaryKeyColumnNames,
                idBoAttrNamesAlreadyConverted, invalidRecordsByRowNumber, dynamicReadService, executionContext);
    }

    /**
     * Process name to id conversion only for columns defined as a simple unique in the unique constraints metadata table
     *
     * @param isPrimaryKeyLookupProcess
     * @param boName
     * @param boAttrName
     * @param boAttrDisplayName
     * @param metadataByboAttrName
     * @param columnToRowsDataMap
     * @param readOldValuesFromDB
     * @param primaryKeyColumnNames
     * @param idBoAttrNamesAlreadyConverted
     * @param invalidRecordsByRowNumber
     * @param dynamicReadService
     * @param executionContext
     */
    private static void processIdConversionFromSingleUniqueColumn(boolean isPrimaryKeyLookupProcess,
                                                                  String boName,
                                                                  String boAttrName,
                                                                  String boAttrDisplayName,
                                                                  Map<String, DynamicDTO> metadataByboAttrName,
                                                                  Map<String, Map<Integer, Object>> columnToRowsDataMap,
                                                                  List<DynamicDTO> readOldValuesFromDB,
                                                                  List<String> primaryKeyColumnNames,
                                                                  Set<String> idBoAttrNamesAlreadyConverted,
                                                                  Map<Integer, DynamicDTO> invalidRecordsByRowNumber,
                                                                  IDynamicReadService dynamicReadService,
                                                                  ExecutionContext executionContext) {
        if (idBoAttrNamesAlreadyConverted.containsAll(primaryKeyColumnNames)) {
            //pk already converted using forieng key relationships no need to convert again using unique constraints
            return;
        }
        // current column data for all rows with row index as key and value is the unique column values
        Map<Integer, Object> rowIndexToCellValueMappingForCurrentBoAttr = columnToRowsDataMap.get(boAttrName);
        // now start reading primary key against unique column name
        // create business object info
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(boName, executionContext);
        List<String> columnsToRead = new ArrayList<>(2);
        // add unique column
        columnsToRead.add(boAttrName);

        for (String pkBoAttrName : primaryKeyColumnNames) {
            // add all pk columns to read from db list
            columnsToRead.add(pkBoAttrName);
            if ((isPrimaryKeyLookupProcess) && (columnToRowsDataMap.containsKey(pkBoAttrName)) && (!(idBoAttrNamesAlreadyConverted.contains(pkBoAttrName)))) {
                // throw error if this is a primary key lookup process not the foreign key lookup process
                String pkBoAttrNameDisplayName = (String) metadataByboAttrName.get(pkBoAttrName).get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME);
                throw new DSPDMException("Cannot convert from Name to Id using simple unique attributes '{}'. Identifier '{}' already exists in excel file for business object '{}'",
                        executionContext.getExecutorLocale(), boAttrDisplayName, pkBoAttrNameDisplayName, boName);
            }
        }
        if (readOldValuesFromDB == null) {
            // old values are not required then only read few columns otherwise read whole object
            businessObjectInfo.addColumnsToSelect(columnsToRead);
        }
        // use hash set to put only unique values in the SQL IN clause
        Object[] uniqueValuesForUniqueColumn = new LinkedHashSet<Object>(rowIndexToCellValueMappingForCurrentBoAttr.values()).toArray();
        // split array in case it is greater than 256 entries
        List<DynamicDTO> uniqueBOList = dynamicReadService.readLongRangeUsingInClause(businessObjectInfo, null,
                boAttrName, uniqueValuesForUniqueColumn, Operator.IN, executionContext);

        if (readOldValuesFromDB != null) {
            // if old values are asked then add all old values which are read from database to the list
            readOldValuesFromDB.addAll(uniqueBOList);
        }

        int index = 0;
        for (String pkBoAttrName : primaryKeyColumnNames) {
            // now create a map which maps unique column value to primary key values
            Map<String, Object> uniqueColumnToIdColumnsMap = new LinkedHashMap<>();
            uniqueBOList.forEach(wellDynamicDTO -> {
                uniqueColumnToIdColumnsMap.put((String) wellDynamicDTO.get(boAttrName), wellDynamicDTO.get(pkBoAttrName));
            });

            // current column data for all rows with row index as key and value is id after lookup
            Map<Integer, Object> rowIdToUniqueFieldIdAfterLookup = new LinkedHashMap<>();
            for (Map.Entry<Integer, Object> rowIndexToCellValueEntry : rowIndexToCellValueMappingForCurrentBoAttr.entrySet()) {
                if (rowIndexToCellValueEntry.getValue() != null) {
                    Object uniqueColumnValue = rowIndexToCellValueEntry.getValue();
                    if (uniqueColumnValue != null) {
                        if (uniqueColumnValue instanceof String) {
                            if (StringUtils.isNullOrEmpty((String) uniqueColumnValue)) {
                                continue;
                            }
                        }
                        Object refNameFieldIdAfterLookup = uniqueColumnToIdColumnsMap.get(uniqueColumnValue);

                        if ((refNameFieldIdAfterLookup == null) && (!(isPrimaryKeyLookupProcess))) {
                            // if ref label found but ref value not found then invalid record due to invalid ref label
                            addInvalidRecordForUniqueIdNotFound(boName, boAttrName, boAttrDisplayName,
                                    uniqueColumnValue, index, primaryKeyColumnNames,
                                    invalidRecordsByRowNumber, executionContext);
                        }
                        // must clear or overwrite existing value
                        // in any case null or not add pk value to the record
                        rowIdToUniqueFieldIdAfterLookup.put(rowIndexToCellValueEntry.getKey(), refNameFieldIdAfterLookup);
                    }
                }
            }
            // put the lookup id map into the main map with the new bo attr name of id value
            columnToRowsDataMap.put(pkBoAttrName, rowIdToUniqueFieldIdAfterLookup);
            // add to already converted
            idBoAttrNamesAlreadyConverted.add(pkBoAttrName);
        }
    }

    /**
     * Process name to id conversion only for columns defined as a composite unique constraints in the unique constraints metadata table
     *
     * @param isPrimaryKeyLookupProcess
     * @param currentBoName
     * @param referenceBoName
     * @param referenceBoMetadataMap
     * @param uniqueReferenceBoAttrNames
     * @param uniqueReferenceBoAttrDisplayNames
     * @param columnToRowsDataMap
     * @param readOldValuesFromDB
     * @param currentBoPrimaryKeyColumnNames
     * @param referenceBoPrimaryKeyColumnNames
     * @param idBoAttrNamesAlreadyConverted
     * @param invalidRecordsByRowNumber
     * @param dynamicReadService
     * @param executionContext
     */
    private static void processIdConversionFromCompositeUniqueConstraint(boolean isPrimaryKeyLookupProcess, String currentBoName,
                                                                         Map<String, String> parentPKToChildPKBoAttrNameMap,
                                                                         List<String> currentBoAttrDisplayNamesBeingLookedUp,
                                                                         String referenceBoName,
                                                                         Map<String, DynamicDTO> referenceBoMetadataMap,
                                                                         List<String> uniqueReferenceBoAttrNames,
                                                                         List<String> uniqueReferenceBoAttrDisplayNames,
                                                                         Map<String, Map<Integer, Object>> columnToRowsDataMap,
                                                                         List<DynamicDTO> readOldValuesFromDB,
                                                                         List<String> currentBoPrimaryKeyColumnNames,
                                                                         List<String> referenceBoPrimaryKeyColumnNames,
                                                                         Set<String> idBoAttrNamesAlreadyConverted,
                                                                         Map<Integer, DynamicDTO> invalidRecordsByRowNumber,
                                                                         IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        if (idBoAttrNamesAlreadyConverted.containsAll(referenceBoPrimaryKeyColumnNames)) {
            // no need to lookup. lookup already done by another bo
            return;
        }
        // now start reading primary key of the reference table against unique column name
        // create business object info
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(referenceBoName, executionContext);
        List<String> columnsToRead = new ArrayList<>(2);
        // add unique column
        columnsToRead.addAll(uniqueReferenceBoAttrNames);
        for (String referenceBoPkBoAttrName : referenceBoPrimaryKeyColumnNames) {
            // add all pk columns to read from db list
            columnsToRead.add(referenceBoPkBoAttrName);
            if ((isPrimaryKeyLookupProcess) && (columnToRowsDataMap.containsKey(referenceBoPkBoAttrName)) && (!idBoAttrNamesAlreadyConverted.contains(referenceBoPkBoAttrName))) {
                // throw error if this is a primary key lookup process not the foreign key lookup process
                String attributeDisplayName = (String) referenceBoMetadataMap.get(referenceBoPkBoAttrName).get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME);
                throw new DSPDMException("Cannot convert from Name to Id using composite unique attributes '{}'. Identifier '{}' already exists in excel file for business object '{}'",
                        executionContext.getExecutorLocale(), CollectionUtils.getCommaSeparated(uniqueReferenceBoAttrDisplayNames), attributeDisplayName, currentBoName);
            }
        }
        if (readOldValuesFromDB == null) {
            // old values are not required then only read few columns otherwise read whole object
            businessObjectInfo.addColumnsToSelect(columnsToRead);
        }
        // calculate the total number of rows in excel using first column size
        //int totalRowsInExcel = columnToRowsDataMap.entrySet().iterator().next().getValue().size();
        Set<Integer> excelDataRowIndexes = columnToRowsDataMap.entrySet().iterator().next().getValue().keySet();
        // below is the hash map with the key comma separated values of unique bo attr names coming from unique constraints which are also columns in excel file and the value
        // of this map is a map/dynamic dto which contains the values for this unique boAttrName for a column in excel
        Map<String, DynamicDTO> uniqueReferenceBoAttrNamesAndTheirValuesFromExcelMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        // row index is starting from one excluding header row at index zero
        for (Integer index : excelDataRowIndexes) {
            // create reference bo name dynamic dto to hold unique column names and their values to send to the db to read primary key
            DynamicDTO dynamicDTO = new DynamicDTO(referenceBoName, (isPrimaryKeyLookupProcess) ? referenceBoPrimaryKeyColumnNames : null, executionContext);
            List<Object> currentRowUniqueValues = new ArrayList<>(uniqueReferenceBoAttrNames.size());
            Object uniqueValue = null;
            for (String referenceBoUniqueBoAttrName : uniqueReferenceBoAttrNames) {
                // read composite unique constraint one boAttrName value for current row. It is a cell value in excel
                uniqueValue = columnToRowsDataMap.get(referenceBoUniqueBoAttrName).get(index);
                // include null values. do not skip. in network connection business object scenario id is null
//                if ((uniqueValue == null) || ((uniqueValue instanceof String) && (((String) uniqueValue).trim().length() == 0))) {
//                    // skip the unique value check for null or empty values
//                    currentRowUniqueValues = null;
//                    break;
//                }
                // put in map to send to db to read pk on its basis
                dynamicDTO.put(referenceBoUniqueBoAttrName, uniqueValue);
                // add to list to create a unique comma separated key to put in map to extract the dto at the time of read
                currentRowUniqueValues.add(uniqueValue);
            }
            // now create a comma separated key from all the boAttrNames in the composite unique constraint
            // overwrite existing if same unique values are found for multiple rows of excel file.
            // Overwrite existing will eliminate the cost of reading duplicate records
            if (CollectionUtils.hasValue(currentRowUniqueValues)) {
                uniqueReferenceBoAttrNamesAndTheirValuesFromExcelMap.put(CollectionUtils.getCommaSeparated(currentRowUniqueValues), dynamicDTO);
            }
        }
        // Read the records from the data store using unique values combination.
        // split array in case it is greater than 256 entries
        List<DynamicDTO> uniqueReferenceBOList = dynamicReadService.readLongRangeUsingCompositeORClause(businessObjectInfo,
                null, uniqueReferenceBoAttrNames, uniqueReferenceBoAttrNamesAndTheirValuesFromExcelMap.values(), executionContext);

        if (readOldValuesFromDB != null) {
            // if old values are asked then add all old values which are read from database to the list
            readOldValuesFromDB.addAll(uniqueReferenceBOList);
        }
        // now convert the list of records which have been read from data store to the map on the basis of composite unique boAttrNames comma separated key
        // perform this for all primary key attributes. Normally it is only one and not a composite primary key
        for (String primaryKeyColumnName : referenceBoPrimaryKeyColumnNames) {
            // now create a map which maps composite unique columns comma separated values to primary key values
            Map<String, Object> uniqueColumnsToPKValueMap = new LinkedHashMap<>();
            // iterate on all the records read from data store
            for (DynamicDTO uniqueDynamicDTO : uniqueReferenceBOList) {
                // create an empty list to hold the values of composite unique boAttrNames
                List<Object> uniqueBoAttrNamesValues = new ArrayList<>(uniqueReferenceBoAttrNames.size());
                Object uniqueValue = null;
                // now iterate over unique boAttrNames to make a unique comma separated key
                for (String uniqueBoAttrName : uniqueReferenceBoAttrNames) {
                    // read composite unique constraint one boAttrName value for current row. It is a cell value in excel
                    uniqueValue = uniqueDynamicDTO.get(uniqueBoAttrName);
                    // include null values. do not skip. in network connection business object scenario id is null
//                    if ((uniqueValue == null) || ((uniqueValue instanceof String) && (((String) uniqueValue).trim().length() == 0))) {
//                        // skip the unique value check for null or empty values. No need to add null or empty unique values
//                        uniqueBoAttrNamesValues = null;
//                        break;
//                    }
                    // add to list to create a unique comma separated key
                    uniqueBoAttrNamesValues.add(uniqueValue);
                }
                if (CollectionUtils.hasValue(uniqueBoAttrNamesValues)) {
                    // if unique values are found for all unique columns then get primary key value and put in map
                    uniqueColumnsToPKValueMap.put(CollectionUtils.getCommaSeparated(uniqueBoAttrNamesValues), uniqueDynamicDTO.get(primaryKeyColumnName));
                }
            }

            // current column data for all rows with row index as key and value is id after lookup
            Map<Integer, Object> rowIdToPKValueMapAfterLookup = new LinkedHashMap<>();
            // row index is starting from one excluding header row at index zero
            for (Integer index : excelDataRowIndexes) {
                List<Object> currentRowUniqueValues = new ArrayList<>(uniqueReferenceBoAttrNames.size());
                Object uniqueValue = null;
                for (String uniqueBoAttrName : uniqueReferenceBoAttrNames) {
                    // read composite unique constraint one boAttrName value for current row. It is a cell value in excel
                    uniqueValue = columnToRowsDataMap.get(uniqueBoAttrName).get(index);
                    // include null values. do not skip. in network connection business object scenario id is null
//                    if ((uniqueValue == null) || ((uniqueValue instanceof String) && (((String) uniqueValue).trim().length() == 0))) {
//                        // skip the unique value check for null or empty values
//                        // must make it null
//                        currentRowUniqueValues = null;
//                        break;
//                    }
                    // add to list to create a combined unique comma separated key
                    currentRowUniqueValues.add(uniqueValue);
                }
                // now create a comma separated key from all the boAttrNames in the composite unique constraint
                // overwrite existing if same unique values are found for multiple rows of excel file
                if (CollectionUtils.hasValue(currentRowUniqueValues)) {
                    String uniqueCompositeValue = CollectionUtils.getCommaSeparated(currentRowUniqueValues);
                    Object pkValue = uniqueColumnsToPKValueMap.get(uniqueCompositeValue);
                    // if pk value is not found and it is a foreign key lookup process then add to invalid record
                    if ((pkValue == null) && (!(isPrimaryKeyLookupProcess))) {
                        addInvalidRecordForForeignKeyUniqueIdNotFound(currentBoName, currentBoAttrDisplayNamesBeingLookedUp, referenceBoName, uniqueReferenceBoAttrNames, uniqueReferenceBoAttrDisplayNames,
                                currentRowUniqueValues, index, currentBoPrimaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
                    }
                    // must clear or overwrite existing value
                    // in any case null or not add pk value to the record
                    rowIdToPKValueMapAfterLookup.put(index, pkValue);
                }
            }
            // put the lookup id map into the main map with the new bo attr name of id value
            if ((parentPKToChildPKBoAttrNameMap != null) && (parentPKToChildPKBoAttrNameMap.get(primaryKeyColumnName) != null)) {
                columnToRowsDataMap.put(parentPKToChildPKBoAttrNameMap.get(primaryKeyColumnName), rowIdToPKValueMapAfterLookup);
                idBoAttrNamesAlreadyConverted.add(parentPKToChildPKBoAttrNameMap.get(primaryKeyColumnName));
            } else {
                columnToRowsDataMap.put(primaryKeyColumnName, rowIdToPKValueMapAfterLookup);
                idBoAttrNamesAlreadyConverted.add(primaryKeyColumnName);
            }
        }
    }

    private static void processIdConversionUsingRelationships(String boName, Map<String, DynamicDTO> metadataByBoAttrName,
                                                              List<String> boAttrNamesFromExcel, Map<String, Map<Integer, Object>> columnToRowsDataMap,
                                                              List<String> primaryKeyColumnNames, Set<String> idBoAttrNamesAlreadyConverted,
                                                              Map<Integer, DynamicDTO> invalidRecordsByRowNumber,
                                                              IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        // read all parent relationships where current bo is a child
        Map<String, List<DynamicDTO>> parentRelationshipsMap = dynamicReadService.readMetadataParentRelationshipsMap(boName, executionContext);
        for (Map.Entry<String, List<DynamicDTO>> entry : parentRelationshipsMap.entrySet()) {
            String parentBoName = entry.getKey();
            List<DynamicDTO> pkRelationships = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(entry.getValue(), DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, true);
            List<DynamicDTO> nonPKRelationships = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(entry.getValue(), DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, false);
            if ((CollectionUtils.hasValue(pkRelationships)) && (CollectionUtils.hasValue(nonPKRelationships))) {
                List<DynamicDTO> allConstraintsForParentBoName = dynamicReadService.readMetadataConstraintsForBOName(parentBoName, executionContext);
                Map<String, List<DynamicDTO>> parentConstraintsByName = CollectionUtils.groupDynamicDTOByPropertyValueIgnoreCase(allConstraintsForParentBoName, DSPDMConstants.BoAttrName.CONSTRAINT_NAME);

                int idConversionDoneForCurrentParentCount = 0;
                // first try id conversion using a single unique column
                for (List<DynamicDTO> parentConstraints : parentConstraintsByName.values()) {
                    if (parentConstraints.size() == 1) {
                        String parentBoAttrName = (String) parentConstraints.get(0).get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                        List<DynamicDTO> currentBoAttrNameRelationships = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(nonPKRelationships, DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME, parentBoAttrName);
                        if (CollectionUtils.hasValue(currentBoAttrNameRelationships)) {
                            // name non pk attribute picked from single unique parentConstraints is found in the relationship
                            // add both child bo and parent bo also has id based relationship too. There fore lookup can proceed.
                            // By default use first available PK relationship in the PK relationships list
                            DynamicDTO pkRelationshipDTO = pkRelationships.get(0);
                            for (DynamicDTO nonPKRelationshipDTO : currentBoAttrNameRelationships) {
                                if (pkRelationships.size() > 1) {
                                    // here we have more than one pk relationships available for the current parent and child
                                    // therefore we cannot use the first one but we should use the metadata field relatedBoAttrName
                                    // to identify the appropriate pk relationship
                                    // first get the child name attribute whose values are to be read from excel file and for this name we are going to lookup id
                                    String childBoAttrNameNonPK = (String) nonPKRelationshipDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME); // Example: WELL_UWI  (as foreign key from wellbore table)
                                    // now check does this child attribute name has a related attribute name registered in its metadata
                                    String relatedBoAttrName = (String) metadataByBoAttrName.get(childBoAttrNameNonPK).get(DSPDMConstants.BoAttrName.RELATED_BO_ATTR_NAME); // Example: WELL UWI
                                    if (StringUtils.hasValue(relatedBoAttrName)) {
                                        // relatedBoAttrName value is found then we use this value
                                        // check is it a valid attribute name by verifying that the metadata map has it as a key
                                        if (metadataByBoAttrName.containsKey(relatedBoAttrName)) {
                                            // now check that this related bo attr name is a part of pk relationships list
                                            List<DynamicDTO> relatedPKRelationships = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(pkRelationships, DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME, relatedBoAttrName);
                                            if(CollectionUtils.hasValue(relatedPKRelationships)) {
                                                // now replace the default first pk relationship already taken by this new one extracted using the relatedBoAttrName metadata field
                                                pkRelationshipDTO = relatedPKRelationships.get(0);
                                            }
                                        }
                                    }
                                }
                                // child id attribute name whose values are to be looked up
                                String childBoAttrNameForIdToLookup = (String) pkRelationshipDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME); // Example: WELL_ID (as foreign key from wellbore table)
                                // child name attribute whose values are to be read from excel file
                                String childBoAttrNameForNameAttributeToReadFromExcel = (String) nonPKRelationshipDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME); // Example: WELL_UWI  (as foreign key from wellbore table)
                                // child name attribute display name to report an error in case the values read from excel is not found in db for parent business object
                                String childBoAttrDisplayNameForNameAttributeToReportError = (String) metadataByBoAttrName.get(childBoAttrNameForNameAttributeToReadFromExcel).get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME); // Example: WELL UWI
                                // parent id attribute name whose values are to be read from database for parent table
                                String parentBoAttrNameForIdAttribute = (String) pkRelationshipDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME); // Example: WELL_ID  (as primary key from well table)
                                // parent name attribute name whose values are same as child name attribute and for these values we will read parent id values.
                                String parentBoAttrNameForNameAttribute = (String) nonPKRelationshipDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME); // Example: UWI  (as unique key from well table)

                                if(idBoAttrNamesAlreadyConverted.contains(childBoAttrNameForIdToLookup)) {
                                    // already converted by another simple join occured before in join sequence.
                                    idConversionDoneForCurrentParentCount++;
                                    if(idConversionDoneForCurrentParentCount == pkRelationships.size()) {
                                        // all id fields have been converted, no need to continue more
                                        break;
                                    }
                                    continue;
                                }

                                if (columnToRowsDataMap.containsKey(childBoAttrNameForNameAttributeToReadFromExcel)) {
                                    injectParentIdUsingSingleParentNameAttribute(boName, childBoAttrNameForIdToLookup,
                                            childBoAttrNameForNameAttributeToReadFromExcel, childBoAttrDisplayNameForNameAttributeToReportError,
                                            parentBoName, parentBoAttrNameForIdAttribute, parentBoAttrNameForNameAttribute, columnToRowsDataMap,
                                            primaryKeyColumnNames, idBoAttrNamesAlreadyConverted, invalidRecordsByRowNumber,
                                            dynamicReadService, executionContext);
                                    // increment converted id count
                                    idConversionDoneForCurrentParentCount++;
                                    if(idConversionDoneForCurrentParentCount == pkRelationships.size()) {
                                        // all id fields have been converted, no need to continue more
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                if (idConversionDoneForCurrentParentCount == 0) {
                    // id conversion using a single unique column failed now try with composite unique column
                    for (List<DynamicDTO> parentConstraints : parentConstraintsByName.values()) {
                        if (parentConstraints.size() > 1) {
                            boolean allParentUniqueColumnsFoundInChildRelationships = true;
                            for (DynamicDTO parentConstraintDTO : parentConstraints) {
                                String parentBoAttrName = (String) parentConstraintDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                                List<DynamicDTO> currentBoAttrNameRelationships = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(nonPKRelationships, DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME, parentBoAttrName);
                                if (CollectionUtils.isNullOrEmpty(currentBoAttrNameRelationships)) {
                                    allParentUniqueColumnsFoundInChildRelationships = false;
                                    break;
                                }
                            }
                            if (allParentUniqueColumnsFoundInChildRelationships) {
                                // make sure first that the id not already converted by some other simple join occurred first in the list
                                if(idBoAttrNamesAlreadyConverted.containsAll(CollectionUtils.getStringValuesFromList(pkRelationships, DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME))) {
                                    // all child composite id columns have already been converted
                                    continue;
                                }
                                // parent pk to child pk map to read from db with parent pk and inject in excel file with child attr for parent pk
                                Map<String, String> parentPKToChildPKBoAttrNameMap = new LinkedHashMap<>(pkRelationships.size());
                                List<String> parentBoAttrNameForIdAttribute = new ArrayList<>(pkRelationships.size()); // Example: WELL_ID  (as primary key from well table)
                                for (DynamicDTO pkRelationshipDTO : pkRelationships) {
                                    String childPkBoAttrNameToLookup = (String) pkRelationshipDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);

                                        parentPKToChildPKBoAttrNameMap.put((String) pkRelationshipDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME), childPkBoAttrNameToLookup);
                                        parentBoAttrNameForIdAttribute.add((String) pkRelationshipDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME));
                                }

                                // child name attribute whose values are to be read from excel file
                                Set<String> childBoAttrNameForNameAttributeToReadFromExcel = new LinkedHashSet<>();
                                // child name attribute display name to report an error in case the values read from excel is not found in db for parent business object
                                Set<String> childBoAttrDisplayNameForNameAttributeToReportError = new LinkedHashSet<>();
                                // parent name attribute name whose values are same as child name attribute and for these values we will read parent id values.
                                Set<String> parentBoAttrNameForNameAttributeFromUniqueConstraints = new LinkedHashSet<>();
                                for (DynamicDTO parentConstraintDTO : parentConstraints) {
                                    String parentBoAttrName = (String) parentConstraintDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                                    List<DynamicDTO> currentBoAttrNameRelationships = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(nonPKRelationships, DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME, parentBoAttrName);
                                    if (CollectionUtils.isNullOrEmpty(currentBoAttrNameRelationships)) {
                                        allParentUniqueColumnsFoundInChildRelationships = false;
                                        break;
                                    } else {
                                        String childBoAttrName = (String) currentBoAttrNameRelationships.get(0).get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
                                        childBoAttrNameForNameAttributeToReadFromExcel.add(childBoAttrName);
                                        childBoAttrDisplayNameForNameAttributeToReportError.add((String) (metadataByBoAttrName.get(childBoAttrName)).get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME));
                                        parentBoAttrNameForNameAttributeFromUniqueConstraints.add(parentBoAttrName);
                                    }
                                }

                                List<String> currentBoAttrDisplayNamesBeingLookedUp = new ArrayList<>(childBoAttrDisplayNameForNameAttributeToReportError);
                                List<String> uniqueReferenceBoAttrNames = new ArrayList<>(parentBoAttrNameForNameAttributeFromUniqueConstraints);
                                List<String> referenceBoPrimaryKeyColumnNames = parentBoAttrNameForIdAttribute;
                                // read parent metadata to send as parameter
                                Map<String, DynamicDTO> referenceBoMetadataMap = dynamicReadService.readMetadataMapForBOName(parentBoName, executionContext);
                                // call the service
                                lookupForeignKeyFromCompositeUniqueConstraint(boName, parentPKToChildPKBoAttrNameMap, currentBoAttrDisplayNamesBeingLookedUp,
                                        parentBoName, referenceBoMetadataMap, uniqueReferenceBoAttrNames, columnToRowsDataMap,
                                        null, primaryKeyColumnNames, referenceBoPrimaryKeyColumnNames, idBoAttrNamesAlreadyConverted,
                                        invalidRecordsByRowNumber, dynamicReadService, executionContext);
                            }
                        }
                    }
                }
            }
        }
    }
}