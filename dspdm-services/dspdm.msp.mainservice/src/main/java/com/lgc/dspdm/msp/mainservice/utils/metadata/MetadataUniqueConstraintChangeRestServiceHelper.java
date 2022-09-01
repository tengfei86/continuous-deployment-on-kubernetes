package com.lgc.dspdm.msp.mainservice.utils.metadata;

import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.core.common.util.metadata.MetadataUniqueConstraintUtils;
import com.lgc.dspdm.service.common.dynamic.read.IDynamicReadService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * It is a Helper class to validate the json request syntax and make sure that
 * the request is properly structured and fulfills the criteria to create or drop a unique constraint
 *
 * @author Muhammad Imran Ansari
 * @since 02-Mar-2021
 */
public class MetadataUniqueConstraintChangeRestServiceHelper {

    /**
     * to validate the json request syntax and make sure that the request is properly structured and fulfills the criteria to create a unique constraint
     *
     * @param busObjAttrUniqueConstraintsToBeCreated
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 02-Mar-2021
     */
    public static void validateAddUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeCreated, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {

        String boName = null;
        String boAttrName = null;
        String constraintName = null;
        for (DynamicDTO busObjAttrUniqueConstraintDTOToBeCreated : busObjAttrUniqueConstraintsToBeCreated) {
            boName = (String) busObjAttrUniqueConstraintDTOToBeCreated.get(DSPDMConstants.BoAttrName.BO_NAME);
            boAttrName = (String) busObjAttrUniqueConstraintDTOToBeCreated.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
            constraintName = (String) busObjAttrUniqueConstraintDTOToBeCreated.get(DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
            // validate mandatory attributes to define a unique constraint
            if (StringUtils.isNullOrEmpty(boName)) {
                throw new DSPDMException("'{}' is mandatory to add a unique constraint", executionContext.getExecutorLocale(), DSPDMConstants.BoAttrName.BO_NAME);
            }
            if (StringUtils.isNullOrEmpty(boAttrName)) {
                throw new DSPDMException("'{}' is mandatory to add a unique constraint", executionContext.getExecutorLocale(), DSPDMConstants.BoAttrName.BO_ATTR_NAME);
            }
            // check if unique constraint name is provided then its length should not be more than 200
            if ((StringUtils.hasValue(constraintName)) && (constraintName.length() > 200)) {
                throw new DSPDMException("'{}' length cannot be greater than 200", executionContext.getExecutorLocale(), DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
            }
            //make sure primary key is not provided in any of the unique constraint object
            if (busObjAttrUniqueConstraintDTOToBeCreated.get(DSPDMConstants.BoAttrName.BUS_OBJ_ATTR_UNIQ_CONS_ID) != null) {
                throw new DSPDMException("Cannot add unique constraint '{}'. Primary key '{}' must not be provided.",
                        executionContext.getExecutorLocale(), constraintName, DSPDMConstants.BoAttrName.BUS_OBJ_ATTR_UNIQ_CONS_ID);
            }

            // validate that business object and business object attribute exists and it is not a primary key
            DynamicDTO boAttrNameDTO = dynamicReadService.readMetadataMapForBOName(boName, executionContext).get(boAttrName);
            if (boAttrNameDTO == null) {
                throw new DSPDMException("Cannot add unique constraint. Business object attribute name '{}' does not exist in business object '{}'.",
                        executionContext.getExecutorLocale(), constraintName, boAttrName, boName);
            } else if (Boolean.TRUE.equals(boAttrNameDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY))) {
                throw new DSPDMException("Cannot add unique constraint. Primary key attribute '{}' cannot be involved in any unique constraint definition.",
                        executionContext.getExecutorLocale(), boAttrName);
            }

            // validate unique constraint name not already exists
            if (StringUtils.hasValue(constraintName)) {
                validateUniqueConstraintName_AddUniqueConstraints(constraintName, dynamicReadService, executionContext);
            }
        }
        // now verify no unique constraint with same attribute set already exists
        verifyUniqueConstraintNotAlreadyExists(busObjAttrUniqueConstraintsToBeCreated, dynamicReadService, executionContext);
    }

    /**
     * verifies that a unique constraint with same name does not already exist
     *
     * @param constraintName
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 02-Feb-2021
     */
    private static void validateUniqueConstraintName_AddUniqueConstraints(String constraintName, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        // constraint name should not have special characters
        if (!(StringUtils.isAlphaNumericOrWhitespaceUnderScore(constraintName))) {
            throw new DSPDMException("Cannot add unique constraint. Constraint name '{}' cannot have special characters.",
                    executionContext.getExecutorLocale(), constraintName);
        }
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, executionContext);
        businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.CONSTRAINT_NAME, constraintName);
        if (dynamicReadService.count(businessObjectInfo, executionContext) > 0) {
            throw new DSPDMException("Cannot add unique constraint. A constraint with name '{}' already exists.",
                    executionContext.getExecutorLocale(), constraintName);
        }
    }

    /**
     * verify unique constraints not already exist for same set of attributes and business objects
     *
     * @param busObjAttrUniqueConstraintsToBeCreated
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 02-Mar-2021
     */
    private static void verifyUniqueConstraintNotAlreadyExists(List<DynamicDTO> busObjAttrUniqueConstraintsToBeCreated, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {

        List<DynamicDTO> simpleUniqueConstraintsToBeCreated = new ArrayList<>(busObjAttrUniqueConstraintsToBeCreated.size());
        List<DynamicDTO> compositeUniqueConstraintsToBeCreated = new ArrayList<>(busObjAttrUniqueConstraintsToBeCreated.size());
        for (DynamicDTO dynamicDTO : busObjAttrUniqueConstraintsToBeCreated) {
            if (StringUtils.isNullOrEmpty((String) dynamicDTO.get(DSPDMConstants.BoAttrName.CONSTRAINT_NAME))) {
                // add to simple as no name is provided
                simpleUniqueConstraintsToBeCreated.add(dynamicDTO);
            } else {
                // add to composite by default as a name is provided
                compositeUniqueConstraintsToBeCreated.add(dynamicDTO);
            }
        }
        // process all composite unique constraints. First group by name and if a group size is one then it means its is a simple unique constraints
        if (CollectionUtils.hasValue(compositeUniqueConstraintsToBeCreated)) {
            Map<String, List<DynamicDTO>> groupByUniqueConstraintName = CollectionUtils.groupDynamicDTOByPropertyValueIgnoreCase(compositeUniqueConstraintsToBeCreated, DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
            for (Map.Entry<String, List<DynamicDTO>> entry : groupByUniqueConstraintName.entrySet()) {
                compositeUniqueConstraintsToBeCreated = entry.getValue();
                if (compositeUniqueConstraintsToBeCreated.size() == 1) {
                    simpleUniqueConstraintsToBeCreated.add(compositeUniqueConstraintsToBeCreated.get(0));
                } else {
                    // 1. verify composite unique constraint not already exists for same attributes
                    verifyCompositeUniqueConstraintNotAlreadyExists(compositeUniqueConstraintsToBeCreated, dynamicReadService, executionContext);
                    // 2. make sure duplicate/repeated attribute does not exist in a composite unique constraint
                    verifyNoCompositeUniqueConstraintHasRepeatedAttribute(compositeUniqueConstraintsToBeCreated, executionContext);
                }
            }
        }
        // process all simple unique constraints individually and independently
        if (CollectionUtils.hasValue(simpleUniqueConstraintsToBeCreated)) {
            TreeSet<String> alreadyVerifiedSimpleUniqueConstraints = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
            // if objects in this group has name then it is unique in the group
            // this group has no unique constraint name it means all objects are independent and must be verified individually/independently
            for (DynamicDTO simpleUniqueConstraintToBeCreated : simpleUniqueConstraintsToBeCreated) {
                String boName = (String) simpleUniqueConstraintToBeCreated.get(DSPDMConstants.BoAttrName.BO_NAME);
                String boAttrName = (String) simpleUniqueConstraintToBeCreated.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                String uniqueName = boName + "_" + boAttrName;
                if (alreadyVerifiedSimpleUniqueConstraints.contains(uniqueName)) {
                    String constraintName = (String) simpleUniqueConstraintToBeCreated.get(DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
                    throw new DSPDMException("Cannot add unique constraint '{}'. The business object attribute '{}' already has a simple unique constraint.",
                            executionContext.getExecutorLocale(), constraintName, boAttrName);
                } else {
                    // verify unique constraint not already exists for attributes and bo name
                    verifySimpleUniqueConstraintNotAlreadyExists(simpleUniqueConstraintToBeCreated, dynamicReadService, executionContext);
                }
                alreadyVerifiedSimpleUniqueConstraints.add(uniqueName);
            }
        }
    }

    /**
     * verify that simple unique constraints not already exists for same attribute and business object
     *
     * @param simpleUniqueConstraint
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 02-Mar-2021
     */
    private static void verifySimpleUniqueConstraintNotAlreadyExists(DynamicDTO simpleUniqueConstraint, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        String boName = (String) simpleUniqueConstraint.get(DSPDMConstants.BoAttrName.BO_NAME);
        String boAttrName = (String) simpleUniqueConstraint.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
        // check all old already existing unique constraints for same business object name
        // read from cache
        List<DynamicDTO> allExistingUniqueConstraintsForBoName = dynamicReadService.readMetadataConstraintsForBOName(boName, executionContext);
        if ((CollectionUtils.hasValue(allExistingUniqueConstraintsForBoName)) && (StringUtils.hasValue(boAttrName))) {
            // call a utility method to check that the given combination already exists or not
            if (MetadataUniqueConstraintUtils.uniqueConstraintExistsForAttributes(allExistingUniqueConstraintsForBoName, boAttrName)) {
                String constraintName = (String) simpleUniqueConstraint.get(DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
                throw new DSPDMException("Cannot add unique constraint '{}'. A unique constraint already exists for business object attribute '{}'.",
                        executionContext.getExecutorLocale(), constraintName, CollectionUtils.getCommaSeparated(boAttrName));
            }
        }
    }

    /**
     * verify composite unique constraints not already exists for same attributes
     *
     * @param compositeUniqueConstraints
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 03-Mar-2021
     */
    private static void verifyCompositeUniqueConstraintNotAlreadyExists(List<DynamicDTO> compositeUniqueConstraints, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        DynamicDTO firstBusObjAttrUniqueConstraintDTO = compositeUniqueConstraints.get(0);
        String boName = (String) firstBusObjAttrUniqueConstraintDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        List<String> boAttrNames = CollectionUtils.getUpperCaseValuesFromListOfMap(compositeUniqueConstraints, DSPDMConstants.BoAttrName.BO_ATTR_NAME);
        // check all old already existing unique constraints for same business object name
        // read from cache
        List<DynamicDTO> allExistingUniqueConstraintsForBoName = dynamicReadService.readMetadataConstraintsForBOName(boName, executionContext);
        if ((CollectionUtils.hasValue(allExistingUniqueConstraintsForBoName)) && (CollectionUtils.hasValue(boAttrNames))) {
            // call a utility method to check that the given combination already exists or not
            if (MetadataUniqueConstraintUtils.uniqueConstraintExistsForAttributes(allExistingUniqueConstraintsForBoName, boAttrNames)) {
                String constraintName = (String) firstBusObjAttrUniqueConstraintDTO.get(DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
                throw new DSPDMException("Cannot add unique constraint '{}'. A unique constraint already exists for business object attributes '{}'.",
                        executionContext.getExecutorLocale(), constraintName, CollectionUtils.getCommaSeparated(boAttrNames));
            }
        }
    }

    /**
     * Makes sure that all the attributes involved in a composite unique constraint are different from each other
     *
     * @param uniqueConstraintsInANamedGroup
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 03-Mar-2021
     */
    private static void verifyNoCompositeUniqueConstraintHasRepeatedAttribute(List<DynamicDTO> uniqueConstraintsInANamedGroup, ExecutionContext executionContext) {
        // now verify no two unique constraints dto objects have same attribute name under same unique constraint name
        List<String> boAttrNames = CollectionUtils.getUpperCaseValuesFromListOfMap(uniqueConstraintsInANamedGroup, DSPDMConstants.BoAttrName.BO_ATTR_NAME);
        // means that all the attributes in a composite unique constraints must be different from each other
        TreeSet<String> boAttrNamesSet = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
        boAttrNamesSet.addAll(boAttrNames);
        if (boAttrNamesSet.size() != uniqueConstraintsInANamedGroup.size()) {
            String constraintName = (String) uniqueConstraintsInANamedGroup.get(0).get(DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
            throw new DSPDMException("Cannot add unique constraint '{}'. A business object attribute has been used more than once.",
                    executionContext.getExecutorLocale(), constraintName);
        }
    }

    /**
     * to validate the json request syntax and make sure that the request is properly structured and fulfills the criteria to drop a unique constraint
     *
     * @param busObjAttrUniqueConstraintsToBeDropped
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 02-Mar-2021
     */
    public static void validateDropUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeDropped,
                                                     IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        String constraintName = null;
        for (DynamicDTO busObjAttrUniqueConstraintDTOToBeDropped : busObjAttrUniqueConstraintsToBeDropped) {
            constraintName = (String) busObjAttrUniqueConstraintDTOToBeDropped.get(DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
            // constraint name is mandatory to drop a unique constraint
            if (StringUtils.isNullOrEmpty(constraintName)) {
                throw new DSPDMException("Cannot drop unique constraint. '{}' is mandatory to drop a unique constraint.",
                        executionContext.getExecutorLocale(), constraintName, DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
            }
            // check if unique constraint name is provided then its length should not be more than 200
            if (constraintName.length() > 200) {
                throw new DSPDMException("'{}' length cannot be greater than 200", executionContext.getExecutorLocale(), DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
            }
            // constraint name should not have special characters
            if (!(StringUtils.isAlphaNumericOrWhitespaceUnderScore(constraintName))) {
                throw new DSPDMException("Cannot drop unique constraints. Constraint name '{}' cannot have special characters.",
                        executionContext.getExecutorLocale(), constraintName);
            }
            //make sure primary key is not provided in any of the unique constraint object
            if (busObjAttrUniqueConstraintDTOToBeDropped.get(DSPDMConstants.BoAttrName.BUS_OBJ_ATTR_UNIQ_CONS_ID) != null) {
                throw new DSPDMException("Cannot drop unique constraint '{}'. Primary key '{}' must not be provided.",
                        executionContext.getExecutorLocale(), constraintName, DSPDMConstants.BoAttrName.BUS_OBJ_ATTR_UNIQ_CONS_ID);
            }
        }
        // now verify no unique constraint name has been used more than once
        verifyConstraintNameIsNotRepeated_DropUniqueConstraints(busObjAttrUniqueConstraintsToBeDropped, executionContext);

        // validate unique constraint name must already exist
        validateUniqueConstraintName_DropUniqueConstraints(busObjAttrUniqueConstraintsToBeDropped, dynamicReadService, executionContext);
    }

    /**
     * verifies that a unique constraint must already exist
     *
     * @param busObjAttrUniqueConstraintsToBeDropped
     * @param dynamicReadService
     * @param executionContext
     *
     * @author Muhammad Imran Ansari
     * @since 16-Mar-2021
     */
    private static void validateUniqueConstraintName_DropUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeDropped, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        // extract all the constraint names from the list
        List<String> constraintNamesToDrop = CollectionUtils.getUpperCaseValuesFromListOfMap(busObjAttrUniqueConstraintsToBeDropped, DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
        // now read constraint name from db
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, executionContext);
        // set read with distinct
        businessObjectInfo.setReadWithDistinct(true);
        // just read constraint name
        businessObjectInfo.addColumnsToSelect(DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
        List<DynamicDTO> busObjAttrUniqueConstraintsReadFromDB = dynamicReadService.readLongRangeUsingInClause(businessObjectInfo, null,
                DSPDMConstants.BoAttrName.CONSTRAINT_NAME, constraintNamesToDrop.toArray(), Operator.IN, executionContext);
        List<String> constraintNamesFromDB = CollectionUtils.getUpperCaseValuesFromListOfMap(
                busObjAttrUniqueConstraintsReadFromDB, DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
        if (constraintNamesToDrop.size() != constraintNamesFromDB.size()) {
            for (String constraintName : constraintNamesToDrop) {
                if (!constraintNamesFromDB.contains(constraintName)) {
                    throw new DSPDMException("Cannot drop unique constraints. No existing unique constraint found with name '{}'.",
                            executionContext.getExecutorLocale(), constraintName);
                }
            }
        }
    }

    /**
     * Makes sure that no constraint name is coming more than once
     *
     * @param busObjAttrUniqueConstraintsToBeDropped
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 13-Mar-2021
     */
    private static void verifyConstraintNameIsNotRepeated_DropUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeDropped, ExecutionContext executionContext) {
        // now verify that no constraint name is coming more than once
        List<String> constraintNames = CollectionUtils.getUpperCaseValuesFromListOfMap(busObjAttrUniqueConstraintsToBeDropped, DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
        // means that all the constraint names must be different from each other
        TreeSet<String> constraintNamesSet = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
        constraintNamesSet.addAll(constraintNames);
        if (constraintNamesSet.size() != busObjAttrUniqueConstraintsToBeDropped.size()) {
            throw new DSPDMException("Cannot drop unique constraints. A constraint name has been used more than once.",
                    executionContext.getExecutorLocale());
        }
    }
}
