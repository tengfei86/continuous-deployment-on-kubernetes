package com.lgc.dspdm.msp.mainservice.utils.metadata;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.config.ConnectionProperties;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.core.common.util.metadata.MetadataUniqueConstraintUtils;
import com.lgc.dspdm.core.common.util.metadata.MetadataUtils;
import com.lgc.dspdm.service.common.dynamic.read.IDynamicReadService;

import java.util.*;

public class MetadataChangeRestServiceHelper {
    protected static DSPDMLogger logger = new DSPDMLogger(MetadataChangeRestServiceHelper.class);

    public static void validateDropBusinessObjects(List<DynamicDTO> businessObjectListToDrop, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {

        List<DynamicDTO> allBusinessObjects = dynamicReadService.readMetadataAllBusinessObjects(executionContext);
        List<String> allBusinessObjectNames = CollectionUtils.getStringValuesFromList(allBusinessObjects, DSPDMConstants.BoAttrName.BO_NAME);
        for (DynamicDTO businessObjectDynamicDTO : businessObjectListToDrop) {
            if (StringUtils.isNullOrEmpty((String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME))) {
                throw new DSPDMException("BO name is mandatory to drop an existing business object", executionContext.getExecutorLocale());
            }
            String boName = (String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);

            if (!(CollectionUtils.containsIgnoreCase(allBusinessObjectNames, boName))) {
                throw new DSPDMException("Cannot drop business object, Business object '{}' does not exist",
                        executionContext.getExecutorLocale(), boName);
            }
        }
    }

    public static void validateIntroduceNewBusinessObjects(List<DynamicDTO> businessObjectListToSave, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        //List<DynamicDTO> businessObjectMetadata = dynamicReadService.readMetadataForBOName(DSPDMConstants.BoName.BUSINESS_OBJECT, executionContext);
        //List<DynamicDTO> businessObjectAttrMetadata = dynamicReadService.readMetadataForBOName(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, executionContext);

        String[] mandatoryAttributes = {
                DSPDMConstants.BoAttrName.BO_NAME,
                DSPDMConstants.BoAttrName.BO_DISPLAY_NAME,
                DSPDMConstants.BoAttrName.ENTITY,
                DSPDMConstants.BoAttrName.BO_DESC,
                DSPDMConstants.BoAttrName.KEY_SEQ_NAME};

        Map<String, Boolean> defaultValueAttributesMap = new HashMap<>(6);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_ACTIVE, true);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_MASTER_DATA, false);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_OPERATIONAL_TABLE, false);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_RESULT_TABLE, false);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_METADATA_TABLE, false);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_REFERENCE_TABLE, false);

        List<DynamicDTO> allBusinessObjects = dynamicReadService.readMetadataAllBusinessObjects(executionContext);
        List<String> allBusinessObjectNames = CollectionUtils.getStringValuesFromList(allBusinessObjects, DSPDMConstants.BoAttrName.BO_NAME);
        String boName = null;
        // create an empty list and add each successful validated in it to be used in relationships as a parent
        List<DynamicDTO> alreadyValidatedBusinessObjects = new ArrayList<>(businessObjectListToSave.size());
        Map<String, Integer> sequenceNoMap = new HashMap<>();
        for (DynamicDTO businessObjectDynamicDTO : businessObjectListToSave) {
            boName = (String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
            if (StringUtils.isNullOrEmpty(boName)) {
                throw new DSPDMException("BO name is mandatory to add new business object", executionContext.getExecutorLocale());
            }

            if (CollectionUtils.containsIgnoreCase(allBusinessObjectNames, boName)) {
                throw new DSPDMException("Cannot define new business object, Business object '{}' already exists",
                        executionContext.getExecutorLocale(), boName);
            } else {
                List<DynamicDTO> businessObjectsWithSameName = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(businessObjectListToSave, DSPDMConstants.BoAttrName.BO_NAME, boName);
                if (businessObjectsWithSameName.size() > 1) {
                    throw new DSPDMException("Cannot define new business object '{}' more than once",
                            executionContext.getExecutorLocale(), boName);
                }
            }

            // bo name is found now validate all other mandatory fields for business object table
            for (String mandatoryAttribute : mandatoryAttributes) {
                Object mandatoryAttributeValue = businessObjectDynamicDTO.get(mandatoryAttribute);
                if (mandatoryAttributeValue == null) {
                    throw new DSPDMException("Cannot define new business object, '{}' is mandatory to add new business object '{}'",
                            executionContext.getExecutorLocale(), mandatoryAttribute, boName);
                }
                if ((mandatoryAttributeValue instanceof String) && (StringUtils.isNullOrEmpty((String) mandatoryAttributeValue))) {
                    throw new DSPDMException("Cannot define new business object, '{}' is mandatory to add new business object '{}'",
                            executionContext.getExecutorLocale(), mandatoryAttribute, boName);
                }
            }
            // putting default values if not exist already
            for (String defaultValueAttributeKey : defaultValueAttributesMap.keySet()) {
                Object defaultValueAttributeValue = businessObjectDynamicDTO.get(defaultValueAttributeKey);
                if (defaultValueAttributeValue == null) {
                    businessObjectDynamicDTO.put(defaultValueAttributeKey, defaultValueAttributesMap.get(defaultValueAttributeKey));
                }
            }

            // now going to check attributes
            if (businessObjectDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY) == null) {
                throw new DSPDMException("Cannot add new business object '{}', mandatory children map is null", executionContext.getExecutorLocale(), boName);
            }
            Map<String, List<DynamicDTO>> children = (Map<String, List<DynamicDTO>>) businessObjectDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
            if (CollectionUtils.isNullOrEmpty(children)) {
                throw new DSPDMException("Cannot add new business object '{}', mandatory children map is empty", executionContext.getExecutorLocale(), boName);
            }
            if (CollectionUtils.isNullOrEmpty(children.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR))) {
                throw new DSPDMException("Cannot add new business object '{}', no attribute details found in children map", executionContext.getExecutorLocale(), boName);
            }
            // validate attributes metadata
            List<DynamicDTO> attributes = children.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
            validateNewBusinessObjectAttributes(businessObjectDynamicDTO, attributes, allBusinessObjectNames, executionContext);
            // validate unique constraints metadata if it exists
            List<DynamicDTO> uniqueConstraints = children.get(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS);
            validateNewBusinessObjectUniqueConstraints(boName, uniqueConstraints, attributes, dynamicReadService, executionContext);
            // validate relationship metadata if it exists
            List<DynamicDTO> relationships = children.get(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP);
            validateNewBusinessObjectRelationships(boName, attributes, uniqueConstraints, relationships,
                    allBusinessObjectNames, alreadyValidatedBusinessObjects, dynamicReadService, executionContext);
            // validate business object group if it exists
            List<DynamicDTO> businessObjectGroups = children.get(DSPDMConstants.BoName.BUSINESS_OBJECT_GROUP);
            validateNewBusinessObjectGroups(boName, businessObjectGroups, sequenceNoMap, dynamicReadService, executionContext);
            // add to validation done list
            alreadyValidatedBusinessObjects.add(businessObjectDynamicDTO);
        }
    }

    private static void validateNewBusinessObjectAttributes(DynamicDTO parentDynamicDTO, List<DynamicDTO> attributes, List<String> allBusinessObjectNames, ExecutionContext executionContext) {
        String boName = (String) parentDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);

        Map<String, Boolean> defaultValueAttributesMap = new HashMap<>(20);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_ACTIVE, true);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_CUSTOM_ATTRIBUTE, false);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_HIDDEN, false);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_INTERNAL, false);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_MANDATORY, true);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY, false);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_READ_ONLY, false);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_REFERENCE_IND, false);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_SORTABLE, true);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_UPLOAD_NEEDED, true);

        logger.info("This code has integrity_constraint_violation fixed with IS_READ_ONLY false");

        int lastAttributeSequenceNumber = 0;
        int primaryKeyColumnsCount = 0;
        int childIndex = 1;
        DSPDMConstants.DataTypes dataType = null;
        for (DynamicDTO attributeDynamicDTO : attributes) {
            String boAttrName = (String) attributeDynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
            // bo attr name
            if (StringUtils.isNullOrEmpty(boAttrName)) {
                throw new DSPDMException("Cannot define new business object '{}', '{}' is mandatory for attribute at index '{}'",
                        executionContext.getExecutorLocale(), boName, DSPDMConstants.BoAttrName.BO_ATTR_NAME, childIndex++);
            }
            // bo name is found now validate all other mandatory fields for business object table
            // attribute
            if (StringUtils.isNullOrEmpty((String) attributeDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE))) {
                throw new DSPDMException("Cannot define new business object '{}', '{}' is mandatory for attribute '{}'",
                        executionContext.getExecutorLocale(), boName, DSPDMConstants.BoAttrName.ATTRIBUTE, boAttrName);
            }
            // display name
            if (StringUtils.isNullOrEmpty((String) attributeDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME))) {
                throw new DSPDMException("Cannot define new business object '{}', '{}' is mandatory for attribute '{}'",
                        executionContext.getExecutorLocale(), boName, DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME, boAttrName);
            }
            // data type
            if (StringUtils.isNullOrEmpty((String) attributeDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE))) {
                throw new DSPDMException("Cannot define new business object '{}', '{}' is mandatory for attribute '{}'",
                        executionContext.getExecutorLocale(), boName, DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE, boAttrName);
            } else {
                String attributeDataType = (String) attributeDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE);
                dataType = DSPDMConstants.DataTypes.fromAttributeDataType(attributeDataType, ConnectionProperties.isMSSQLServerDialect());
                validateAttributeDataType(boName, boAttrName, attributeDataType, dataType, executionContext);
            }

            // control type
            if (StringUtils.hasValue((String) attributeDynamicDTO.get(DSPDMConstants.BoAttrName.CONTROL_TYPE))) {
                String controlType = (String) attributeDynamicDTO.get(DSPDMConstants.BoAttrName.CONTROL_TYPE);
                validateAttributeControlType(boName, boAttrName, controlType, executionContext);
            }

            // putting default values if not exist already
            for (String defaultValueAttributeKey : defaultValueAttributesMap.keySet()) {
                Object defaultValueAttributeValue = attributeDynamicDTO.get(defaultValueAttributeKey);
                if (defaultValueAttributeValue == null) {
                    if ((defaultValueAttributeKey.equalsIgnoreCase(DSPDMConstants.BoAttrName.IS_HIDDEN)) || (defaultValueAttributeKey.equalsIgnoreCase(DSPDMConstants.BoAttrName.IS_INTERNAL))) {
                        // if primary key then both hidden and internal to true otherwise no
                        if (Boolean.TRUE.equals(attributeDynamicDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY))) {
                            attributeDynamicDTO.put(defaultValueAttributeKey, true);
                        } else if ((boAttrName.equalsIgnoreCase(DSPDMConstants.BoAttrName.ROW_CREATED_BY)) ||
                                (boAttrName.equalsIgnoreCase(DSPDMConstants.BoAttrName.ROW_CREATED_DATE)) ||
                                (boAttrName.equalsIgnoreCase(DSPDMConstants.BoAttrName.ROW_CHANGED_BY)) ||
                                (boAttrName.equalsIgnoreCase(DSPDMConstants.BoAttrName.ROW_CHANGED_DATE))) {
                            if (defaultValueAttributeKey.equalsIgnoreCase(DSPDMConstants.BoAttrName.IS_HIDDEN)) {
                                attributeDynamicDTO.put(defaultValueAttributeKey, false);
                            } else {
                                attributeDynamicDTO.put(defaultValueAttributeKey, true);
                            }
                        } else {
                            attributeDynamicDTO.put(defaultValueAttributeKey, defaultValueAttributesMap.get(defaultValueAttributeKey));
                        }
                    } else {
                        attributeDynamicDTO.put(defaultValueAttributeKey, defaultValueAttributesMap.get(defaultValueAttributeKey));
                    }
                }
            }

            // primary key count and data type validation
            if (Boolean.TRUE.equals(attributeDynamicDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY))) {
                if (!(dataType.isPrimaryKeyDataType())) {
                    throw new DSPDMException("Cannot define new business object, '{}' is inappropriate for primary key attribute '{}'",
                            executionContext.getExecutorLocale(), dataType, boAttrName);
                }
                primaryKeyColumnsCount++;
            }
            // verify reference bo name if it exists
            if (StringUtils.hasValue((String) attributeDynamicDTO.get(DSPDMConstants.BoAttrName.REFERENCE_BO_NAME))) {
                String referenceBoName = (String) attributeDynamicDTO.get(DSPDMConstants.BoAttrName.REFERENCE_BO_NAME);
                if (!(CollectionUtils.containsIgnoreCase(allBusinessObjectNames, referenceBoName))) {
                    throw new DSPDMException("Cannot define new business object '{}'. Reference business object '{}' does not exist",
                            executionContext.getExecutorLocale(), boName, referenceBoName);
                }
//                else {
//                    throw new DSPDMException("Cannot define new business object '{}'. Reference business object name '{}' can only be set through relationships.",
//                            executionContext.getExecutorLocale(), boName, referenceBoName);
//                }
            }

            // verify the value in related bo attr name
            if (StringUtils.hasValue((String) attributeDynamicDTO.get(DSPDMConstants.BoAttrName.RELATED_BO_ATTR_NAME))) {
                String relatedBoAttrName = (String) attributeDynamicDTO.get(DSPDMConstants.BoAttrName.RELATED_BO_ATTR_NAME);
                if (boAttrName.equalsIgnoreCase(relatedBoAttrName)) {
                    throw new DSPDMException("Cannot define new business object '{}'. Related business object attribute name '{}' cannot be same as the main attribute name",
                            executionContext.getExecutorLocale(), boName, relatedBoAttrName);
                }
                DynamicDTO relatedBoAttrDynamicDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(attributes, DSPDMConstants.BoAttrName.BO_ATTR_NAME, relatedBoAttrName);
                if (relatedBoAttrDynamicDTO == null) {
                    throw new DSPDMException("Cannot define new business object '{}'. Related business object attribute '{}' does not exist",
                            executionContext.getExecutorLocale(), boName, relatedBoAttrName);
                }
            }

            // set parent fields in child
            String boDisplayName = (String) parentDynamicDTO.get(DSPDMConstants.BoAttrName.BO_DISPLAY_NAME);
            String boDescription = (String) parentDynamicDTO.get(DSPDMConstants.BoAttrName.BO_DESC);
            String schemaName = (String) parentDynamicDTO.get(DSPDMConstants.BoAttrName.SCHEMA_NAME);
            String entity = (String) parentDynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY);

            attributeDynamicDTO.put(DSPDMConstants.BoAttrName.BO_NAME, boName);
            attributeDynamicDTO.put(DSPDMConstants.BoAttrName.BO_DISPLAY_NAME, boDisplayName);
            attributeDynamicDTO.put(DSPDMConstants.BoAttrName.ENTITY, entity);
            attributeDynamicDTO.put(DSPDMConstants.BoAttrName.BO_DESC, boDescription);
            attributeDynamicDTO.put(DSPDMConstants.BoAttrName.SCHEMA_NAME, schemaName);
            attributeDynamicDTO.put(DSPDMConstants.BoAttrName.SEQUENCE_NUM, ++lastAttributeSequenceNumber);
        }

        // validate exactly one primary key column exists
        if (primaryKeyColumnsCount == 0) {
            throw new DSPDMException("Cannot add new business object '{}', no primary key column found", executionContext.getExecutorLocale(), boName);
        } else if (primaryKeyColumnsCount > 1) {
            throw new DSPDMException("Cannot add new business object '{}', composite primary key not allowed", executionContext.getExecutorLocale(), boName);
        }
    }

    private static void validateAttributeDataType(String boName, String boAttrName, String attributeDataType, DSPDMConstants.DataTypes dataType, ExecutionContext executionContext) {
        if (dataType == null) {
            throw new DSPDMException("Cannot define new business object '{}'. Data type '{}' for attribute '{}' is not valid",
                    executionContext.getExecutorLocale(), boName, attributeDataType, boAttrName);
        }
        int dataTypeLength = MetadataUtils.getDataTypeLengthFromString(attributeDataType, boAttrName, boName, executionContext);
        if ((dataType.isStringDataTypeWithLength()) && (dataTypeLength > ConfigProperties.getInstance().max_allowed_length_for_string_data_type.getIntegerValue())) {
            throw new DSPDMException("Cannot define new business object '{}'. Data type length '{}' for attribute '{}' is greater than allowed value '{}'",
                    executionContext.getExecutorLocale(), boName, dataTypeLength, boAttrName, ConfigProperties.getInstance().max_allowed_length_for_string_data_type.getIntegerValue());
        } else if ((dataType.isIntegerDataType()) && (dataTypeLength > 20)) {
            throw new DSPDMException("Cannot define new business object '{}'. Data type length '{}' for attribute '{}' is greater than allowed value 20",
                    executionContext.getExecutorLocale(), boName, dataTypeLength, boAttrName);
        } else if (dataType.isFloatingDataType()) {
            if (dataTypeLength > 20) {
                throw new DSPDMException("Cannot define new business object '{}'. Data type length '{}' for attribute '{}' is greater than allowed value  20",
                        executionContext.getExecutorLocale(), boName, dataTypeLength, boAttrName);
            }
            int decimalLength = MetadataUtils.getDataTypeDecimalLengthFromString(attributeDataType, boAttrName, boName, executionContext);
            if (decimalLength > 20) {
                throw new DSPDMException("Cannot define new business object '{}'. Data type decimal points length '{}' for attribute '{}' is greater than allowed value 20",
                        executionContext.getExecutorLocale(), boName, dataTypeLength, boAttrName);
            }
        }
    }

    private static void validateAttributeControlType(String boName, String boAttrName, String controlType, ExecutionContext executionContext) {
        boolean matched = false;
        for (String existingControlType : DSPDMConstants.ControlTypes.ALL_CONTROL_TYPES) {
            if (existingControlType.equalsIgnoreCase(controlType)) {
                matched = true;
                break;
            }
        }
        if (!matched) {
            throw new DSPDMException("Cannot define new business object '{}'. Control type '{}' for attribute '{}' is invalid",
                    executionContext.getExecutorLocale(), boName, controlType, boAttrName);
        }
    }

    /**
     * validates the data inside the unique constraints
     *
     * @param boName
     * @param uniqueConstraints
     * @param attributes
     * @param executionContext
     */
    private static void validateNewBusinessObjectUniqueConstraints(String boName, List<DynamicDTO> uniqueConstraints, List<DynamicDTO> attributes, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        if (CollectionUtils.hasValue(uniqueConstraints)) {
            String uniqueConstraitsBoName = null;
            String boAttrName = null;
            String constraintName = null;
            for (DynamicDTO uniqueConstraitsDTO : uniqueConstraints) {
                uniqueConstraitsBoName = (String) uniqueConstraitsDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
                boAttrName = (String) uniqueConstraitsDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                constraintName = (String) uniqueConstraitsDTO.get(DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
                if (StringUtils.isNullOrEmpty(uniqueConstraitsBoName)) {
                    throw new DSPDMException("Cannot define new business object '{}', '{}' is mandatory for defining unique constraint",
                            executionContext.getExecutorLocale(), boName, DSPDMConstants.BoAttrName.BO_NAME);
                }
                if (StringUtils.isNullOrEmpty(boAttrName)) {
                    throw new DSPDMException("Cannot define new business object '{}', '{}' is mandatory for defining unique constraint",
                            executionContext.getExecutorLocale(), boName, DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                }
                // check if unique constraint name is provided then its length should not be more than 200
                if ((StringUtils.hasValue(constraintName)) && (constraintName.length() > 200)) {
                    throw new DSPDMException("Cannot define new business object '{}','{}' length cannot be greater than 200", executionContext.getExecutorLocale(), boName, DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
                }
                //make sure primary key is not provided in any of the unique constraint object
                if (uniqueConstraitsDTO.get(DSPDMConstants.BoAttrName.BUS_OBJ_ATTR_UNIQ_CONS_ID) != null) {
                    throw new DSPDMException("Cannot define new business object '{}'. cannot add unique constraint, primary key '{}' must not be provided.",
                            executionContext.getExecutorLocale(), boName, DSPDMConstants.BoAttrName.BUS_OBJ_ATTR_UNIQ_CONS_ID);
                }
                // validate that business object attribute exists and it is not a primary key
                DynamicDTO boAttrNameDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(attributes, DSPDMConstants.BoAttrName.BO_ATTR_NAME, boAttrName);
                if (boAttrNameDTO == null) {
                    throw new DSPDMException("Cannot define new business object '{}', '{}' mentioned in unique constraint '{}' does not exist in business object '{}'",
                            executionContext.getExecutorLocale(), boName, boAttrName, constraintName, boName);
                } else if (Boolean.TRUE.equals(boAttrNameDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY))) {
                    throw new DSPDMException("Cannot define new business object '{}', cannot add unique constraint. Primary key attribute '{}' cannot be involved in any unique constraint definition.",
                            executionContext.getExecutorLocale(), boName, boAttrName);
                }
                // validate unique constraint name not already exists
                if (StringUtils.hasValue(constraintName)) {
                    // constraint name should not have special characters
                    if (!(StringUtils.isAlphaNumericOrWhitespaceUnderScore(constraintName))) {
                        throw new DSPDMException("Cannot define new business object '{}', cannot add unique constraint. Constraint name '{}' cannot have special characters.",
                                executionContext.getExecutorLocale(), boName, constraintName);
                    }
                    BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, executionContext);
                    businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.CONSTRAINT_NAME, constraintName);
                    if (dynamicReadService.count(businessObjectInfo, executionContext) > 0) {
                        throw new DSPDMException("Cannot define new business object '{}', cannot add unique constraint. A constraint with name '{}' already exists.",
                                executionContext.getExecutorLocale(), boName, constraintName);
                    }
                }
            }
            // make sure that the request does not have two duplicate constraints
            if (uniqueConstraints.size() != new HashSet<>(uniqueConstraints).size()) {
                throw new DSPDMException("Cannot define new business object '{}', cannot add unique constraint. A constraint definition is duplicate in request.",
                        executionContext.getExecutorLocale(), boName);
            }
        }
    }

    /**
     * validates relationship definition. Validates parent and child bo name and bo attr names
     *
     * @param boName
     * @param attributes
     * @param relationships
     * @param allBusinessObjectNames
     * @param dynamicReadService
     * @param executionContext
     */
    private static void validateNewBusinessObjectRelationships(String boName,
                                                               List<DynamicDTO> attributes,
                                                               List<DynamicDTO> uniqueConstraints,
                                                               List<DynamicDTO> relationships,
                                                               List<String> allBusinessObjectNames,
                                                               List<DynamicDTO> alreadyValidatedBusinessObjects,
                                                               IDynamicReadService dynamicReadService,
                                                               ExecutionContext executionContext) {
        if (CollectionUtils.hasValue(relationships)) {
            Set<DynamicDTO> relationshipsWithNewParents = new LinkedHashSet<>(relationships.size());
            Set<DynamicDTO> relationshipsWithExistingParents = new LinkedHashSet<>(relationships.size());
            for (DynamicDTO relationshipDto : relationships) {
                String childBoName = (String) relationshipDto.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME);
                String childBoAttrName = (String) relationshipDto.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
                String parentBoName = (String) relationshipDto.get(DSPDMConstants.BoAttrName.PARENT_BO_NAME);
                String parentBoAttrName = (String) relationshipDto.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME);
                String relationshipName = (String) relationshipDto.get(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME);
                // VALIDATE CHILD BUSINESS OBJECT NAME
                if (!(boName.equalsIgnoreCase(childBoName))) {
                    throw new DSPDMException("Cannot define new business object '{}'. Found an incorrect relationship definition. Child business object '{}' must be same as current business object '{}'.",
                            executionContext.getExecutorLocale(), boName, childBoName, boName);
                }
                // VALIDATE CHILD BUSINESS OBJECT ATTR NAME
                if (!(CollectionUtils.containsIgnoreCase(CollectionUtils.getStringValuesFromList(attributes, DSPDMConstants.BoAttrName.BO_ATTR_NAME), childBoAttrName))) {
                    throw new DSPDMException("Cannot define new business object '{}'. Found an incorrect relationship definition. Child business object attribute '{}' must exist in business object '{}'.",
                            executionContext.getExecutorLocale(), boName, childBoAttrName, childBoName);
                }
                DynamicDTO childBoAttrNameDynamicDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(attributes, DSPDMConstants.BoAttrName.BO_ATTR_NAME, childBoAttrName);
                // VALIDATE PARENT BUSINESS OBJECT NAME AND PARENT BUSINESS OBJECT ATTRIBUTE NAME
                DynamicDTO parentBoAttrNameDynamicDTO = null;
                if (CollectionUtils.containsIgnoreCase(allBusinessObjectNames, parentBoName)) {
                    // parent business object already exists in the system. already registered
                    // VALIDATE PARENT BUSINESS OBJECT ATTR NAME
                    Map<String, DynamicDTO> parentBoAttrMetadataMap = dynamicReadService.readMetadataMapForBOName(parentBoName, executionContext);
                    if (CollectionUtils.containsKeyIgnoreCase(parentBoAttrMetadataMap, parentBoAttrName)) {
                        parentBoAttrNameDynamicDTO = parentBoAttrMetadataMap.get(parentBoAttrName);
                        // check parent attr is either primary of unique key
                        verifyParentAttrIsEitherPrimaryKeyOrUniqueKey(childBoName, parentBoName, parentBoAttrName, parentBoAttrNameDynamicDTO,
                                dynamicReadService, alreadyValidatedBusinessObjects, null, executionContext);
                        // add to existing parent relationship set to check later that parent attributes are unique in parent business object
                        relationshipsWithExistingParents.add(relationshipDto);
                    }
                } else if (parentBoName.equalsIgnoreCase(childBoName)) {
                    // self join case like AREA is a parent of an area
                    if (parentBoAttrName.equalsIgnoreCase(childBoAttrName)) {
                        throw new DSPDMException("Cannot define new business object '{}'. Found an incorrect relationship definition. Child business object attribute '{}' and parent business object attribute '{}' cannot be same.",
                                executionContext.getExecutorLocale(), boName, childBoAttrName, parentBoAttrName);
                    }
                    parentBoAttrNameDynamicDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(attributes, DSPDMConstants.BoAttrName.BO_ATTR_NAME, parentBoAttrName);
                    // check parent attr is either primary of unique key
                    verifyParentAttrIsEitherPrimaryKeyOrUniqueKey(childBoName, parentBoName, parentBoAttrName, parentBoAttrNameDynamicDTO,
                            dynamicReadService, alreadyValidatedBusinessObjects, uniqueConstraints, executionContext);
                    // add to new parent relationship set to check later that parent attributes are unique in parent business object
                    relationshipsWithNewParents.add(relationshipDto);
                } else {
                    // case when parent is in same json request and already validated above
                    DynamicDTO parentDynamicDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(alreadyValidatedBusinessObjects, DSPDMConstants.BoAttrName.BO_NAME, parentBoName);
                    if (parentDynamicDTO != null) {
                        Map<String, List<DynamicDTO>> children = (Map<String, List<DynamicDTO>>) parentDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
                        List<DynamicDTO> parentAttributes = children.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
                        parentBoAttrNameDynamicDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(parentAttributes, DSPDMConstants.BoAttrName.BO_ATTR_NAME, parentBoAttrName);
                        // check parent attr is either primary of unique key
                        verifyParentAttrIsEitherPrimaryKeyOrUniqueKey(childBoName, parentBoName, parentBoAttrName, parentBoAttrNameDynamicDTO,
                                dynamicReadService, alreadyValidatedBusinessObjects, null, executionContext);
                        // add to new parent relationship set to check later that parent attributes are unique in parent business object
                        relationshipsWithNewParents.add(relationshipDto);
                    } else {
                        throw new DSPDMException("Cannot define new business object '{}'. Found an incorrect relationship definition. Parent business object '{}' does not exist.",
                                executionContext.getExecutorLocale(), boName, parentBoName);
                    }
                }

                if (parentBoAttrNameDynamicDTO == null) {
                    throw new DSPDMException("Cannot define new business object '{}'. Found an incorrect relationship definition. Parent business object attribute '{}' must exist in business object '{}'.",
                            executionContext.getExecutorLocale(), boName, parentBoAttrName, parentBoName);
                }

                // parent attribute is unique now verify data type
                MetadataRelationshipChangeRestServiceHelper.validateParentAndChildAttributeDataType(parentBoAttrNameDynamicDTO, childBoAttrNameDynamicDTO, childBoAttrName, parentBoAttrName, executionContext);

                // validate relationship name not already exists
                if (StringUtils.hasValue(relationshipName)) {
                    MetadataRelationshipChangeRestServiceHelper.validateRelationshipName(relationshipName, dynamicReadService, executionContext);
                }
            }

            // verify that the parent attributes are either primary key or unique in existing parent
            if (relationshipsWithExistingParents.size() > 0) {
                MetadataRelationshipChangeRestServiceHelper.verifyParentBoAttrNamesUnique(new ArrayList<>(relationshipsWithExistingParents), dynamicReadService, executionContext);
            }

            // verify that the parent attributes are unique in new parent
            if (relationshipsWithNewParents.size() > 0) {
                verifyNewParentBoAttrNamesUnique(new ArrayList<>(relationshipsWithNewParents), attributes, uniqueConstraints, alreadyValidatedBusinessObjects, executionContext);
            }
            // now verify no relationship with same attribute set exists more than once
            verifyNoDuplicateRelationshipExistsInNewBusinessObject(relationships, executionContext);
        }
    }

    /**
     * validates business object group definition. Validates group category name
     *
     * @param boName
     * @param busObjGroupsToBeCreated
     * @param dynamicReadService
     * @param executionContext
     */
    public static void validateNewBusinessObjectGroups(String boName, List<DynamicDTO> busObjGroupsToBeCreated, Map<String, Integer> sequenceNoMap, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        if (CollectionUtils.hasValue(busObjGroupsToBeCreated)) {
            if (busObjGroupsToBeCreated.size() > 1) {
                throw new DSPDMException("Cannot define new business object '{}', it can only belong to one business object group.", executionContext.getExecutorLocale(), boName);
            }
            String groupCategoryName = null;
            for (DynamicDTO busObjGroupToBeCreated : busObjGroupsToBeCreated) {
                groupCategoryName = (String) busObjGroupToBeCreated.get(DSPDMConstants.BoAttrName.GROUP_CATEGORY_NAME);
                // validate mandatory attributes to define a business object group
                if (StringUtils.isNullOrEmpty(groupCategoryName)) {
                    throw new DSPDMException("Cannot define new business object '{}', '{}' is mandatory for defining business object group.", executionContext.getExecutorLocale(), boName, DSPDMConstants.BoAttrName.GROUP_CATEGORY_NAME);
                }
                // validate that rGroupCategoryID and groupCategoryName exist in group category
                List<DynamicDTO> rGroupCategorys = dynamicReadService.read(DSPDMConstants.BoName.R_GROUP_CATEGORY, executionContext);
                DynamicDTO rGroupCategory = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(rGroupCategorys, DSPDMConstants.BoAttrName.GROUP_CATEGORY_NAME, groupCategoryName);
                if (rGroupCategory == null) {
                    throw new DSPDMException("Cannot define new business object '{}'. GROUP CATEGORY NAME '{}' do not exist in '{}'.",
                            executionContext.getExecutorLocale(), boName, groupCategoryName, DSPDMConstants.BoName.R_GROUP_CATEGORY);
                } else {
                    busObjGroupToBeCreated.put(DSPDMConstants.BoAttrName.R_GROUP_CATEGORY_ID, rGroupCategory.get(DSPDMConstants.BoAttrName.R_GROUP_CATEGORY_ID));
                }

                // now set sequence number of the business object going to be added in the current group.
                Integer nextSequenceNumber = null;
                if (sequenceNoMap.containsKey(groupCategoryName)) {
                    Integer lastSequenceNumber = sequenceNoMap.get(groupCategoryName);
                    nextSequenceNumber = lastSequenceNumber + 1;
                } else {
                    Integer lastSequenceNumber = MetadataGroupChangeRestServiceHelper.readMaxSequenceNumberForCategoryFromDB(groupCategoryName, null, dynamicReadService, executionContext);
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
     * It makes sure that the given parent attribute names are either primary key or they are unique at least
     *
     * @param busObjRelationshipsToCreate
     * @param childAttributes
     * @param childUniqueConstraints
     * @param alreadyValidatedBusinessObjects
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 02-Feb-2021
     */
    public static void verifyNewParentBoAttrNamesUnique(List<DynamicDTO> busObjRelationshipsToCreate,
                                                        List<DynamicDTO> childAttributes,
                                                        List<DynamicDTO> childUniqueConstraints,
                                                        List<DynamicDTO> alreadyValidatedBusinessObjects,
                                                        ExecutionContext executionContext) {

        List<DynamicDTO> simpleRelationshipsToBeCreated = new ArrayList<>(busObjRelationshipsToCreate.size());
        List<DynamicDTO> compositeRelationshipsToBeCreated = new ArrayList<>(busObjRelationshipsToCreate.size());
        for (DynamicDTO dynamicDTO : busObjRelationshipsToCreate) {
            if (StringUtils.isNullOrEmpty((String) dynamicDTO.get(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME))) {
                // add to simple as no name is provided
                simpleRelationshipsToBeCreated.add(dynamicDTO);
            } else {
                // add to composite by default as a name is provided
                compositeRelationshipsToBeCreated.add(dynamicDTO);
            }
        }
        // process all composite relationships. First group by name and if a group size is one then it means its is a simple relationship
        if (CollectionUtils.hasValue(compositeRelationshipsToBeCreated)) {
            Map<String, List<DynamicDTO>> groupByRelationshipName = CollectionUtils.groupDynamicDTOByPropertyValueIgnoreCase(compositeRelationshipsToBeCreated, DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME);
            String relationshipName = null;
            for (Map.Entry<String, List<DynamicDTO>> entry : groupByRelationshipName.entrySet()) {
                compositeRelationshipsToBeCreated = entry.getValue();
                if (compositeRelationshipsToBeCreated.size() == 1) {
                    simpleRelationshipsToBeCreated.add(compositeRelationshipsToBeCreated.get(0));
                } else {
                    // verify that parent attribute combination is unique in parent business object
                    verifyNewParentBoAttrNamesAreUniqueForRelationships(compositeRelationshipsToBeCreated, childAttributes, childUniqueConstraints, alreadyValidatedBusinessObjects, executionContext);
                }
            }
        }
        // process all simple relationships individually and independently
        if (CollectionUtils.hasValue(simpleRelationshipsToBeCreated)) {
            // this group has no relationship name it means all objects are independent and must be verified individually/indendently
            for (DynamicDTO simpleRelationship : simpleRelationshipsToBeCreated) {
                // validate parent attribute is pk or unique
                verifyNewParentBoAttrNamesAreUniqueForRelationships(Arrays.asList(simpleRelationship), childAttributes, childUniqueConstraints, alreadyValidatedBusinessObjects, executionContext);
            }
        }
    }

    /**
     * It makes sure that the given parent attribute names are either primary key or they are unique at least
     *
     * @param relationships
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 02-Feb-2021
     */
    private static void verifyNewParentBoAttrNamesAreUniqueForRelationships(List<DynamicDTO> relationships,
                                                                            List<DynamicDTO> childAttributes,
                                                                            List<DynamicDTO> childUniqueConstraints,
                                                                            List<DynamicDTO> alreadyValidatedBusinessObjects,
                                                                            ExecutionContext executionContext) {
        if (CollectionUtils.hasValue(relationships)) {
            boolean unique = false;
            String parentBoName = (String) relationships.get(0).get(DSPDMConstants.BoAttrName.PARENT_BO_NAME);
            String childBoName = (String) relationships.get(0).get(DSPDMConstants.BoAttrName.CHILD_BO_NAME);
            String[] parentBoAttrNames = CollectionUtils.getValuesFromList(relationships, DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME).toArray(new String[0]);
            List<DynamicDTO> parentAttributes = null;
            List<DynamicDTO> parentUniqueConstraints = null;
            if (parentBoName.equalsIgnoreCase(childBoName)) {
                parentAttributes = childAttributes;
                parentUniqueConstraints = childUniqueConstraints;
            } else {
                DynamicDTO parentDynamicDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(alreadyValidatedBusinessObjects, DSPDMConstants.BoAttrName.BO_NAME, parentBoName);
                Map<String, List<DynamicDTO>> children = (Map<String, List<DynamicDTO>>) parentDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
                parentAttributes = children.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
                parentUniqueConstraints = children.get(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS);
            }
            Map<String, DynamicDTO> parentBoAttrMetadataMap = CollectionUtils.prepareMapFromValuesOfKey(parentAttributes, DSPDMConstants.BoAttrName.BO_ATTR_NAME);
            // filter map only for the relationship involved attributes
            parentBoAttrMetadataMap = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(parentBoAttrMetadataMap, DSPDMConstants.BoAttrName.BO_ATTR_NAME, parentBoAttrNames);
            if (CollectionUtils.hasValue(parentBoAttrMetadataMap)) {
                int pkCount = 0;
                for (DynamicDTO parentBoAttrMetadataDTO : parentBoAttrMetadataMap.values()) {
                    if (Boolean.TRUE.equals(parentBoAttrMetadataDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY))) {
                        pkCount++;
                    }
                }
                if (pkCount == parentBoAttrMetadataMap.size()) {
                    // all attributes are pk. good
                    unique = true;
                } else {
                    if (CollectionUtils.hasValue(parentUniqueConstraints)) {
                        // verify that the parent business object attribute is defined as a unique by read all unique constraints on bo name
                        if (MetadataUniqueConstraintUtils.uniqueConstraintExistsForAttributes(parentUniqueConstraints, parentBoAttrNames)) {
                            unique = true;
                        }
                    }
                }
                if (!unique) {
                    throw new DSPDMException("Cannot add business object relationship. Parent business object attribute(s) '{}' must be either primary key or unique in business object '{}'.",
                            executionContext.getExecutorLocale(), CollectionUtils.getCommaSeparated((Object[]) parentBoAttrNames), parentBoName);
                }
            } else {
                throw new DSPDMException("Cannot add business object relationship. Parent business object attribute(s) name '{}' does not exist in business object '{}'",
                        executionContext.getExecutorLocale(), CollectionUtils.getCommaSeparated((Object[]) parentBoAttrNames), parentBoName);
            }
        }
    }

    /**
     * Make sure that there is no repeated relationship object in the given list
     *
     * @param busObjRelationshipsToCreate
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 24-Feb-2021
     */
    public static void verifyNoDuplicateRelationshipExistsInNewBusinessObject(List<DynamicDTO> busObjRelationshipsToCreate,
                                                                              ExecutionContext executionContext) {
        TreeMap<String, DynamicDTO> treeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        String key = null;
        for (DynamicDTO dto : busObjRelationshipsToCreate) {
            key = ((String) dto.get(DSPDMConstants.BoAttrName.PARENT_BO_NAME))
                    + ((String) dto.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME))
                    + ((String) dto.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME))
                    + ((String) dto.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME));
            if (treeMap.containsKey(key)) {
                throw new DSPDMException("Cannot add business object '{}'. A relationship definition occurred more than once for child business object attribute '{}'.",
                        executionContext.getExecutorLocale(), dto.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME), dto.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME));
            } else {
                treeMap.put(key, dto);
            }
        }
    }

    /**
     * validated the request
     *
     * @param businessObjectListToGenerateMetadata
     * @param dynamicReadService
     * @param executionContext
     */
    public static void validateGenerateMetadataForExistingTable(List<DynamicDTO> businessObjectListToGenerateMetadata,
                                                                IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        //List<DynamicDTO> businessObjectMetadata = dynamicReadService.readMetadataForBOName(DSPDMConstants.BoName.BUSINESS_OBJECT, executionContext);
        //List<DynamicDTO> businessObjectAttrMetadata = dynamicReadService.readMetadataForBOName(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, executionContext);

        String[] mandatoryAttributes = {
                DSPDMConstants.BoAttrName.BO_NAME,
                DSPDMConstants.BoAttrName.BO_DISPLAY_NAME,
                DSPDMConstants.BoAttrName.ENTITY,
                DSPDMConstants.BoAttrName.BO_DESC};

        Map<String, Boolean> defaultValueAttributesMap = new HashMap<>(6);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_ACTIVE, true);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_MASTER_DATA, false);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_OPERATIONAL_TABLE, false);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_RESULT_TABLE, false);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_METADATA_TABLE, false);
        defaultValueAttributesMap.put(DSPDMConstants.BoAttrName.IS_REFERENCE_TABLE, false);

        List<DynamicDTO> allBusinessObjects = dynamicReadService.readMetadataAllBusinessObjects(executionContext);
        List<String> allBusinessObjectNames = CollectionUtils.getStringValuesFromList(allBusinessObjects, DSPDMConstants.BoAttrName.BO_NAME);
        String boName = null;
        String entity = null;
        String sequenceName = null;
        // create an empty list and add each successful validated in it to be used in relationships as a parent
        List<DynamicDTO> alreadyValidatedBusinessObjects = new ArrayList<>(businessObjectListToGenerateMetadata.size());
        for (DynamicDTO businessObjectDynamicDTO : businessObjectListToGenerateMetadata) {
            boName = (String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
            if (StringUtils.isNullOrEmpty(boName)) {
                throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                        "BO name is mandatory to generate metadata for an existing table or view", executionContext.getExecutorLocale());
            } else {
                boName = boName.trim().toUpperCase();
                businessObjectDynamicDTO.put(DSPDMConstants.BoAttrName.BO_NAME, boName);
            }

            if (CollectionUtils.containsIgnoreCase(allBusinessObjectNames, boName)) {
                throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                        "Cannot generate metadata for a table or view, Business object '{}' already exists",
                        executionContext.getExecutorLocale(), boName);
            } else {
                List<DynamicDTO> businessObjectsWithSameName = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(businessObjectListToGenerateMetadata, DSPDMConstants.BoAttrName.BO_NAME, boName);
                if (businessObjectsWithSameName.size() > 1) {
                    throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                            "Cannot generate metadata for a table or view with business object name '{}' more than once",
                            executionContext.getExecutorLocale(), boName);
                }
            }

            // bo name is found now validate all other mandatory fields for business object table
            for (String mandatoryAttribute : mandatoryAttributes) {
                Object mandatoryAttributeValue = businessObjectDynamicDTO.get(mandatoryAttribute);
                if (mandatoryAttributeValue == null) {
                    throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                            "Cannot generate metadata for a table or view, '{}' is mandatory to add generate metadata for business object '{}'",
                            executionContext.getExecutorLocale(), mandatoryAttribute, boName);
                }
                if ((mandatoryAttributeValue instanceof String) && (StringUtils.isNullOrEmpty((String) mandatoryAttributeValue))) {
                    throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                            "Cannot generate metadata for a table or view, '{}' is mandatory to add generate metadata for business object '{}'",
                            executionContext.getExecutorLocale(), mandatoryAttribute, boName);
                }
            }
            // putting default values if not exist already
            for (String defaultValueAttributeKey : defaultValueAttributesMap.keySet()) {
                Object defaultValueAttributeValue = businessObjectDynamicDTO.get(defaultValueAttributeKey);
                if (defaultValueAttributeValue == null) {
                    businessObjectDynamicDTO.put(defaultValueAttributeKey, defaultValueAttributesMap.get(defaultValueAttributeKey));
                }
            }

            entity = (String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY);
            entity = entity.trim().toUpperCase();
            businessObjectDynamicDTO.put(DSPDMConstants.BoAttrName.ENTITY, entity);
            // check if sequence exists then set sequence name
            sequenceName = (String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.KEY_SEQ_NAME);
            if (StringUtils.isNullOrEmpty(sequenceName)) {
                sequenceName = "seq_" + entity.toLowerCase();
            }
            try {
                logger.info("Going to verify that a database sequence with name '{}' exists or not", sequenceName);
                Integer[] nextFromSequence = dynamicReadService.getNextFromSequence(boName, sequenceName, 1, executionContext);
                businessObjectDynamicDTO.put(DSPDMConstants.BoAttrName.KEY_SEQ_NAME, sequenceName);
                logger.info("Database sequence found with name '{}' and set in business object '{}'. Next sequence number is '{}'", sequenceName, boName, nextFromSequence[0]);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                logger.info("No database sequence exists with name '{}'", sequenceName);
            }
            // Adding dbName and schemaName to the dto that is going to be used in DAO layer
            businessObjectDynamicDTO.put(DSPDMConstants.DATABASE_NAME, dynamicReadService.getCurrentUserDBName(executionContext));
            businessObjectDynamicDTO.put(DSPDMConstants.SCHEMA_NAME, dynamicReadService.getCurrentUserDBSchemaName(executionContext));
            // add to validation done list
            alreadyValidatedBusinessObjects.add(businessObjectDynamicDTO);
        }
    }

    /**
     * validated the request
     *
     * @param businessObjectListToDeleteMetadata
     * @param dynamicReadService
     * @param executionContext
     */
    public static void validateDeleteMetadataForExistingTable(List<DynamicDTO> businessObjectListToDeleteMetadata,
                                                              IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        List<DynamicDTO> allBusinessObjects = dynamicReadService.readMetadataAllBusinessObjects(executionContext);
        List<String> allBusinessObjectNames = CollectionUtils.getStringValuesFromList(allBusinessObjects, DSPDMConstants.BoAttrName.BO_NAME);
        String boName = null;
        for (DynamicDTO businessObjectDynamicDTO : businessObjectListToDeleteMetadata) {
            boName = (String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
            if (StringUtils.isNullOrEmpty(boName)) {
                throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                        "BO name is mandatory to delete metadata for an existing table or view", executionContext.getExecutorLocale());
            }
            if (!(CollectionUtils.containsIgnoreCase(allBusinessObjectNames, boName))) {
                throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                        "Cannot delete metadata. BO name '{}' does not exist.", executionContext.getExecutorLocale(), boName);
            }
        }
        Set<String> givenBusinessObjectNames = CollectionUtils.getValuesSetFromList(allBusinessObjects, DSPDMConstants.BoAttrName.BO_NAME);
        if (allBusinessObjects.size() != givenBusinessObjectNames.size()) {
            throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                    "Cannot delete metadata. Business object name cannot be repeated.", executionContext.getExecutorLocale(), boName);
        }
    }

    public static void validateGenerateMetadataForAll(DynamicDTO businessObjectToGenerateMetadataForAll,
                                                      IDynamicReadService dynamicReadService, ExecutionContext executionContext) {

        String overwritePolicy = (String) businessObjectToGenerateMetadataForAll.get(DSPDMConstants.OVERWRITE_POLICY);
        if (StringUtils.isNullOrEmpty(overwritePolicy)) {
            throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                    "'{}' is mandatory to generate metadata for all", executionContext.getExecutorLocale(), DSPDMConstants.OVERWRITE_POLICY);
        }
        if (overwritePolicy.equalsIgnoreCase(DSPDMConstants.SKIP) || overwritePolicy.equalsIgnoreCase(DSPDMConstants.ABORT)
                || overwritePolicy.equalsIgnoreCase(DSPDMConstants.OVERWRITE)) {
            String tableType = (String) businessObjectToGenerateMetadataForAll.get(DSPDMConstants.TABLE_TYPE);
            if (!StringUtils.isNullOrEmpty(tableType)) {
                if (tableType.equalsIgnoreCase(DSPDMConstants.TABLE) || tableType.equalsIgnoreCase(DSPDMConstants.VIEW) || tableType.equalsIgnoreCase(DSPDMConstants.TABLE_VIEW)) {

                } else {
                    throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                            "Invalid '{}'. Only  ({}), ({}) or ({}) is allowed", executionContext.getExecutorLocale(),
                            DSPDMConstants.TABLE_TYPE,
                            DSPDMConstants.TABLE,
                            DSPDMConstants.VIEW,
                            DSPDMConstants.TABLE_VIEW);
                }
            } else {
                throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                        "'{}' is mandatory to generate metadata for all", executionContext.getExecutorLocale(), DSPDMConstants.TABLE_TYPE);
            }
        } else {
            throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                    "Invalid '{}'. Only  ({}), ({}) or ({}) is allowed", executionContext.getExecutorLocale(),
                    DSPDMConstants.OVERWRITE_POLICY,
                    DSPDMConstants.SKIP,
                    DSPDMConstants.ABORT,
                    DSPDMConstants.OVERWRITE);
        }
        if (StringUtils.isNullOrEmpty((String) businessObjectToGenerateMetadataForAll.get(DSPDMConstants.DATABASE_NAME))) {
            businessObjectToGenerateMetadataForAll.put(DSPDMConstants.DATABASE_NAME, dynamicReadService.getCurrentUserDBName(executionContext));
        }
        if (StringUtils.isNullOrEmpty((String) businessObjectToGenerateMetadataForAll.get(DSPDMConstants.SCHEMA_NAME))) {
            businessObjectToGenerateMetadataForAll.put(DSPDMConstants.SCHEMA_NAME, dynamicReadService.getCurrentUserDBSchemaName(executionContext));
        }
    }

    public static void validateDeleteMetadataForAll(DynamicDTO businessObjectToGenerateMetadataForAll,
                                                    IDynamicReadService dynamicReadService, ExecutionContext executionContext) {

        String tableType = (String) businessObjectToGenerateMetadataForAll.get(DSPDMConstants.TABLE_TYPE);
        if (!StringUtils.isNullOrEmpty(tableType)) {
            if (tableType.equalsIgnoreCase(DSPDMConstants.TABLE)
                    || tableType.equalsIgnoreCase(DSPDMConstants.VIEW)
                    || tableType.equalsIgnoreCase(DSPDMConstants.TABLE_VIEW)) {

            } else {
                throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                        "Invalid '{}'. Only  ({}), ({}) or ({}) is allowed", executionContext.getExecutorLocale(),
                        DSPDMConstants.TABLE_TYPE,
                        DSPDMConstants.TABLE,
                        DSPDMConstants.VIEW,
                        DSPDMConstants.TABLE_VIEW);
            }
        } else {
            throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                    "'{}' is mandatory to generate metadata for all", executionContext.getExecutorLocale(), DSPDMConstants.TABLE_TYPE);
        }
        if (StringUtils.isNullOrEmpty((String) businessObjectToGenerateMetadataForAll.get(DSPDMConstants.DATABASE_NAME))) {
            businessObjectToGenerateMetadataForAll.put(DSPDMConstants.DATABASE_NAME, dynamicReadService.getCurrentUserDBName(executionContext));
        }
        if (StringUtils.isNullOrEmpty((String) businessObjectToGenerateMetadataForAll.get(DSPDMConstants.SCHEMA_NAME))) {
            businessObjectToGenerateMetadataForAll.put(DSPDMConstants.SCHEMA_NAME, dynamicReadService.getCurrentUserDBSchemaName(executionContext));
        }
    }

    public static Set<String> generateExclusionList(DynamicDTO dynamicDTO, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        TreeSet<String> finalExclusionList = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
//        Map<String, BusinessObjectDTO> businessObjectsMap = DynamicDAOFactory.getInstance(executionContext).getBusinessObjectsMap(executionContext);
        List<DynamicDTO> metadataAllBusinessObjects = dynamicReadService.readMetadataAllBusinessObjects(executionContext);
        Map<String, DynamicDTO> businessObjectsMap = CollectionUtils.prepareIgnoreCaseMapFromStringValuesOfKey(metadataAllBusinessObjects, DSPDMConstants.BoAttrName.BO_NAME);
        ArrayList<String> tempExclusionList = new ArrayList<>();
        tempExclusionList.addAll(DSPDMConstants.NO_CHANGE_TRACK_BO_NAMES);
        tempExclusionList.addAll(DSPDMConstants.SEARCH_DB_BO_NAMES);
        tempExclusionList.addAll(DSPDMConstants.SERVICE_DB_BO_NAMES);
        for (String boName : tempExclusionList) {
            String entityName = (String) businessObjectsMap.get(boName).get(DSPDMConstants.BoAttrName.ENTITY);
            if (StringUtils.hasValue(entityName)) {
                finalExclusionList.add(entityName.toLowerCase());
            }
        }
        List<String> userProvidedExclusionList = (List<String>) dynamicDTO.get(DSPDMConstants.EXCLUSION_LIST);
        boolean userProvidedExclusionListHasValue = CollectionUtils.hasValue(userProvidedExclusionList);
        for (Map.Entry<String, DynamicDTO> entry : businessObjectsMap.entrySet()) {
            String boName = entry.getKey();
            DynamicDTO businessObjectDynamicDTO = entry.getValue();
            if (Boolean.TRUE.equals(businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.IS_METADATA_TABLE))) {
                finalExclusionList.add((String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY));
            } else if (userProvidedExclusionListHasValue) {
                if (CollectionUtils.containsIgnoreCase(userProvidedExclusionList, boName)) {
                    // replace bo name with entity/table name
                    userProvidedExclusionList.set(CollectionUtils.indexOfIgnoreCase(userProvidedExclusionList, boName),
                            (String) businessObjectDynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY));
                }
            }
        }
        if (userProvidedExclusionListHasValue) {
            finalExclusionList.addAll(userProvidedExclusionList);
        }
        return finalExclusionList;
    }

    public static void verifyOverwritePolicyForGenerateMetadataForAll(List<String> tablesNamesGeneratedFromDB,
                                                                      String overwritePolicy, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        List<DynamicDTO> businessObjectsList = dynamicReadService.readMetadataAllBusinessObjects(executionContext);
        List<String> physicalTablesNamesFromMetadata = CollectionUtils.getStringValuesFromList(businessObjectsList, DSPDMConstants.BoAttrName.ENTITY);
        Iterator<String> iterator = tablesNamesGeneratedFromDB.iterator();
        while (iterator.hasNext()) {
            String tableNameGeneratedFromDB = iterator.next();
            if (CollectionUtils.containsIgnoreCase(physicalTablesNamesFromMetadata, tableNameGeneratedFromDB)) {
                if (overwritePolicy.equalsIgnoreCase(DSPDMConstants.ABORT)) {
                    throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST,
                            "Metadata already available for table or view '{}'. Since overwrite policy is abort, aborting operation", executionContext.getExecutorLocale(),
                            tableNameGeneratedFromDB);
                } else if (overwritePolicy.equalsIgnoreCase(DSPDMConstants.SKIP)) {
                    // since overwrite policy is to skipp, so if metadata is already available, skipping it by removing it from dtoList
                    iterator.remove();
                }
            }
        }
    }

    public static List<DynamicDTO> checkOverwriteForDeleteAllMetadata(List<DynamicDTO> dynamicDTOList, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        List<String> businessObjectNames = CollectionUtils.getStringValuesFromList(
                dynamicReadService.readMetadataAllBusinessObjects(executionContext), DSPDMConstants.BoAttrName.BO_NAME);
        Iterator<DynamicDTO> iterator = dynamicDTOList.iterator();
        while (iterator.hasNext()) {
            DynamicDTO dynamicDTO = iterator.next();
            // If there's no available metadata to delete, remove from the list
            if (!CollectionUtils.containsIgnoreCase(businessObjectNames, (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME))) {
                iterator.remove();
            }
        }
        return dynamicDTOList;
    }

    public static void setSequenceNamesForGenerateMetadata(List<DynamicDTO> dynamicDTOList, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        for (DynamicDTO dynamicDTO : dynamicDTOList) {
            String boName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
            String sequenceName = "seq_" + dynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY);
            try {
                logger.info("Going to verify that a data model database sequence with name '{}' exists or not", sequenceName);
                Integer[] nextFromSequence = dynamicReadService.getNextFromSequenceFromDataModelDB(boName, sequenceName, 1, executionContext);
                dynamicDTO.put(DSPDMConstants.BoAttrName.KEY_SEQ_NAME, sequenceName);
                logger.info("Database sequence found with name '{}' and set in business object '{}'. Next sequence number is '{}'"
                        , sequenceName, dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME), nextFromSequence[0]);
            } catch (Exception e) {
                logger.info("No database sequence exists with name '{}'", sequenceName);
            }
        }
    }

    /**
     * verifies that the parent attribute is either a primary key or it is part of a unique constraint in the parent business object
     * @param childBoName
     * @param parentBoName
     * @param parentBoAttrName
     * @param parentBoAttrNameDynamicDTO
     * @param dynamicReadService
     * @param alreadyValidatedBusinessObjects
     * @param childUniqueConstraints
     * @param executionContext
     */
    private static void verifyParentAttrIsEitherPrimaryKeyOrUniqueKey(String childBoName, String parentBoName,
                                                                      String parentBoAttrName,
                                                                      DynamicDTO parentBoAttrNameDynamicDTO,
                                                                      IDynamicReadService dynamicReadService,
                                                                      List<DynamicDTO> alreadyValidatedBusinessObjects,
                                                                      List<DynamicDTO> childUniqueConstraints,
                                                                      ExecutionContext executionContext) {
        boolean isParentBoAttrNameHasUniqueConstraint = false;
        if ((parentBoAttrNameDynamicDTO != null) && ((Boolean) parentBoAttrNameDynamicDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY))) {
            isParentBoAttrNameHasUniqueConstraint = true;
        } else if (childBoName.equalsIgnoreCase(parentBoName)) {
            // recursive relationship case
            if (CollectionUtils.hasValue(childUniqueConstraints)) {
                if (childUniqueConstraints.stream().anyMatch(dynamicDTO ->
                        ((String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME)).equalsIgnoreCase(parentBoAttrName))) {
                    isParentBoAttrNameHasUniqueConstraint = true;
                }
            }
        } else {
            // checking parentBoAttrName is PK or UNIQUE from request packet - start.
            DynamicDTO parentDynamicDTO = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(alreadyValidatedBusinessObjects,
                    DSPDMConstants.BoAttrName.BO_NAME, parentBoName);
            if (parentDynamicDTO != null) {
                // means we've parent in the same request. Now will check the parentBoAttrName UNIQUE or not.
                Map<String, List<DynamicDTO>> children = (Map<String, List<DynamicDTO>>) parentDynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
                // Checking if parentBoAttr is Unique Key
                List<DynamicDTO> parentUniqueConstraints = children.get(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS);
                if (CollectionUtils.hasValue(parentUniqueConstraints)) {
                    if (parentUniqueConstraints.stream().anyMatch(dynamicDTO ->
                            ((String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME)).equalsIgnoreCase(parentBoAttrName))) {
                        isParentBoAttrNameHasUniqueConstraint = true;
                    }
                }
            } else {
                try {
                    // If we didn't find the attr to be PK or Unique in above snippet (i.e., from request packet), that means we've to check that from DB
                    List<DynamicDTO> parentUniqueConstraints = dynamicReadService.readMetadataConstraintsForBOName(parentBoName, executionContext);
                    if (CollectionUtils.hasValue(parentUniqueConstraints)) {
                        if (parentUniqueConstraints.stream().anyMatch(dynamicDTO ->
                                ((String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME)).equalsIgnoreCase(parentBoAttrName))) {
                            isParentBoAttrNameHasUniqueConstraint = true;
                        }
                    }
                } catch (Exception e) {
                    // eat exception in case of unique constraint not found in db.
                    logger.info("Unique constraint not found in DB, now searching in request packet");
                }
            }
        }
        if (!isParentBoAttrNameHasUniqueConstraint) {
            throw new DSPDMException("Parent business object '{}' attribute '{}' should either be primary key or unique key",
                    executionContext.getExecutorLocale(), parentBoName, parentBoAttrName);
        }
    }
}
