package com.lgc.dspdm.msp.mainservice.utils.metadata;

import com.lgc.dspdm.core.common.config.ConnectionProperties;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.core.common.util.metadata.MetadataRelationshipUtils;
import com.lgc.dspdm.core.common.util.metadata.MetadataUniqueConstraintUtils;
import com.lgc.dspdm.service.common.dynamic.read.IDynamicReadService;

import java.util.*;

/**
 * It is a Helper class to validate the json request syntax and make sure that
 * the request is properly structured and fulfills the criteria to create or drop a relationship
 *
 * @author Muhammad Imran Ansari
 * @since 02-Feb-2021
 */
public class MetadataRelationshipChangeRestServiceHelper {

    /**
     * It validates that either the relationship name or the constraint name must be provided to delete a relationship
     *
     * @param busObjRelationshipsToDrop
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 02-Feb-2021
     */
    public static void validateDropBusinessObjectRelationships(List<DynamicDTO> busObjRelationshipsToDrop, ExecutionContext executionContext) {
        String relationshipName = null;
        String constraintName = null;
        for (DynamicDTO busObjRelationshipDynamicDTO : busObjRelationshipsToDrop) {
            relationshipName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME);
            constraintName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
            if ((StringUtils.isNullOrEmpty(relationshipName)) && (StringUtils.isNullOrEmpty(constraintName))) {
                throw new DSPDMException("Please provide '{}' or '{}' to drop a business object relationship.",
                        executionContext.getExecutorLocale(), DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME, DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
            }
        }
    }

    /**
     * to validate the json request syntax and make sure that the request is properly structured and fulfills the criteria to create a relationship
     *
     * @param busObjRelationshipsToCreate
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 02-Feb-2021
     */
    public static void validateAddBusinessObjectRelationships(List<DynamicDTO> busObjRelationshipsToCreate, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        String parentBoName = null;
        String childBoName = null;
        String parentBoAttrName = null;
        String childBoAttrName = null;
        String relationshipName = null;
        List<DynamicDTO> allBusinessObjects = dynamicReadService.readMetadataAllBusinessObjects(executionContext);
        List<String> allBusinessObjectNames = CollectionUtils.getStringValuesFromList(allBusinessObjects, DSPDMConstants.BoAttrName.BO_NAME);
        for (DynamicDTO busObjRelationshipDynamicDTO : busObjRelationshipsToCreate) {
            parentBoName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_NAME);
            childBoName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME);
            parentBoAttrName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME);
            childBoAttrName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
            relationshipName = (String) busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME);
            // validate mandatory attributes to define a relationship
            if (StringUtils.isNullOrEmpty(parentBoName)) {
                throw new DSPDMException("'{}' is mandatory to add a relationship", executionContext.getExecutorLocale(), DSPDMConstants.BoAttrName.PARENT_BO_NAME);
            }
            if (StringUtils.isNullOrEmpty(childBoName)) {
                throw new DSPDMException("'{}' is mandatory to add a relationship", executionContext.getExecutorLocale(), DSPDMConstants.BoAttrName.CHILD_BO_NAME);
            }
            if (StringUtils.isNullOrEmpty(parentBoAttrName)) {
                throw new DSPDMException("'{}' is mandatory to add a relationship", executionContext.getExecutorLocale(), DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME);
            }
            if (StringUtils.isNullOrEmpty(childBoAttrName)) {
                throw new DSPDMException("'{}' is mandatory to add a relationship", executionContext.getExecutorLocale(), DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
            }
            // check if relationship name is provided then its length should not be more than 200
            if ((StringUtils.hasValue(relationshipName)) && (relationshipName.length() > 200)) {
                throw new DSPDMException("'{}' length cannot be greater than 100", executionContext.getExecutorLocale(), DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME);
            }
            //make sure primary key is not provided in any of the relationship object
            if (busObjRelationshipDynamicDTO.get(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_ID) != null) {
                throw new DSPDMException("Cannot add business object relationship. Primary key '{}' must not be provided.",
                        executionContext.getExecutorLocale(), DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_ID);
            }
            // validate that parent business object exists
            if (!(CollectionUtils.containsIgnoreCase(allBusinessObjectNames, parentBoName))) {
                throw new DSPDMException("Cannot add business object relationship. Parent business object '{}' does not exist", executionContext.getExecutorLocale(), parentBoName);
            }
            // validate that child business object exists
            if (!(CollectionUtils.containsIgnoreCase(allBusinessObjectNames, childBoName))) {
                throw new DSPDMException("Cannot add business object relationship. Child business object '{}' does not exist", executionContext.getExecutorLocale(), childBoName);
            }
            // validate that parent business object attribute exists
            Map<String, DynamicDTO> parentBoAttrMetadataMap = dynamicReadService.readMetadataMapForBOName(parentBoName, executionContext);
            if (!(CollectionUtils.containsKeyIgnoreCase(parentBoAttrMetadataMap, parentBoAttrName))) {
                throw new DSPDMException("Cannot add business object relationship. Parent business object attribute name '{}' does not exist in business object '{}'.",
                        executionContext.getExecutorLocale(), parentBoAttrName, parentBoName);
            }
            // validate that child business object attribute exists
            Map<String, DynamicDTO> childBoAttrMetadataMap = dynamicReadService.readMetadataMapForBOName(childBoName, executionContext);
            if (!(CollectionUtils.containsKeyIgnoreCase(childBoAttrMetadataMap, childBoAttrName))) {
                throw new DSPDMException("Cannot add business object relationship. Child business object attribute name '{}' does not exist in business object '{}'.",
                        executionContext.getExecutorLocale(), childBoAttrName, childBoName);
            }
            // validate relationship name not already exists
            if (StringUtils.hasValue(relationshipName)) {
                validateRelationshipName(relationshipName, dynamicReadService, executionContext);
            }
            // if this is a recursive relationship then make sure both child and parent attributes are different
            if ((childBoName.equalsIgnoreCase(parentBoName)) && (childBoAttrName.equalsIgnoreCase(parentBoAttrName))) {
                throw new DSPDMException("Cannot add business object relationship. Child business object attribute '{}' and parent business object attribute '{}' cannot be same.",
                        executionContext.getExecutorLocale(), childBoAttrName, parentBoAttrName);
            }
            // parent attribute is unique now verify data type
            DynamicDTO childBoAttrNameDynamicDTO = childBoAttrMetadataMap.get(childBoAttrName);
            DynamicDTO parentBoAttrNameDynamicDTO = parentBoAttrMetadataMap.get(parentBoAttrName);
            validateParentAndChildAttributeDataType(parentBoAttrNameDynamicDTO, childBoAttrNameDynamicDTO, childBoAttrName, parentBoAttrName, executionContext);
        }
        // also verify that the parent attributes are unique in parent
        verifyParentBoAttrNamesUnique(busObjRelationshipsToCreate, dynamicReadService, executionContext);
        // now verify no relationship with same attribute set exists
        verifyRelationshipNotAlreadyExists(busObjRelationshipsToCreate, dynamicReadService, executionContext);
    }

    /**
     * verifies that the data type is same otherwise throw eror
     *
     * @param parentBoAttrNameDynamicDTO
     * @param childBoAttrNameDynamicDTO
     * @param childBoAttrName
     * @param parentBoAttrName
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 02-Feb-2021
     */
    protected static void validateParentAndChildAttributeDataType(DynamicDTO parentBoAttrNameDynamicDTO, DynamicDTO childBoAttrNameDynamicDTO, String childBoAttrName, String parentBoAttrName, ExecutionContext executionContext) {
        String parentAttributeDataType = (String) parentBoAttrNameDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE);
        DSPDMConstants.DataTypes parentDataType = DSPDMConstants.DataTypes.fromAttributeDataType(parentAttributeDataType, ConnectionProperties.isMSSQLServerDialect());
        // Now get child attribute data type
        String childAttributeDataType = (String) childBoAttrNameDynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE);
        DSPDMConstants.DataTypes childDataType = DSPDMConstants.DataTypes.fromAttributeDataType(childAttributeDataType, ConnectionProperties.isMSSQLServerDialect());

        if (childDataType != parentDataType) {
            throw new DSPDMException("Cannot add business object relationship. Child business object attribute '{}' with data type '{}' "
                    + "and parent business object attribute '{}' with data type '{}' cannot have relationship. Both attributes must have same data type.",
                    executionContext.getExecutorLocale(), childBoAttrName, childAttributeDataType, parentBoAttrName, parentAttributeDataType);
        }
    }

    /**
     * verifies that the a relationship with same name does not already exist
     *
     * @param relationshipName
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 02-Feb-2021
     */
    protected static void validateRelationshipName(String relationshipName, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        // relationship name should not have special characters
        if (!(StringUtils.isAlphaNumericOrWhitespaceUnderScore(relationshipName))) {
            throw new DSPDMException("Cannot add business object relationship. Relationship name '{}' cannot have special characters.",
                    executionContext.getExecutorLocale(), relationshipName);
        }
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, executionContext);
        businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME, relationshipName);
        if (dynamicReadService.count(businessObjectInfo, executionContext) > 0) {
            throw new DSPDMException("Cannot add business object relationship. A relationship with name '{}' already exists.",
                    executionContext.getExecutorLocale(), relationshipName);
        }
    }

    /**
     * It makes sure that the given parent attribute names are either primary key or they are unique at least
     *
     * @param busObjRelationshipsToCreate
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 02-Feb-2021
     */
    public static void verifyParentBoAttrNamesUnique(List<DynamicDTO> busObjRelationshipsToCreate, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {

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
                    verifyParentBoAttrNamesAreUniqueForRelationships(compositeRelationshipsToBeCreated, dynamicReadService, executionContext);
                }
            }
        }
        // process all simple relationships individually and independently
        if (CollectionUtils.hasValue(simpleRelationshipsToBeCreated)) {
            // this group has no relationship name it means all objects are independent and must be verified individually/indendently
            for (DynamicDTO simpleRelationship : simpleRelationshipsToBeCreated) {
                // validate parent attribute is pk or unique
                verifyParentBoAttrNamesAreUniqueForRelationships(Arrays.asList(simpleRelationship), dynamicReadService, executionContext);
            }
        }
    }

    /**
     * It makes sure that the given parent attribute names are either primary key or they are unique at least
     *
     * @param relationships
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 02-Feb-2021
     */
    private static void verifyParentBoAttrNamesAreUniqueForRelationships(List<DynamicDTO> relationships, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        if (CollectionUtils.hasValue(relationships)) {
            boolean unique = false;
            String parentBoName = (String) relationships.get(0).get(DSPDMConstants.BoAttrName.PARENT_BO_NAME);
            String[] parentBoAttrNames = CollectionUtils.getValuesFromList(relationships, DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME).toArray(new String[0]);
            Map<String, DynamicDTO> parentBoAttrMetadataMap = dynamicReadService.readMetadataMapForBOName(parentBoName, executionContext);
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
                    // verify that the parent business object attribute is defined as a unique by read all unique constraints on bo name
                    List<DynamicDTO> uniqueConstraints = dynamicReadService.readMetadataConstraintsForBOName(parentBoName, executionContext);
                    if (MetadataUniqueConstraintUtils.uniqueConstraintExistsForAttributes(uniqueConstraints, parentBoAttrNames)) {
                        unique = true;
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
     * verify relationship not already exists for same attributes and business objects
     *
     * @param busObjRelationshipsToCreate
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 02-Feb-2021
     */
    private static void verifyRelationshipNotAlreadyExists(List<DynamicDTO> busObjRelationshipsToCreate, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {

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
                    // 1. verify composite relationship not already exists for same attributes
                    verifyCompositeRelationshipNotAlreadyExists(compositeRelationshipsToBeCreated, dynamicReadService, executionContext);
                    // 2. make sure duplicate/repeated attribute does not exist in a composite relationship
                    verifyNoCompositeRelationshipHasRepeatedAttribute(compositeRelationshipsToBeCreated, dynamicReadService, executionContext);
                }
            }
        }
        // process all simple relationships individually and independently
        if (CollectionUtils.hasValue(simpleRelationshipsToBeCreated)) {
            // this group has no relationship name it means all objects are independent and must be verified individually/independently
            for (DynamicDTO simpleRelationship : simpleRelationshipsToBeCreated) {
                // verify relationship not already exists for same parent and child attributes
                verifySimpleRelationshipNotAlreadyExists(simpleRelationship, dynamicReadService, executionContext);
            }
        }
    }

    /**
     * verify that simple relationship not already exists for same attribute and business object
     *
     * @param relationship
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 02-Feb-2021
     */
    private static void verifySimpleRelationshipNotAlreadyExists(DynamicDTO relationship, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        String parentBoName = (String) relationship.get(DSPDMConstants.BoAttrName.PARENT_BO_NAME);
        String childBoName = (String) relationship.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME);
        // check all old already existing relationships for same child and parent business objects
        Map<String, List<DynamicDTO>> parentRelationshipsMap = dynamicReadService.readMetadataParentRelationshipsMap(childBoName, executionContext);
        // read from cache
        List<DynamicDTO> parentRelationships = parentRelationshipsMap.get(parentBoName);
        // if exists some relationships for same parent and child then match attributes
        // if exists some relationships for same parent and child then match attributes
        if (CollectionUtils.hasValue(parentRelationships)) {
            String parentBoAttrName = (String) relationship.get(DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME);
            String childBoAttrName = (String) relationship.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
            if (MetadataRelationshipUtils.relationshipExistsForAttributes(parentRelationships, parentBoAttrName, childBoAttrName)) {
                String relationshipName = (String) relationship.get(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME);
                if (StringUtils.hasValue(relationshipName)) {
                    throw new DSPDMException("Cannot add business object relationship '{}'. A relationship already exists for child business object attribute '{}' and parent business object attribute '{}'.",
                            executionContext.getExecutorLocale(), relationshipName, childBoAttrName, parentBoAttrName);
                } else {
                    throw new DSPDMException("Cannot add business object relationship . A relationship already exists for child business object attribute '{}' and parent business object attribute '{}'.",
                            executionContext.getExecutorLocale(), childBoAttrName, parentBoAttrName);
                }
            }
        }
    }

    /**
     * verify composite relationship not already exists for same attributes and business objects
     *
     * @param relationship
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 12-Feb-2021
     */
    private static void verifyCompositeRelationshipNotAlreadyExists(List<DynamicDTO> relationship, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        DynamicDTO firstBusObjRelationshipDTO = relationship.get(0);
        String parentBoName = (String) firstBusObjRelationshipDTO.get(DSPDMConstants.BoAttrName.PARENT_BO_NAME);
        String childBoName = (String) firstBusObjRelationshipDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME);
        // check all old already existing relationships for same child and parent business objects
        Map<String, List<DynamicDTO>> parentRelationshipsMap = dynamicReadService.readMetadataParentRelationshipsMap(childBoName, executionContext);
        // read from cache
        List<DynamicDTO> parentRelationships = parentRelationshipsMap.get(parentBoName);
        // if exists some relationships for same parent and child then match attributes
        if (CollectionUtils.hasValue(parentRelationships)) {
            List<String> parentBoAttrNames = CollectionUtils.getUpperCaseValuesFromListOfMap(relationship, DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME);
            List<String> childBoAttrNames = CollectionUtils.getUpperCaseValuesFromListOfMap(relationship, DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
            if (CollectionUtils.hasValue(parentRelationships)) {
                if (MetadataRelationshipUtils.relationshipExistsForAttributes(parentRelationships, parentBoAttrNames, childBoAttrNames)) {
                    String relationshipName = (String) firstBusObjRelationshipDTO.get(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME);
                    throw new DSPDMException("Cannot add business object relationship '{}'. A relationship already exists for child business object attributes '{}' and parent business object attributes '{}'.",
                            executionContext.getExecutorLocale(), relationshipName, CollectionUtils.getCommaSeparated(childBoAttrNames), CollectionUtils.getCommaSeparated(parentBoAttrNames));
                }
            }
        }
    }

    /**
     * Makes sure that all the attributes involved in a composite relationship are different from each other
     *
     * @param relationshipsInANamedGroup
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 12-Feb-2021
     */
    private static void verifyNoCompositeRelationshipHasRepeatedAttribute(List<DynamicDTO> relationshipsInANamedGroup, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        // now verify no two relationship dto objects have same parent or child attribute name under same relationship name
        List<String> parentBoAttrNames = CollectionUtils.getUpperCaseValuesFromListOfMap(relationshipsInANamedGroup, DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME);
        List<String> childBoAttrNames = CollectionUtils.getUpperCaseValuesFromListOfMap(relationshipsInANamedGroup, DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
        // means that all the attributes in a composite relationship must be different from each other
        TreeSet<String> parentBoAttrNamesSet = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
        TreeSet<String> childBoAttrNamesSet = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
        parentBoAttrNamesSet.addAll(parentBoAttrNames);
        childBoAttrNamesSet.addAll(childBoAttrNames);
        if (parentBoAttrNamesSet.size() != relationshipsInANamedGroup.size()) {
            String relationshipName = (String) relationshipsInANamedGroup.get(0).get(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME);
            throw new DSPDMException("Cannot add business object relationship '{}'. A parent business object attribute has been used more than once.",
                    executionContext.getExecutorLocale(), relationshipName);
        } else if (childBoAttrNamesSet.size() != relationshipsInANamedGroup.size()) {
            String relationshipName = (String) relationshipsInANamedGroup.get(0).get(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME);
            throw new DSPDMException("Cannot add business object relationship '{}'. A child business object attribute has been used more than once.",
                    executionContext.getExecutorLocale(), relationshipName);
        }
    }
}
