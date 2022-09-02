package com.lgc.dspdm.msp.mainservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lgc.dspdm.core.common.config.ConnectionProperties;
import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.msp.mainservice.model.BOQuery;
import com.lgc.dspdm.msp.mainservice.model.CriteriaFilter;
import com.lgc.dspdm.msp.mainservice.model.DSPDMResponse;
import org.junit.Test;

import java.util.*;

public class MetadataRelationshipTest extends BaseServiceTest {
   /* private static DSPDMLogger logger = new DSPDMLogger(MetadataRelationshipTest.class);

    *//**
     * Main unit test method to introduce a new business object by cloning the metadata of an existing business object
     * by using its bo name and creates new business object with provided new business object name
     *//*
    @Test
    public void testIntroduceDropNewBusinessObjects_WithRelationship() {
        String oldBoName = "WELL TEST";
        String newBoName = oldBoName + "_COPY_" + StringUtils.getNextRandomNumber(5);
        // create
        testIntroduceNewBusinessObjects_WithRelationship(oldBoName, newBoName);
        // drop just a relationship
        dropExistingRelationships(newBoName);
        // drop with full drop true
        dropBusinessObjects(newBoName, true);
    }

    private void testIntroduceNewBusinessObjects_WithRelationship(String oldBoName, String newBoName) {
        try {
            ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
            // prepare business object (table)
            DynamicDTO newBusinessObjectDTOToSave = cloneBOMetadataWithNewBoName(oldBoName, newBoName, executionContext);
            // add columns to the table
            newBusinessObjectDTOToSave.putWithOrder(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY, prepareAttributeRelationshipChildrenMap(oldBoName, newBoName, executionContext));
            // now prepare final request map
            Map<String, Object> businessObjectsMap = new LinkedHashMap<>(5);
            businessObjectsMap.put("language", "en");
            businessObjectsMap.put("timezone", "GMT+08:00");
            businessObjectsMap.put("data", Arrays.asList(newBusinessObjectDTOToSave));
            // now make final request json
            String body = "{\n" +
                    "  \"BUSINESS OBJECT\": " +
                    new ObjectMapper().writeValueAsString(businessObjectsMap) +
                    "}";
            // going to send request
            DSPDMResponse response = super.introduceNewBusinessObjectsPost(body);
            if (DSPDMConstants.Status.SUCCESS == response.getStatus()) {
                // print each message at appropriate log level
                response.getMessages().forEach(logger::log);
                logger.info("testIntroduceNewBusinessObjects Passed.");
            } else {
                logger.info("testIntroduceNewBusinessObjects failed.");
                throw response.getException();
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, Locale.ENGLISH);
        }
    }

    private void dropExistingRelationships(String newBoName) {
        try {
            List<DynamicDTO> relationshipNamesForChildBusinessObject = readRelationshipNamesForChildBusinessObject(newBoName);
            // now make final request json
            String body = "{\n" +
                    "    \"BUS OBJ RELATIONSHIP\": {\n" +
                    "        \"language\": \"en\",\n" +
                    "        \"readBack\": false,\n" +
                    "        \"timezone\": \"GMT+08:00\",\n" +
                    "        \"data\": " + new ObjectMapper().writeValueAsString(relationshipNamesForChildBusinessObject) +
                    "    }\n" +
                    "}";
            // going to send request
            DSPDMResponse response = super.dropRelationships(body);
            if (DSPDMConstants.Status.SUCCESS == response.getStatus()) {
                // print each message at appropriate log level
                response.getMessages().forEach(logger::log);
                logger.info("testDropRelationships passed for child bo name : {}", newBoName);
            } else {
                logger.info("testDropRelationships failed for child bo name : {}", newBoName);
                throw response.getException();
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, Locale.ENGLISH);
        }
    }

    *//**
     * reads and retuns all the relationships names for the given child business object name
     *
     * @return
     *//*
    private List<DynamicDTO> readRelationshipNamesForChildBusinessObject(String childBoName) {
        List<DynamicDTO> relationships = null;
        BOQuery boAttrEntity = new BOQuery();
        boAttrEntity.setBoName(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP);
        boAttrEntity.setLanguage("en");
        boAttrEntity.setTimezone("GMT+08:00");
        // read relationship name
        boAttrEntity.addColumnsToSelect(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME);
        boAttrEntity.setCriteriaFilters(Arrays.asList(new CriteriaFilter(DSPDMConstants.BoAttrName.CHILD_BO_NAME, Operator.EQUALS, new Object[]{childBoName})));
        boAttrEntity.setReadWithDistinct(true);
        boAttrEntity.setReadAllRecords(true);
        DSPDMResponse dspdmResponse = super.commonPost(boAttrEntity);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            PagedList<DynamicDTO> pagedList = (PagedList<DynamicDTO>) data.get(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP);
            if (CollectionUtils.hasValue(pagedList)) {
                relationships = pagedList;
            } else {
                throw new DSPDMException("No business object relationship found for child bo name '{}'",
                        Locale.ENGLISH, childBoName);
            }
        } else {
            throw new DSPDMException("No data found for reading all relationship names for child business object '{}'", Locale.ENGLISH, childBoName);
        }
        return relationships;
    }

    private void dropBusinessObjects(String newBoName, boolean fullDrop) {
        try {
            // now make final request json
            String body = "{\n" +
                    "    \"BUSINESS OBJECT\": {\n" +
                    "        \"language\": \"en\",\n" +
                    "        \"readBack\": false,\n" +
                    "        \"fullDrop\": " + fullDrop + ",\n" +
                    "        \"timezone\": \"GMT+08:00\",\n" +
                    "        \"showSQLStats\": true,\n" +
                    "        \"collectSQLScript\": 11,\n" +
                    "        \"data\": [\n" +
                    "            {\n" +
                    "                \"BO_NAME\": \"" + newBoName + "\"\n" +
                    "            }\n" +
                    "        ]\n" +
                    "    }\n" +
                    "}";
            // going to send request
            DSPDMResponse response = super.dropBusinessObjects(body);
            if (DSPDMConstants.Status.SUCCESS == response.getStatus()) {
                // print each message at appropriate log level
                response.getMessages().forEach(logger::log);
                logger.info("testDropBusinessObjects Passed with fullDrop : {}", fullDrop);
            } else {
                logger.info("testDropBusinessObjects failed with fullDrop : {}", fullDrop);
                throw response.getException();
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, Locale.ENGLISH);
        }
    }

    *//**
     * Returns and prepares all the columns details for the table to create newly.
     *
     * @param oldBoName
     * @param newBoName
     * @param executionContext
     * @return
     *//*
    private Map<String, Object> prepareAttributeRelationshipChildrenMap(String oldBoName, String newBoName, ExecutionContext executionContext) {
        // prepare children map
        Map<String, Object> childrenMap = new LinkedHashMap<>(1);
        // prepare attributes (columns)
        Map<String, Object> businessObjectAttributesMap = new LinkedHashMap<>(3);
        businessObjectAttributesMap.put("language", "en");
        businessObjectAttributesMap.put("timezone", "GMT+08:00");
        businessObjectAttributesMap.put("data", cloneAttributeMetadataWithNewBoName(oldBoName, newBoName, executionContext));
        childrenMap.put(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, businessObjectAttributesMap);
        // prepare relationships (columns)
        Map<String, Object> businessObjectRelationshipsMap = new LinkedHashMap<>(3);
        businessObjectRelationshipsMap.put("language", "en");
        businessObjectRelationshipsMap.put("timezone", "GMT+08:00");
        businessObjectRelationshipsMap.put("data", cloneRelationshipMetadataWithNewBoName(oldBoName, newBoName, executionContext));
        childrenMap.put(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, businessObjectRelationshipsMap);
        return childrenMap;
    }

    *//**
     * Returns a business object metadata dynamic dto.
     * First it reads the old existing business object dynamic dto by using provided first parameter
     * Then it changes the values in the dynamic dto after reading and returns
     *
     * @param boNameToReadMetadata
     * @param newBoName
     * @return
     *//*
    private DynamicDTO cloneBOMetadataWithNewBoName(String boNameToReadMetadata, String newBoName, ExecutionContext executionContext) {
        DynamicDTO dynamicDTO = readExistingBOMetadataForIntroduceNewBusinessObjects(boNameToReadMetadata, executionContext);
        dynamicDTO = new DynamicDTO(dynamicDTO, executionContext);
        String newEntityName = (newBoName.contains(" ")) ? newBoName.replaceAll(" ", "_") : newBoName;

        dynamicDTO.put(DSPDMConstants.BoAttrName.ENTITY, newEntityName);
        dynamicDTO.put(DSPDMConstants.BoAttrName.BO_NAME, newBoName);
        dynamicDTO.put(DSPDMConstants.BoAttrName.BO_DISPLAY_NAME, newBoName);
        dynamicDTO.put(DSPDMConstants.BoAttrName.BO_DESC, newBoName);
        dynamicDTO.put(DSPDMConstants.BoAttrName.KEY_SEQ_NAME, "SEQ_" + newEntityName);
        return dynamicDTO;
    }

    *//**
     * Returns a list of business object attributes metadata list for the given boName.
     * The list will not have primary key attribute in their objects
     * First it reads the old existing business object attr dynamic dto list by using provided first parameter
     * Then it changes the values in the dynamic dto after reading by new bo name and then returns
     *
     * @param boNameToReadMetadata
     * @param newBoName
     * @param executionContext
     * @return
     *//*
    private List<DynamicDTO> cloneAttributeMetadataWithNewBoName(String boNameToReadMetadata, String newBoName, ExecutionContext executionContext) {
        PagedList<DynamicDTO> pagedList = readExistingBOAttributeMetadataForIntroduceNewBusinessObjects(boNameToReadMetadata, executionContext);
        String newEntityName = (newBoName.contains(" ")) ? newBoName.replaceAll(" ", "_") : newBoName;
        List<DynamicDTO> list = new ArrayList<>(pagedList.size());
        for (DynamicDTO dynamicDTO : pagedList) {
            if (Boolean.TRUE.equals(dynamicDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY))) {
                dynamicDTO.put(DSPDMConstants.BoAttrName.BO_ATTR_NAME, newEntityName + "_ID");
                dynamicDTO.put(DSPDMConstants.BoAttrName.ATTRIBUTE, newEntityName + "_ID");
                dynamicDTO.put(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME, newEntityName + " ID");
            }
            dynamicDTO.put(DSPDMConstants.BoAttrName.ENTITY, newEntityName);
            dynamicDTO.put(DSPDMConstants.BoAttrName.BO_NAME, newBoName);
            dynamicDTO.put(DSPDMConstants.BoAttrName.BO_DISPLAY_NAME, newBoName);
            list.add(dynamicDTO);
        }
        return list;
    }

    private List<DynamicDTO> cloneRelationshipMetadataWithNewBoName(String boNameToReadMetadata, String newBoName, ExecutionContext executionContext) {
        PagedList<DynamicDTO> pagedList = readExistingBusObjRelationshipMetadataForIntroduceNewBusinessObjects(boNameToReadMetadata, executionContext);
        List<DynamicDTO> list = new ArrayList<>(pagedList.size());
        String newEntityName = (newBoName.contains(" ")) ? newBoName.replaceAll(" ", "_") : newBoName;
        Map<String, List<DynamicDTO>> groupByRelationshipName = CollectionUtils.groupDynamicDTOByPropertyValueIgnoreCase(pagedList, DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME);
        List<DynamicDTO> sameNameRelationshipsToBeCreated = null;
        for (Map.Entry<String, List<DynamicDTO>> entry : groupByRelationshipName.entrySet()) {
            sameNameRelationshipsToBeCreated = entry.getValue();
            if (sameNameRelationshipsToBeCreated.size() == 1) {
                DynamicDTO dynamicDTO = sameNameRelationshipsToBeCreated.get(0);
                dynamicDTO.put(DSPDMConstants.BoAttrName.CHILD_ENTITY_NAME, newEntityName);
                dynamicDTO.put(DSPDMConstants.BoAttrName.CHILD_BO_NAME, newBoName);
                dynamicDTO.put(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME, "FK_" + newEntityName + "_" + dynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME));
                list.add(dynamicDTO);
            } else {
                String relationshipName = "FK_" + newEntityName;
                for (DynamicDTO dynamicDTO : sameNameRelationshipsToBeCreated) {
                    relationshipName = relationshipName + "_" + dynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
                }
                // use same new relationship name for all the objects in a composite relationship
                for (DynamicDTO dynamicDTO : sameNameRelationshipsToBeCreated) {
                    dynamicDTO.put(DSPDMConstants.BoAttrName.CHILD_ENTITY_NAME, newEntityName);
                    dynamicDTO.put(DSPDMConstants.BoAttrName.CHILD_BO_NAME, newBoName);
                    dynamicDTO.put(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME, relationshipName);
                    list.add(dynamicDTO);
                }
            }
        }
        return list;
    }

    *//**
     * reads and returns the existing metadata business object for the given bo name. It will be used to clone new business object definition
     *
     * @return
     *//*
    private DynamicDTO readExistingBOMetadataForIntroduceNewBusinessObjects(String boNameToReadMetadata, ExecutionContext executionContext) {
        DynamicDTO dynamicDTO = null;
        BOQuery boAttrEntity = new BOQuery();
        boAttrEntity.setBoName(DSPDMConstants.BoName.BUSINESS_OBJECT);
        boAttrEntity.setLanguage("en");
        boAttrEntity.setTimezone("GMT+08:00");
        // read all column names and set in this one except primary key column
        boAttrEntity.setSelectList(readAllMetadataAttributeNamesForIntroduceNewBusinessObjects(DSPDMConstants.BoName.BUSINESS_OBJECT, executionContext));
        List<CriteriaFilter> criteriaFilters = new ArrayList<>(1);
        criteriaFilters.add(new CriteriaFilter(DSPDMConstants.BoAttrName.BO_NAME, Operator.EQUALS, new Object[]{boNameToReadMetadata}));
        boAttrEntity.setCriteriaFilters(criteriaFilters);
        boAttrEntity.setReadFirst(true);
        DSPDMResponse dspdmResponse = super.commonPost(boAttrEntity);

        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            PagedList<DynamicDTO> pagedList = (PagedList<DynamicDTO>) data.get(DSPDMConstants.BoName.BUSINESS_OBJECT);
            if (CollectionUtils.hasValue(pagedList)) {
                dynamicDTO = pagedList.get(0);
            } else {
                throw new DSPDMException("No business object attribute found for bo name '{}' while reading all attribute names",
                        Locale.ENGLISH, DSPDMConstants.BoName.BUSINESS_OBJECT);
            }
        } else {
            throw new DSPDMException("No data found for reading all attribute names", Locale.ENGLISH);
        }
        return dynamicDTO;
    }

    *//**
     * reads and returns the existing metadata business object attributes for the given bo name. These attributes will be used to clone new business object attributes  definition
     *
     * @return
     *//*
    private PagedList<DynamicDTO> readExistingBOAttributeMetadataForIntroduceNewBusinessObjects(String boNameToReadMetadata, ExecutionContext executionContext) {
        PagedList<DynamicDTO> pagedList = null;
        BOQuery boAttrEntity = new BOQuery();
        boAttrEntity.setBoName(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
        boAttrEntity.setLanguage("en");
        boAttrEntity.setTimezone("GMT+08:00");
        // read all column names and set in this one except primary key column
        boAttrEntity.setSelectList(readAllMetadataAttributeNamesForIntroduceNewBusinessObjects(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, executionContext));
        List<CriteriaFilter> criteriaFilters = new ArrayList<>(1);
        criteriaFilters.add(new CriteriaFilter(DSPDMConstants.BoAttrName.BO_NAME, Operator.EQUALS, new Object[]{boNameToReadMetadata}));
        boAttrEntity.setCriteriaFilters(criteriaFilters);
        boAttrEntity.setReadAllRecords(true);
        DSPDMResponse dspdmResponse = super.commonPost(boAttrEntity);

        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            pagedList = (PagedList<DynamicDTO>) data.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
            if (CollectionUtils.isNullOrEmpty(pagedList)) {
                throw new DSPDMException("No business object attribute found for bo name '{}' while reading all attribute names",
                        Locale.ENGLISH, DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
            }
        } else {
            throw new DSPDMException("No data found for reading all attribute names", Locale.ENGLISH);
        }
        return pagedList;
    }

    private PagedList<DynamicDTO> readExistingBusObjRelationshipMetadataForIntroduceNewBusinessObjects(String boNameToReadMetadata, ExecutionContext executionContext) {
        PagedList<DynamicDTO> pagedList = null;
        BOQuery boAttrEntity = new BOQuery();
        boAttrEntity.setBoName(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP);
        boAttrEntity.setLanguage("en");
        boAttrEntity.setTimezone("GMT+08:00");
        // read all column names and set in this one except primary key column
        boAttrEntity.setSelectList(readAllMetadataAttributeNamesForIntroduceNewBusinessObjects(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, executionContext));
        List<CriteriaFilter> criteriaFilters = new ArrayList<>(1);
        // find all those relationships where current business object is a child
        criteriaFilters.add(new CriteriaFilter(DSPDMConstants.BoAttrName.CHILD_BO_NAME, Operator.EQUALS, new Object[]{boNameToReadMetadata}));
        boAttrEntity.setCriteriaFilters(criteriaFilters);
        boAttrEntity.setReadAllRecords(true);
        DSPDMResponse dspdmResponse = super.commonPost(boAttrEntity);

        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            pagedList = (PagedList<DynamicDTO>) data.get(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP);
        }
        return pagedList;
    }

    *//**
     * reads and returns all the existing metadata business object attribute names for the given bo name excluding primary key.
     * These attribute names will be used to read the attributes definition to use attribute details to clone new business object attributes
     *
     * @param boName
     * @param executionContext
     * @return
     *//*
    private List<String> readAllMetadataAttributeNamesForIntroduceNewBusinessObjects(String boName, ExecutionContext executionContext) {
        List<String> attributeNames = null;
        BOQuery boAttrEntity = new BOQuery();
        boAttrEntity.setBoName(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
        boAttrEntity.setLanguage("en");
        boAttrEntity.setTimezone("GMT+08:00");
        boAttrEntity.setSelectList(Arrays.asList(DSPDMConstants.BoAttrName.BO_ATTR_NAME));
        List<CriteriaFilter> criteriaFilters = new ArrayList<>(1);
        criteriaFilters.add(new CriteriaFilter(DSPDMConstants.BoAttrName.BO_NAME, Operator.EQUALS, new Object[]{boName}));
        criteriaFilters.add(new CriteriaFilter(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY, Operator.EQUALS, new Object[]{false}));
        boAttrEntity.setCriteriaFilters(criteriaFilters);
        boAttrEntity.setReadAllRecords(true);
        DSPDMResponse dspdmResponse = super.commonPost(boAttrEntity);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            PagedList<DynamicDTO> pagedList = (PagedList<DynamicDTO>) data.get(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
            if (CollectionUtils.hasValue(pagedList)) {
                attributeNames = CollectionUtils.getStringValuesFromList(pagedList, DSPDMConstants.BoAttrName.BO_ATTR_NAME);
            } else {
                throw new DSPDMException("No business object attribute found for bo name '{}' while reading all attribute names",
                        Locale.ENGLISH, DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR);
            }
        } else {
            throw new DSPDMException("No data found for reading all attribute names", Locale.ENGLISH);
        }
        return attributeNames;
    }

    @Test
    public void testAddDropCompositeRelationship() {
        String wellTestChildBoName = "WELL TEST CHILD " + StringUtils.getNextRandomNumber(5);
        // introduce a new business object well test child. it has associated object name and start date and end date as its columns
        introduceNewBusinessObject_WellTestChild(wellTestChildBoName);
        // add composite relationship associated object name and start date and end date
        addSimplePKRelationshipToExistingBusinessObject(wellTestChildBoName);
        addCompositeRelationshipToExistingBusinessObject(wellTestChildBoName);
        //drop composite relationship
        dropExistingRelationships(wellTestChildBoName);
        // add composite relationship associated object name and start date and end date
        addSimplePKRelationshipToExistingBusinessObject(wellTestChildBoName);
        addCompositeRelationshipToExistingBusinessObject(wellTestChildBoName);
        // drop with full drop true
        dropBusinessObjects(wellTestChildBoName, true);
    }

    private void introduceNewBusinessObject_WellTestChild(String boName) {
        try {
            // now make final request json
            String body = "{\n" +
                    "    \"BUSINESS OBJECT\": {\n" +
                    "        \"language\": \"en\",\n" +
                    "        \"readBack\": true,\n" +
                    "        \"timezone\": \"GMT+08:00\",\n" +
                    "        \"showSQLStats\": true,\n" +
                    "        \"collectSQLScript\": 11,\n" +
                    "        \"data\": [\n" +
                    "            {\n" +
                    "                \"BO_NAME\": \"" + boName + "\",\n" +
                    "                \"BO_DISPLAY_NAME\": \"" + boName + "\",\n" +
                    "                \"ENTITY\": \"" + boName + "\",\n" +
                    "                \"BO_DESC\": \"" + boName + "\",\n" +
                    "                \"KEY_SEQ_NAME\": \"SEQ_" + boName + "\",\n" +
                    "                \"IS_MASTER_DATA\": true,\n" +
                    "                \"IS_OPERATIONAL_TABLE\": false,\n" +
                    "                \"IS_RESULT_TABLE\": false,\n" +
                    "                \"IS_METADATA_TABLE\": false,\n" +
                    "                \"IS_REFERENCE_TABLE\": false,\n" +
                    "                \"IS_ACTIVE\": true,\n" +
                    "                \"children\": {\n" +
                    "                    \"BUSINESS OBJECT ATTR\": {\n" +
                    "                        \"language\": \"en\",\n" +
                    "                        \"readBack\": false,\n" +
                    "                        \"timezone\": \"GMT+08:00\",\n" +
                    "                        \"data\": [\n" +
                    "                            {\n" +
                    "                                \"BO_ATTR_NAME\": \"" + boName + "_ID\",\n" +
                    "                                \"ATTRIBUTE\": \"" + boName + "_ID\",\n" +
                    "                                \"ATTRIBUTE_DISPLAYNAME\": \"" + boName + " ID\",\n" +
                    "                                \"ATTRIBUTE_DATATYPE\": \"" + DSPDMConstants.DataTypes.INTEGER.getAttributeDataType(ConnectionProperties.isMSSQLServerDialect()) + "\",\n" +
                    "                                \"IS_MANDATORY\": true,\n" +
                    ((ConnectionProperties.isMSSQLServerDialect()) ?
                            "                                \"ATTRIBUTE_DEFAULT\": \"NEXT VALUE FOR [seq_" + boName.replaceAll(" ", "_") + "]\",\n" :
                            "                                \"ATTRIBUTE_DEFAULT\": \"nextval('seq_" + boName.replaceAll(" ", "_") + "'::regclass)\",\n") +
                    "                                \"IS_UPLOAD_NEEDED\": false,\n" +
                    "                                \"IS_HIDDEN\": true,\n" +
                    "                                \"IS_REFERENCE_IND\": false,\n" +
                    "                                \"IS_PRIMARY_KEY\": true,\n" +
                    "                                \"IS_ACTIVE\": true,\n" +
                    "                                \"IS_SORTABLE\": true,\n" +
                    "                                \"IS_INTERNAL\": true\n" +
                    "                            },\n" +
                    "                            {\n" +
                    "                                \"BO_ATTR_NAME\": \"" + boName + "_NAME\",\n" +
                    "                                \"ATTRIBUTE\": \"" + boName + "_NAME\",\n" +
                    "                                \"ATTRIBUTE_DISPLAYNAME\": \"" + boName + " Name\",\n" +
                    "                                \"ATTRIBUTE_DATATYPE\": \"" + DSPDMConstants.DataTypes.CHARACTER_VARYING.getAttributeDataType(ConnectionProperties.isMSSQLServerDialect()) + "(50)\",\n" +
                    "                                \"IS_MANDATORY\": true,\n" +
                    "                                \"ATTRIBUTE_DEFAULT\": \"char attr default test\",\n" +
                    "                                \"IS_UPLOAD_NEEDED\": true,\n" +
                    "                                \"IS_HIDDEN\": false,\n" +
                    "                                \"IS_REFERENCE_IND\": false,\n" +
                    "                                \"IS_PRIMARY_KEY\": false,\n" +
                    "                                \"IS_ACTIVE\": true,\n" +
                    "                                \"IS_SORTABLE\": true,\n" +
                    "                                \"IS_INTERNAL\": false,\n" +
                    "                                \"RELATED_BO_ATTR_NAME\": \"" + boName + "_ID\"\n" +
                    "                            },\n" +
                    "                            {\n" +
                    "                                \"BO_ATTR_NAME\": \"WELL_TEST_ID\",\n" +
                    "                                \"ATTRIBUTE\": \"WELL_TEST_ID\",\n" +
                    "                                \"ATTRIBUTE_DISPLAYNAME\": \"WELL TEST ID\",\n" +
                    "                                \"ATTRIBUTE_DATATYPE\": \"" + DSPDMConstants.DataTypes.INTEGER.getAttributeDataType(ConnectionProperties.isMSSQLServerDialect()) + "\",\n" +
                    "                                \"IS_MANDATORY\": true,\n" +
                    "                                \"IS_UPLOAD_NEEDED\": false,\n" +
                    "                                \"IS_HIDDEN\": true,\n" +
                    "                                \"IS_REFERENCE_IND\": false,\n" +
                    "                                \"IS_PRIMARY_KEY\": false,\n" +
                    "                                \"IS_ACTIVE\": true,\n" +
                    "                                \"IS_SORTABLE\": true,\n" +
                    "                                \"IS_INTERNAL\": true,\n" +
                    "                                \"ATTRIBUTE_DESC\": \"Foreign key well test id\"\n" +
                    "                            },\n" +
                    "                            {\n" +
                    "                                \"BO_ATTR_NAME\": \"REPORTING_ENTITY_ID\",\n" +
                    "                                \"BO_DISPLAY_NAME\": \"Well Test\",\n" +
                    "                                \"ATTRIBUTE\": \"REPORTING_ENTITY_ID\",\n" +
                    "                                \"ATTRIBUTE_DISPLAYNAME\": \"REPORTING_ENTITY_ID\",\n" +
                    "                                \"ATTRIBUTE_DATATYPE\": \"" + DSPDMConstants.DataTypes.INTEGER.getAttributeDataType(ConnectionProperties.isMSSQLServerDialect()) + "(50)\",\n" +
                    "                                \"CONTROL_TYPE\": \"autoComplete\",\n" +
                    "                                \"IS_MANDATORY\": true,\n" +
                    "                                \"IS_UPLOAD_NEEDED\": true,\n" +
                    "                                \"IS_HIDDEN\": false,\n" +
                    "                                \"IS_REFERENCE_IND\": false,\n" +
                    "                                \"IS_PRIMARY_KEY\": false,\n" +
                    "                                \"IS_ACTIVE\": true,\n" +
                    "                                \"IS_SORTABLE\": true,\n" +
                    "                                \"ATTRIBUTE_DESC\": \"Reporting entity id.\",\n" +
                    "                                \"IS_INTERNAL\": false,\n" +
                    "                                \"IS_READ_ONLY\": false,\n" +
                    "                                \"IS_CUSTOM_ATTRIBUTE\": false\n" +
                    "                            },\n" +
                    "                            {\n" +
                    "                                \"BO_ATTR_NAME\": \"START_DATE\",\n" +
                    "                                \"BO_DISPLAY_NAME\": \"Well Test\",\n" +
                    "                                \"ATTRIBUTE\": \"START_DATE\",\n" +
                    "                                \"ATTRIBUTE_DISPLAYNAME\": \"Start Date\",\n" +
                    "                                \"ATTRIBUTE_DATATYPE\": \"" + DSPDMConstants.DataTypes.TIMESTAMP_WITHOUT_TIMEZONE.getAttributeDataType(ConnectionProperties.isMSSQLServerDialect()) + "\",\n" +
                    "                                \"CONTROL_TYPE\": \"calenderWithDateTime\",\n" +
                    ((ConnectionProperties.isMSSQLServerDialect()) ?
                            "                                \"ATTRIBUTE_DEFAULT\": \"SYSDATETIME()\",\n" :
                            "                                \"ATTRIBUTE_DEFAULT\": \"now()\",\n") +
                    "                                \"IS_MANDATORY\": true,\n" +
                    "                                \"IS_UPLOAD_NEEDED\": true,\n" +
                    "                                \"IS_HIDDEN\": false,\n" +
                    "                                \"IS_REFERENCE_IND\": false,\n" +
                    "                                \"IS_PRIMARY_KEY\": false,\n" +
                    "                                \"IS_ACTIVE\": true,\n" +
                    "                                \"IS_SORTABLE\": true,\n" +
                    "                                \"ATTRIBUTE_DESC\": \"START TIME: The start date of the stable flow of  this well test.\",\n" +
                    "                                \"IS_INTERNAL\": false,\n" +
                    "                                \"IS_READ_ONLY\": false,\n" +
                    "                                \"IS_CUSTOM_ATTRIBUTE\": false\n" +
                    "                            },\n" +
                    "                            {\n" +
                    "                                \"BO_ATTR_NAME\": \"END_DATE\",\n" +
                    "                                \"BO_DISPLAY_NAME\": \"Well Test\",\n" +
                    "                                \"ATTRIBUTE\": \"END_DATE\",\n" +
                    "                                \"ATTRIBUTE_DISPLAYNAME\": \"End Date\",\n" +
                    "                                \"ATTRIBUTE_DATATYPE\": \"" + DSPDMConstants.DataTypes.TIMESTAMP_WITHOUT_TIMEZONE.getAttributeDataType(ConnectionProperties.isMSSQLServerDialect()) + "\",\n" +
                    "                                \"CONTROL_TYPE\": \"calenderWithDateTime\",\n" +
                    ((ConnectionProperties.isMSSQLServerDialect()) ?
                            "                                \"ATTRIBUTE_DEFAULT\": \"SYSDATETIME()\",\n" :
                            "                                \"ATTRIBUTE_DEFAULT\": \"now()\",\n") +
                    "                                \"IS_MANDATORY\": true,\n" +
                    "                                \"IS_UPLOAD_NEEDED\": true,\n" +
                    "                                \"IS_HIDDEN\": false,\n" +
                    "                                \"IS_REFERENCE_IND\": false,\n" +
                    "                                \"IS_PRIMARY_KEY\": false,\n" +
                    "                                \"IS_ACTIVE\": true,\n" +
                    "                                \"IS_SORTABLE\": true,\n" +
                    "                                \"ATTRIBUTE_DESC\": \"START TIME: The end date of the stable flow of  this well test.\",\n" +
                    "                                \"IS_INTERNAL\": false,\n" +
                    "                                \"IS_READ_ONLY\": false,\n" +
                    "                                \"IS_CUSTOM_ATTRIBUTE\": false\n" +
                    "                            }\n" +
                    "                        ]\n" +
                    "                    }\n" +
                    "                }\n" +
                    "            }\n" +
                    "        ]\n" +
                    "    }\n" +
                    "}";
            // going to send request
            DSPDMResponse response = super.introduceNewBusinessObjectsPost(body);
            if (DSPDMConstants.Status.SUCCESS == response.getStatus()) {
                // print each message at appropriate log level
                response.getMessages().forEach(logger::log);
                logger.info("introduceNewBusinessObject_WellTestChild Passed.");
            } else {
                logger.info("introduceNewBusinessObject_WellTestChild failed.");
                throw response.getException();
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, Locale.ENGLISH);
        }
    }

    private void addSimplePKRelationshipToExistingBusinessObject(String boName) {
        try {
            // now make final request json
            String body = "{\n" +
                    "    \"BUS OBJ RELATIONSHIP\": {\n" +
                    "        \"language\": \"en\",\n" +
                    "        \"readBack\": true,\n" +
                    "        \"timezone\": \"GMT+08:00\",\n" +
                    "        \"showSQLStats\": true,\n" +
                    "        \"collectSQLScript\": 11,\n" +
                    "        \"data\": [\n" +
                    // simple relationship
                    "            {\n" +
                    "                \"BUS_OBJ_RELATIONSHIP_NAME\": \"FK_" + boName + "_WELL_TEST_ID\",\n" +
                    "                \"CHILD_BO_NAME\": \"" + boName + "\",\n" +
                    "                \"CHILD_BO_ATTR_NAME\": \"WELL_TEST_ID\",\n" +
                    "                \"PARENT_BO_NAME\": \"WELL TEST\",\n" +
                    "                \"PARENT_BO_ATTR_NAME\": \"WELL_TEST_ID\"\n" +
                    "            }\n" +
                    "        ]\n" +
                    "    }\n" +
                    "}";
            // going to send request
            DSPDMResponse response = super.addRelationships(body);
            if (DSPDMConstants.Status.SUCCESS == response.getStatus()) {
                // print each message at appropriate log level
                response.getMessages().forEach(logger::log);
                logger.info("testAddCompositeRelationshipToExistingBusinessObject passed.");
            } else {
                logger.info("testAddCompositeRelationshipToExistingBusinessObject failed");
                throw response.getException();
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, Locale.ENGLISH);
        }
    }

    private void addCompositeRelationshipToExistingBusinessObject(String boName) {
        try {
            // now make final request json
            String body = "{\n" +
                    "    \"BUS OBJ RELATIONSHIP\": {\n" +
                    "        \"language\": \"en\",\n" +
                    "        \"readBack\": true,\n" +
                    "        \"timezone\": \"GMT+08:00\",\n" +
                    "        \"showSQLStats\": true,\n" +
                    "        \"collectSQLScript\": 11,\n" +
                    "        \"data\": [\n" +
                    // composite relationship
                    "            {\n" +
                    "                \"BUS_OBJ_RELATIONSHIP_NAME\": \"FK_" + boName + "_REPORTING_ENTITY_ID_START_DATE_END_DATE\",\n" +
                    "                \"CHILD_BO_NAME\": \"" + boName + "\",\n" +
                    "                \"CHILD_BO_ATTR_NAME\": \"REPORTING_ENTITY_ID\",\n" +
                    "                \"PARENT_BO_NAME\": \"WELL TEST\",\n" +
                    "                \"PARENT_BO_ATTR_NAME\": \"REPORTING_ENTITY_ID\"\n" +
                    "            },\n" +
                    "            {\n" +
                    "                \"BUS_OBJ_RELATIONSHIP_NAME\": \"FK_" + boName + "_REPORTING_ENTITY_ID_START_DATE_END_DATE\",\n" +
                    "                \"CHILD_BO_NAME\": \"" + boName + "\",\n" +
                    "                \"CHILD_BO_ATTR_NAME\": \"START_DATE\",\n" +
                    "                \"PARENT_BO_NAME\": \"WELL TEST\",\n" +
                    "                \"PARENT_BO_ATTR_NAME\": \"START_DATE\"\n" +
                    "            },\n" +
                    "            {\n" +
                    "                \"BUS_OBJ_RELATIONSHIP_NAME\": \"FK_" + boName + "_REPORTING_ENTITY_ID_START_DATE_END_DATE\",\n" +
                    "                \"CHILD_BO_NAME\": \"" + boName + "\",\n" +
                    "                \"CHILD_BO_ATTR_NAME\": \"END_DATE\",\n" +
                    "                \"PARENT_BO_NAME\": \"WELL TEST\",\n" +
                    "                \"PARENT_BO_ATTR_NAME\": \"END_DATE\"\n" +
                    "            },\n" +
                    // recursive relationship
                    "            {\n" +
                    "                \"BUS_OBJ_RELATIONSHIP_NAME\": \"FK_" + boName + "_WELL_TEST_ID_WELL_TEST_CHILD_ID\",\n" +
                    "                \"CHILD_BO_NAME\": \"" + boName + "\",\n" +
                    "                \"CHILD_BO_ATTR_NAME\": \"WELL_TEST_ID\",\n" +
                    "                \"PARENT_BO_NAME\": \"" + boName + "\",\n" +
                    "                \"PARENT_BO_ATTR_NAME\": \"" + boName + "_ID\"\n" +
                    "            }" +
                    "        ]\n" +
                    "    }\n" +
                    "}";
            // going to send request
            DSPDMResponse response = super.addRelationships(body);
            if (DSPDMConstants.Status.SUCCESS == response.getStatus()) {
                // print each message at appropriate log level
                response.getMessages().forEach(logger::log);
                logger.info("testAddCompositeRelationshipToExistingBusinessObject passed.");
            } else {
                logger.info("testAddCompositeRelationshipToExistingBusinessObject failed");
                throw response.getException();
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, Locale.ENGLISH);
        }
    }

    @Test
    public void testAddDropNewBusinessObjectWithCompositeRelationship() {
        String wellTestChildBoName = "WELL TEST CHILD " + StringUtils.getNextRandomNumber(5);
        // introduce a new business object well test child. it has UWI and start time as its columns
        introduceNewBusinessObject_WellTestChild_WithCompositeRelationship(wellTestChildBoName);
        // drop with full drop true
        dropBusinessObjects(wellTestChildBoName, true);
    }

    private void introduceNewBusinessObject_WellTestChild_WithCompositeRelationship(String boName) {
        try {
            // now make final request json
            String body = "{\n" +
                    "    \"BUSINESS OBJECT\": {\n" +
                    "        \"language\": \"en\",\n" +
                    "        \"readBack\": true,\n" +
                    "        \"timezone\": \"GMT+08:00\",\n" +
                    "        \"showSQLStats\": true,\n" +
                    "        \"collectSQLScript\": 11,\n" +
                    "        \"data\": [\n" +
                    "            {\n" +
                    "                \"BO_NAME\": \"" + boName + "\",\n" +
                    "                \"BO_DISPLAY_NAME\": \"" + boName + "\",\n" +
                    "                \"ENTITY\": \"" + boName + "\",\n" +
                    "                \"BO_DESC\": \"" + boName + "\",\n" +
                    "                \"KEY_SEQ_NAME\": \"SEQ_" + boName + "\",\n" +
                    "                \"IS_MASTER_DATA\": true,\n" +
                    "                \"IS_OPERATIONAL_TABLE\": false,\n" +
                    "                \"IS_RESULT_TABLE\": false,\n" +
                    "                \"IS_METADATA_TABLE\": false,\n" +
                    "                \"IS_REFERENCE_TABLE\": false,\n" +
                    "                \"IS_ACTIVE\": true,\n" +
                    "                \"children\": {\n" +
                    "                    \"BUSINESS OBJECT ATTR\": {\n" +
                    "                        \"language\": \"en\",\n" +
                    "                        \"readBack\": false,\n" +
                    "                        \"timezone\": \"GMT+08:00\",\n" +
                    "                        \"data\": [\n" +
                    "                            {\n" +
                    "                                \"BO_ATTR_NAME\": \"" + boName + "_ID\",\n" +
                    "                                \"ATTRIBUTE\": \"" + boName + "_ID\",\n" +
                    "                                \"ATTRIBUTE_DISPLAYNAME\": \"" + boName + " ID\",\n" +
                    "                                \"ATTRIBUTE_DATATYPE\": \"" + DSPDMConstants.DataTypes.INTEGER.getAttributeDataType(ConnectionProperties.isMSSQLServerDialect()) + "\",\n" +
                    "                                \"SEQUENCE_NUM\": 1,\n" +
                    "                                \"IS_MANDATORY\": true,\n" +
                    "                                \"IS_UPLOAD_NEEDED\": false,\n" +
                    "                                \"IS_HIDDEN\": true,\n" +
                    "                                \"IS_REFERENCE_IND\": false,\n" +
                    "                                \"IS_PRIMARY_KEY\": true,\n" +
                    "                                \"IS_ACTIVE\": true,\n" +
                    "                                \"IS_SORTABLE\": true,\n" +
                    "                                \"IS_INTERNAL\": true\n" +
                    "                            },\n" +
                    "                            {\n" +
                    "                                \"BO_ATTR_NAME\": \"" + boName + "_NAME\",\n" +
                    "                                \"ATTRIBUTE\": \"" + boName + "_NAME\",\n" +
                    "                                \"ATTRIBUTE_DISPLAYNAME\": \"" + boName + " Name\",\n" +
                    "                                \"ATTRIBUTE_DATATYPE\": \"" + DSPDMConstants.DataTypes.CHARACTER_VARYING.getAttributeDataType(ConnectionProperties.isMSSQLServerDialect()) + "(50)\",\n" +
                    "                                \"SEQUENCE_NUM\": 2,\n" +
                    "                                \"IS_MANDATORY\": true,\n" +
                    "                                \"IS_UPLOAD_NEEDED\": true,\n" +
                    "                                \"IS_HIDDEN\": false,\n" +
                    "                                \"IS_REFERENCE_IND\": false,\n" +
                    "                                \"IS_PRIMARY_KEY\": false,\n" +
                    "                                \"IS_ACTIVE\": true,\n" +
                    "                                \"IS_SORTABLE\": true,\n" +
                    "                                \"IS_INTERNAL\": false,\n" +
                    "                                \"RELATED_BO_ATTR_NAME\": \"" + boName + "_ID\"\n" +
                    "                            },\n" +
                    "                            {\n" +
                    "                                \"BO_ATTR_NAME\": \"WELL_TEST_ID\",\n" +
                    "                                \"ATTRIBUTE\": \"WELL_TEST_ID\",\n" +
                    "                                \"ATTRIBUTE_DISPLAYNAME\": \"WELL TEST ID\",\n" +
                    "                                \"ATTRIBUTE_DATATYPE\": \"" + DSPDMConstants.DataTypes.INTEGER.getAttributeDataType(ConnectionProperties.isMSSQLServerDialect()) + "\",\n" +
                    "                                \"SEQUENCE_NUM\": 3,\n" +
                    "                                \"IS_MANDATORY\": true,\n" +
                    "                                \"IS_UPLOAD_NEEDED\": false,\n" +
                    "                                \"IS_HIDDEN\": true,\n" +
                    "                                \"IS_REFERENCE_IND\": false,\n" +
                    "                                \"IS_PRIMARY_KEY\": false,\n" +
                    "                                \"IS_ACTIVE\": true,\n" +
                    "                                \"IS_SORTABLE\": true,\n" +
                    "                                \"IS_INTERNAL\": true,\n" +
                    "                                \"ATTRIBUTE_DESC\": \"Foreign key well test id\"\n" +
                    "                            },\n" +
                   "                             {\n" +
                    "                                \"BO_ATTR_NAME\": \"WELL_ID\",\n" +
                    "                                \"ATTRIBUTE\": \"WELL_ID\",\n" +
                    "                                \"ATTRIBUTE_DISPLAYNAME\": \"WELL WELL_ID Attribute\",\n" +
                    "                                \"ATTRIBUTE_DATATYPE\": \"" + DSPDMConstants.DataTypes.INTEGER.getAttributeDataType(ConnectionProperties.isMSSQLServerDialect()) + "\",\n" +
                    "                                \"SEQUENCE_NUM\": 3,\n" +
                    "                                \"IS_MANDATORY\": true,\n" +
                    "                                \"IS_UPLOAD_NEEDED\": false,\n" +
                    "                                \"IS_HIDDEN\": true,\n" +
                    "                                \"IS_REFERENCE_IND\": false,\n" +
                    "                                \"IS_PRIMARY_KEY\": false,\n" +
                    "                                \"IS_ACTIVE\": true,\n" +
                    "                                \"IS_SORTABLE\": true,\n" +
                    "                                \"IS_INTERNAL\": true,\n" +
                    "                                \"ATTRIBUTE_DESC\": \"Foreign key well  id\"\n" +
                    "                            },\n" +
                    "                            {\n" +
                    "                                \"BO_ATTR_NAME\": \"REPORTING_ENTITY_ID\",\n" +
                    "                                \"BO_DISPLAY_NAME\": \"Well Test\",\n" +
                    "                                \"ATTRIBUTE\": \"REPORTING_ENTITY_ID\",\n" +
                    "                                \"ATTRIBUTE_DISPLAYNAME\": \"REPORTING_ENTITY_ID\",\n" +
                    "                                \"ATTRIBUTE_DATATYPE\": \"" + DSPDMConstants.DataTypes.INTEGER.getAttributeDataType(ConnectionProperties.isMSSQLServerDialect()) + "(50)\",\n" +
                    "                                \"CONTROL_TYPE\": \"autoComplete\",\n" +
                    "                                \"SEQUENCE_NUM\": 4,\n" +
                    "                                \"IS_MANDATORY\": true,\n" +
                    "                                \"IS_UPLOAD_NEEDED\": true,\n" +
                    "                                \"IS_HIDDEN\": false,\n" +
                    "                                \"IS_REFERENCE_IND\": false,\n" +
                    "                                \"IS_PRIMARY_KEY\": false,\n" +
                    "                                \"IS_ACTIVE\": true,\n" +
                    "                                \"IS_SORTABLE\": true,\n" +
                    "                                \"ATTRIBUTE_DESC\": \"Reporting entity id.\",\n" +
                    "                                \"IS_INTERNAL\": false,\n" +
                    "                                \"IS_READ_ONLY\": false,\n" +
                    "                                \"IS_CUSTOM_ATTRIBUTE\": false\n" +
                    "                            },\n" +
                    "                            {\n" +
                    "                                \"BO_ATTR_NAME\": \"START_DATE\",\n" +
                    "                                \"BO_DISPLAY_NAME\": \"Well Test\",\n" +
                    "                                \"ATTRIBUTE\": \"START_DATE\",\n" +
                    "                                \"ATTRIBUTE_DISPLAYNAME\": \"Start Date\",\n" +
                    "                                \"ATTRIBUTE_DATATYPE\": \"" + DSPDMConstants.DataTypes.TIMESTAMP_WITHOUT_TIMEZONE.getAttributeDataType(ConnectionProperties.isMSSQLServerDialect()) + "\",\n" +
                    "                                \"CONTROL_TYPE\": \"calenderWithDateTime\",\n" +
                    "                                \"SEQUENCE_NUM\": 5,\n" +
                    "                                \"IS_MANDATORY\": true,\n" +
                    "                                \"IS_UPLOAD_NEEDED\": true,\n" +
                    "                                \"IS_HIDDEN\": false,\n" +
                    "                                \"IS_REFERENCE_IND\": false,\n" +
                    "                                \"IS_PRIMARY_KEY\": false,\n" +
                    "                                \"IS_ACTIVE\": true,\n" +
                    "                                \"IS_SORTABLE\": true,\n" +
                    "                                \"ATTRIBUTE_DESC\": \"START DATE: The start date of the stable flow of  this well test.\",\n" +
                    "                                \"IS_INTERNAL\": false,\n" +
                    "                                \"IS_READ_ONLY\": false,\n" +
                    "                                \"IS_CUSTOM_ATTRIBUTE\": false\n" +
                    "                            },\n" +
                    "                            {\n" +
                    "                                \"BO_ATTR_NAME\": \"END_DATE\",\n" +
                    "                                \"BO_DISPLAY_NAME\": \"Well Test\",\n" +
                    "                                \"ATTRIBUTE\": \"END_DATE\",\n" +
                    "                                \"ATTRIBUTE_DISPLAYNAME\": \"End Date\",\n" +
                    "                                \"ATTRIBUTE_DATATYPE\": \"" + DSPDMConstants.DataTypes.TIMESTAMP_WITHOUT_TIMEZONE.getAttributeDataType(ConnectionProperties.isMSSQLServerDialect()) + "\",\n" +
                    "                                \"CONTROL_TYPE\": \"calenderWithDateTime\",\n" +
                    "                                \"SEQUENCE_NUM\": 5,\n" +
                    "                                \"IS_MANDATORY\": true,\n" +
                    "                                \"IS_UPLOAD_NEEDED\": true,\n" +
                    "                                \"IS_HIDDEN\": false,\n" +
                    "                                \"IS_REFERENCE_IND\": false,\n" +
                    "                                \"IS_PRIMARY_KEY\": false,\n" +
                    "                                \"IS_ACTIVE\": true,\n" +
                    "                                \"IS_SORTABLE\": true,\n" +
                    "                                \"ATTRIBUTE_DESC\": \"START DATE: The End date of the stable flow of  this well test.\",\n" +
                    "                                \"IS_INTERNAL\": false,\n" +
                    "                                \"IS_READ_ONLY\": false,\n" +
                    "                                \"IS_CUSTOM_ATTRIBUTE\": false\n" +
                    "                            }\n" +
                    "                        ]\n" +
                    "                    },\n" +
                    "                    \"BUS OBJ RELATIONSHIP\": {\n" +
                    "                        \"language\": \"en\",\n" +
                    "                        \"readBack\": true,\n" +
                    "                        \"timezone\": \"GMT+08:00\",\n" +
                    "                        \"showSQLStats\": true,\n" +
                    "                        \"collectSQLScript\": 11,\n" +
                    "                        \"data\": [\n" +
                    "                            {\n" +
                    "                                \"BUS_OBJ_RELATIONSHIP_NAME\": \"FK_" + boName + "_REPORTING_ENTITY_ID_START_DATE_END_DATE1\",\n" +
                    "                                \"CHILD_BO_NAME\": \"" + boName + "\",\n" +
                    "                                \"CHILD_BO_ATTR_NAME\": \"REPORTING_ENTITY_ID\",\n" +
                    "                                \"PARENT_BO_NAME\": \"WELL TEST\",\n" +
                    "                                \"PARENT_BO_ATTR_NAME\": \"REPORTING_ENTITY_ID\"\n" +
                    "                            },\n" +
                    "                            {\n" +
                    "                                \"BUS_OBJ_RELATIONSHIP_NAME\": \"FK_" + boName + "_REPORTING_ENTITY_ID_START_DATE_END_DATE1\",\n" +
                    "                                \"CHILD_BO_NAME\": \"" + boName + "\",\n" +
                    "                                \"CHILD_BO_ATTR_NAME\": \"START_DATE\",\n" +
                    "                                \"PARENT_BO_NAME\": \"WELL TEST\",\n" +
                    "                                \"PARENT_BO_ATTR_NAME\": \"START_DATE\"\n" +
                    "                            },\n" +
                    "                            {\n" +
                    "                                \"BUS_OBJ_RELATIONSHIP_NAME\": \"FK_" + boName + "_REPORTING_ENTITY_ID_START_DATE_END_DATE1\",\n" +
                    "                                \"CHILD_BO_NAME\": \"" + boName + "\",\n" +
                    "                                \"CHILD_BO_ATTR_NAME\": \"END_DATE\",\n" +
                    "                                \"PARENT_BO_NAME\": \"WELL TEST\",\n" +
                    "                                \"PARENT_BO_ATTR_NAME\": \"END_DATE\"\n" +
                    "                            },\n" +
                    "                            {\n" +
                    "                                \"BUS_OBJ_RELATIONSHIP_NAME\": \"FK_" + boName + "_WELL_TEST_ID1\",\n" +
                    "                                \"CHILD_BO_NAME\": \"" + boName + "\",\n" +
                    "                                \"CHILD_BO_ATTR_NAME\": \"WELL_TEST_ID\",\n" +
                    "                                \"PARENT_BO_NAME\": \"WELL TEST\",\n" +
                    "                                \"PARENT_BO_ATTR_NAME\": \"WELL_TEST_ID\"\n" +
                    "                            },\n" +
                    "                            {\n" +
                    "                                \"BUS_OBJ_RELATIONSHIP_NAME\": \"FK_" + boName + "_WELL_TEST_ID_WELL_TEST_CHILD_ID1\",\n" +
                    "                                \"CHILD_BO_NAME\": \"" + boName + "\",\n" +
                    "                                \"CHILD_BO_ATTR_NAME\": \"WELL_TEST_ID\",\n" +
                    "                                \"PARENT_BO_NAME\": \"" + boName + "\",\n" +
                    "                                \"PARENT_BO_ATTR_NAME\": \"" + boName + "_ID\"\n" +
                    "                            },\n" +
                    "                           {\n" +
                    "                                \"BUS_OBJ_RELATIONSHIP_NAME\": \"FK_" + boName + "_WELL_TEST_ID_WELL_TEST_CHILD_ID2\",\n" +
                    "                                \"CHILD_BO_NAME\": \"" + boName + "\",\n" +
                    "                                \"CHILD_BO_ATTR_NAME\": \"WELL_ID\",\n" +
                    "                                \"PARENT_BO_NAME\": \"" + boName + "\",\n" +
                    "                                \"PARENT_BO_ATTR_NAME\": \"" + boName + "_ID\"\n" +
                    "                            }\n" +
                    "                        ]\n" +
                    "                    }\n" +
                    "                }\n" +
                    "            }\n" +
                    "        ]\n" +
                    "    }\n" +
                    "}";
            // going to send request
            DSPDMResponse response = super.introduceNewBusinessObjectsPost(body);
            if (DSPDMConstants.Status.SUCCESS == response.getStatus()) {
                // print each message at appropriate log level
                response.getMessages().forEach(logger::log);
                logger.info("introduceNewBusinessObject_WellTestChild_WithCompositeRelationship Passed.");
            } else {
                logger.info("introduceNewBusinessObject_WellTestChild_WithCompositeRelationship failed.");
                throw response.getException();
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, Locale.ENGLISH);
        }
    }

   // @Test
    public void generateMetadataForAll() {
        try {
            deleteMetadataForAll();

            String overwritePolicy = "skip";
            String tableType = "table_view";
            ArrayList<String> exclusionList = new ArrayList<>();
            String exclude_1 = "sysdiagrams";
            String exclude_2 = "";
            //String[] exclusionList = new String[]{"sysdiagrams,electric_compressor_meas"};
            // now make final request json
            String body = "{\n" +
                    "    \"BUSINESS OBJECT\": {\n" +
                    "        \"language\": \"en\",\n" +
                    "        \"readBack\": true,\n" +
                    "        \"timezone\": \"GMT+08:00\",\n" +
                    "        \"showSQLStats\": true,\n" +
                    "        \"data\": [\n" +
                    "            {\n" +
                    "                \"overwritePolicy\": \"" + overwritePolicy + "\",\n" +
                    "                \"exclusionList\": [\"" + exclude_1 + "\",\"" + exclude_2 + "\"],\n" +
                    "                \"tableType\": \"" + tableType + "\"\n" +
                    "            }\n" +
                    "        ]\n" +
                    "    }\n" +
                    "}";
            // going to send request
            DSPDMResponse response = super.generateMetadataForAllPost(body);
            if (DSPDMConstants.Status.SUCCESS == response.getStatus()) {
                // print each message at appropriate log level
                //response.getMessages().forEach(logger::log);
                logger.info("generateMetadataForAllPost Passed.");
            } else {
                logger.info("generateMetadataForAllPost failed.");
                throw response.getException();
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, Locale.ENGLISH);
        }
    }
    //@Test
    public void deleteMetadataForAll() {
        try {
            String tableType = "table_view";
            // now make final request json
            String body = "{\n" +
                    "    \"BUSINESS OBJECT\": {\n" +
                    "        \"language\": \"en\",\n" +
                    "        \"readBack\": true,\n" +
                    "        \"timezone\": \"GMT+08:00\",\n" +
                    "        \"showSQLStats\": true,\n" +
                    "        \"data\": [\n" +
                    "            {\n" +
                    "                \"tableType\": \"" + tableType + "\"\n" +

                    "            }\n" +
                    "        ]\n" +
                    "    }\n" +
                    "}";
            // going to send request
            DSPDMResponse response = super.deleteMetadataForAllPost(body);
            if (DSPDMConstants.Status.SUCCESS == response.getStatus()) {
                // print each message at appropriate log level
                //response.getMessages().forEach(logger::log);
                logger.info("deleteMetadataForAllPost Passed.");
            } else {
                logger.info("deleteMetadataForAllPost failed.");
                throw response.getException();
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, Locale.ENGLISH);
        }
    }

    @Test
    public void testAddDeleteMetadataForExistingTable() {
        String wellTestChildBoName = "WELL TEST CHILD " + StringUtils.getNextRandomNumber(5);
        // introduce a new business object well test child. it has UWI and start time as its columns
        introduceNewBusinessObject_WellTestChild_WithCompositeRelationship(wellTestChildBoName);
        // delete metadata
        deleteMetadataForExistingTable(wellTestChildBoName);
        // generate metadata
        generateMetadataForExistingTable(wellTestChildBoName);
        // drop with full drop true
        dropBusinessObjects(wellTestChildBoName, true);
    }

    private void generateMetadataForExistingTable(String entityName) {
        try {
            // now make final request json
            String body = "{\n" +
                    "    \"BUSINESS OBJECT\": {\n" +
                    "        \"language\": \"en\",\n" +
                    "        \"readBack\": true,\n" +
                    "        \"timezone\": \"GMT+08:00\",\n" +
                    "        \"showSQLStats\": true,\n" +
                    "        \"data\": [\n" +
                    "            {\n" +
                    "                \"BO_NAME\": \"" + entityName.toUpperCase().replaceAll("_", " ") + "\",\n" +
                    "                \"BO_DISPLAY_NAME\": \"" + entityName.toUpperCase().replaceAll("_", " ") + "\",\n" +
                    "                \"ENTITY\": \"" + entityName + "\",\n" +
                    "                \"BO_DESC\": \"" + entityName.toUpperCase().replaceAll("_", " ") + "\"\n" +
                    "            }\n" +
                    "        ]\n" +
                    "    }\n" +
                    "}";
            // going to send request
            DSPDMResponse response = super.generateMetadataForExistingTablePost(body);
            if (DSPDMConstants.Status.SUCCESS == response.getStatus()) {
                // print each message at appropriate log level
                response.getMessages().forEach(logger::log);
                logger.info("generateMetadataForExistingTablePost Passed.");
            } else {
                logger.info("generateMetadataForExistingTablePost failed.");
                throw response.getException();
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, Locale.ENGLISH);
        }
    }

    private void deleteMetadataForExistingTable(String entityName) {
        try {
            // now make final request json
            String body = "{\n" +
                    "  \"BUSINESS OBJECT\": {\n" +
                    "    \"language\": \"en\",\n" +
                    "    \"readBack\": true,\n" +
                    "    \"timezone\": \"GMT+08:00\",\n" +
                    "    \"showSQLStats\": true,\n" +
                    "    \"data\": [\n" +
                    "      {\n" +
                    "        \"BO_NAME\": \"" + entityName + "\"\n" +
                    "      }\n" +
                    "    ]\n" +
                    "  }\n" +
                    "}";
            // going to send request
            DSPDMResponse response = super.deleteMetadataForExistingTablePost(body);
            if (DSPDMConstants.Status.SUCCESS == response.getStatus()) {
                // print each message at appropriate log level
                response.getMessages().forEach(logger::log);
                logger.info("deleteMetadataForExistingTable Passed.");
            } else {
                logger.info("deleteMetadataForExistingTable failed.");
                throw response.getException();
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, Locale.ENGLISH);
        }
    }*/
}
