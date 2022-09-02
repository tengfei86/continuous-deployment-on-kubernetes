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
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class MetadataTest extends BaseServiceTest {
   /* private DSPDMLogger logger = new DSPDMLogger(MetadataTest.class);

    @Test
    public void testRefreshMetadata() {
        DSPDMResponse dspdmResponse = super.refreshMetadataGet();
        logger.info("testRefreshMetadata Passed.");
    }

    @Test
    public void testValidateMetadata() {
        DSPDMResponse dspdmResponse = super.validateMetadataGet(null);
        logger.info("testValidateMetadata Passed.");
    }

    @Test
    public void testBoHierarchy() {
        DSPDMResponse dspdmResponse = super.boHierarchyGet("WELLBORE");
        logger.info("response: ", dspdmResponse.getData().toString());
        logger.info("boHierarchy Passed.");
    }

    @Test
    public void testValidateMetadataForBoName() {
        DSPDMResponse response = super.validateMetadataGet("WELL");
        if (DSPDMConstants.Status.SUCCESS == response.getStatus()) {
            // print each message at appropriate log level
            response.getMessages().forEach(logger::log);
            response.getData().forEach((key, value) -> {
                logger.info("Key : {}, Value : {}", key, value);
            });
            logger.info("testValidateMetadata For business object WELL Passed.");
        } else {
            logger.info("testValidateMetadata For business object WELL failed.");
            DSPDMException.throwException(response.getException(), Locale.ENGLISH);
        }
    }

    @Test
    public void testValidateAndRefreshMetadata() {
        DSPDMResponse dspdmResponse = super.validateAndRefreshMetadataGet();
        logger.info("testValidateAndRefreshMetadata Passed.");
    }

    @Test
    public void testReadAllBOList() {
        DSPDMResponse dspdmResponse = super.readAllBusinessObjectsGet();
        logger.info("testReadAllBOList Passed.");
    }

    @Test
    public void testAddUpdateDeleteSimpleCustomAttribute() {
        String oldBoName = "ZONE";
        String newBoName = "ZONE" + "_COPY" + StringUtils.getNextRandomNumber(10);
        // create
        testIntroduceNewBusinessObjects(oldBoName, newBoName);

        String unitTestAttributeName = "UNIT_TEST" + java.util.UUID.randomUUID().toString().substring(30).toUpperCase();
        List<Map<String, Object>> dataTypeMaps = DSPDMConstants.DataTypes.getSupportedDataTypes(ConnectionProperties.isMSSQLServerDialect());
        String dataTypeString = null;
        boolean mssqlServerDialect = ConnectionProperties.isMSSQLServerDialect();
        boolean updatedTested = false;
        for (DSPDMConstants.DataTypes dataTypeEnum : DSPDMConstants.DataTypes.values()) {
            dataTypeString = dataTypeEnum.getAttributeDataType(mssqlServerDialect);
            if ((mssqlServerDialect) && (dataTypeEnum == DSPDMConstants.DataTypes.JSONB)) {
                continue;
            } else if (dataTypeEnum.isStringDataTypeWithLength()) {
                dataTypeString = dataTypeString + "(100)";
            } else if (dataTypeEnum.isFloatingDataType()) {
                switch (dataTypeEnum) {
                    case FLOAT:
                        dataTypeString = dataTypeString + "(24)";
                        break;
                    case REAL:
                    case DOUBLE_PRECISION:
                        dataTypeString = dataTypeString;
                        break;
                    default:
                        dataTypeString = dataTypeString + "(38,18)";
                }
            }else if((dataTypeEnum.isBinaryDataType()) && (dataTypeEnum.getMaxLength() != null) && (mssqlServerDialect) ){
                dataTypeString = dataTypeString + "(" + dataTypeEnum.getMaxLength() + ")";
            }

            try {
                this.testAddSimpleCustomAttribute(newBoName, unitTestAttributeName, dataTypeString);
                if (!updatedTested) {
                    this.testUpdateSimpleAttributeMetadata(newBoName, unitTestAttributeName);
                    updatedTested = true;
                }
                this.testDeleteSimpleCustomAttribute(newBoName, unitTestAttributeName);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                throw e;
            }
        }

        // drop with full drop true
        testDropBusinessObjects(newBoName, true);
    }

    private void testAddSimpleCustomAttribute(String boName, String unitTestAttributeName, String unitTestAttributeDataType) {
        try {
            String body = "{\n" +
                    "  \"BUSINESS OBJECT ATTR\": {\n" +
                    "    \"language\": \"en\",\n" +
                    "    \"readBack\": true,\n" +
                    "    \"timezone\": \"GMT+08:00\",\n" +
                    "    \"data\": [\n" +
                    "      {\n" +
                    "      \t\"BO_NAME\" : \"" + boName + "\",\n" +
                    "      \t\"ATTRIBUTE_DISPLAYNAME\" : \"" + unitTestAttributeName + "\",\n" +
                    "      \t\"ATTRIBUTE_DATATYPE\" : \"" + unitTestAttributeDataType + "\"\n" +
                    "      }\n" +
                    "    ]\n" +
                    "  }\n" +
                    "}";
            DSPDMResponse dspdmResponse = super.addCustomAttributesPost(body);
            Map<String, Object> data = dspdmResponse.getData();
            if (data != null) {
                String responseBoName = data.keySet().iterator().next();
                Assert.assertEquals("BUSINESS OBJECT ATTR", responseBoName);
                logger.info("testAddSimpleCustomAttribute of {} datatype Passed.", unitTestAttributeDataType);
            }
        } catch (Exception e) {
            logger.error(unitTestAttributeDataType + e.getMessage(), e);
            throw e;
        }
    }

    private void testDeleteSimpleCustomAttribute(String boName, String unitTestAttributeName) {
        try {
            String body = "{\n" +
                    "  \"BUSINESS OBJECT ATTR\": {\n" +
                    "    \"language\": \"en\",\n" +
                    "    \"readBack\": false,\n" +
                    "    \"timezone\": \"GMT+08:00\",\n" +
                    "    \"data\": [\n" +
                    "      {\n" +
                    "      \t\"BO_NAME\" : \"" + boName + "\",\n" +
                    "      \t\"BO_ATTR_NAME\" : \"" + unitTestAttributeName + "\"\n" +
                    "      }\n" +
                    "    ]\n" +
                    "  }\n" +
                    "}";
            DSPDMResponse dspdmResponse = super.deleteCustomAttributesPost(body);
            Map<String, Object> data = dspdmResponse.getData();
            if (data != null) {
                String responseBoName = data.keySet().iterator().next();
                Assert.assertEquals("BUSINESS OBJECT ATTR", responseBoName);
                logger.info("testDeleteSimpleCustomAttribute Passed.");
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }


    private void testUpdateSimpleAttributeMetadata(String boName, String unitTestAttributeName) {
        try {

            String displayName = unitTestAttributeName + "update test";
            String body = "{\n" +
                    "  \"BUSINESS OBJECT ATTR\": {\n" +
                    "    \"language\": \"en\",\n" +
                    "    \"readBack\": true,\n" +
                    "    \"timezone\": \"GMT+08:00\",\n" +
                    "    \"data\": [\n" +
                    "      {\n" +
                    "      \t\"BO_NAME\" : \"" + boName + "\",\n" +
                    "      \t\"BO_ATTR_NAME\" : \"" + unitTestAttributeName + "\",\n" +
                    "      \t\"IS_CUSTOM_ATTRIBUTE\" : \"" + true + "\",\n" +
                    "      \t\"ATTRIBUTE_DISPLAYNAME\" : \"" + displayName + "\"\n" +
                    "      }\n" +
                    "    ]\n" +
                    "  }\n" +
                    "}";

            DSPDMResponse dspdmResponse = super.updateCustomAttributesPost(body);
            Map<String, Object> data = dspdmResponse.getData();
            if (data != null) {
                String responseBoName = data.keySet().iterator().next();
                Assert.assertEquals("BUSINESS OBJECT ATTR", responseBoName);
                logger.info("testDeleteSimpleCustomAttribute Passed.");
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }

    }

    *//**
     * Main unit test method to introduce a new business object by cloning the metadata of an existing business object
     * by using its bo name and creates new business object with provided new business object name
     *//*
    @Test
    public void testIntroduceDropNewBusinessObjects() {
        try {
            String oldBoName = "WELL";
            String newBoName = "WELL" + "_COPY" + StringUtils.getNextRandomNumber(10);
            String uwi = "UNIT_TEST" + UUID.randomUUID().toString().substring(30).toUpperCase();
            String well_name = "UNIT_TEST" + UUID.randomUUID().toString().substring(30).toUpperCase();
            // create
            testIntroduceNewBusinessObjects(oldBoName, newBoName);
            // save data
            saveRecordInNewBusinessObjects(newBoName, uwi, well_name);
            // drop with full drop false
            try {
                testDropBusinessObjects(newBoName, false);
            } catch (Exception e) {
                // suppress exception
            }
            // drop with full drop true
            testDropBusinessObjects(newBoName, true);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    private void testIntroduceNewBusinessObjects(String oldBoName, String newBoName) {
        try {
            ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
            // prepare business object (table)
            DynamicDTO newBusinessObjectDTOToSave = cloneBOMetadataWithNewBoName(oldBoName, newBoName, executionContext);
            // add columns to the table
            newBusinessObjectDTOToSave.putWithOrder(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY, prepareChildrenMap(oldBoName, newBoName, executionContext));
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

    private void saveRecordInNewBusinessObjects(String newBoName, String uwi, String well_name) {
        try {
            String body = "{\n" +
                    "  \"" + newBoName + "\": { \n" +
                    "    \"language\": \"en\", \n" +
                    "    \"readBack\": true, \n" +
                    "    \"timezone\": \"GMT+08:00\",\n" +
                    "    \"dspdmUnits\":[ \n" +
                    "        		{  \n" +
                    "   			 	\"boAttrName\":\"X_COORDINATE\", \n" +
                    "    				\"sourceUnit\":\"rad\" \n" +
                    "         		} \n" +
                    "     ] ,\n" +
                    "    \"data\": [\n" +
                    "         {\n" +
                    "          \"UWI\": \"" + uwi + "\", \n" +
                    "           \"WELL_NAME\": \"" + well_name + "\"\n" +
                    "         }\n" +
                    "      ]\n" +
                    "  }\n" +
                    "}";
            DSPDMResponse response = super.savePost(body);
            if (DSPDMConstants.Status.SUCCESS == response.getStatus()) {
                // print each message at appropriate log level
                response.getMessages().forEach(logger::log);
                logger.info("testInsert Passed.");
            } else {
                logger.info("testInsert failed.");
                throw response.getException();
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, Locale.ENGLISH);
        }
    }

    private void testDropBusinessObjects(String newBoName, boolean fullDrop) {
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
    private Map<String, Object> prepareChildrenMap(String oldBoName, String newBoName, ExecutionContext executionContext) {
        // prepare children map
        Map<String, Object> childrenMap = new LinkedHashMap<>(1);
        // prepare attributes (columns)
        Map<String, Object> businessObjectAttributesMap = new LinkedHashMap<>(3);
        businessObjectAttributesMap.put("language", "en");
        businessObjectAttributesMap.put("timezone", "GMT+08:00");
        businessObjectAttributesMap.put("data", cloneAttributeMetadataWithNewBoName(oldBoName, newBoName, executionContext));
        childrenMap.put(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR, businessObjectAttributesMap);
        // prepare unique constraints
        Map<String, Object> businessObjectUniqueConstraintMap = new LinkedHashMap<>(3);
        businessObjectUniqueConstraintMap.put("language", "en");
        businessObjectUniqueConstraintMap.put("timezone", "GMT+08:00");
        businessObjectUniqueConstraintMap.put("data", cloneUniqueConstraintMetadataWithNewBoName(oldBoName, newBoName, executionContext));
        childrenMap.put(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, businessObjectUniqueConstraintMap);
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

    private List<DynamicDTO> cloneUniqueConstraintMetadataWithNewBoName(String boNameToReadMetadata, String newBoName, ExecutionContext executionContext) {
        PagedList<DynamicDTO> pagedList = readExistingBusObjAttrUniqueConstraintMetadataForIntroduceNewBusinessObjects(boNameToReadMetadata, executionContext);
        String newEntityName = (newBoName.contains(" ")) ? newBoName.replaceAll(" ", "_") : newBoName;
        List<DynamicDTO> list = new ArrayList<>(pagedList.size());
        for (DynamicDTO dynamicDTO : pagedList) {
            dynamicDTO.put(DSPDMConstants.BoAttrName.ENTITY, newEntityName);
            dynamicDTO.put(DSPDMConstants.BoAttrName.BO_NAME, newBoName);
            // must make constraint name null so that new constraint name can be generated
            dynamicDTO.put(DSPDMConstants.BoAttrName.CONSTRAINT_NAME, null);
            list.add(dynamicDTO);
        }
        return list;
    }

    private List<DynamicDTO> cloneRelationshipMetadataWithNewBoName(String boNameToReadMetadata, String newBoName, ExecutionContext executionContext) {
        PagedList<DynamicDTO> pagedList = readExistingBusObjRelationshipMetadataForIntroduceNewBusinessObjects(boNameToReadMetadata, executionContext);
        String newEntityName = (newBoName.contains(" ")) ? newBoName.replaceAll(" ", "_") : newBoName;
        List<DynamicDTO> list = new ArrayList<>(pagedList.size());
        for (DynamicDTO dynamicDTO : pagedList) {
            dynamicDTO.put(DSPDMConstants.BoAttrName.CHILD_ENTITY_NAME, newEntityName);
            dynamicDTO.put(DSPDMConstants.BoAttrName.CHILD_BO_NAME, newBoName);
            dynamicDTO.put(DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME, "FK_" + newEntityName + "_" + dynamicDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME));
            list.add(dynamicDTO);
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

    private PagedList<DynamicDTO> readExistingBusObjAttrUniqueConstraintMetadataForIntroduceNewBusinessObjects(String boNameToReadMetadata, ExecutionContext executionContext) {
        PagedList<DynamicDTO> pagedList = null;
        BOQuery boAttrEntity = new BOQuery();
        boAttrEntity.setBoName(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS);
        boAttrEntity.setLanguage("en");
        boAttrEntity.setTimezone("GMT+08:00");
        // read all column names and set in this one except primary key column
        boAttrEntity.setSelectList(readAllMetadataAttributeNamesForIntroduceNewBusinessObjects(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS, executionContext));
        List<CriteriaFilter> criteriaFilters = new ArrayList<>(1);
        // find all those unique constraints where current business object is a child
        criteriaFilters.add(new CriteriaFilter(DSPDMConstants.BoAttrName.BO_NAME, Operator.EQUALS, new Object[]{boNameToReadMetadata}));
        boAttrEntity.setCriteriaFilters(criteriaFilters);
        boAttrEntity.setReadAllRecords(true);
        DSPDMResponse dspdmResponse = super.commonPost(boAttrEntity);

        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            pagedList = (PagedList<DynamicDTO>) data.get(DSPDMConstants.BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS);
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
    }*/


}
