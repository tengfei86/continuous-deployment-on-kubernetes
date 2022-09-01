package com.lgc.dspdm.msp.mainservice;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lgc.dspdm.core.common.config.ConnectionProperties;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.msp.mainservice.model.BOQuery;
import com.lgc.dspdm.msp.mainservice.model.CriteriaFilter;
import com.lgc.dspdm.msp.mainservice.model.DSPDMResponse;
import com.lgc.dspdm.msp.mainservice.model.Order;
import com.lgc.dspdm.msp.mainservice.model.OrderBy;
import com.lgc.dspdm.msp.mainservice.model.Pagination;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class BusinessObjectGroupTest extends BaseServiceTest {
    private static DSPDMLogger logger = new DSPDMLogger(BusinessObjectGroupTest.class);
    private static int R_GROUP_CATEGORY_ID=3;
    private static String GROUP_CATEGORY_NAME="ASSET";
    private static int BUSINESS_OBJECT_ID;

    @Test
    public void testAddAndDeleteGroupForExistingBusinessObject() {
        String testBoName = "TEST " + StringUtils.getNextRandomNumber(5);
        // introduce a new business object
        introduceNewBusinessObject(testBoName);
        // add the new business object group
        addBusinessObjectGroup(testBoName);
        // drop the new business object with full drop true
        dropBusinessObject(testBoName, true);
    }

    @Test
    public void testAddAndDeleteIntroduceNewBusinessObjectAndGroup() {
        String testBoName = "TEST " + StringUtils.getNextRandomNumber(5);
        // introduce a new business object and group
        introduceNewBusinessObjectAndGroup(testBoName);
        // drop the new business object with full drop true
        dropBusinessObject(testBoName, true);
    }
    
    private void introduceNewBusinessObject(String boName) {
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
                    "                                \"IS_UPLOAD_NEEDED\": true,\n" +
                    "                                \"IS_HIDDEN\": false,\n" +
                    "                                \"IS_REFERENCE_IND\": false,\n" +
                    "                                \"IS_PRIMARY_KEY\": false,\n" +
                    "                                \"IS_ACTIVE\": true,\n" +
                    "                                \"IS_SORTABLE\": true,\n" +
                    "                                \"IS_INTERNAL\": false,\n" +
                    "                                \"RELATED_BO_ATTR_NAME\": \"" + boName + "_ID\"\n" +
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
                logger.info("introduceNewBusinessObject passed.");
            } else {
                logger.info("introduceNewBusinessObject failed.");
                throw response.getException();
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, Locale.ENGLISH);
        }
    }

    private void introduceNewBusinessObjectAndGroup(String boName) {
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
                    "                                \"IS_UPLOAD_NEEDED\": true,\n" +
                    "                                \"IS_HIDDEN\": false,\n" +
                    "                                \"IS_REFERENCE_IND\": false,\n" +
                    "                                \"IS_PRIMARY_KEY\": false,\n" +
                    "                                \"IS_ACTIVE\": true,\n" +
                    "                                \"IS_SORTABLE\": true,\n" +
                    "                                \"IS_INTERNAL\": false,\n" +
                    "                                \"RELATED_BO_ATTR_NAME\": \"" + boName + "_ID\"\n" +
                    "                            }\n" +
                    "                        ]\n" +
                    "                    },\n" +
                    "                    \"BUSINESS OBJECT GROUP\": {\n" +
                    "                        \"language\": \"en\",\n" +
                    "                        \"readBack\": false,\n" +
                    "                        \"timezone\": \"GMT+08:00\",\n" +
                    "                        \"data\": [\n" +
                    "                            {\n" +
                    "                                \"GROUP_CATEGORY_NAME\": \"ASSET\"\n" +
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
                logger.info("introduceNewBusinessObject and group passed.");
            } else {
                logger.info("introduceNewBusinessObject and group failed.");
                throw response.getException();
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, Locale.ENGLISH);
        }
    }
    
    private void dropBusinessObject(String boName, boolean fullDrop) {
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
                    "                \"BO_NAME\": \"" + boName + "\"\n" +
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

    private void addBusinessObjectGroup(String boName) {
        try {
        	// now make final request json
            String body = "{\n" +
                    "    \"BUSINESS OBJECT GROUP\": {\n" +
                    "        \"language\": \"en\",\n" +
                    "        \"readBack\": true,\n" +
                    "        \"timezone\": \"GMT+08:00\",\n" +
                    "        \"showSQLStats\": true,\n" +
                    "        \"data\": [\n" +
                    "            {\n" +
                    "                \"GROUP_CATEGORY_NAME\": \"" + GROUP_CATEGORY_NAME + "\",\n" +
                    "                \"BO_NAME\": \"" + boName + "\"\n" +
                    "            }\n" +
                    "        ]\n" +
                    "    }\n" +
                    "}";
            // going to send request
            DSPDMResponse response = super.addBusinessObjectGroups(body);
            if (DSPDMConstants.Status.SUCCESS == response.getStatus()) {
                // print each message at appropriate log level
                response.getMessages().forEach(logger::log);
                logger.info("addBusinessObjectGroup passed.");
            } else {
                logger.info("addBusinessObjectGroup failed");
                throw response.getException();
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, Locale.ENGLISH);
        }
    }
}
