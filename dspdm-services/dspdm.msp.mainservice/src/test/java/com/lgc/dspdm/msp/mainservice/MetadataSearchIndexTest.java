package com.lgc.dspdm.msp.mainservice;

import com.lgc.dspdm.core.common.config.ConnectionProperties;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.msp.mainservice.model.DSPDMResponse;
import org.junit.Test;

import java.util.Locale;

public class MetadataSearchIndexTest extends BaseServiceTest {
/*    private static DSPDMLogger logger = new DSPDMLogger(MetadataSearchIndexTest.class);

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

    @Test
    public void testAddDropSearchIndexOnExistingBusinessObject() {
        String wellTestChildBoName = "WELL TEST CHILD " + StringUtils.getNextRandomNumber(5);
        // introduce a new business object well test child. it has UWI and start time as its columns
        introduceNewBusinessObject_WellTestChild(wellTestChildBoName);
        // add search index UWI and START TIME
        addSearchIndex(wellTestChildBoName);
        // drop search index
        dropSearchIndex("SIX_" + wellTestChildBoName + "_UWI_START_TIME");
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
                    "                                \"BO_ATTR_NAME\": \"UWI\",\n" +
                    "                                \"BO_DISPLAY_NAME\": \"Well Test\",\n" +
                    "                                \"ATTRIBUTE\": \"UWI\",\n" +
                    "                                \"ATTRIBUTE_DISPLAYNAME\": \"UWI\",\n" +
                    "                                \"ATTRIBUTE_DATATYPE\": \"" + DSPDMConstants.DataTypes.CHARACTER_VARYING.getAttributeDataType(ConnectionProperties.isMSSQLServerDialect()) + "(50)\",\n" +
                    "                                \"CONTROL_TYPE\": \"autoComplete\",\n" +
                    "                                \"IS_MANDATORY\": true,\n" +
                    "                                \"IS_UPLOAD_NEEDED\": true,\n" +
                    "                                \"IS_HIDDEN\": false,\n" +
                    "                                \"IS_REFERENCE_IND\": false,\n" +
                    "                                \"IS_PRIMARY_KEY\": false,\n" +
                    "                                \"IS_ACTIVE\": true,\n" +
                    "                                \"IS_SORTABLE\": true,\n" +
                    "                                \"ATTRIBUTE_DESC\": \"UNIQUE WELL IDENTIFIER: A unique name, code or number designated.\",\n" +
                    "                                \"IS_INTERNAL\": false,\n" +
                    "                                \"IS_READ_ONLY\": false,\n" +
                    "                                \"IS_CUSTOM_ATTRIBUTE\": false\n" +
                    "                            },\n" +
                    "                            {\n" +
                    "                                \"BO_ATTR_NAME\": \"START_TIME\",\n" +
                    "                                \"BO_DISPLAY_NAME\": \"Well Test\",\n" +
                    "                                \"ATTRIBUTE\": \"START_TIME\",\n" +
                    "                                \"ATTRIBUTE_DISPLAYNAME\": \"Start Time\",\n" +
                    "                                \"ATTRIBUTE_DATATYPE\": \"" + DSPDMConstants.DataTypes.TIMESTAMP.getAttributeDataType(ConnectionProperties.isMSSQLServerDialect()) + "\",\n" +
                    "                                \"CONTROL_TYPE\": \"calenderWithDateTime\",\n" +
                    "                                \"IS_MANDATORY\": true,\n" +
                    "                                \"IS_UPLOAD_NEEDED\": true,\n" +
                    "                                \"IS_HIDDEN\": false,\n" +
                    "                                \"IS_REFERENCE_IND\": false,\n" +
                    "                                \"IS_PRIMARY_KEY\": false,\n" +
                    "                                \"IS_ACTIVE\": true,\n" +
                    "                                \"IS_SORTABLE\": true,\n" +
                    "                                \"ATTRIBUTE_DESC\": \"START TIME: The start time of the stable flow of  this well test.\",\n" +
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

    private void addSearchIndex(String boName) {
        try {
            // now make final request json
            String body = "{\n" +
                    "    \"BUS OBJ ATTR SEARCH INDEXES\": {\n" +
                    "        \"language\": \"en\",\n" +
                    "        \"readBack\": true,\n" +
                    "        \"timezone\": \"GMT+08:00\",\n" +
                    "        \"showSQLStats\": true,\n" +
                    "        \"collectSQLScript\": 11,\n" +
                    "        \"data\": [\n" +
                    // composite search index
                    "            {\n" +
                    "                \"INDEX_NAME\": \"SIX_" + boName + "_UWI_START_TIME\",\n" +
                    "                \"BO_NAME\": \"" + boName + "\",\n" +
                    "                \"BO_ATTR_NAME\": \"UWI\"\n" +
                    "            },\n" +
                    "            {\n" +
                    "                \"INDEX_NAME\": \"SIX_" + boName + "_UWI_START_TIME\",\n" +
                    "                \"BO_NAME\": \"" + boName + "\",\n" +
                    "                \"BO_ATTR_NAME\": \"START_TIME\"\n" +
                    "            },\n" +
                    // simple search index
                    "            {\n" +
                    "                \"INDEX_NAME\": \"SIX_" + boName + "_NAME\",\n" +
                    "                \"BO_NAME\": \"" + boName + "\",\n" +
                    "                \"BO_ATTR_NAME\": \"" + boName + "_NAME\"\n" +
                    "            }\n" +
                    "        ]\n" +
                    "    }\n" +
                    "}";
            // going to send request
            DSPDMResponse response = super.addSearchIndexes(body);
            if (DSPDMConstants.Status.SUCCESS == response.getStatus()) {
                // print each message at appropriate log level
                response.getMessages().forEach(logger::log);
                logger.info("addCompositeSearchIndex passed.");
            } else {
                logger.info("addCompositeSearchIndex failed");
                throw response.getException();
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, Locale.ENGLISH);
        }
    }

    private void dropSearchIndex(String indexName) {
        try {
            // now make final request json
            String body = "{\n" +
                    "    \"BUS OBJ ATTR SEARCH INDEXES\": {\n" +
                    "        \"language\": \"en\",\n" +
                    "        \"readBack\": true,\n" +
                    "        \"timezone\": \"GMT+08:00\",\n" +
                    "        \"showSQLStats\": false,\n" +
                    "        \"collectSQLScript\": 11,\n" +
                    "        \"data\": [\n" +
                    "            {\n" +
                    "                \"INDEX_NAME\": \"" + indexName + "\"\n" +
                    "            }\n" +
                    "        ]\n" +
                    "    }\n" +
                    "}";
            // going to send request
            DSPDMResponse response = super.dropSearchIndexes(body);
            if (DSPDMConstants.Status.SUCCESS == response.getStatus()) {
                // print each message at appropriate log level
                response.getMessages().forEach(logger::log);
                logger.info("dropSearchIndexes passed.");
            } else {
                logger.info("dropSearchIndexes failed");
                throw response.getException();
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, Locale.ENGLISH);
        }
    }

    @Test
    public void testAddDropUniqueConstraintOnNewBusinessObject() {
        String wellTestChildBoName = "WELL TEST CHILD " + StringUtils.getNextRandomNumber(5);
        // introduce a new business object well test child. it has UWI and start time as its columns
        introduceNewBusinessObject_WellTestChild_With_SearchIndexes(wellTestChildBoName);

        // drop search indexes
        dropSearchIndex("SIX_" + wellTestChildBoName + "_UWI_START_TIME");
        // drop with full drop true
        dropBusinessObjects(wellTestChildBoName, true);
    }

    private void introduceNewBusinessObject_WellTestChild_With_SearchIndexes(String boName) {
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
                    "                                \"BO_ATTR_NAME\": \"UWI\",\n" +
                    "                                \"BO_DISPLAY_NAME\": \"Well Test\",\n" +
                    "                                \"ATTRIBUTE\": \"UWI\",\n" +
                    "                                \"ATTRIBUTE_DISPLAYNAME\": \"UWI\",\n" +
                    "                                \"ATTRIBUTE_DATATYPE\": \"" + DSPDMConstants.DataTypes.CHARACTER_VARYING.getAttributeDataType(ConnectionProperties.isMSSQLServerDialect()) + "(50)\",\n" +
                    "                                \"CONTROL_TYPE\": \"autoComplete\",\n" +
                    "                                \"IS_MANDATORY\": true,\n" +
                    "                                \"IS_UPLOAD_NEEDED\": true,\n" +
                    "                                \"IS_HIDDEN\": false,\n" +
                    "                                \"IS_REFERENCE_IND\": false,\n" +
                    "                                \"IS_PRIMARY_KEY\": false,\n" +
                    "                                \"IS_ACTIVE\": true,\n" +
                    "                                \"IS_SORTABLE\": true,\n" +
                    "                                \"ATTRIBUTE_DESC\": \"UNIQUE WELL IDENTIFIER: A unique name, code or number designated.\",\n" +
                    "                                \"IS_INTERNAL\": false,\n" +
                    "                                \"IS_READ_ONLY\": false,\n" +
                    "                                \"IS_CUSTOM_ATTRIBUTE\": false\n" +
                    "                            },\n" +
                    "                            {\n" +
                    "                                \"BO_ATTR_NAME\": \"START_TIME\",\n" +
                    "                                \"BO_DISPLAY_NAME\": \"Well Test\",\n" +
                    "                                \"ATTRIBUTE\": \"START_TIME\",\n" +
                    "                                \"ATTRIBUTE_DISPLAYNAME\": \"Start Time\",\n" +
                    "                                \"ATTRIBUTE_DATATYPE\": \"" + DSPDMConstants.DataTypes.TIMESTAMP.getAttributeDataType(ConnectionProperties.isMSSQLServerDialect()) + "\",\n" +
                    "                                \"CONTROL_TYPE\": \"calenderWithDateTime\",\n" +
                    "                                \"IS_MANDATORY\": true,\n" +
                    "                                \"IS_UPLOAD_NEEDED\": true,\n" +
                    "                                \"IS_HIDDEN\": false,\n" +
                    "                                \"IS_REFERENCE_IND\": false,\n" +
                    "                                \"IS_PRIMARY_KEY\": false,\n" +
                    "                                \"IS_ACTIVE\": true,\n" +
                    "                                \"IS_SORTABLE\": true,\n" +
                    "                                \"ATTRIBUTE_DESC\": \"START TIME: The start time of the stable flow of  this well test.\",\n" +
                    "                                \"IS_INTERNAL\": false,\n" +
                    "                                \"IS_READ_ONLY\": false,\n" +
                    "                                \"IS_CUSTOM_ATTRIBUTE\": false\n" +
                    "                            }\n" +
                    "                        ]\n" +
                    "                    },\n" +
                    "                    \"BUS OBJ ATTR SEARCH INDEXES\": {\n" +
                    "                        \"language\": \"en\",\n" +
                    "                        \"readBack\": true,\n" +
                    "                        \"timezone\": \"GMT+08:00\",\n" +
                    "                        \"showSQLStats\": true,\n" +
                    "                        \"collectSQLScript\": 11,\n" +
                    "                        \"data\": [\n" +
                    // composite search index
                    "                            {\n" +
                    "                                \"INDEX_NAME\": \"SIX_" + boName + "_UWI_START_TIME\",\n" +
                    "                                \"BO_NAME\": \"" + boName + "\",\n" +
                    "                                \"BO_ATTR_NAME\": \"UWI\"\n" +
                    "                            },\n" +
                    "                            {\n" +
                    "                                \"INDEX_NAME\": \"SIX_" + boName + "_UWI_START_TIME\",\n" +
                    "                                \"BO_NAME\": \"" + boName + "\",\n" +
                    "                                \"BO_ATTR_NAME\": \"START_TIME\"\n" +
                    "                            },\n" +
                    // simple search index
                    "                            {\n" +
                    "                                \"INDEX_NAME\": \"SIX_" + boName + "_NAME\",\n" +
                    "                                \"BO_NAME\": \"" + boName + "\",\n" +
                    "                                \"BO_ATTR_NAME\": \"" + boName + "_NAME\"\n" +
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
    }*/
}
