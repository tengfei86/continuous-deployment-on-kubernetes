package com.lgc.dspdm.msp.mainservice;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;


public class ImportParseTest extends BaseServiceTest {
    private static final DSPDMLogger logger = new DSPDMLogger(ImportParseTest.class);
    private static final String FILE_NAME = "localhost/test_parseimport/WELL.xlsx";
    private static final String WELL_JOINED_WELLBORE = "localhost/test_parseimport/WELL_JOINED_WELLBORE.xlsx";
    private static final String SHEET_NAMES_FILE_NAME = "localhost/test_sheetnames/ReadSheetNames.xlsx";

    //@Test
    public void testReadSheetNames() throws IOException {
        try {
            URL resource = getClass().getClassLoader().getResource(SHEET_NAMES_FILE_NAME);
            File file = new File(resource.getFile());
            if (file != null) {
                Map parts = new HashMap();
                parts.put("params", "{ \"language\": \"en\",\n" +
                        "          \"timezone\": \"GMT+08:00\"" +
                        "  } ");
                String uploadUrl = getWebBaseUrl() + "/secure/parse/boNames";
                String result = super.parseImportPost(uploadUrl, "uploadedFile", Arrays.asList(file), parts);
                ObjectMapper mapper = new ObjectMapper();
                JsonNode resultNode = mapper.readTree(result);
                JsonNode dataNode = resultNode.get("data");
                if ((dataNode != null) && (dataNode.size() > 0)) {
                    Iterator<String> fieldNames = dataNode.fieldNames();
                    while (fieldNames.hasNext()) {
                        String boName = fieldNames.next();
                        JsonNode bo = dataNode.get(boName);
                        JsonNode totalRecords = bo.get(DSPDMConstants.DSPDM_RESPONSE.TOTAL_RECORDS_KEY);
                        logger.info("boName : {}, totalRecords : {}", boName, totalRecords.intValue());
                    }
                    Assert.assertNotEquals(0, dataNode.size());
                    logger.info("testReadSheetNames Passed.");
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test
    //@Order(2)
    public void testParse() throws IOException {
        try {
            File file;
            ClassLoader classLoader = getClass().getClassLoader();
            file = new File(classLoader.getResource(FILE_NAME).getFile());
            if (file != null) {
                String boName = "WELL";
                Map parts = new HashMap();
                parts.put("params", "{\n" +
                        "    \"" + boName + "\": {\n" +
                        "    \"language\": \"en\",\n" +
                        "    \"timezone\": \"GMT+08:00\",\n" +
                        "    \"readBack\": true,\n" +
                        "    \"unitRow\": 2,\n" +
                        "    \"offset\": 0,\n" +
                        "    \"limit\": 10,\n" +
                        "    \"dspdmUnits\":[ \n" +
                        "        		{  \n" +
                        "   			 	\"boAttrName\":\"X_COORDINATE\", \n" +
                        "    				\"targetUnit\":\"rad\" \n" +
                        "         		} \n" +
                        "     ] \n" +
                        "  }" +
                        "}");
                String uploadUrl = getWebBaseUrl() + "/secure/parse";
                String result = super.parseImportPost(uploadUrl, "uploadedFile", Arrays.asList(file), parts);
                Assert.assertTrue(result.contains("WELL"));
                logger.info("testParse Passed.");
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test
    //@Order(3)
    public void testParseBulk() throws IOException {
        try {
            File file;
            ClassLoader classLoader = getClass().getClassLoader();
            file = new File(classLoader.getResource(FILE_NAME).getFile());
            if (file != null) {
                String boName = "WELL";
                Map parts = new HashMap();
                parts.put("params", "{\n" +
                        "    \"" + boName + "\": {\n" +
                        "    \"language\": \"en\",\n" +
                        "    \"timezone\": \"GMT+08:00\",\n" +
                        "    \"readBack\": true,\n" +
                        "    \"unitRow\": 2,\n" +
                        "    \"offset\": 0,\n" +
                        "    \"limit\": 10,\n" +
                        "    \"dspdmUnits\":[ \n" +
                        "        		{  \n" +
                        "   			 	\"boAttrName\":\"X_COORDINATE\", \n" +
                        "    				\"targetUnit\":\"rad\" \n" +
                        "         		} \n" +
                        "     ] \n" +
                        "  }" +
                        "}");
                String uploadUrl = getWebBaseUrl() + "/secure/parse/bulk";
                String result = super.parseImportPost(uploadUrl, "uploadedFile", Arrays.asList(file), parts);
                Assert.assertTrue(result.contains("WELL"));
                logger.info("testParseBulk Passed.");
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test
    //@Order(1)
    public void testImport() throws IOException {

        try {
            File file;
            ClassLoader classLoader = getClass().getClassLoader();
            file = new File(classLoader.getResource(FILE_NAME).getFile());
            if (file != null) {
                String boName = "WELL";
                Map parts = new HashMap();
                parts.put("params", "{\n" +
                        "    \"" + boName + "\": {\n" +
                        "    \"language\": \"en\",\n" +
                        "    \"timezone\": \"GMT+08:00\",\n" +
                        "    \"readBack\": true,\n" +
                        "    \"unitRow\": 2,\n" +
                        "    \"offset\": 0,\n" +
                        "    \"limit\": 10,\n" +
                        "    \"dspdmUnits\":[ \n" +
                        "        		{  \n" +
                        "   			 	\"boAttrName\":\"X_COORDINATE\", \n" +
                        "    				\"targetUnit\":\"rad\" \n" +
                        "         		} \n" +
                        "     ] \n" +
                        "  }" +
                        "}");
                String uploadUrl = getWebBaseUrl() + "/secure/import";
                String result = super.parseImportPost(uploadUrl, "uploadedFile", Arrays.asList(file), parts);
                Assert.assertTrue(result.contains(DSPDMConstants.Status.SUCCESS.getLabel()));
                logger.info("testImport Passed.");
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test
    public void testParseJoined() {
        try {
            File file;
            ClassLoader classLoader = getClass().getClassLoader();
            file = new File(classLoader.getResource(WELL_JOINED_WELLBORE).getFile());
            if (file != null) {
                String boName = "WELL";
                Map parts = new HashMap();
                parts.put("params", "{\n" +
                        "    \"WELL\": {\n" +
                        "        \"language\": \"en\",\n" +
                        "        \"timezone\": \"GMT+08:00\",\n" +
                        "        \"offset\": 0,\n" +
                        "        \"limit\": 10,\n" +
                        "        \"simpleJoins\": [\n" +
                        "            {\n" +
                        "                \"boName\": \"WELLBORE\"\n" +
                        "            }\n" +
                        "        ],\n" +
                        "        \"unitRow\": 2,\n" +
                        "        \"dspdmUnits\": [\n" +
                        "            {\n" +
                        "                \"boAttrName\": \"DEEPEST_DEPTH\",\n" +
                        "                \"targetUnit\": \"m\"\n" +
                        "            },\n" +
                        "            {\n" +
                        "                \"boAttrName\": \"X_COORDINATE\",\n" +
                        "                \"targetUnit\": \"dega\"\n" +
                        "            },\n" +
                        "            {\n" +
                        "                \"boAttrName\": \"drill_td\",\n" +
                        "                \"targetUnit\": \"ft\"\n" +
                        "            }\n" +
                        "        ]\n" +
                        "    }\n" +
                        "}");
                String uploadUrl = getWebBaseUrl() + "/secure/parse";
                String result = super.parseImportPost(uploadUrl, "uploadedFile", Arrays.asList(file), parts);
                Assert.assertTrue(result.contains(DSPDMConstants.Status.SUCCESS.getLabel()));
                logger.info("testParseJoined Passed.");
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }
}
