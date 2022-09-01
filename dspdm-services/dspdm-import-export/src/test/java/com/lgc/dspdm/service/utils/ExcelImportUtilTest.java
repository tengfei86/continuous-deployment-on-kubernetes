package com.lgc.dspdm.service.utils;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.lgc.dspdm.config.AppConfigInit;
import com.lgc.dspdm.core.common.config.PropertySource;
import com.lgc.dspdm.core.common.data.common.BulkImportResponseDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.service.common.dynamic.read.DynamicReadService;
import com.lgc.dspdm.service.common.dynamic.read.IDynamicReadService;
import com.lgc.dspdm.service.utils.excel.ExcelImportJoinUtil;
import junit.framework.TestCase;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ExcelImportUtilTest extends TestCase {
    private static DSPDMLogger logger = new DSPDMLogger(ExcelImportUtilTest.class);
    private static final String FILE_NAME = "test_parseimport/WELL.xlsx";
    private static final String WELL_JOINED_WELLBORE = "test_parseimport/WELL_JOINED_WELLBORE.xlsx";
    private static final String SHEET_NAMES_FILE_NAME = "test_sheetnames/ReadSheetNames.xlsx";

    @BeforeAll
    static void setup() {
        ExecutionContext executionContext = null;
        try {
            executionContext = ExecutionContext.getTestUserExecutionContext();
        } catch (Exception e) {
            logger.error("Unable to get execution context for test user");
            DSPDMException.throwException(e, Locale.getDefault());
        }
        // initialize application configurations
        AppConfigInit.init(PropertySource.MAP_PROVIDED, executionContext);
    }

    @Test
    @Order(1)
    public void test01() throws Exception {
        File file;
        ClassLoader classLoader = getClass().getClassLoader();
        file = new File(classLoader.getResource(SHEET_NAMES_FILE_NAME).getFile());
        InputStream targetStream = null;
        if (file != null) {
            try {
                IDynamicReadService dynamicReadService = DynamicReadService.getInstance();
                targetStream = new FileInputStream(file);
                Map<String, Integer> sheetNamesAndRecordsCount = ExcelImportJoinUtil.getSheetNamesAndRecordsCount(targetStream, ExecutionContext.getTestUserExecutionContext());
                for (String sheetName : sheetNamesAndRecordsCount.keySet()) {
                    logger.info("sheetName : {}, totalRecords : {}", sheetName, sheetNamesAndRecordsCount.get(sheetName));
                }
                assertNotEquals(0, sheetNamesAndRecordsCount.size());
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                DSPDMException.throwException(e, ExecutionContext.getTestUserExecutionContext());
            } finally {
                if (targetStream != null) {
                    targetStream.close();
                }
            }
        }
    }

    @Test
    @Order(2)
    public void test02() throws Exception {
        File file;
        ClassLoader classLoader = getClass().getClassLoader();
        file = new File(classLoader.getResource(FILE_NAME).getFile());
        InputStream targetStream = null;
        if (file != null) {
            try {
                IDynamicReadService dynamicReadService = DynamicReadService.getInstance();
                targetStream = new FileInputStream(file);
                Map<String, BulkImportResponseDTO> result = ExcelImportJoinUtil.read(getWellParam(), targetStream, true, dynamicReadService, ExecutionContext.getTestUserExecutionContext());
                BulkImportResponseDTO bulkImportResponseDTO = result.get("WELL");
                assertNotEquals(0, (bulkImportResponseDTO.getParsedValues().size() + bulkImportResponseDTO.getInvalidValues().size()));
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                DSPDMException.throwException(e, ExecutionContext.getTestUserExecutionContext());
            } finally {
                if (targetStream != null) {
                    targetStream.close();
                }
            }
        }
    }

    private Map getWellParam() throws IOException {
        return new ObjectMapper().readValue("{\n" +
                "  \"WELL\": {\n" +
                "    \"language\": \"en\",\n" +
                "    \"timezone\": \"GMT+08:00\",\n" +
                "    \"readBack\": true,\n" +
                "    \"unitRow\": 2,\n" +
                "    \"offset\": 1,\n" +
                "    \"limit\": 10,\n" +
                "    \"dspdmUnits\": [\n" +
                "      {\n" +
                "        \"boAttrName\": \"DEEPEST_DEPTH\",\n" +
                "        \"targetUnit\": \"ft\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"boAttrName\": \"X_COORDINATE\",\n" +
                "        \"targetUnit\": \"dega\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"boAttrName\": \"Y_COORDINATE\",\n" +
                "        \"targetUnit\": \"dega\"\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}", Map.class);
    }

    @Test
    @Order(3)
    public void test03() throws Exception {
        File file;
        ClassLoader classLoader = getClass().getClassLoader();
        file = new File(classLoader.getResource(WELL_JOINED_WELLBORE).getFile());
        InputStream targetStream = null;
        if (file != null) {
            try {
                IDynamicReadService dynamicReadService = DynamicReadService.getInstance();
                targetStream = new FileInputStream(file);
                Map<String, BulkImportResponseDTO> result = ExcelImportJoinUtil.read(getWellJoinedWellboreParam(), targetStream, true, dynamicReadService, ExecutionContext.getTestUserExecutionContext());
                BulkImportResponseDTO bulkImportResponseDTO = result.get("WELL");
                assertNotEquals(0, (bulkImportResponseDTO.getParsedValues().size() + bulkImportResponseDTO.getInvalidValues().size()));
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                DSPDMException.throwException(e, ExecutionContext.getTestUserExecutionContext());
            } finally {
                if (targetStream != null) {
                    targetStream.close();
                }
            }
        }
    }

    private Map getWellJoinedWellboreParam() throws IOException {
        return new ObjectMapper().readValue("{\n" +
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
                "}", Map.class);
    }
}
