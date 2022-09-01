package com.lgc.dspdm.service.utils;

import com.lgc.dspdm.config.AppConfigInit;
import com.lgc.dspdm.core.common.config.PropertySource;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.service.common.dynamic.read.DynamicReadService;
import com.lgc.dspdm.service.common.dynamic.read.IDynamicReadService;
import com.lgc.dspdm.service.utils.excel.ExcelExportUtil;
import junit.framework.TestCase;
import org.apache.poi.ss.usermodel.Workbook;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ExcelExportUtilTest extends TestCase {
    private static DSPDMLogger logger = new DSPDMLogger(ExcelExportUtilTest.class);
    private static IDynamicReadService dynamicReadService = null;

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
        AppConfigInit.init(PropertySource.FILE, executionContext);
        // init service instance
        dynamicReadService = DynamicReadService.getInstance();
    }

    @Test
    public void testSingleSheet() throws Exception {
        Workbook workbook = null;
        try {
            ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo("WELL", executionContext);
            // SET READ METADATA ALONG WITH DATA
            businessObjectInfo.setReadMetadata(true);
            // FETCH LIST FROM DATA STORE        
            Map<String, Object> businessObjectsAndMetadataMap = dynamicReadService.readWithDetails(businessObjectInfo,
                    executionContext);
            List<DynamicDTO> businessObjects =
                    (List<DynamicDTO>) businessObjectsAndMetadataMap.get(businessObjectInfo.getBusinessObjectType());
            List<DynamicDTO> metadata = (List<DynamicDTO>) businessObjectsAndMetadataMap.get("BUSINESS OBJECT ATTR");

            Map<String, DynamicDTO> map = dynamicReadService.readMetadataMapForBOName("WELL", executionContext);
            workbook = ExcelExportUtil.createWorkBook(executionContext);
            ExcelExportUtil.createWorkBookSingleSheet(workbook, "WELL", metadata, businessObjects,  executionContext);
            assertEquals(workbook.getSheet("WELL").getLastRowNum(), businessObjects.size());
        } catch (Exception e) {
            logger.error(e);
            throw e;
        } finally {
            ExcelExportUtil.disposeWorkBook(workbook);
        }
    }
}
