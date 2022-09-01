package com.lgc.dspdm.service.common;

import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicPK;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.service.common.dynamic.read.DynamicReadService;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DynamicReadServiceTest {
    private static DSPDMLogger logger = new DSPDMLogger(DynamicReadServiceTest.class);
    private static List<String> boNames = null;
    
    static {
        boNames = new ArrayList(65);
        boNames.add("BUSINESS OBJECT");
        boNames.add("BUSINESS OBJECT ATTR");
        boNames.add("BUS OBJ ATTR UNIQ CONSTRAINTS");
        boNames.add("BUS OBJ RELATIONSHIP");
        boNames.add("WELL");
    }
    
    
    @Test
    public void testFindAll() {
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        try {
            List<String[]> failedBONames = new ArrayList<>();
            List<String> successBONames = new ArrayList<>();
            for (String boName : boNames) {
                try {
                    BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(boName, executionContext);
                    DynamicReadService.getInstance().readSimple(businessObjectInfo, executionContext).forEach(dynamicDTO -> logger.info(dynamicDTO.toString()));
                    successBONames.add(boName);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    failedBONames.add(new String[]{boName, e.getMessage()});
                }
            }
            
            successBONames.forEach(boName -> logger.info("fineAll() Success BO : {} ", boName));
            failedBONames.forEach(boNameAndErrorMessage -> logger.info("findAll() Failed BO : {}, Reason : {}", boNameAndErrorMessage[0], boNameAndErrorMessage[1]));
            
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
    }
    
    @Test
    public void testFindAllFithFilters() {
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        try {
            List<String[]> failedBONames = new ArrayList<>();
            List<String> successBONames = new ArrayList<>();
            // test all bos
            boNames.clear();
            boNames.add("WELL");
            for (String boName : boNames) {
                try {
                    BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(boName, executionContext);
                    businessObjectInfo.addColumnsToSelect("WELL_ID", "UWI", "SPUD_DATE");
                    businessObjectInfo.addFilter("WELL_ID", Operator.IN, 16, 17, 18, 25, 30, 31, 37);
                    businessObjectInfo.addFilter("WELL_ID", Operator.GREATER_OR_EQUALS, 20);
                    businessObjectInfo.addOrderByAsc("UWI");
                    businessObjectInfo.addOrderByDesc("SPUD_DATE");
                    businessObjectInfo.addPagesToRead(executionContext, 10, 1, 5, 11);
                    businessObjectInfo.setReadAllRecords(false);
                    businessObjectInfo.setReadMetadata(true);
                    businessObjectInfo.setReadReferenceData(true);
                    businessObjectInfo.setReadFirst(false);
                    businessObjectInfo.setReadUnique(false);
                    
                    Map<String, Object> map = DynamicReadService.getInstance().readWithDetails(businessObjectInfo, executionContext);
                    
                    map.forEach((key, value) -> logger.info(value.toString()));
                    successBONames.add(boName);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    failedBONames.add(new String[]{boName, e.getMessage()});
                }
            }
            
            successBONames.forEach(boName -> logger.info("fineAll() Success BO : {} ", boName));
            failedBONames.forEach(boNameAndErrorMessage -> logger.info("findAll() Failed BO : {}, Reason : {}", boNameAndErrorMessage[0], boNameAndErrorMessage[1]));
            
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
    }
    
    @Test
    public void testPagination() {
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        try {
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo("WELL", executionContext);
            businessObjectInfo.addPagesToRead(executionContext, 30, 3, 2);
            DynamicReadService.getInstance().readSimple(businessObjectInfo, executionContext).forEach(dynamicDTO -> logger.info(dynamicDTO.toString()));
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
    }
    
    @Test
    public void testReadMetadataAllBusinessObjects() {
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        try {
            List<DynamicDTO> list = DynamicReadService.getInstance().readMetadataAllBusinessObjects(executionContext);
            list.forEach(dynamicDTO -> logger.debug(dynamicDTO.toString()));
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
    }
    
    @Test
    public void testReadOne() {
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        try {
            DynamicDTO dynamicDTO = DynamicReadService.getInstance().readOne(new DynamicPK("WELL", 1), executionContext);
            logger.debug(Objects.toString(dynamicDTO));
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
    }
}
