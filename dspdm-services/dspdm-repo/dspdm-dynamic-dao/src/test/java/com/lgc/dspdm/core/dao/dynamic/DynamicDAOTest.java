package com.lgc.dspdm.core.dao.dynamic;

import com.lgc.dspdm.config.AppConfigInit;
import com.lgc.dspdm.core.common.config.PropertySource;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateColumn;
import com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateFunction;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.DateTimeUtils;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.dao.dynamic.businessobject.DynamicDAOFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class DynamicDAOTest {
    private static DSPDMLogger logger = new DSPDMLogger(DynamicDAOTest.class);
    private static List<String> businessObjectBONames = null;

    static {

        businessObjectBONames = new ArrayList<>();
        businessObjectBONames.add("WELL");
        businessObjectBONames.add("WELLBORE");
    }

    @BeforeAll
    public static void setUp() throws Exception {
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
    public void testAllBusinessObjectDAOs() {
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        try {
            // test all bos
            for (String boName : businessObjectBONames) {
                DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(boName, executionContext).read(new BusinessObjectInfo(boName, executionContext), executionContext).forEach(dynamicDTO -> logger.info(dynamicDTO.toString()));
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
    }

    @Test
    public void testNumericLike() {
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        try {
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo("WELL", executionContext);
            businessObjectInfo.addFilter("WELL_ID", Operator.LIKE, 1);
            DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(businessObjectInfo.getBusinessObjectType(), executionContext)
                    .read(businessObjectInfo, executionContext).forEach(dynamicDTO -> logger.info(dynamicDTO.toString()));
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
    }

    @Test
    public void testTimestamp() {
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        try {
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo("WELL", executionContext);
            businessObjectInfo.addFilter("COMPLETION_DATE", Operator.GREATER_OR_EQUALS, DateTimeUtils.getTimestamp(DateTimeUtils.parse("2019-06-30T19:30:00.000Z", executionContext)));
            DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(businessObjectInfo.getBusinessObjectType(), executionContext)
                    .read(businessObjectInfo, executionContext).forEach(dynamicDTO -> logger.info(dynamicDTO.toString()));
        } catch (Exception e) {
            logger.error(e);
            DSPDMException.throwException(e, executionContext);
        }
    }

    @Test
    public void testSelectList() {
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        try {
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo("WELL", executionContext);
            businessObjectInfo.addColumnsToSelect(
                    "WELL_ID",
                    "IS_ACTIVE",
                    "UWI",
                    "COMPLETION_DATE"
            );
            DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(businessObjectInfo.getBusinessObjectType(), executionContext)
                    .read(businessObjectInfo, executionContext).forEach(dynamicDTO -> logger.info(dynamicDTO.toString()));
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
    }

    @Test
    public void testSimpleAggregateMaxWithoutCustomAlias() {
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        try {
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo("BUSINESS OBJECT ATTR", executionContext);
            businessObjectInfo.addAggregateColumnToSelect(new AggregateColumn(AggregateFunction.MAX, DSPDMConstants.BoAttrName.SEQUENCE_NUM));
            businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.BO_NAME, "WELL");
            DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(businessObjectInfo.getBusinessObjectType(), executionContext)
                    .read(businessObjectInfo, executionContext).forEach(dynamicDTO -> logger.info(dynamicDTO.toString()));
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
    }

    @Test
    public void testSimpleAggregateMaxWithCustomAlias() {
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        try {
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo("BUSINESS OBJECT ATTR", executionContext);
            businessObjectInfo.addAggregateColumnToSelect(new AggregateColumn(AggregateFunction.MAX, DSPDMConstants.BoAttrName.SEQUENCE_NUM, "MAX_SEQUENCE"));
            businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.BO_NAME, "WELL");
            List<DynamicDTO> dtoList = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(businessObjectInfo.getBusinessObjectType(), executionContext).read(businessObjectInfo, executionContext);
            for (DynamicDTO dynamicDTO : dtoList) {
                Integer max = (Integer) dynamicDTO.get("MAX_SEQUENCE");
                logger.info("MAX : {} ", max.toString());
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
    }

    @Test
    public void testSimpleAggregateAvgWithoutCustomAlias() {
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        try {
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo("BUSINESS OBJECT ATTR", executionContext);
            businessObjectInfo.addAggregateColumnToSelect(new AggregateColumn(AggregateFunction.AVG, DSPDMConstants.BoAttrName.SEQUENCE_NUM));
            businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.BO_NAME, "WELL");
            List<DynamicDTO> dtoList = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(businessObjectInfo.getBusinessObjectType(), executionContext).read(businessObjectInfo, executionContext);
            for (DynamicDTO dynamicDTO : dtoList) {
                logger.info(dynamicDTO.toString());
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
    }

    @Test
    public void testSimpleAggregateMaxWithGroupBy() {
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        try {
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo("BUSINESS OBJECT ATTR", executionContext);
            businessObjectInfo.addColumnsToSelect(DSPDMConstants.BoAttrName.BO_NAME);
            businessObjectInfo.addAggregateColumnToSelect(new AggregateColumn(AggregateFunction.MAX, DSPDMConstants.BoAttrName.SEQUENCE_NUM, "MAX_SEQUENCE"));
            List<DynamicDTO> dtoList = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(businessObjectInfo.getBusinessObjectType(), executionContext).read(businessObjectInfo, executionContext);
            for (DynamicDTO dynamicDTO : dtoList) {
                Integer max = (Integer) dynamicDTO.get("MAX_SEQUENCE");
                logger.info("BO_NAME : {}, MAX COLUMN COUNT : {} ", dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME), max.toString());
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
    }

    @Test
    public void testHaving() {
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        try {
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo("BUSINESS OBJECT ATTR", executionContext);
            businessObjectInfo.addColumnsToSelect(DSPDMConstants.BoAttrName.BO_NAME);
            businessObjectInfo.addAggregateColumnToSelect(new AggregateColumn(AggregateFunction.COUNT, DSPDMConstants.BoAttrName.SEQUENCE_NUM, "Total_Columns_Count"));
            businessObjectInfo.addHavingFilter(new AggregateColumn(AggregateFunction.COUNT, DSPDMConstants.BoAttrName.SEQUENCE_NUM), Operator.GREATER_OR_EQUALS, 25);
            DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(businessObjectInfo.getBusinessObjectType(), executionContext)
                    .read(businessObjectInfo, executionContext).forEach(dynamicDTO -> logger.info(dynamicDTO.toString()));
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
    }

    @Test
    public void testHavingWithInOperator() {
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        try {
            BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo("BUSINESS OBJECT ATTR", executionContext);
            businessObjectInfo.addColumnsToSelect(DSPDMConstants.BoAttrName.BO_NAME);
            businessObjectInfo.addAggregateColumnToSelect(new AggregateColumn(AggregateFunction.COUNT, DSPDMConstants.BoAttrName.SEQUENCE_NUM, "Total_Columns_Count"));
            businessObjectInfo.addHavingFilter(new AggregateColumn(AggregateFunction.COUNT, DSPDMConstants.BoAttrName.SEQUENCE_NUM), Operator.NOT_IN, 25, 48, 42);
            DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(businessObjectInfo.getBusinessObjectType(), executionContext)
                    .read(businessObjectInfo, executionContext).forEach(dynamicDTO -> logger.info(dynamicDTO.toString()));
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
    }

    @Test
    public void testSaveBOSearch() {
//        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
//        try {
//            IDynamicDAO searchDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.BO_SEARCH, executionContext);
//            DynamicDTO dynamicDTO = new DynamicDTO(DSPDMConstants.BoName.BO_SEARCH, searchDAO.getPrimaryKeyColumnNames());
//            dynamicDTO.put("BO_NAME", "WELL");
//            dynamicDTO.put("PK_BO_ATTR_NAMES", "WELL_ID");
//            dynamicDTO.put("PK_BO_ATTR_VALUES", "SOLR");
//            dynamicDTO.put("SEARCH_JSONB", "[\"WELL\" , \"q12\"]");
//            searchDAO.saveOrUpdate(Arrays.asList(dynamicDTO), executionContext);
//        } catch (Exception e) {
//            DSPDMException.throwException(e, executionContext);
//        }
    }
}
