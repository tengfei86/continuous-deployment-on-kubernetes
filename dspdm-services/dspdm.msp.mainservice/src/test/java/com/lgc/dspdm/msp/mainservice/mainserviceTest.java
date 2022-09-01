package com.lgc.dspdm.msp.mainservice;

import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateFunction;
import com.lgc.dspdm.core.common.data.criteria.join.JoinType;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.msp.mainservice.model.*;
import com.lgc.dspdm.msp.mainservice.model.join.DynamicJoinClause;
import com.lgc.dspdm.msp.mainservice.model.join.DynamicTable;
import com.lgc.dspdm.msp.mainservice.model.join.JoiningCondition;
import com.lgc.dspdm.msp.mainservice.model.join.SimpleJoinClause;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

public class mainserviceTest extends BaseServiceTest {
    private DSPDMLogger logger = new DSPDMLogger(mainserviceTest.class);

    @Test
    public void testReadWithPostForBusinessObjectAttr() {
        String boName = DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR;
        BOQuery boQuery = new BOQuery();
        boQuery.setBoName(boName);
        boQuery.setLanguage("en");
        boQuery.setTimezone("GMT+08:00");
        DSPDMResponse dspdmResponse = super.commonPost(boQuery);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            Assert.assertEquals(boName, data.keySet().iterator().next());
            logger.info("Common read initial test passed.");
        }
    }

    @Test
    public void testReadWithPostForAggregateCode() {
        String boName = DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR;
        BOQuery boQuery = new BOQuery();
        boQuery.setBoName(boName);
        boQuery.setLanguage("en");
        boQuery.setTimezone("GMT+08:00");
        boQuery.addColumnsToSelect(DSPDMConstants.BoAttrName.BO_NAME);
        boQuery.addAggregateColumnsToSelect(new AggregateColumn(DSPDMConstants.BoAttrName.SEQUENCE_NUM, AggregateFunction.COUNT, "Total_Columns_Count"));
        boQuery.setHavingFilters(Arrays.asList(new HavingFilter(new AggregateColumn(DSPDMConstants.BoAttrName.SEQUENCE_NUM, AggregateFunction.COUNT), Operator.GREATER_OR_EQUALS, new Object[]{25})));
        DSPDMResponse dspdmResponse = super.commonPost(boQuery);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            String responseBoName = data.keySet().iterator().next();
            Assert.assertEquals(boName, responseBoName);
            PagedList<DynamicDTO> pagedList = (PagedList<DynamicDTO>) data.get(responseBoName);
            if (CollectionUtils.hasValue(pagedList)) {
                Integer totalColumnsCount = null;
                for (DynamicDTO dynamicDTO : pagedList) {
                    totalColumnsCount = (Integer) dynamicDTO.get("Total_Columns_Count");
                    Assert.assertTrue(totalColumnsCount >= 25);
                    logger.info("BO_NAME : {} , Total_Columns_Count : {}", dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME), totalColumnsCount);
                }
            }
            logger.info("Aggregate Functions Test Passed.");
        }
    }

    @Test
    public void testReadWithPostForSimpleJoin() {
        String boName = "WELL";
        String alias = "well_1";
        String joinedBoName = "WELLBORE";
        String joinAlias = "wellbore_1";
        BOQuery boQuery = new BOQuery();
        boQuery.setBoName(boName);
        boQuery.setLanguage("en");
        boQuery.setTimezone("GMT+08:00");
        boQuery.setJoinAlias(alias);
        boQuery.addColumnsToSelect("WELL_ID", "UWI");
        boQuery.addAggregateColumnsToSelect(new AggregateColumn("UWI", AggregateFunction.COUNT, "Total_WELL_Count"));
        boQuery.setCriteriaFilters(Arrays.asList(new CriteriaFilter("UWI", Operator.LIKE, new Object[]{"PDM"})));
        boQuery.setHavingFilters(Arrays.asList(new HavingFilter(new AggregateColumn("UWI", AggregateFunction.COUNT), Operator.GREATER_OR_EQUALS, new Object[]{5})));
        boQuery.setPagination(new Pagination(30, Arrays.asList(1)));
        // create simple join clause
        SimpleJoinClause wellboreJoinClause = new SimpleJoinClause();
        wellboreJoinClause.setBoName(joinedBoName);
        wellboreJoinClause.setJoinAlias(joinAlias);
        wellboreJoinClause.setJoinType(JoinType.INNER.name());
        wellboreJoinClause.setSelectList(Arrays.asList("WELLBORE_NAME", "WELL_UWI"));
        wellboreJoinClause.setCriteriaFilters(Arrays.asList(new CriteriaFilter("WELL_UWI", Operator.LIKE, new Object[]{"PDM"})));
        wellboreJoinClause.setOrderBy(Arrays.asList(new OrderBy("WELL_UWI", Order.DESC)));
        wellboreJoinClause.setJoiningConditions(Arrays.asList(new JoiningCondition()
                .setLeftSide(new JoiningCondition.JoiningConditionOperand().setJoinAlias(alias).setBoAttrName("UWI"))
                .setOperator(Operator.EQUALS).
                        setRightSide(new JoiningCondition.JoiningConditionOperand().setJoinAlias(joinAlias).setBoAttrName("WELL_UWI"))));
        // add simple join to BOQuery
        boQuery.setSimpleJoins(Arrays.asList(wellboreJoinClause));
        boQuery.setPrependAlias(true);
        // send request
        DSPDMResponse dspdmResponse = super.commonPost(boQuery);
        // if there is no error it means query executed successfully.
        logger.info("Simple join Test Passed.");
    }

    @Test
    public void testReadWithPostForSimpleJoinNoRelationship() {
        String boName = "WELL";
        String alias = "well_1";
        String joinedBoName = "Well VOL DAILY";
        String joinAlias = "wvd_1";
        BOQuery boQuery = new BOQuery();
        boQuery.setBoName(boName);
        boQuery.setLanguage("en");
        boQuery.setTimezone("GMT+08:00");
        boQuery.setJoinAlias(alias);
        boQuery.addColumnsToSelect("WELL_ID");
        boQuery.addAggregateColumnsToSelect(new AggregateColumn("UWI", AggregateFunction.COUNT, "Total_WELL_Count"));
        boQuery.setCriteriaFilters(Arrays.asList(new CriteriaFilter("UWI", Operator.LIKE, new Object[]{"PDM"})));
        boQuery.setHavingFilters(Arrays.asList(new HavingFilter(new AggregateColumn("UWI", AggregateFunction.COUNT), Operator.GREATER_OR_EQUALS, new Object[]{5})));
        boQuery.setPagination(new Pagination(30, Arrays.asList(1)));
        // create simple join clause
        SimpleJoinClause vwdJoinClause = new SimpleJoinClause();
        vwdJoinClause.setSelectList(Arrays.asList("VOLUME_DATE"));
        vwdJoinClause.setBoName(joinedBoName);
        vwdJoinClause.setJoinAlias(joinAlias);
        vwdJoinClause.setJoinType(JoinType.INNER.name());
        vwdJoinClause.setHavingFilters(Arrays.asList(new HavingFilter(new AggregateColumn("VOLUME_DATE", AggregateFunction.MAX), Operator.GREATER_OR_EQUALS, new Object[]{"2018-05-08 16:51:54.049"})));
        vwdJoinClause.setOrderBy(Arrays.asList(new OrderBy("VOLUME_DATE", Order.DESC)));
        vwdJoinClause.setJoiningConditions(Arrays.asList(new JoiningCondition()
                .setLeftSide(new JoiningCondition.JoiningConditionOperand().setJoinAlias(alias).setBoAttrName("WELL_ID"))
                .setOperator(Operator.EQUALS).
                        setRightSide(new JoiningCondition.JoiningConditionOperand().setJoinAlias(joinAlias).setBoAttrName("WELL_ID"))));
        // add simple join to BOQuery
        boQuery.setSimpleJoins(Arrays.asList(vwdJoinClause));
        boQuery.setPrependAlias(false);
        // send request
        DSPDMResponse dspdmResponse = super.commonPost(boQuery);
        // if there is no error it means query executed successfully.
        logger.info("Simple join Test Passed.");
    }

    @Test
    public void testReadWithPostForDynamicJoin() {
        String boName = DSPDMConstants.BoName.WELL_VOL_DAILY;
        String alias = "a";
        String joinedBoName = DSPDMConstants.BoName.WELL_VOL_DAILY;
        String joinAlias = "b";
        String max_volume_date = "max_volume_date";
        BOQuery boQuery = new BOQuery();
        boQuery.setBoName(boName);
        boQuery.setLanguage("en");
        boQuery.setTimezone("GMT+08:00");
        boQuery.setJoinAlias(alias);
        boQuery.addColumnsToSelect(DSPDMConstants.BoAttrName.UWI, DSPDMConstants.BoAttrName.BOE_VOLUME);
        // create dynamic join clause
        DynamicJoinClause dynamicJoinClause = new DynamicJoinClause();
        dynamicJoinClause.setSelectList(Arrays.asList("UWI", max_volume_date));
        dynamicJoinClause.setJoinType(JoinType.INNER.name());
        dynamicJoinClause.setJoinAlias(joinAlias);
        dynamicJoinClause.setJoinType(JoinType.INNER.name());
        // dynamic table
        DynamicTable dynamicTable = new DynamicTable();
        dynamicTable.setBoName(joinedBoName);
        dynamicTable.setSelectList(Arrays.asList(DSPDMConstants.BoAttrName.UWI));
        dynamicTable.setAggregateSelectList(Arrays.asList(new AggregateColumn(DSPDMConstants.BoAttrName.VOLUME_DATE, AggregateFunction.MAX, max_volume_date)));
        dynamicJoinClause.setDynamicTables(Arrays.asList(dynamicTable));
        // joining condition
        JoiningCondition uwiJoiningCondition = new JoiningCondition()
                .setLeftSide(new JoiningCondition.JoiningConditionOperand().setJoinAlias(alias).setBoAttrName(DSPDMConstants.BoAttrName.UWI))
                .setOperator(Operator.EQUALS).
                        setRightSide(new JoiningCondition.JoiningConditionOperand().setJoinAlias(joinAlias).setBoAttrName(DSPDMConstants.BoAttrName.UWI));
        JoiningCondition volumeDateJoiningCondition = new JoiningCondition()
                .setLeftSide(new JoiningCondition.JoiningConditionOperand().setJoinAlias(alias).setBoAttrName(DSPDMConstants.BoAttrName.VOLUME_DATE))
                .setOperator(Operator.EQUALS).
                        setRightSide(new JoiningCondition.JoiningConditionOperand().setJoinAlias(joinAlias).setBoAttrName(max_volume_date));
        dynamicJoinClause.setJoiningConditions(Arrays.asList(uwiJoiningCondition, volumeDateJoiningCondition));
        // criteria filter
        dynamicJoinClause.setCriteriaFilters(Arrays.asList(new CriteriaFilter(max_volume_date, Operator.GREATER_OR_EQUALS, new Object[]{"2018-05-08 16:51:54.049"})));
        // order by
        dynamicJoinClause.setOrderBy(Arrays.asList(new OrderBy(DSPDMConstants.BoAttrName.UWI, Order.ASC)));
        // add simple join to BOQuery
        boQuery.setDynamicJoins(Arrays.asList(dynamicJoinClause));
        boQuery.setPrependAlias(false);
        boQuery.setReadAllRecords(true);
        // send request
        DSPDMResponse dspdmResponse = super.commonPost(boQuery);
        // if there is no error it means query executed successfully.
        logger.info("Dynamic join Test Passed.");
    }
    
    @Test
    public void testReadBOEVolumeData() {
        String boName = DSPDMConstants.BoName.WELL_VOL_DAILY;
        BOQuery boQuery = new BOQuery();
        boQuery.setBoName(boName);
        boQuery.setLanguage("en");
        boQuery.setTimezone("GMT+08:00");
        DSPDMResponse dspdmResponse = super.customPostBoe(boQuery);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            Assert.assertEquals(boName, data.keySet().iterator().next());
            logger.info("Custom BOE read initial test passed.");
        }
    }
}
