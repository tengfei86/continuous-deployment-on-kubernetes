package com.lgc.dspdm.msp.mainservice;

import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.msp.mainservice.model.BOQuery;
import com.lgc.dspdm.msp.mainservice.model.CriteriaFilter;
import com.lgc.dspdm.msp.mainservice.model.DSPDMResponse;
import com.lgc.dspdm.msp.mainservice.model.FilterGroup;
import com.lgc.dspdm.msp.mainservice.model.Pagination;
import com.lgc.dspdm.msp.mainservice.model.join.JoiningCondition;
import com.lgc.dspdm.msp.mainservice.model.join.SimpleJoinClause;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SearchTest extends BaseServiceTest {
/*    private static DSPDMLogger logger = new DSPDMLogger(SearchTest.class);

    @Test
    public void testSimpleSearch() throws Throwable {
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName("WELL");
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        CriteriaFilter search_jsonb = new CriteriaFilter("SEARCH_JSONB", Operator.JSONB_FIND_LIKE, new Object[]{"ting"});
        queryEntity.setCriteriaFilters(Arrays.asList(search_jsonb));
        DSPDMResponse response = super.searchPost(queryEntity);
        if (DSPDMConstants.Status.SUCCESS == response.getStatus()) {
            logger.info("testSimpleSearch Passed.");
        } else {
            logger.info("testSimpleSearch failed.");
            throw response.getException();
        }
    }

    @Test
    public void testIndexAllForAREABoName() throws Throwable {
        DSPDMResponse response = super.searchPostIndexAllForBoName("WELL TEST");
        if (response.getException() == null) {
            logger.info("testIndexAllForAREABoName Passed.");
        } else {
            logger.info("testIndexAllForAREABoName failed.");
            throw response.getException();
        }
    }
    
    @Test
    public void testSearchJoinOr() throws Throwable {
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName("WELL");
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        queryEntity.setJoinAlias("well_1"); 
        CriteriaFilter search_criteria = new CriteriaFilter("UWI", Operator.JSONB_FIND_LIKE, new Object[]{"PDM"});
        CriteriaFilter search_jsonb = new CriteriaFilter("SEARCH_JSONB", Operator.JSONB_FIND_LIKE, new Object[]{"well"});
        queryEntity.setCriteriaFilters(Arrays.asList(search_criteria,search_jsonb));
        Pagination pg = new Pagination();
        pg.setRecordsPerPage(10);
        pg.setPages(Arrays.asList(1));
        queryEntity.setPagination(pg);
        List<SimpleJoinClause> simpleJoins= new ArrayList<>();
        SimpleJoinClause simpleJoin = new SimpleJoinClause();
        simpleJoin.setBoName("WELLBORE");
        simpleJoin.setJoinType("INNER");
        simpleJoin.setJoinAlias("wellbore_1");
        List<CriteriaFilter> filters = new ArrayList<>();
        filters.add(new CriteriaFilter("WELL_UWI", Operator.LIKE, new Object[]{"PDM"}));
        simpleJoin.setCriteriaFilters(filters); 
        simpleJoins.add(simpleJoin);
        queryEntity.setSimpleJoins(simpleJoins);
        DSPDMResponse response = super.searchPost(queryEntity);
        if (DSPDMConstants.Status.SUCCESS == response.getStatus()) {
            logger.info("testSimpleSearch Passed.");
        } else {
            logger.info("testSimpleSearch failed.");
            throw response.getException();
        }
    }
    
    @Test
    public void testSearchCriteriaJoins() throws Throwable {
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName("WELL");
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        queryEntity.setJoinAlias("W"); 
        CriteriaFilter search_jsonb = new CriteriaFilter("SEARCH_JSONB", Operator.JSONB_FIND_LIKE, new Object[]{"hal"});
        queryEntity.setCriteriaFilters(Arrays.asList(search_jsonb));
        Pagination pg = new Pagination();
        pg.setRecordsPerPage(30);
        pg.setPages(Arrays.asList(1));
        queryEntity.setPagination(pg);
        queryEntity.setSelectList(Arrays.asList( new String[]{"WELL_ID", "UWI", "WELL_NAME"}));
        List<SimpleJoinClause> simpleJoins= new ArrayList<>();
        SimpleJoinClause simpleJoin = new SimpleJoinClause();
        simpleJoin.setBoName("WELLBORE");
        simpleJoin.setJoinType("INNER");
        simpleJoin.setJoinAlias("WB");
        simpleJoin.setSelectList(Arrays.asList( new String[]{"WELLBORE_ID", "WELLBORE_NAME"}));
        simpleJoin.setJoiningConditions(Arrays.asList(new JoiningCondition()
                .setLeftSide(new JoiningCondition.JoiningConditionOperand().setJoinAlias("W").setBoAttrName("WELL_ID"))
                .setOperator(Operator.EQUALS).
                        setRightSide(new JoiningCondition.JoiningConditionOperand().setJoinAlias("WB").setBoAttrName("WELL_ID"))));
        simpleJoins.add(simpleJoin);

        simpleJoin = new SimpleJoinClause();
        simpleJoin.setBoName("WELL EQUIPMENT");
        simpleJoin.setJoinType("INNER");
        simpleJoin.setJoinAlias("WE");
        simpleJoin.setSelectList(Arrays.asList( new String[]{"EQUIPMENT_ID", "EQUIPMENT_NAME"}));
        simpleJoin.setJoiningConditions(Arrays.asList(new JoiningCondition()
                .setLeftSide(new JoiningCondition.JoiningConditionOperand().setJoinAlias("WB").setBoAttrName("WELLBORE_ID"))
                .setOperator(Operator.EQUALS).
                        setRightSide(new JoiningCondition.JoiningConditionOperand().setJoinAlias("WE").setBoAttrName("WELLBORE_ID"))));
        simpleJoins.add(simpleJoin);

        simpleJoin = new SimpleJoinClause();
        simpleJoin.setBoName("EQUIPMENT MEASUREMENT TAGS");
        simpleJoin.setJoinType("INNER");
        simpleJoin.setJoinAlias("EMT");
        simpleJoin.setSelectList(Arrays.asList( new String[]{"tags_id", "tags_info_id","equipment_measurement","R_EQUIPMENT_MEASUREMENT_ID"}));
        List<CriteriaFilter> filters = new ArrayList<>();
        filters.add(new CriteriaFilter("EQUIPMENT_MEASUREMENT", Operator.EQUALS, new Object[]{"Fluid Volume"}));
        simpleJoin.setCriteriaFilters(filters); 
        simpleJoin.setJoiningConditions(Arrays.asList(new JoiningCondition()
                .setLeftSide(new JoiningCondition.JoiningConditionOperand().setJoinAlias("WE").setBoAttrName("EQUIPMENT_ID"))
                .setOperator(Operator.EQUALS).
                        setRightSide(new JoiningCondition.JoiningConditionOperand().setJoinAlias("EMT").setBoAttrName("EQUIPMENT_ID"))));
        simpleJoins.add(simpleJoin);
        
        queryEntity.setSimpleJoins(simpleJoins);
        DSPDMResponse response = super.searchPost(queryEntity);
        if (DSPDMConstants.Status.SUCCESS == response.getStatus()) {
            logger.info("testSearchCriteriaJoins Passed.");
        } else {
            logger.info("testSearchCriteriaJoins failed.");
            throw response.getException();
        }
    }*/
}
