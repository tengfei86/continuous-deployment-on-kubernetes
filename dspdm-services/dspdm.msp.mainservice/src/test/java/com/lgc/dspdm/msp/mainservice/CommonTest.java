package com.lgc.dspdm.msp.mainservice;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.SelectList;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.msp.mainservice.model.*;
import com.lgc.dspdm.msp.mainservice.model.join.SimpleJoinClause;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

public class CommonTest extends BaseServiceTest {
/*    private static DSPDMLogger logger = new DSPDMLogger(CommonTest.class);

    @Test
    public void testCommonGet() {
        try {
            DSPDMResponse dspdmResponse_well = super.commonGet("WELL");
            logger.info("testCommonGet Passed.");
        } catch (Exception e) {
            logger.error(e);
            throw e;
        }
    }


    @Test
    public void testPostWithOneChild() {
        try {
            String boName = "WELL";
            BOQuery wellEntity = new BOQuery();
            wellEntity.setBoName(boName);
            wellEntity.setLanguage("en");
            wellEntity.setTimezone("GMT+08:00");
            String[] select = {"WELL_ID", "UWI"};
            wellEntity.setSelectList(Arrays.asList(select));
            wellEntity.setPagination(new Pagination(10, Arrays.asList(1)));
            wellEntity.setOrderBy(Arrays.asList(new OrderBy("WELL_ID", Order.DESC)));
            List<BOQuery> childrenList = new ArrayList<>();
            BOQuery wellBoreEntity = new BOQuery();
            select = new String[]{"WELLBORE_ID", "WELLBORE_UWI", "WELL_ID", "WELL_UWI"};
            wellBoreEntity.setBoName("WELLBORE");
            wellBoreEntity.setLanguage("en");
            wellBoreEntity.setTimezone("GMT+08:00");
            wellBoreEntity.setSelectList(Arrays.asList(select));
            wellBoreEntity.setPagination(new Pagination(1, Arrays.asList(1)));
            childrenList.add(wellBoreEntity);
            wellEntity.setReadChildBO(childrenList);
            DSPDMResponse dspdmResponse = super.commonPost(wellEntity);
            Map<String, Object> data = dspdmResponse.getData();
            if (data != null) {
                Assert.assertEquals(boName, data.keySet().iterator().next());
                ObjectMapper mapper = new ObjectMapper();
                JsonNode wellArray = mapper.valueToTree(data.get(boName));
                if ((wellArray != null) && (wellArray.size() > 0)) {
                    // validate total records
                    Assert.assertTrue(wellArray.size() <= (wellEntity.getPagination().getRecordsPerPage() * wellEntity.getPagination().getPages().size()));
                    int columnCount = 0;
                    for (String field : wellEntity.getSelectList()) {
                        if (wellArray.get(0).has(field)) {
                            columnCount++;
                        }
                    }
                    // validate select list columns
                    Assert.assertEquals(columnCount, wellEntity.getSelectList().size());
                    for (int i = 0; i < wellArray.size(); i++) {
                        if (i > 0) {
                            // validate order by
                            Assert.assertTrue(wellArray.get(i).get("WELL_ID").asInt() <= wellArray.get(i - 1).get("WELL_ID").asInt());
                        }
                        JsonNode wellBoreArray = wellArray.get(i).get("WELLBORE");
                        if (wellBoreArray != null) {
                            if (wellBoreArray.size() > 0) {
                                // validate child records per parent
                                Assert.assertTrue(wellBoreArray.size() == (wellBoreEntity.getPagination().getRecordsPerPage() * wellBoreEntity.getPagination().getPages().size()));
                            }
                            columnCount = 0;
                            for (String field : wellBoreEntity.getSelectList()) {
                                if (wellBoreArray.get(0).has(field)) {
                                    columnCount++;
                                }
                            }
                            // validate select list columns
                            Assert.assertTrue(columnCount == wellBoreEntity.getSelectList().size());
                            // Match foreign key column values
                            for (int j = 0; j < wellBoreArray.size(); j++) {
                                Assert.assertTrue(wellArray.get(i).get("WELL_ID").asInt() == wellBoreArray.get(j).get("WELL_ID").asInt());
                            }
                        }
                    }
                }
                logger.info("testPostWithOneChild passed.");
            }
        } catch (IllegalArgumentException e) {
            logger.error(e);
            throw e;
        }
    }

    @Test
    public void testPostWithChildren() {
        try {
            String boName = "WELL";
            BOQuery wellEntity = new BOQuery();
            wellEntity.setBoName(boName);
            wellEntity.setLanguage("en");
            wellEntity.setTimezone("GMT+08:00");
            String[] select = {"WELL_ID", "UWI"};
            wellEntity.setSelectList(Arrays.asList(select));
            List<CriteriaFilter> criteriaFilters = new ArrayList<>(1);
            criteriaFilters.add(new CriteriaFilter("UWI", Operator.LIKE, new Object[]{"%TW%"}));
            wellEntity.setCriteriaFilters(criteriaFilters);
            wellEntity.setPagination(new Pagination(10, Arrays.asList(1)));
            wellEntity.setOrderBy(Arrays.asList(new OrderBy("WELL_ID", Order.DESC)));
            List<BOQuery> childrenList = new ArrayList<>();
            BOQuery wellBoreEntity = new BOQuery();
            select = new String[]{"WELLBORE_ID", "WELLBORE_UWI", "WELL_ID", "WELL_UWI"};
            wellBoreEntity.setBoName("WELLBORE");
            wellBoreEntity.setLanguage("en");
            wellBoreEntity.setTimezone("GMT+08:00");
            wellBoreEntity.setSelectList(Arrays.asList(select));
            wellBoreEntity.setPagination(new Pagination(2, Arrays.asList(1)));
            childrenList.add(wellBoreEntity);
            BOQuery wellAliasEntity = new BOQuery();
            wellAliasEntity.setBoName("WELL ALIAS");
            wellAliasEntity.setLanguage("en");
            wellAliasEntity.setTimezone("GMT+08:00");
            select = new String[]{"WELL_ID", "R_ALIAS_REASON_ID"};
            wellAliasEntity.setSelectList(Arrays.asList(select));
            wellAliasEntity.setPagination(new Pagination(10, Arrays.asList(1)));
            childrenList.add(wellAliasEntity);
            wellEntity.setReadChildBO(childrenList);
            DSPDMResponse dspdmResponse = super.commonPost(wellEntity);

            Map<String, Object> data = dspdmResponse.getData();
            if (data != null) {
                Assert.assertEquals(boName, data.keySet().iterator().next());
                ObjectMapper mapper = new ObjectMapper();
                JsonNode wellArray = mapper.valueToTree(data.get(boName));
                if (wellArray != null) {
                    if (wellArray.size() > 0) {
                        // validate total records
                        Assert.assertTrue(wellArray.size() <= (wellEntity.getPagination().getRecordsPerPage() * wellEntity.getPagination().getPages().size()));
                        int columnCount = 0;
                        for (String field : wellEntity.getSelectList()) {
                            if (wellArray.get(0).has(field)) {
                                columnCount++;
                            }
                        }
                        // validate select list columns
                        Assert.assertTrue(columnCount == wellEntity.getSelectList().size());
                        for (int i = 0; i < wellArray.size(); i++) {
                            Assert.assertTrue(wellArray.get(i).get("UWI").asText().contains("TW"));
                            if (i > 0) {
                                // validate order by
                                Assert.assertTrue(wellArray.get(i).get("WELL_ID").asInt() <= wellArray.get(i - 1).get("WELL_ID").asInt());
                            }
                            // Process WELLBORE Data
                            JsonNode wellBoreArray = wellArray.get(i).get("WELLBORE");
                            if (wellBoreArray != null) {
                                if (wellBoreArray.size() > 0) {
                                    // validate child records per parent
                                    Assert.assertTrue(wellBoreArray.size() <= (wellBoreEntity.getPagination().getRecordsPerPage() * wellBoreEntity.getPagination().getPages().size()));
                                }
                                columnCount = 0;
                                for (String field : wellBoreEntity.getSelectList()) {
                                    if (wellBoreArray.get(0).has(field)) {
                                        columnCount++;
                                    }
                                }
                                // validate select list columns
                                Assert.assertTrue(columnCount == wellBoreEntity.getSelectList().size());
                                // Match foreign key column values
                                for (int j = 0; j < wellBoreArray.size(); j++) {
                                    Assert.assertTrue(wellArray.get(i).get("WELL_ID").asInt() == wellBoreArray.get(j).get("WELL_ID").asInt());
                                }
                            }
                            // Process WELL ALIAS DATA
                            JsonNode wellAliasArray = wellArray.get(i).get("WELL ALIAS");
                            if (wellAliasArray != null) {
                                if (wellAliasArray.size() > 0) {
                                    Assert.assertTrue(wellAliasArray.size() <= (wellAliasEntity.getPagination().getRecordsPerPage() * wellAliasEntity.getPagination().getPages().size()));
                                }
                                columnCount = 0;
                                for (String field : wellAliasEntity.getSelectList()) {
                                    if (wellAliasArray.get(0).has(field)) {
                                        columnCount++;
                                    }
                                }
                                // validate select list columns
                                Assert.assertTrue(columnCount == wellAliasEntity.getSelectList().size());
                                // Match foreign key column values
                                for (int j = 0; j < wellAliasArray.size(); j++) {
                                    Assert.assertTrue(wellArray.get(i).get("WELL_ID").asInt() == wellAliasArray.get(j).get("WELL_ID").asInt());
                                }
                            }
                        }
                    }
                }
                logger.info("testPostWithChildren passed.");
            }
        } catch (IllegalArgumentException e) {
            logger.error(e);
            throw e;
        }
    }


    @Test
    public void testPostWithNestedChildren() {
        try {
            String boName = "WELL";
            BOQuery wellEntity = new BOQuery();
            wellEntity.setBoName(boName);
            wellEntity.setLanguage("en");
            wellEntity.setTimezone("GMT+08:00");
            String[] select = {"WELL_ID", "UWI"};
            wellEntity.setSelectList(Arrays.asList(select));
            List<CriteriaFilter> criteriaFilters = new ArrayList<>();
            criteriaFilters.add(new CriteriaFilter("UWI", Operator.LIKE, new Object[]{"%TW%"}));
            wellEntity.setCriteriaFilters(criteriaFilters);
            wellEntity.setPagination(new Pagination(10, Arrays.asList(1)));
            wellEntity.setOrderBy(Arrays.asList(new OrderBy("WELL_ID", Order.DESC)));
            List<BOQuery> childrenList = new ArrayList<>();
            BOQuery wellBoreEntity = new BOQuery();
            select = new String[]{"WELLBORE_ID", "WELLBORE_UWI", "WELL_ID", "WELL_UWI"};
            wellBoreEntity.setBoName("WELLBORE");
            wellBoreEntity.setLanguage("en");
            wellBoreEntity.setTimezone("GMT+08:00");
            wellBoreEntity.setSelectList(Arrays.asList(select));
            wellBoreEntity.setPagination(new Pagination(2, Arrays.asList(1)));
            List<BOQuery> nestedChildren = new ArrayList<>();
            BOQuery wellEquipmentEntity = new BOQuery();
            wellEquipmentEntity.setBoName("WELL EQUIPMENT");
            wellEquipmentEntity.setLanguage("en");
            wellEquipmentEntity.setTimezone("GMT+08:00");
            select = new String[]{"WELLBORE_ID", "WELLBORE_UWI", "INSTALL_BASE_DEPTH"};
            wellEquipmentEntity.setSelectList(Arrays.asList(select));
            wellEquipmentEntity.setPagination(new Pagination(2, Arrays.asList(1)));
            wellEquipmentEntity.setOrderBy(Arrays.asList(new OrderBy("INSTALL_BASE_DEPTH", Order.DESC)));
            nestedChildren.add(wellEquipmentEntity);
            wellBoreEntity.setReadChildBO(nestedChildren);
            childrenList.add(wellBoreEntity);
            BOQuery wellAliasEntity = new BOQuery();
            wellAliasEntity.setBoName("WELL ALIAS");
            wellAliasEntity.setLanguage("en");
            wellAliasEntity.setTimezone("GMT+08:00");
            select = new String[]{"WELL_ID", "R_ALIAS_REASON_ID"};
            wellAliasEntity.setSelectList(Arrays.asList(select));
            wellAliasEntity.setPagination(new Pagination(10, Arrays.asList(1)));
            childrenList.add(wellAliasEntity);
            wellEntity.setReadChildBO(childrenList);
            DSPDMResponse dspdmResponse = super.commonPost(wellEntity);
            Map<String, Object> data = dspdmResponse.getData();
            if (data != null) {
                Assert.assertEquals(boName, data.keySet().iterator().next());
                ObjectMapper mapper = new ObjectMapper();
                JsonNode wellArray = mapper.valueToTree(data.get(boName));
                if (wellArray != null) {
                    if (wellArray.size() > 0) {
                        // validate total records
                        Assert.assertTrue(wellArray.size() <= (wellEntity.getPagination().getRecordsPerPage() * wellEntity.getPagination().getPages().size()));
                        int columnCount = 0;
                        for (String field : wellEntity.getSelectList()) {
                            if (wellArray.get(0).has(field)) {
                                columnCount++;
                            }
                        }
                        // validate select list columns
                        Assert.assertTrue(columnCount == wellEntity.getSelectList().size());
                        for (int i = 0; i < wellArray.size(); i++) {
                            Assert.assertTrue(wellArray.get(i).get("UWI").asText().contains("TW"));
                            if (i > 0) {
                                // validate order by
                                Assert.assertTrue(wellArray.get(i).get("WELL_ID").asInt() <= wellArray.get(i - 1).get("WELL_ID").asInt());
                            }
                            // Process WELLBORE Data
                            JsonNode wellBoreArray = wellArray.get(i).get("WELLBORE");
                            if (wellBoreArray != null) {
                                if (wellBoreArray.size() > 0) {
                                    // validate child records per parent
                                    Assert.assertTrue(wellBoreArray.size() <= (wellBoreEntity.getPagination().getRecordsPerPage() * wellBoreEntity.getPagination().getPages().size()));
                                }
                                columnCount = 0;
                                for (String field : wellBoreEntity.getSelectList()) {
                                    if (wellBoreArray.get(0).has(field)) {
                                        columnCount++;
                                    }
                                }
                                // validate select list columns
                                Assert.assertTrue(columnCount == wellBoreEntity.getSelectList().size());
                                // Match foreign key column values
                                for (int j = 0; j < wellBoreArray.size(); j++) {
                                    Assert.assertTrue(wellArray.get(i).get("WELL_ID").asInt() == wellBoreArray.get(j).get("WELL_ID").asInt());
                                    // Process WELL EQUIPMENT Data
                                    JsonNode wellEquipmentArray = wellBoreArray.get(j).get("WELL EQUIPMENT");
                                    if (wellEquipmentArray != null) {
                                        if (wellEquipmentArray.size() > 0) {
                                            Assert.assertTrue(wellEquipmentArray.size()
                                                    <= (wellEquipmentEntity.getPagination().getRecordsPerPage() * wellEquipmentEntity.getPagination().getPages().size()));
                                        }
                                        columnCount = 0;
                                        for (String field : wellAliasEntity.getSelectList()) {
                                            if (wellEquipmentArray.get(0).has(field)) {
                                                columnCount++;
                                            }
                                        }
                                        // validate select list columns
                                        Assert.assertTrue(columnCount == wellAliasEntity.getSelectList().size());
                                        // Match foreign key column values
                                        // validate criteria filter
                                        for (int k = 0; k < wellEquipmentArray.size(); k++) {
                                            Assert.assertTrue(wellBoreArray.get(i).get("WELLBORE_ID").asInt() == wellEquipmentArray.get(k).get("WELLBORE_ID").asInt());
                                            if (k > 0 && wellEquipmentArray.get(k) != null && wellEquipmentArray.get(k).get("INSTALL_BASE_DEPTH") != null) {
                                                Assert.assertTrue(wellEquipmentArray.get(k).get("INSTALL_BASE_DEPTH").asDouble() <= wellEquipmentArray.get(k - 1).get("INSTALL_BASE_DEPTH").asDouble());
                                            }
                                        }
                                    }
                                }
                            }
                            // Process WELL ALIAS DATA
                            JsonNode wellAliasArray = wellArray.get(i).get("WELL ALIAS");
                            if (wellAliasArray != null) {
                                if (wellAliasArray.size() > 0) {
                                    Assert.assertTrue(wellAliasArray.size() <= (wellAliasEntity.getPagination().getRecordsPerPage() * wellAliasEntity.getPagination().getPages().size()));
                                }
                                columnCount = 0;
                                for (String field : wellAliasEntity.getSelectList()) {
                                    if (wellAliasArray.get(0).has(field)) {
                                        columnCount++;
                                    }
                                }
                                // validate select list columns
                                Assert.assertTrue(columnCount == wellAliasEntity.getSelectList().size());
                                // Match foreign key column values
                                for (int j = 0; j < wellAliasArray.size(); j++) {
                                    Assert.assertTrue(wellArray.get(i).get("WELL_ID").asInt() == wellAliasArray.get(j).get("WELL_ID").asInt());
                                }
                            }
                        }
                    }
                }
                logger.info("testPostWithNestedChildren passed.");
            }
        } catch (IllegalArgumentException e) {
            logger.error(e);
            throw e;
        }
    }

    @Test
    public void testPagination() {
        try {
            String boName = "WELL";
            BOQuery queryEntity = new BOQuery();
            queryEntity.setBoName(boName);
            queryEntity.setLanguage("en");
            queryEntity.setTimezone("GMT+08:00");
            Pagination pg = new Pagination();
            pg.setRecordsPerPage(10);
            queryEntity.setPagination(pg);
            DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
            Map<String, Object> data = dspdmResponse.getData();
            if (data != null) {
                Assert.assertEquals(boName, data.keySet().iterator().next());
                logger.info("testPagination passed.");
            }
        } catch (Exception e) {
            logger.error(e);
            throw e;
        }
    }

    @Test
    public void testSelectList() {
        try {
            String boName = "WELL";
            BOQuery queryEntity = new BOQuery();
            queryEntity.setBoName(boName);
            queryEntity.setLanguage("en");
            queryEntity.setTimezone("GMT+08:00");
            Pagination pg = new Pagination();
            pg.setRecordsPerPage(1);
            queryEntity.setPagination(pg);
            String[] select = {"UWI", "ROW_CREATED_DATE", "ROW_CREATED_BY"};
            queryEntity.setSelectList(Arrays.asList(select));
            DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
            Map<String, Object> data = dspdmResponse.getData();
            if (data != null) {
                Assert.assertEquals(boName, data.keySet().iterator().next());
                ObjectMapper mapper = new ObjectMapper();
                JsonNode wellArray = mapper.valueToTree(data.get(boName));
                if ((wellArray != null) && (wellArray.size() > 0)) {
                    int columnCount = 0;
                    for (String field : select) {
                        if (wellArray.get(0).has(field)) {
                            columnCount++;
                        }
                    }
                    Assert.assertTrue(columnCount > 0);
                    Assert.assertEquals(wellArray.size(), 1);
                }
                logger.info("testSelectList passed.");
            }
        } catch (IllegalArgumentException e) {
            logger.error(e);
            throw e;
        }
    }

    @Test
    public void testOrderBy() {
        try {
            String boName = "WELL";
            BOQuery queryEntity = new BOQuery();
            queryEntity.setBoName(boName);
            queryEntity.setLanguage("en");
            queryEntity.setTimezone("GMT+08:00");
            Pagination pg = new Pagination();
            pg.setRecordsPerPage(100);
            queryEntity.setPagination(pg);
            List<OrderBy> ob = new ArrayList<>();
            ob.add(new OrderBy("UWI", Order.ASC));
            ob.add(new OrderBy("ROW_CHANGED_BY", Order.DESC));
            queryEntity.setOrderBy(ob);
            DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
            Map<String, Object> data = dspdmResponse.getData();
            if (data != null) {
                Assert.assertEquals(boName, data.keySet().iterator().next());
                logger.info("testOrderBy passed.");
            }
        } catch (Exception e) {
            logger.error(e);
            throw e;
        }
    }
    
    @Test
    public void testMixed() {
        try {
            String boName = "WELL";
            BOQuery queryEntity = new BOQuery();
            queryEntity.setBoName(boName);
            queryEntity.setLanguage("en");
            queryEntity.setTimezone("GMT+08:00");
            Pagination pg = new Pagination();
            pg.setRecordsPerPage(300);
            queryEntity.setPagination(pg);
            List<OrderBy> ob = new ArrayList<>();
            ob.add(new OrderBy("UWI", Order.ASC));
            ob.add(new OrderBy("ROW_CHANGED_BY", Order.DESC));
            queryEntity.setOrderBy(ob);
            List<CriteriaFilter> filters = new ArrayList<>();
            filters.add(new CriteriaFilter("WELL_ID", Operator.GREATER_THAN, new Object[]{80487}));
            filters.add(new CriteriaFilter("WELL_ID", Operator.NOT_LIKE, new Object[]{4}));
            queryEntity.setCriteriaFilters(filters);
            DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
            Map<String, Object> data = dspdmResponse.getData();
            if (data != null) {
                Assert.assertEquals(boName, data.keySet().iterator().next());
                ObjectMapper mapper = new ObjectMapper();
                JsonNode wellArray = mapper.valueToTree(data.get(boName));
                if ((wellArray != null) && (wellArray.size() > 0)) {
                    wellArray.iterator().forEachRemaining(jsonNode -> {
                        Assert.assertTrue(jsonNode.get("WELL_ID").intValue() > 80487);
                        Assert.assertTrue(Integer.toString(jsonNode.get("WELL_ID").intValue()).indexOf('4') < 0);
                    });
                }
                logger.info("testMixed passed.");
            }
        } catch (IllegalArgumentException e) {
            logger.error(e);
            throw e;
        }
    }


    @Test
    public void testReadWithDistinct() {
        try {
            //String boName = DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR;
            String boName = "WELL";
            BOQuery queryEntity = new BOQuery();
            queryEntity.setBoName(boName);
            queryEntity.setLanguage("en");
            queryEntity.setTimezone("GMT+08:00");
            queryEntity.addColumnsToSelect("well_id", "UWI");
            queryEntity.setCriteriaFilters(Arrays.asList(new CriteriaFilter("UWI", Operator.LIKE, new Object[]{"PDM"})));
            queryEntity.setReadWithDistinct(true);
            DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
            Map<String, Object> data = dspdmResponse.getData();
            if (data != null) {
                Assert.assertEquals(boName, data.keySet().iterator().next());
                logger.info("testReadWithDistinct passed.");
            }
        } catch (Exception e) {
            logger.error(e);
            throw e;
        }
    }

    @Test
    public void testReadRecordsCount() {
        try {
            String boName = DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR;
            BOQuery queryEntity = new BOQuery();
            queryEntity.setBoName(boName);
            queryEntity.setLanguage("en");
            queryEntity.setTimezone("GMT+08:00");
            //queryEntity.addColumnsToSelect("REFERENCE_TABLE");
            //queryEntity.setReadRecordsCount(true);
            DSPDMResponse dspdmResponse = super.commonCountPost(queryEntity);
            Map<String, Object> data = dspdmResponse.getData();
            if (data != null) {
                Assert.assertEquals("count", data.keySet().iterator().next());
                logger.info("testReadRecordsCount passed.");
            }
        } catch (Exception e) {
            logger.error(e);
            throw e;
        }
    }

    @Test
    public void testFillRelatedDetails() {
        try {
            String boName = "WELLBORE";
            BOQuery queryEntity = new BOQuery();
            queryEntity.setBoName(boName);
            queryEntity.setLanguage("en");
            queryEntity.setTimezone("GMT+08:00");
            queryEntity.setReadParentBO(Arrays.asList(new String[]{"WELL"}));
            DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
            Map<String, Object> data = dspdmResponse.getData();
            if (data != null) {
                Assert.assertEquals(boName, data.keySet().iterator().next());
                ObjectMapper mapper = new ObjectMapper();
                JsonNode wellArray = mapper.valueToTree(data.get(boName));
                if ((wellArray != null) && (wellArray.size() > 0)) {
                    Iterator<JsonNode> iterator = wellArray.iterator();
                    int parentCount = 0;
                    while (iterator.hasNext()) {
                        if (iterator.next().get("WELL") != null) {
                            parentCount++;
                        }
                    }
                    Assert.assertNotEquals(0, parentCount);
                }
                logger.info("testFillRelatedDetails passed.");
            }
        } catch (IllegalArgumentException e) {
            logger.error(e);
            throw e;
        }
    }
    
    @Test
    public void testFilter() throws Exception {
        try {
            testEQ();
            testNE();
            testGT();
            testGTE();
            testLT();
            testLTE();
            testBETWEEN();
            testNOTBETWEEN();
            testIN();
            testINMoreThan256();
            testNotINMoreThan256();
            testNOTIN();
            testLIKE();
            testNOTLIKE();
        	testFilterGroups();
        	testSimpleJoinFilterGroups();
        } catch (Exception e) {
            logger.error(e);
            throw e;
        }
    }
    
    private void testEQ() throws IOException, URISyntaxException {
        String boName = "WELL";
        List<CriteriaFilter> filters = new ArrayList<>();
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName(boName);
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        Pagination pg = new Pagination();
        pg.setRecordsPerPage(100);
        queryEntity.setPagination(pg);
        filters.add(new CriteriaFilter("ROW_CREATED_BY", Operator.EQUALS, new Object[]{"admin"}));
        queryEntity.setCriteriaFilters(filters);
        DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            Assert.assertEquals(boName, data.keySet().iterator().next());
            ObjectMapper mapper = new ObjectMapper();
            JsonNode wellArray = mapper.valueToTree(data.get(boName));
            if ((wellArray != null) && (wellArray.size() > 0)) {
                wellArray.iterator().forEachRemaining(jsonNode -> Assert.assertEquals(jsonNode.get("ROW_CREATED_BY").asText(), "admin"));
            }
            logger.info("testEQ passed.");
        }
    }

    private void testNE() throws IOException, URISyntaxException {
        String boName = "WELL";
        List<CriteriaFilter> filters = new ArrayList<>();
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName(boName);
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        Pagination pg = new Pagination();
        pg.setRecordsPerPage(100);
        queryEntity.setPagination(pg);
        filters.add(new CriteriaFilter("ROW_CREATED_BY", Operator.NOT_EQUALS, new Object[]{"admin"}));
        queryEntity.setCriteriaFilters(filters);
        DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            Assert.assertEquals(boName, data.keySet().iterator().next());
            ObjectMapper mapper = new ObjectMapper();
            JsonNode wellArray = mapper.valueToTree(data.get(boName));
            if ((wellArray != null) && (wellArray.size() > 0)) {
                wellArray.iterator().forEachRemaining(jsonNode -> Assert.assertNotEquals(jsonNode.get("ROW_CREATED_BY").asText(), "admin"));
            }
            logger.info("testNE passed.");
        }
    }

    private void testGT() throws IOException, URISyntaxException {
        String boName = "WELL";
        List<CriteriaFilter> filters = new ArrayList<>();
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName(boName);
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        Pagination pg = new Pagination();
        pg.setRecordsPerPage(100);
        queryEntity.setPagination(pg);
        filters.add(new CriteriaFilter("WELL_ID", Operator.GREATER_THAN, new Object[]{80487}));
        queryEntity.setCriteriaFilters(filters);
        DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            Assert.assertEquals(boName, data.keySet().iterator().next());
            ObjectMapper mapper = new ObjectMapper();
            JsonNode wellArray = mapper.valueToTree(data.get(boName));
            if ((wellArray != null) && (wellArray.size() > 0)) {
                wellArray.iterator().forEachRemaining(jsonNode -> Assert.assertTrue(jsonNode.get("WELL_ID").intValue() > 80487));
            }
            logger.info("testGT passed.");
        }
    }

    private void testGTE() throws IOException, URISyntaxException {
        String boName = "WELL";
        List<CriteriaFilter> filters = new ArrayList<>();
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName(boName);
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        Pagination pg = new Pagination();
        pg.setRecordsPerPage(100);
        queryEntity.setPagination(pg);
        filters.add(new CriteriaFilter("WELL_ID", Operator.GREATER_OR_EQUALS, new Object[]{80487}));
        queryEntity.setCriteriaFilters(filters);
        DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            Assert.assertEquals(boName, data.keySet().iterator().next());
            ObjectMapper mapper = new ObjectMapper();
            JsonNode wellArray = mapper.valueToTree(data.get(boName));
            if ((wellArray != null) && (wellArray.size() > 0)) {
                wellArray.iterator().forEachRemaining(jsonNode -> Assert.assertTrue(jsonNode.get("WELL_ID").intValue() >= 80487));
            }
            logger.info("testGTE passed.");
        }
    }

    private void testLT() throws IOException, URISyntaxException {
        String boName = "WELL";
        List<CriteriaFilter> filters = new ArrayList<>();
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName(boName);
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        Pagination pg = new Pagination();
        pg.setRecordsPerPage(100);
        queryEntity.setPagination(pg);
        filters.add(new CriteriaFilter("WELL_ID", Operator.LESS_THAN, new Object[]{80487}));
        queryEntity.setCriteriaFilters(filters);
        DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            Assert.assertEquals(boName, data.keySet().iterator().next());
            ObjectMapper mapper = new ObjectMapper();
            JsonNode wellArray = mapper.valueToTree(data.get(boName));
            if ((wellArray != null) && (wellArray.size() > 0)) {
                wellArray.iterator().forEachRemaining(jsonNode -> Assert.assertTrue(jsonNode.get("WELL_ID").intValue() < 80487));
            }
            logger.info("testLT passed.");
        }
    }

    private void testLTE() throws IOException, URISyntaxException {
        String boName = "WELL";
        List<CriteriaFilter> filters = new ArrayList<>();
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName(boName);
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        Pagination pg = new Pagination();
        pg.setRecordsPerPage(100);
        queryEntity.setPagination(pg);
        filters.add(new CriteriaFilter("WELL_ID", Operator.LESS_OR_EQUALS, new Object[]{80487}));
        queryEntity.setCriteriaFilters(filters);
        DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            Assert.assertEquals(boName, data.keySet().iterator().next());
            ObjectMapper mapper = new ObjectMapper();
            JsonNode wellArray = mapper.valueToTree(data.get(boName));
            if ((wellArray != null) && (wellArray.size() > 0)) {
                wellArray.iterator().forEachRemaining(jsonNode -> Assert.assertTrue(jsonNode.get("WELL_ID").intValue() <= 80487));
            }
            logger.info("testLTE passed.");
        }
    }

    private void testBETWEEN() throws IOException, URISyntaxException {
        String boName = "WELL";
        List<CriteriaFilter> filters = new ArrayList<>();
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName(boName);
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        Pagination pg = new Pagination();
        pg.setRecordsPerPage(100);
        queryEntity.setPagination(pg);
        filters.add(new CriteriaFilter("WELL_ID", Operator.BETWEEN, new Object[]{10000, 80487}));
        queryEntity.setCriteriaFilters(filters);
        DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            Assert.assertEquals(boName, data.keySet().iterator().next());
            ObjectMapper mapper = new ObjectMapper();
            JsonNode wellArray = mapper.valueToTree(data.get(boName));
            if ((wellArray != null) && (wellArray.size() > 0)) {
                wellArray.iterator().forEachRemaining(jsonNode -> Assert.assertTrue(jsonNode.get("WELL_ID").intValue() <= 80487 &&
                        jsonNode.get("WELL_ID").intValue() >= 10000));
            }
            logger.info("testBETWEEN passed.");
        }
    }

    private void testNOTBETWEEN() throws IOException, URISyntaxException {
        String boName = "WELL";
        List<CriteriaFilter> filters = new ArrayList<>();
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName(boName);
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        Pagination pg = new Pagination();
        pg.setRecordsPerPage(100);
        queryEntity.setPagination(pg);
        filters.add(new CriteriaFilter("WELL_ID", Operator.NOT_BETWEEN, new Object[]{10000, 80487}));
        queryEntity.setCriteriaFilters(filters);
        DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            Assert.assertEquals(boName, data.keySet().iterator().next());
            ObjectMapper mapper = new ObjectMapper();
            JsonNode wellArray = mapper.valueToTree(data.get(boName));
            if ((wellArray != null) && (wellArray.size() > 0)) {
                wellArray.iterator().forEachRemaining(jsonNode -> Assert.assertFalse(jsonNode.get("WELL_ID").intValue() <= 80487 &&
                        jsonNode.get("WELL_ID").intValue() >= 10000));
            }
            logger.info("testNOTBETWEEN passed.");
        }
    }

    private void testIN() throws IOException, URISyntaxException {
        String boName = "WELL";
        List<CriteriaFilter> filters = new ArrayList<>();
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName(boName);
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        Pagination pg = new Pagination();
        pg.setRecordsPerPage(100);
        queryEntity.setPagination(pg);
        filters.add(new CriteriaFilter("WELL_ID", Operator.IN, new Object[]{80484, 80474, 11, 29, 74, 22, 23, 114514}));
        queryEntity.setCriteriaFilters(filters);
        DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            Assert.assertEquals(boName, data.keySet().iterator().next());
            ObjectMapper mapper = new ObjectMapper();
            JsonNode wellArray = mapper.valueToTree(data.get(boName));
            if ((wellArray != null) && (wellArray.size() > 0)) {
                wellArray.iterator().forEachRemaining(jsonNode -> Assert.assertTrue(
                        Arrays.asList(new Object[]{80484, 80474, 11, 29, 74, 22, 23, 114514}).contains(jsonNode.get("WELL_ID").intValue())));
            }
            logger.info("testIN passed.");
        }
    }

    private void testFilterGroups() throws IOException, URISyntaxException {
        String boName = "WELL";
        List<CriteriaFilter> filters = new ArrayList<>();
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName(boName);
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        Pagination pg = new Pagination();
        pg.setRecordsPerPage(1000);
        queryEntity.setPagination(pg);
        filters.add(new CriteriaFilter("WELL_NAME", Operator.LIKE, new Object[]{"LE"}));
        queryEntity.setCriteriaFilters(filters);
        
        List<FilterGroup> filterGroups = new ArrayList<>();
        FilterGroup childFilterGroup1=new FilterGroup(Operator.AND,Operator.OR,new CriteriaFilter[] {
                new CriteriaFilter("WELL_ID", Operator.IN, new Object[]{"1"}),
                new CriteriaFilter("WELL_ID", Operator.IN, new Object[]{"2"}),
        });
        FilterGroup gandsonFilterGroup = new FilterGroup(Operator.AND, Operator.OR, new CriteriaFilter[]{
                new CriteriaFilter("WELL_ID", Operator.IN, new Object[]{"3"}),
                new CriteriaFilter("WELL_ID", Operator.IN, new Object[]{"4"}),
        });
        FilterGroup childFilterGroup2 = new FilterGroup(Operator.OR, Operator.AND, new CriteriaFilter[]{new CriteriaFilter("WELL_ID", Operator.IN, new Object[]{"2"})}, new FilterGroup[]{gandsonFilterGroup});
        FilterGroup childFilterGroup3 = new FilterGroup(Operator.OR, Operator.OR, new CriteriaFilter[]{new CriteriaFilter("WELL_ID", Operator.IN, new Object[]{"4"})});
        filterGroups.add(childFilterGroup1);
        filterGroups.add(childFilterGroup2);
        filterGroups.add(childFilterGroup3);
        queryEntity.setFilterGroups(filterGroups);
        
        DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            Assert.assertEquals(boName, data.keySet().iterator().next());            
            logger.info("testFilterGroups passed.");
        }
    }
    
    private void testSimpleJoinFilterGroups() throws IOException, URISyntaxException {
        String boName = "WELL";
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName(boName);
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        queryEntity.setJoinAlias("well_1");
        Pagination pg = new Pagination();
        pg.setRecordsPerPage(1000);
        queryEntity.setPagination(pg);
        List<SimpleJoinClause> simpleJoins = new ArrayList<>();
        SimpleJoinClause simpleJoin = new SimpleJoinClause();
        simpleJoin.setBoName("WELLBORE");
        simpleJoin.setJoinType("INNER");
        simpleJoin.setJoinAlias("wellbore_1");
        List<CriteriaFilter> filters = new ArrayList<>();
        filters.add(new CriteriaFilter("WELLBORE_NAME", Operator.LIKE, new Object[]{"S"}));
        simpleJoin.setCriteriaFilters(filters);
        List<FilterGroup> filterGroups = new ArrayList<>();
        FilterGroup childFilterGroup1 = new FilterGroup(Operator.AND, Operator.OR, new CriteriaFilter[]{new CriteriaFilter("FIELD_ID", Operator.IN, new Object[]{2, 3, 4})});
        FilterGroup childFilterGroup2 = new FilterGroup(Operator.OR, Operator.AND, new CriteriaFilter[]{new CriteriaFilter("FIELD_ID", Operator.IN, new Object[]{5, 6, 7})});
        FilterGroup childFilterGroup3 = new FilterGroup(Operator.OR, Operator.OR, new CriteriaFilter[]{new CriteriaFilter("FIELD_ID", Operator.IN, new Object[]{8})});
        filterGroups.add(childFilterGroup1);
        filterGroups.add(childFilterGroup2);
        filterGroups.add(childFilterGroup3);
        simpleJoin.setFilterGroups(filterGroups);
        simpleJoins.add(simpleJoin);
        queryEntity.setSimpleJoins(simpleJoins);
        DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            Assert.assertEquals(boName, data.keySet().iterator().next());
            logger.info("testSimpleJoinFilterGroups passed.");
        }
    }
    
    private void testINMoreThan256() throws IOException, URISyntaxException {
        String boName = "WELL";
        List<CriteriaFilter> filters = new ArrayList<>();
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName(boName);
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        queryEntity.setReadAllRecords(true);
        filters.add(new CriteriaFilter("WELL_ID", Operator.IN, new Object[]{13449, 13428, 26220, 10590, 10591, 10592, 10585, 10634, 10635, 10643, 10645, 10646, 10703, 10705, 10708, 10709, 10710, 10714, 10716, 10717, 10718, 10733, 10752, 10753, 10758, 10764, 10780, 10791, 10792, 10799, 10806, 10808, 10809, 10818, 10821, 10822, 10840, 10841, 10827, 11268, 16894, 16895, 16905, 16911, 16912, 16913, 16914, 16920, 16922, 16934, 16935, 16949, 16950, 16955, 16960, 16972, 16976, 16979, 16980, 16984, 16993, 16994, 16966, 17000, 17001, 17004, 17032, 17037, 17040, 17049, 17050, 17052, 17056, 17111, 17236, 23600, 23623, 23624, 23626, 23628, 23633, 23634, 23635, 23639, 23640, 23647, 23648, 23649, 23682, 23687, 23690, 23694, 23695, 23696, 23703, 23704, 23714, 23715, 23684, 23718, 23728, 23717, 23761,10589, 10749, 10838, 10952, 10967, 10968, 10970, 10961, 10996, 10997, 11028, 11073, 11087, 11096, 11089, 11204, 11207, 11208, 11231, 11238, 11262, 16893, 17038, 17112, 17115, 17116, 17141, 17142, 17143, 17144, 17205, 17207, 17285, 17296, 17302, 17303, 23868, 23869, 23870, 23872, 23876, 23931, 24002, 24012, 24013, 24016, 24017, 24011, 24098, 24106, 24119, 24135, 30195, 30214, 30337, 30477, 30486, 30489, 30538, 30543, 30562, 30563, 30566, 30567, 30628, 30631, 30633, 30634, 30637, 30731, 30739, 30877, 30878, 30883, 37057, 37155, 37161, 37244, 37254, 37255, 37256, 37257, 37258, 37259, 37303, 37312, 37316, 37345, 37354, 37387, 10630, 16904, 23911, 30487, 30632, 30750, 23749, 24014, 30547, 3054,10593, 10597, 10601, 10602, 10605, 10606, 10607, 10608, 10609, 10599, 10600, 10640, 10662, 10699, 10765, 10807, 10853, 10855, 10881, 10942, 10943, 10945, 10951, 10953, 10962, 10978, 10980, 11001, 11002, 11005, 11014, 11015, 11016, 11017, 11018, 11019, 11020, 11021, 11031, 11034, 11035, 11036, 11037, 11038, 11039, 11041, 11045, 11047, 11048, 11050, 11051, 11061, 11062, 11108, 11111, 11112, 11136, 11122, 11226, 11227, 11228, 11229, 11230, 11237, 11240, 11248, 11249, 11255, 11258, 11259, 11260, 11278, 11296, 16897, 16916, 16926, 16927, 16928, 16951, 16962, 17046, 17130, 17149, 17152, 17155, 17156, 17159, 17129, 17170, 17173, 17174, 17175, 17176, 17177, 17178, 17179, 17180, 17181, 17182, 17183}));
        queryEntity.setCriteriaFilters(filters);
        DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
        Map<String, Object> data = dspdmResponse.getData();

        logger.info("testINMoreThan256 passed.");
    }

    private void testNotINMoreThan256() throws IOException, URISyntaxException {
        String boName = "WELL";
        List<CriteriaFilter> filters = new ArrayList<>();
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName(boName);
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        // add pagination otherwise test will fail
        Pagination pg = new Pagination();
        pg.setRecordsPerPage(100);
        queryEntity.setPagination(pg);
        filters.add(new CriteriaFilter("WELL_ID", Operator.NOT_IN, new Object[]{13449, 13428, 26220, 10590, 10591, 10592, 10585, 10634, 10635, 10643, 10645, 10646, 10703, 10705, 10708, 10709, 10710, 10714, 10716, 10717, 10718, 10733, 10752, 10753, 10758, 10764, 10780, 10791, 10792, 10799, 10806, 10808, 10809, 10818, 10821, 10822, 10840, 10841, 10827, 11268, 16894, 16895, 16905, 16911, 16912, 16913, 16914, 16920, 16922, 16934, 16935, 16949, 16950, 16955, 16960, 16972, 16976, 16979, 16980, 16984, 16993, 16994, 16966, 17000, 17001, 17004, 17032, 17037, 17040, 17049, 17050, 17052, 17056, 17111, 17236, 23600, 23623, 23624, 23626, 23628, 23633, 23634, 23635, 23639, 23640, 23647, 23648, 23649, 23682, 23687, 23690, 23694, 23695, 23696, 23703, 23704, 23714, 23715, 23684, 23718, 23728, 23717, 23761,10589, 10749, 10838, 10952, 10967, 10968, 10970, 10961, 10996, 10997, 11028, 11073, 11087, 11096, 11089, 11204, 11207, 11208, 11231, 11238, 11262, 16893, 17038, 17112, 17115, 17116, 17141, 17142, 17143, 17144, 17205, 17207, 17285, 17296, 17302, 17303, 23868, 23869, 23870, 23872, 23876, 23931, 24002, 24012, 24013, 24016, 24017, 24011, 24098, 24106, 24119, 24135, 30195, 30214, 30337, 30477, 30486, 30489, 30538, 30543, 30562, 30563, 30566, 30567, 30628, 30631, 30633, 30634, 30637, 30731, 30739, 30877, 30878, 30883, 37057, 37155, 37161, 37244, 37254, 37255, 37256, 37257, 37258, 37259, 37303, 37312, 37316, 37345, 37354, 37387, 10630, 16904, 23911, 30487, 30632, 30750, 23749, 24014, 30547, 3054,10593, 10597, 10601, 10602, 10605, 10606, 10607, 10608, 10609, 10599, 10600, 10640, 10662, 10699, 10765, 10807, 10853, 10855, 10881, 10942, 10943, 10945, 10951, 10953, 10962, 10978, 10980, 11001, 11002, 11005, 11014, 11015, 11016, 11017, 11018, 11019, 11020, 11021, 11031, 11034, 11035, 11036, 11037, 11038, 11039, 11041, 11045, 11047, 11048, 11050, 11051, 11061, 11062, 11108, 11111, 11112, 11136, 11122, 11226, 11227, 11228, 11229, 11230, 11237, 11240, 11248, 11249, 11255, 11258, 11259, 11260, 11278, 11296, 16897, 16916, 16926, 16927, 16928, 16951, 16962, 17046, 17130, 17149, 17152, 17155, 17156, 17159, 17129, 17170, 17173, 17174, 17175, 17176, 17177, 17178, 17179, 17180, 17181, 17182, 17183}));
        queryEntity.setCriteriaFilters(filters);
        DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
        Map<String, Object> data = dspdmResponse.getData();

        logger.info("testNotINMoreThan256 passed.");
    }

    private void testNOTIN() throws IOException, URISyntaxException {
        String boName = "WELL";
        List<CriteriaFilter> filters = new ArrayList<>();
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName(boName);
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        Pagination pg = new Pagination();
        pg.setRecordsPerPage(100);
        queryEntity.setPagination(pg);
        filters.add(new CriteriaFilter("WELL_ID", Operator.NOT_IN, new Object[]{80484, 80474, 11, 29, 74, 22, 23,
                114514}));
        queryEntity.setCriteriaFilters(filters);
        DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            Assert.assertEquals(boName, data.keySet().iterator().next());
            ObjectMapper mapper = new ObjectMapper();
            JsonNode wellArray = mapper.valueToTree(data.get(boName));
            if ((wellArray != null) && (wellArray.size() > 0)) {
                wellArray.iterator().forEachRemaining(jsonNode -> Assert.assertFalse(
                        Arrays.asList(new Object[]{80484, 80474, 11, 29, 74, 22, 23, 114514}).contains(jsonNode.get("WELL_ID").intValue())));
            }
            logger.info("testNOTIN passed.");
        }
    }

    private void testLIKE() throws IOException, URISyntaxException {
        String boName = "WELL";
        List<CriteriaFilter> filters = new ArrayList<>();
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName(boName);
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        Pagination pg = new Pagination();
        pg.setRecordsPerPage(100);
        queryEntity.setPagination(pg);
        filters.add(new CriteriaFilter("WELL_ID", Operator.LIKE, new Object[]{4}));
        queryEntity.setCriteriaFilters(filters);
        DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            Assert.assertEquals(boName, data.keySet().iterator().next());
            ObjectMapper mapper = new ObjectMapper();
            JsonNode wellArray = mapper.valueToTree(data.get(boName));
            if ((wellArray != null) && (wellArray.size() > 0)) {
                wellArray.iterator().forEachRemaining(jsonNode -> Assert.assertTrue(Integer.toString(jsonNode.get(
                        "WELL_ID").intValue()).indexOf('4') > -1));
            }
            logger.info("testLIKE passed.");
        }
    }

    private void testNOTLIKE() throws IOException, URISyntaxException {
        String boName = "WELL";
        List<CriteriaFilter> filters = new ArrayList<>();
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName(boName);
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        Pagination pg = new Pagination();
        pg.setRecordsPerPage(100);
        queryEntity.setPagination(pg);
        filters.add(new CriteriaFilter("WELL_ID", Operator.NOT_LIKE, new Object[]{4}));
        queryEntity.setCriteriaFilters(filters);
        DSPDMResponse dspdmResponse = super.commonPost(queryEntity);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null) {
            Assert.assertEquals(boName, data.keySet().iterator().next());
            ObjectMapper mapper = new ObjectMapper();
            JsonNode wellArray = mapper.valueToTree(data.get(boName));
            if ((wellArray != null) && (wellArray.size() > 0)) {
                wellArray.iterator().forEachRemaining(jsonNode -> Assert.assertTrue(Integer.toString(jsonNode.get(
                        "WELL_ID").intValue()).indexOf('4') < 0));
            }
            logger.info("testNOTLIKE passed.");
        }
    }*/
}