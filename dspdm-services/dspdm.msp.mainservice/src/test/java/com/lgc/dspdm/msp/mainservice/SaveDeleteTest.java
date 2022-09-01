package com.lgc.dspdm.msp.mainservice;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.msp.mainservice.model.BOQuery;
import com.lgc.dspdm.msp.mainservice.model.CriteriaFilter;
import com.lgc.dspdm.msp.mainservice.model.DSPDMResponse;
import com.lgc.dspdm.msp.mainservice.model.Pagination;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.*;

//test order by test method name 
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SaveDeleteTest extends BaseServiceTest {
    private static DSPDMLogger logger = new DSPDMLogger(SaveDeleteTest.class);

    private static Integer wellID = null;
    private static Integer wellboreID = null;
    private static Integer wellCompletionID = null;
    private static String randomStr = java.util.UUID.randomUUID().toString().substring(30).toUpperCase();
    private static String uwi = "UNIT_TEST" + randomStr;
    private static String wellbore_uwi = "UNIT_TEST" + randomStr;
    private static Integer completionObsNo = Integer.valueOf(Double.valueOf(Math.random() * 10).intValue() + 1);
    private static String completionDate = "1959-04-" + ((completionObsNo == 10) ? "10" : "0" + completionObsNo);
    private static String completionName = "HAL-COMPLETION-" + Integer.valueOf(Double.valueOf(Math.random() * 100000000).intValue() + 1);

    private void setWellID() {
        String boName = "WELL";
        BOQuery wellEntity = new BOQuery();
        wellEntity.setBoName(boName);
        wellEntity.setLanguage("en");
        wellEntity.setTimezone("GMT+08:00");
        String[] select = {"WELL_ID", "UWI"};
        wellEntity.setSelectList(Arrays.asList(select));
        List<CriteriaFilter> criteriaFilters = new ArrayList<>(1);
        criteriaFilters.add(new CriteriaFilter("UWI", Operator.EQUALS, new Object[]{uwi}));
        wellEntity.setCriteriaFilters(criteriaFilters);
        wellEntity.setPagination(new Pagination(1, Arrays.asList(1)));
        DSPDMResponse dspdmResponse = super.commonPost(wellEntity);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null && data.get(boName) != null) {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode wellArray = mapper.valueToTree(data.get(boName));
            if (wellArray.get(0).has("WELL_ID")) {
                wellID = wellArray.get(0).get("WELL_ID").intValue();
            }
        }
    }

    private void setWellboreID() {
        String boName = "WELLBORE";
        BOQuery wellboreEntity = new BOQuery();
        wellboreEntity.setBoName(boName);
        wellboreEntity.setLanguage("en");
        wellboreEntity.setTimezone("GMT+08:00");
        String[] select = {"WELLBORE_ID", "WELLBORE_UWI"};
        wellboreEntity.setSelectList(Arrays.asList(select));
        List<CriteriaFilter> criteriaFilters = new ArrayList<>(1);
        criteriaFilters.add(new CriteriaFilter("WELLBORE_UWI", Operator.EQUALS, new Object[]{wellbore_uwi}));
        wellboreEntity.setCriteriaFilters(criteriaFilters);
        wellboreEntity.setPagination(new Pagination(1, Arrays.asList(1)));
        DSPDMResponse dspdmResponse = super.commonPost(wellboreEntity);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null && data.get(boName) != null) {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode wellArray = mapper.valueToTree(data.get(boName));
            if (wellArray.get(0).has("WELLBORE_ID")) {
                wellboreID = wellArray.get(0).get("WELLBORE_ID").intValue();
            }
        }
    }

    private void setWellCompletionID() {
        String boName = "WELL COMPLETION";
        BOQuery wellCompletionEntity = new BOQuery();
        wellCompletionEntity.setBoName(boName);
        wellCompletionEntity.setLanguage("en");
        wellCompletionEntity.setTimezone("GMT+08:00");
        String[] select = {"WELL_COMPLETION_ID", "WELLBORE_UWI", "COMPLETION_DATE", "COMPLETION_OBS_NO"};
        wellCompletionEntity.setSelectList(Arrays.asList(select));
        List<CriteriaFilter> criteriaFilters = new ArrayList<>(1);
        criteriaFilters.add(new CriteriaFilter("WELLBORE_UWI", Operator.EQUALS, new Object[]{wellbore_uwi}));
        criteriaFilters.add(new CriteriaFilter("COMPLETION_DATE", Operator.EQUALS, new Object[]{completionDate}));
        criteriaFilters.add(new CriteriaFilter("COMPLETION_OBS_NO", Operator.EQUALS, new Object[]{completionObsNo}));
        wellCompletionEntity.setCriteriaFilters(criteriaFilters);
        wellCompletionEntity.setPagination(new Pagination(1, Arrays.asList(1)));
        DSPDMResponse dspdmResponse = super.commonPost(wellCompletionEntity);
        Map<String, Object> data = dspdmResponse.getData();
        if (data != null && data.get(boName) != null) {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode wellArray = mapper.valueToTree(data.get(boName));
            if (wellArray.get(0).has("WELL_COMPLETION_ID")) {
                wellCompletionID = wellArray.get(0).get("WELL_COMPLETION_ID").intValue();
            }
        }
    }

    @Test
    public void test001Insert() {
        String body = "{\n" +
                "  \"WELL\": { \n" +
                "    \"language\": \"en\", \n" +
                "    \"readBack\": true, \n" +
                "    \"timezone\": \"GMT+08:00\",\n" +
                "    \"dspdmUnits\":[ \n" +
                "        		{  \n" +
                "   			 	\"boAttrName\":\"X_COORDINATE\", \n" +
                "    				\"sourceUnit\":\"deg\" \n" +
                "         		} \n" +
                "     ] ,\n" +
                "    \"data\": [\n" +
                "      {\n" +
                "          \"WELL_ID\": null,\n" +
                "          \"UWI\": \"" + uwi + "\",\n" +
                "          \"WELL_NAME\": \"TW00001\",\n" +
                "          \"FIELD_ID\": null,\n" +
                "          \"OFFSHORE_IND\": false,\n" +
                "          \"IS_ACTIVE\": true,\n" +
                "          \"WELL_OWNER\": \"Ting Li\",\n" +
                "          \"SPUD_DATE\": \"1984-03-08T06:00:00+06\",\n" +
                "          \"COMPLETION_DATE\": \"1984-06-08T06:00:00+06\",\n" +
                "          \"PRODUCTION_START_DATE\": \"1984-07-08T06:00:00+06\",\n" +
                "          \"X_COORDINATE\": 639.267,\n" +
                "          \"Y_COORDINATE\": 42.0799,\n" +
                "          \"CRS_ID\": 1,\n" +
                "          \"CRS_NAME\": \"WGS 84\",\n" +
                "          \"PRODUCTION_METHOD_ID\": 3,\n" +
                "          \"PRODUCTION_METHOD\": \"Gas lift\",\n" +
                "          \"WELL_TYPE_ID\": 2,\n" +
                "          \"WELL_TYPE\": \"Natural Gas\",\n" +
                "          \"CURRENT_STATUS_ID\": 2,\n" +
                "          \"FIELD_NAME\": \"HAL-Alaska Field\",\n" +
                "          \"R_WELL_STATUS_NAME\": \"Active\",\n" +
                "          \"children\": {\n" +
                "               \"WELLBORE\": {\n" +
                "                   \"language\": \"en\", \n" +
                "                   \"timezone\": \"GMT+08:00\",\n" +
                "                   \"data\": [\n" +
                "                      {\n" +
                "                         \"WELLBORE_ID\": null,\n" +
                "                         \"WELL_UWI\": \"" + uwi + "\",\n" +
                "                         \"WELLBORE_UWI\": \"" + wellbore_uwi + "\",\n" +
                "                          \"WELLBORE_NAME\": \" " + wellbore_uwi + "\",\n" +
                "                         \"WELLBORE_NUMBER\": \"0\",\n" +
                "                         \"X_COORDINATE\": -90.650200000,\n" +
                "                         \"Y_COORDINATE\": 47.272300000,\n" +
                "                         \"CRS_ID\": 1,\n" +
                "                         \"CRS_NAME\": \"WGS 84\",\n" +
                "                         \"FIELD_ID\": 6,\n" +
                "                         \"FIELD_NAME\": \"HAL-Alaska Field\",\n" +
                "                         \"KB_ELEV\": 27.000000000,\n" +
                "                         \"KB_ELEV_OUOM\": \"ft\",\n" +
                "                         \"GROUND_ELEV\": 18.000000000,\n" +
                "                         \"GROUND_ELEV_OUOM\": \"ft\",\n" +
                "                         \"SPUD_DATE\": \"1960-02-29\",\n" +
                "                         \"SPUD_DATE_TZ\": \"UTC+00:00\",\n" +
                "                         \"R_TIMEZONE_ID\": 16,\n" +
                "                         \"COMPLETION_DATE\": \"1960-05-29\",\n" +
                "                         \"DRILL_TD\": 5436.000000000,\n" +
                "                         \"DRILL_TD_OUOM\": \"ft\",\n" +
                "                         \"PLUGBACK_DEPTH\": 5426.000000000,\n" +
                "                         \"PLUGBACK_DEPTH_OUOM\": \"ft\",\n" +
                "                         \"CURRENT_STATUS_ID\": 3,\n" +
                "                         \"CURRENT_STATUS\": \"Abandoned\",\n" +
                "                         \"ROW_CHANGED_BY\": \"YH\",\n" +
                "                         \"ROW_CHANGED_DATE\": \"2019-12-29\",\n" +
                "                         \"ROW_CREATED_BY\": \"YG\",\n" +
                "                         \"ROW_CREATED_DATE\": \"2017-11-13\",\n" +
                "                         \"children\": {\n" +
                "                             \"WELL COMPLETION\": {\n" +
                "                                 \"language\": \"en\", \n" +
                "                                 \"timezone\": \"GMT+08:00\",\n" +
                "                                 \"data\": [\n" +
                "                                    {\n" +
                "                                          \"WELL_COMPLETION_ID\": null,\n" +
                "                                          \"WELLBORE_UWI\": \"" + wellbore_uwi + "\",\n" +
                "                                          \"COMPLETION_DATE\": \"" + completionDate + "\",\n" +
                "                                          \"COMPLETION_OBS_NO\": " + completionObsNo + ",\n" +
                "                                          \"WELL_COMPLETION_NAME\": \"" + completionName + "\",\n" +
                "                                          \"COMPLETION_TYPE_ID\": 1,\n" +
                "                                          \"COMPLETION_TYPE\": \"BRP\",\n" +
                "                                          \"IS_ACTIVE\": true,\n" +
                "                                          \"TOP_DEPTH\": 7559.00000,\n" +
                "                                          \"TOP_DEPTH_OUOM\": \"ft\",\n" +
                "                                          \"BASE_DEPTH\": 7569.00000,\n" +
                "                                          \"BASE_DEPTH_OUOM\": \"ft\"\n" +
                "                                    }\n" +
                "                                ]\n" +
                "                            }\n" +
                "                        }\n" +
                "                     }\n" +
                "                  ]\n" +
                "               }\n" +
                "            }\n" +
                "         }\n" +
                "      ]\n" +
                "  }\n" +
                "}";
        DSPDMResponse dspdmResponse = super.savePost(body);
        logger.info("testInsert Passed.");

        setWellID();
        setWellboreID();
        setWellCompletionID();
    }

    @Test
    public void test002Update() {
        if (wellID != null) {
            String body = "{\n" +
                    "  \"WELL\": { \n" +
                    "    \"language\": \"en\", \n" +
                    "    \"readBack\": true, \n" +
                    "    \"timezone\": \"GMT+08:00\",\n" +
                    "    \"dspdmUnits\":[ \n" +
                    "        		{  \n" +
                    "   			 	\"boAttrName\":\"X_COORDINATE\", \n" +
                    "    				\"sourceUnit\":\"deg\" \n" +
                    "         		} \n" +
                    "     ] ,\n" +
                    "    \"data\": [\n" +
                    "      {\n" +
                    "          \"WELL_ID\": " + wellID + ",\n" +
                    "          \"UWI\": \"" + uwi + "\",\n" +
                    "          \"WELL_NAME\": \"TW00001\",\n" +
                    "          \"FIELD_ID\": null,\n" +
                    "          \"OFFSHORE_IND\": false,\n" +
                    "          \"IS_ACTIVE\": true,\n" +
                    "          \"WELL_OWNER\": \"Ting Li\",\n" +
                    "          \"SPUD_DATE\": \"1984-03-08T06:00:00+06\",\n" +
                    "          \"COMPLETION_DATE\": \"1984-06-08T06:00:00+06\",\n" +
                    "          \"PRODUCTION_START_DATE\": \"1984-07-08T06:00:00+06\",\n" +
                    "          \"X_COORDINATE\": 639.267,\n" +
                    "          \"Y_COORDINATE\": 42.0799,\n" +
                    "          \"CRS_ID\": 1,\n" +
                    "          \"CRS_NAME\": \"WGS 84\",\n" +
                    "          \"PRODUCTION_METHOD_ID\": 3,\n" +
                    "          \"PRODUCTION_METHOD\": \"Gas lift\",\n" +
                    "          \"WELL_TYPE_ID\": 2,\n" +
                    "          \"WELL_TYPE\": \"Natural Gas\",\n" +
                    "          \"CURRENT_STATUS_ID\": 2,\n" +
                    "          \"FIELD_NAME\": \"HAL-Alaska Field\",\n" +
                    "          \"R_WELL_STATUS_NAME\": \"Active\",\n" +
                    "          \"children\": {\n" +
                    "               \"WELLBORE\": {\n" +
                    "                   \"language\": \"en\", \n" +
                    "                   \"timezone\": \"GMT+08:00\",\n" +
                    "                   \"data\": [\n" +
                    "                       {\n" +
                    "                          \"WELLBORE_ID\": " + wellboreID + ",\n" +
                    "                          \"WELL_UWI\": \"" + uwi + "\",\n" +
                    "                          \"WELLBORE_UWI\": \"" + wellbore_uwi + "\",\n" +
                    "                          \"WELLBORE_NAME\": \" " + wellbore_uwi + "\",\n" +
                    "                          \"WELLBORE_NUMBER\": \"0\",\n" +
                    "                          \"X_COORDINATE\": -90.650200000,\n" +
                    "                          \"Y_COORDINATE\": 47.272300000,\n" +
                    "                          \"CRS_ID\": 1,\n" +
                    "                          \"CRS_NAME\": \"WGS 84\",\n" +
                    "                          \"FIELD_ID\": 6,\n" +
                    "                          \"FIELD_NAME\": \"HAL-Alaska Field\",\n" +
                    "                          \"KB_ELEV\": 27.000000000,\n" +
                    "                          \"KB_ELEV_OUOM\": \"ft\",\n" +
                    "                          \"GROUND_ELEV\": 18.000000000,\n" +
                    "                          \"GROUND_ELEV_OUOM\": \"ft\",\n" +
                    "                          \"SPUD_DATE\": \"1960-02-29\",\n" +
                    "                          \"SPUD_DATE_TZ\": \"UTC+00:00\",\n" +
                    "                          \"R_TIMEZONE_ID\": 16,\n" +
                    "                          \"COMPLETION_DATE\": \"1960-05-29\",\n" +
                    "                          \"DRILL_TD\": 5436.000000000,\n" +
                    "                          \"DRILL_TD_OUOM\": \"ft\",\n" +
                    "                          \"PLUGBACK_DEPTH\": 5426.000000000,\n" +
                    "                          \"PLUGBACK_DEPTH_OUOM\": \"ft\",\n" +
                    "                          \"CURRENT_STATUS_ID\": 3,\n" +
                    "                          \"CURRENT_STATUS\": \"Abandoned\",\n" +
                    "                          \"ROW_CHANGED_BY\": \"YH\",\n" +
                    "                          \"ROW_CHANGED_DATE\": \"2019-12-29\",\n" +
                    "                          \"ROW_CREATED_BY\": \"YG\",\n" +
                    "                          \"ROW_CREATED_DATE\": \"2017-11-13\"," +
                    "                          \"children\": {\n" +
                    "                              \"WELL COMPLETION\": {\n" +
                    "                                  \"language\": \"en\", \n" +
                    "                                  \"timezone\": \"GMT+08:00\",\n" +
                    "                                  \"data\": [\n" +
                    "                                      {\n" +
                    "                                          \"WELL_COMPLETION_ID\": " + wellCompletionID + ",\n" +
                    "                                          \"WELLBORE_UWI\": \"" + wellbore_uwi + "\",\n" +
                    "                                          \"COMPLETION_DATE\": \"" + completionDate + "\",\n" +
                    "                                          \"COMPLETION_OBS_NO\": " + completionObsNo + ",\n" +
                    "                                          \"WELL_COMPLETION_NAME\": \"" + completionName + "\",\n" +
                    "                                          \"COMPLETION_TYPE_ID\": 1,\n" +
                    "                                          \"COMPLETION_TYPE\": \"BRP\",\n" +
                    "                                          \"IS_ACTIVE\": true,\n" +
                    "                                          \"TOP_DEPTH\": 7559.00000,\n" +
                    "                                          \"TOP_DEPTH_OUOM\": \"ft\",\n" +
                    "                                          \"BASE_DEPTH\": 7569.00000,\n" +
                    "                                          \"BASE_DEPTH_OUOM\": \"ft\"\n" +
                    "                                      }\n" +
                    "                                 ]\n" +
                    "                             }\n" +
                    "                         }\n" +
                    "                     }\n" +
                    "                  ]\n" +
                    "              }\n" +
                    "          }\n" +
                    "      }\n" +
                    "    ]\n" +
                    "  }\n" +
                    "}";
            DSPDMResponse dspdmResponse = super.savePost(body);
        }
        logger.info("testUpdate Passed.");
    }

//    @Test
//    public void test003Delete() {
//        if (wellCompletionID != null) {
//            DSPDMResponse dspdmResponseWell = super.deleteDelete("WELL COMPLETION", wellCompletionID);
//        }
//        if (wellboreID != null) {
//            DSPDMResponse dspdmResponseWell = super.deleteDelete("WELLBORE", wellboreID);
//        }
//        if (wellID != null) {
//            DSPDMResponse dspdmResponseWell = super.deleteDelete("WELL", wellID);
//        }
//        logger.info("testDelete Passed.");
//    }

    @Test
    public void test004Delete() {

        if (wellID != null) {
            Map<String, Object> wellDataMap = new HashMap<>();
            wellDataMap.put("WELL_ID", wellID);
            Map<String, Object> wellMap = new HashMap<>();
            wellMap.put(DSPDMConstants.DSPDM_REQUEST.LANGUAGE_KEY, "en");
            wellMap.put(DSPDMConstants.DSPDM_REQUEST.TIMEZONE_KEY, "GMT+08:00");
            wellMap.put(DSPDMConstants.DSPDM_REQUEST.DATA_KEY, Arrays.asList(wellDataMap));
            if (wellboreID != null) {
                Map<String, Object> wellboreDataMap = new HashMap<>();
                wellboreDataMap.put("WELLBORE_ID", wellboreID);
                Map<String, Object> wellboreMap = new HashMap<>();
                wellboreMap.put(DSPDMConstants.DSPDM_REQUEST.LANGUAGE_KEY, "en");
                wellboreMap.put(DSPDMConstants.DSPDM_REQUEST.TIMEZONE_KEY, "GMT+08:00");
                wellboreMap.put(DSPDMConstants.DSPDM_REQUEST.DATA_KEY, Arrays.asList(wellboreDataMap));
                Map<String, Object> wellChildrenMap = new HashMap<>();
                wellChildrenMap.put("WELLBORE", wellboreMap);
                wellDataMap.put(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY, wellChildrenMap);
                if (wellCompletionID != null) {
                    Map<String, Object> wellCompletionDataMap = new HashMap<>();
                    wellCompletionDataMap.put("WELL_COMPLETION_ID", wellCompletionID);
                    Map<String, Object> wellCompletionMap = new HashMap<>();
                    wellCompletionMap.put(DSPDMConstants.DSPDM_REQUEST.LANGUAGE_KEY, "en");
                    wellCompletionMap.put(DSPDMConstants.DSPDM_REQUEST.TIMEZONE_KEY, "GMT+08:00");
                    wellCompletionMap.put(DSPDMConstants.DSPDM_REQUEST.DATA_KEY, Arrays.asList(wellCompletionDataMap));
                    Map<String, Object> wellboreChildrenMap = new HashMap<>();
                    wellboreChildrenMap.put("WELL COMPLETION", wellCompletionMap);
                    wellboreDataMap.put(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY, wellboreChildrenMap);
                }
            }

            Map<String, Object> formParams = new HashMap<>();
            formParams.put("WELL", wellMap);
            super.deletePost(formParams);
        }
        logger.info("testDeleteWithChildren Passed.");
    }

    @Test
    public void test005DeleteCascade() {
        test001Insert();

        if (wellID != null) {
            Map<String, Object> wellDataMap = new HashMap<>();
            wellDataMap.put("WELL_ID", wellID);
            Map<String, Object> wellMap = new HashMap<>();
            wellMap.put(DSPDMConstants.DSPDM_REQUEST.LANGUAGE_KEY, "en");
            wellMap.put(DSPDMConstants.DSPDM_REQUEST.TIMEZONE_KEY, "GMT+08:00");
            wellMap.put(DSPDMConstants.DSPDM_REQUEST.DELETE_CASCADE_KEY, true);
            wellMap.put(DSPDMConstants.DSPDM_REQUEST.DATA_KEY, Arrays.asList(wellDataMap));

            Map<String, Object> formParams = new HashMap<>();
            formParams.put("WELL", wellMap);
            super.deletePost(formParams);
        }
        logger.info("testDeleteCascade Passed.");
    }

    //@Test
    public void test006ValidateSaveNode() {
        String nodeName = "testNode" + Integer.valueOf(Double.valueOf(Math.random() * 10000000).intValue() + 1);
        String body = "{\n" +
                "  \"NODE\": { \n" +
                "    \"language\": \"en\", \n" +
                "    \"readBack\": true, \n" +
                "    \"timezone\": \"GMT+08:00\",\n" +
                "    \"data\": [\n" +
                "      {\n" +
                "           \"NODE_NAME\": \"" + nodeName + "\",\n" +
                "           \"ENTITY_NAME\": \"NoExistsWellName\",\n" +
                "           \"ENTITY_TYPE\": \"Well\"\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
        //Save the not exists well
        try {
            super.savePost(body);
            // throw new DSPDMException("test006SaveNode not passed. Because we can save 'NoExistsWellName' to Node.ENTITY_NAME", Locale.getDefault());
        } catch (DSPDMException ex) {
            if (ex.getMessage().contains("Unable to process the records")) {
                logger.info("test006SaveNode Passed.");
            } else {
                logger.error(ex.getMessage());
                throw ex;
            }
        }
    }
}
