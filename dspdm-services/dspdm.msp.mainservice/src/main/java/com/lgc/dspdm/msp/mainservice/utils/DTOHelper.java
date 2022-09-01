package com.lgc.dspdm.msp.mainservice.utils;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.config.ConnectionProperties;
import com.lgc.dspdm.core.common.config.WafByPassRulesProperties;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.CriteriaFilter;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.aggregate.HavingFilter;
import com.lgc.dspdm.core.common.data.criteria.join.DynamicJoinClause;
import com.lgc.dspdm.core.common.data.criteria.join.DynamicTable;
import com.lgc.dspdm.core.common.data.criteria.join.SimpleJoinClause;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.*;
import com.lgc.dspdm.core.common.util.metadata.MetadataUtils;
import com.lgc.dspdm.msp.mainservice.model.BOQuery;
import com.lgc.dspdm.service.common.dynamic.read.IDynamicReadService;

import java.util.*;

/**
 * @author Yuan Tian
 */
public class DTOHelper {

    private static DSPDMLogger logger = new DSPDMLogger(DTOHelper.class);

    /**
     * throws exception if any metadata business object exists in the request
     *
     * @param operationName
     * @param businessObjectsMap
     * @param dynamicReadService
     * @param executionContext
     * @author Muhammad Imran Ansari
     * @since 05-Nov-2020
     */
    public static void verifyNoMetadataBusinessObjectExists(String operationName, Map<String, List<DynamicDTO>> businessObjectsMap, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        List<DynamicDTO> metadataBusinessObjects = dynamicReadService.readMetadataAllBusinessObjects(executionContext);
        metadataBusinessObjects = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(metadataBusinessObjects, DSPDMConstants.BoAttrName.IS_METADATA_TABLE, true);
        Map<String, DynamicDTO> metadataBusinessObjectsMap = CollectionUtils.prepareMapFromValuesOfKey(metadataBusinessObjects, DSPDMConstants.BoAttrName.BO_NAME);
        // validate
        verifyNoMetadataBusinessObjectExists(operationName, businessObjectsMap, metadataBusinessObjectsMap, executionContext);
    }

    /**
     * throws exception if any metadata business object exists in the request
     *
     * @param operationName
     * @param businessObjectsMap
     * @param metadataBusinessObjectsMap
     * @param executionContext
     */
    private static void verifyNoMetadataBusinessObjectExists(String operationName, Map<String, List<DynamicDTO>> businessObjectsMap, Map<String, DynamicDTO> metadataBusinessObjectsMap, ExecutionContext executionContext) {
        Set<Map.Entry<String, List<DynamicDTO>>> entrySet = businessObjectsMap.entrySet();
        for (Map.Entry<String, List<DynamicDTO>> entry : entrySet) {
            if (metadataBusinessObjectsMap.containsKey(entry.getKey())) {
                throw new DSPDMException("Cannot perform {} on metadata business object '{}' directly. Please use dedicated metadata service.", executionContext.getExecutorLocale(), operationName, entry.getKey());
            } else if ((DSPDMConstants.BoName.USER_PERFORMED_OPR.equalsIgnoreCase(entry.getKey())) ||
                    (DSPDMConstants.BoName.BUS_OBJ_ATTR_CHANGE_HISTORY.equalsIgnoreCase(entry.getKey()) ||
                            (DSPDMConstants.BoName.BO_SEARCH.equalsIgnoreCase(entry.getKey())))) {
                throw new DSPDMException("Cannot perform {} on business object '{}' directly.", executionContext.getExecutorLocale(), operationName, entry.getKey());
            }
            List<DynamicDTO> list = entry.getValue();
            if (CollectionUtils.hasValue(list)) {
                for (DynamicDTO dynamicDTO : list) {
                    if (metadataBusinessObjectsMap.containsKey(dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME))) {
                        throw new DSPDMException("Cannot perform {} on metadata business object '{}' directly. Please use dedicated metadata service.", executionContext.getExecutorLocale(), operationName, dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME));
                    } else if ((DSPDMConstants.BoName.USER_PERFORMED_OPR.equalsIgnoreCase(dynamicDTO.getType())) ||
                            (DSPDMConstants.BoName.BUS_OBJ_ATTR_CHANGE_HISTORY.equalsIgnoreCase(dynamicDTO.getType())) ||
                            (DSPDMConstants.BoName.BO_SEARCH.equalsIgnoreCase(dynamicDTO.getType()))) {
                        throw new DSPDMException("Cannot perform {} on business object '{}' directly.", executionContext.getExecutorLocale(), operationName, entry.getKey());
                    }
                    Map<String, List<DynamicDTO>> childrenMap = (Map<String, List<DynamicDTO>>) dynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
                    if (CollectionUtils.hasValue(childrenMap)) {
                        // call same method recursively on children map
                        verifyNoMetadataBusinessObjectExists(operationName, childrenMap, metadataBusinessObjectsMap, executionContext);
                    }
                }
            }
        }
    }

    public static Map<String, List<DynamicDTO>> buildBONameAndBOListMapFromRequestJSON(Map<String, Object> payload, String operationName, boolean allowNewFields, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        // Use linked hash map instead of a simple hash map. We do not want to change the order of the request. Anything coming first in request should be process first. 
        Map<String, List<DynamicDTO>> dynamicDTOMap = new LinkedHashMap<>();
        boolean readBack = false; // read data after save update
        boolean fullDrop = false; // drop table along with the data
        boolean deleteCascade = false; // drop table along with the data
        boolean showSQLStats = false; // send all executed sql statements back in response
        boolean writeNullValues = false; // print properties with null values in jason response
        TimeZone timezone = null; // timezone of the requester
        Locale locale = null; // Language locale of the requester

        for (Map.Entry<String, Object> e : payload.entrySet()) {
            String boName = e.getKey().toUpperCase();
            Object value = e.getValue();
            logger.debug("Building business object from request for boName : {}", boName);

            if (value instanceof Map) {
                Map<String, Object> boAsMap = (Map<String, Object>) value;
                Object language = boAsMap.get(DSPDMConstants.DSPDM_REQUEST.LANGUAGE_KEY);
                if ((language != null) && (language instanceof String)) {
                    locale = LocaleUtils.getLocale(((String) language).trim());
                    if (locale == null) {
                        throw new DSPDMException("Invalid input for {}: Value for {} not recognized : '{}'", executionContext.getExecutorLocale(), operationName, DSPDMConstants.DSPDM_REQUEST.LANGUAGE_KEY, language);
                    }
                    executionContext.setExecutorLocale(locale);
                } else {
                    throw new DSPDMException("Invalid input for {}: Value for {} must be provided for bo name : '{}'", executionContext.getExecutorLocale(), operationName, DSPDMConstants.DSPDM_REQUEST.LANGUAGE_KEY, boName);
                }
                Object timezoneStr = boAsMap.get(DSPDMConstants.DSPDM_REQUEST.TIMEZONE_KEY);
                if ((timezoneStr != null) && (timezoneStr instanceof String)) {
                    timezone = LocaleUtils.getTimezone((String) timezoneStr);
                    if (timezone == null) {
                        throw new DSPDMException("Invalid input for {}: Value for {} not recognized : '{}'", executionContext.getExecutorLocale(), operationName, DSPDMConstants.DSPDM_REQUEST.TIMEZONE_KEY, timezoneStr);
                    }
                    executionContext.setExecutorTimeZone(timezone.toZoneId());
                } else {
                    throw new DSPDMException("Invalid input for {}: Value for {} must be provided for bo name : '{}'", executionContext.getExecutorLocale(), operationName, DSPDMConstants.DSPDM_REQUEST.TIMEZONE_KEY, boName);
                }
                Object obj = boAsMap.get(DSPDMConstants.DSPDM_REQUEST.READ_BACK_KEY);
                if ((obj != null) && (!(executionContext.isReadBack()))) {
                    // If the old value for read back is false then we can only change the value.
                    // If old value is true then we will keep it true even for nested saves and updates
                    if (obj instanceof String) {
                        readBack = Boolean.valueOf(((String) obj).trim());
                    } else if (obj instanceof Boolean) {
                        readBack = ((Boolean) obj).booleanValue();
                    }
                    executionContext.setReadBack(readBack);
                }
                if (ConfigProperties.getInstance().allow_sql_stats_collection.getBooleanValue()) {
                    // stats
                    obj = boAsMap.get(DSPDMConstants.DSPDM_REQUEST.SHOW_SQL_STATS_KEY);
                    if (obj != null) {
                        if (obj instanceof String) {
                            showSQLStats = Boolean.valueOf(((String) obj).trim());
                        } else if (obj instanceof Boolean) {
                            showSQLStats = ((Boolean) obj).booleanValue();
                        }
                        executionContext.setCollectSQLStats(showSQLStats);
                    }
                    // script
                    obj = boAsMap.get(DSPDMConstants.DSPDM_REQUEST.COLLECT_SQL_SCRIPT_KEY);
                    if (obj != null) {
                        int collectSQLScript = 0;
                        CollectSQLScriptOptions option = null;
                        if (obj instanceof String) {
                            collectSQLScript = Integer.valueOf(((String) obj).trim());
                            option = CollectSQLScriptOptions.getByValue(collectSQLScript);
                        } else if (obj instanceof Number) {
                            collectSQLScript = NumberUtils.convertToInteger(obj, executionContext);
                            option = CollectSQLScriptOptions.getByValue(collectSQLScript);
                        }
                        if (option != null) {
                            executionContext.setCollectSQLScriptOptions(option);
                        } else {
                            throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST, "Invalid value '{}' given for param '{}'. {}",
                                    executionContext.getExecutorLocale(), obj, DSPDMConstants.DSPDM_REQUEST.COLLECT_SQL_SCRIPT_KEY, CollectSQLScriptOptions.description);
                        }
                    }
                }

                obj = boAsMap.get(DSPDMConstants.DSPDM_REQUEST.FULL_DROP_KEY);
                if (obj != null) {
                    if (obj instanceof String) {
                        fullDrop = Boolean.valueOf(((String) obj).trim());
                    } else if (obj instanceof Boolean) {
                        fullDrop = ((Boolean) obj).booleanValue();
                    }
                    executionContext.setFullDrop(fullDrop);
                }
                obj = boAsMap.get(DSPDMConstants.DSPDM_REQUEST.DELETE_CASCADE_KEY);
                if (obj != null) {
                    if (obj instanceof String) {
                        deleteCascade = Boolean.valueOf(((String) obj).trim());
                    } else if (obj instanceof Boolean) {
                        deleteCascade = ((Boolean) obj).booleanValue();
                    }
                    executionContext.setDeleteCascade(deleteCascade);
                }
                obj = boAsMap.get(DSPDMConstants.DSPDM_REQUEST.WRITE_NULL_VALUES_KEY);
                if (obj != null) {
                    if (obj instanceof String) {
                        writeNullValues = Boolean.valueOf(((String) obj).trim());
                    } else if (obj instanceof Boolean) {
                        writeNullValues = ((Boolean) obj).booleanValue();
                    }
                    executionContext.setWriteNullValues(writeNullValues);
                }
                obj = boAsMap.get(DSPDMConstants.DSPDM_REQUEST.OPERATION_NAME_KEY);
                if (obj != null) {
                    if (obj instanceof String) {
                        String currentOperationName = ((String) obj).trim();
                        if (DSPDMConstants.BusinessObjectOperations.BULK_IMPORT_OPR.getOperationShortName().equalsIgnoreCase(currentOperationName)) {
                            executionContext.setCurrentOperationId(DSPDMConstants.BusinessObjectOperations.BULK_IMPORT_OPR.getId());
                        } else if (DSPDMConstants.BusinessObjectOperations.SAVE_AND_UPDATE_OPR.getOperationShortName().equalsIgnoreCase(currentOperationName)) {
                            executionContext.setCurrentOperationId(DSPDMConstants.BusinessObjectOperations.SAVE_AND_UPDATE_OPR.getId());
                        }
                    }
                    if (executionContext.getCurrentOperationId() == null) {
                        throw new DSPDMException("Invalid value found for an optional parameter '{}'. Possible values are '{}' and '{}'.",
                                executionContext.getExecutorLocale(), operationName, DSPDMConstants.DSPDM_REQUEST.OPERATION_NAME_KEY,
                                DSPDMConstants.BusinessObjectOperations.BULK_IMPORT_OPR.getOperationShortName(),
                                DSPDMConstants.BusinessObjectOperations.SAVE_AND_UPDATE_OPR.getOperationShortName(), boName);
                    }
                } else {
                    // since Operation Name is an optional param, when we dont receive this in request, by default we set the operation name to be save/update
                    executionContext.setCurrentOperationId(DSPDMConstants.BusinessObjectOperations.SAVE_AND_UPDATE_OPR.getId());
                }
                if (boAsMap.get(DSPDMConstants.DSPDM_REQUEST.DATA_KEY) != null) {
                    if (boAsMap.get(DSPDMConstants.DSPDM_REQUEST.DATA_KEY) instanceof List) {
                        List<Map<String, Object>> businessObjects = (List<Map<String, Object>>) boAsMap.get(DSPDMConstants.DSPDM_REQUEST.DATA_KEY);
                        if (CollectionUtils.hasValue(businessObjects)) {
                            if (businessObjects.size() <= ConfigProperties.getInstance().max_records_to_parse.getIntegerValue()) {
                                Map<String, DynamicDTO> metadataMap = dynamicReadService.readMetadataMapForBOName(boName, executionContext);
                                List<DynamicDTO> list = new ArrayList<>();
                                for (Map<String, Object> businessObject : businessObjects) {
                                    list.add(buildDynamicDTOFromMap(boName, businessObject, metadataMap, operationName, allowNewFields, dynamicReadService, executionContext));
                                }
                                dynamicDTOMap.put(boName, list);
                            } else {
                                throw new DSPDMException("Invalid input for {} : {} Cannot process more than '{}' records in a single request for bo name '{}'",
                                        executionContext.getExecutorLocale(), operationName, DSPDMConstants.DSPDM_REQUEST.DATA_KEY, ConfigProperties.getInstance().max_records_to_parse.getIntegerValue(), boName);
                            }
                        } else {
                            throw new DSPDMException("Invalid input for {} : {} array has no records for bo name : '{}'", executionContext.getExecutorLocale(), operationName, DSPDMConstants.DSPDM_REQUEST.DATA_KEY, boName);
                        }
                    } else {
                        throw new DSPDMException("Invalid input for {} : {} payload must be an array for bo name : '{}'", executionContext.getExecutorLocale(), operationName, DSPDMConstants.DSPDM_REQUEST.DATA_KEY, boName);
                    }
                } else {
                    throw new DSPDMException("Invalid input for {} : no {} found for bo name : '{}'", executionContext.getExecutorLocale(), operationName, DSPDMConstants.DSPDM_REQUEST.DATA_KEY, boName);
                }
            } else {
                throw new DSPDMException("Invalid input for {} : For bo name : '{}'", executionContext.getExecutorLocale(), operationName, DSPDMConstants.DSPDM_REQUEST.DATA_KEY, boName);
            }
        }
        return dynamicDTOMap;
    }

    //initialize the save/update parameters for the user current unit templates
    public static Map<String, Object> initUserCurrentUnitTemplateParams(ExecutionContext executionContext,
                                                                        IDynamicReadService dynamicReadService, List<DynamicDTO> userUnitSettingsDTOs) {
        Map<String, Object> userCurrentUnitTemplateParams = new HashMap<String, Object>();
        Map<String, Object> param = new HashMap<String, Object>();
        if (userUnitSettingsDTOs != null && userUnitSettingsDTOs.size() > 0) {
            param.put(DSPDMConstants.DSPDM_REQUEST.LANGUAGE_KEY, executionContext.getExecutorLocale().getLanguage());
            param.put(DSPDMConstants.DSPDM_REQUEST.TIMEZONE_KEY, LocaleUtils.getTimezone(TimeZone.getTimeZone(executionContext.getExecutorTimeZone())));
            param.put(DSPDMConstants.DSPDM_REQUEST.READ_BACK_KEY, false);
            List<Map<String, Object>> dataParams = new ArrayList<Map<String, Object>>();
            for (DynamicDTO userUnitSettingsDTO : userUnitSettingsDTOs) {
                Map<String, Object> dataParam = new HashMap<String, Object>();
                String userName = userUnitSettingsDTO.get(DSPDMConstants.Units.USER_NAME).toString();
                String unitTemplateId = userUnitSettingsDTO.get(DSPDMConstants.Units.UNIT_TEMPLATE_ID).toString();
                Integer userCurrentUnitTemplateId = DTOHelper.getUserCurrentUnitTemplateId(executionContext, dynamicReadService,
                        userName);
                if (userCurrentUnitTemplateId > 0) {
                    dataParam.put(DSPDMConstants.Units.USER_CURRENT_UNIT_TEMPLATE_ID, userCurrentUnitTemplateId);
                }
                dataParam.put(DSPDMConstants.Units.USER_NAME, userName);
                dataParam.put(DSPDMConstants.Units.UNIT_TEMPLATE_ID, unitTemplateId);
                dataParams.add(dataParam);
            }
            param.put(DSPDMConstants.DSPDM_REQUEST.DATA_KEY, dataParams);
        }
        userCurrentUnitTemplateParams.put(DSPDMConstants.Units.USER_CURRENT_UNIT_TEMPLATES, param);
        return userCurrentUnitTemplateParams;
    }

    // get current unit template id of user
    private static Integer getUserCurrentUnitTemplateId(ExecutionContext executionContext,
                                                        IDynamicReadService dynamicReadService, String userName) {
        BOQuery boQuery = new BOQuery();
        boQuery.setBoName(DSPDMConstants.Units.USER_CURRENT_UNIT_TEMPLATES);
        boQuery.setReadAllRecords(true);
        // set language and timezone of current execution context
        boQuery.setLanguage(executionContext.getExecutorLocale().getLanguage());
        boQuery.setTimezone(LocaleUtils.getTimezone(TimeZone.getTimeZone(executionContext.getExecutorTimeZone())));
        boQuery.addColumnsToSelect(DSPDMConstants.Units.USER_CURRENT_UNIT_TEMPLATE_ID);
        boQuery.setCriteriaFilters(Arrays.asList(new com.lgc.dspdm.msp.mainservice.model.CriteriaFilter(
                DSPDMConstants.Units.USER_NAME, Operator.EQUALS, new Object[]{userName})));
        BusinessObjectInfo businessObjectInfo = BusinessObjectBuilder.build(boQuery, executionContext);
        Map<String, Object> map = dynamicReadService.readWithDetails(businessObjectInfo, executionContext);
        List userCurrentUnitTemplates = (ArrayList) map.get(DSPDMConstants.Units.USER_CURRENT_UNIT_TEMPLATES);
        if (userCurrentUnitTemplates != null && userCurrentUnitTemplates.size() > 0) {
            DynamicDTO userUunitCurrentTemplateDynamicDTO = (DynamicDTO) userCurrentUnitTemplates.get(0);
            return (Integer) userUunitCurrentTemplateDynamicDTO.get(DSPDMConstants.Units.USER_CURRENT_UNIT_TEMPLATE_ID);
        }
        return -1;
    }

    private static DynamicDTO buildDynamicDTOFromMap(String boName, Map<String, Object> businessObject, Map<String, DynamicDTO> metadataMap, String operationName, boolean allowNewFields, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        // create dynamic dto without knowing primary key columns names. Therefore pass null as primary key column names
        DynamicDTO dynamicDTO = new DynamicDTO(boName, null, executionContext);
        for (Map.Entry<String, Object> entry : businessObject.entrySet()) {
            String key = entry.getKey();
            // Bypassing Azure WAF rules
            for (WafByPassRulesProperties.WAFByPassRuleProperty wafByPassRulesProperty : WafByPassRulesProperties.getInstance().getWafByPassProperties()) {
                key = wafByPassRulesProperty.applyWafRule(key);
            }
            Object value = entry.getValue();
            if (DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY.equalsIgnoreCase(key)) {
                if (value instanceof Map) {
                    Map<String, Object> childPayLoad = (Map<String, Object>) value;
                    dynamicDTO.put(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY, DTOHelper.buildBONameAndBOListMapFromRequestJSON(childPayLoad, operationName, allowNewFields, dynamicReadService, executionContext));
                } else {
                    throw new DSPDMException("Invalid input for {} : {} must be an object", executionContext.getExecutorLocale(), operationName, DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
                }
            } else {
                // uppercase bo_names
                String boAttrName = key.toUpperCase();
                if ((value != null) && (value instanceof String)) {
                    value = ((String) value).trim();
                    if (key.equalsIgnoreCase(DSPDMConstants.BoAttrName.ATTRIBUTE) || key.equalsIgnoreCase(DSPDMConstants.BoAttrName.BO_ATTR_NAME)
                            || key.equalsIgnoreCase(DSPDMConstants.BoAttrName.BO_NAME) || key.equalsIgnoreCase(DSPDMConstants.BoAttrName.CONSTRAINT_NAME)) {
                        if (Character.isDigit(((String) value).charAt(0))) {
                            throw new DSPDMException("All DB object names cannot start with number.",
                                    executionContext.getExecutorLocale(), value);
                        }
                    }
                    // Bypassing Azure WAF rules
                    for (WafByPassRulesProperties.WAFByPassRuleProperty wafByPassRulesProperty : WafByPassRulesProperties.getInstance().getWafByPassProperties()) {
                        value = wafByPassRulesProperty.applyWafRule((String) value);
                    }
                    if (DSPDMConstants.UPPER_CASE_BO_ATTR_NAMES.contains(boAttrName)) {
                        value = ((String) value).toUpperCase();
                        if (DSPDMConstants.NO_SPACE_BO_ATTR_NAMES.contains(boAttrName)) {
                            value = ((String) value).replaceAll(" ", "_");
                        }
                    } else if (DSPDMConstants.LOWER_CASE_BO_ATTR_NAMES.contains(boAttrName)) {
                        value = ((String) value).toLowerCase();
                    }
                }
                DynamicDTO metadataDTO = metadataMap.get(boAttrName);
                if ((metadataDTO == null) && (boAttrName.lastIndexOf(DSPDMConstants.DOT) > 0)) {
                    int indexDot = boAttrName.lastIndexOf(DSPDMConstants.DOT) + 1;
                    metadataDTO = metadataMap.get(boAttrName.substring(indexDot));
                }
                if (metadataDTO != null) {
                    try {
                        dynamicDTO.put(boAttrName, validateAndConvertDataType(value, metadataDTO, operationName, executionContext));
                    } catch (Exception e) {
                        DSPDMException.throwException(e, executionContext);
                    }
                } else {
                    if (allowNewFields) {
                        dynamicDTO.put(boAttrName, value);
                    } else {
                        logger.info("********** Invalid input for '{}' : extra field '{}' is not found in metadata.", operationName, key);
                    }
                }
            }
        }
        return dynamicDTO;
    }

    private static Object validateAndConvertDataType(Object value, DynamicDTO metadataDTO, String operationName, ExecutionContext executionContext) {
        if (value == null) {
            return null;
        }
        String boAttrName = metadataDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME).toString();
        String boName = metadataDTO.get(DSPDMConstants.BoAttrName.BO_NAME).toString();
        String dataType = metadataDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE).toString();
        Class javaDataType = (Class) metadataDTO.get(DSPDMConstants.BoAttrName.JAVA_DATA_TYPE);
        DSPDMConstants.DataTypes dataTypeEnum = DSPDMConstants.DataTypes.fromAttributeDataType(dataType, ConnectionProperties.isMSSQLServerDialect());
        if (dataTypeEnum.isRealDataType()) {
            if ((value instanceof Double) && ((Double) value > DSPDMConstants.MAX_DOUBLE_VALUE_FOR_REAL_DATA_TYPE)) {
                throw new DSPDMException("Real value for attribute '{}' is out of range. max value is '{}'",
                        executionContext.getExecutorLocale(), boAttrName, DSPDMConstants.MAX_DOUBLE_VALUE_FOR_REAL_DATA_TYPE);
            } else if ((value instanceof Integer) && ((Integer) value > DSPDMConstants.MAX_INTEGER_VALUE_FOR_REAL_DATA_TYPE)) {
                throw new DSPDMException("Real value for attribute '{}' is out of range. max value is '{}",
                        executionContext.getExecutorLocale(), boAttrName, DSPDMConstants.MAX_INTEGER_VALUE_FOR_REAL_DATA_TYPE);
            }
        }
        // string to other data type conversion should happen first
        if (value instanceof String) {
            value = MetadataUtils.convertValueToJavaDataTypeFromString((String) value, javaDataType, boName, boAttrName, executionContext);
        }
        // do not put in else block. make each type check to work separately
        if (value instanceof Number) {
            value = NumberUtils.convertTo(value, javaDataType, executionContext);
        }
        // must be separate from above if. In above if strings will be converted to timestamps and then this if should run on them
        if (value instanceof java.sql.Timestamp) {
            // adjusting timezone from client timezone to UTC before applying it to the where clause
            if (ConfigProperties.getInstance().use_utc_timezone_to_save.getBooleanValue()) {
                value = DateTimeUtils.convertTimezoneFromClientToUTC((java.sql.Timestamp) value, executionContext);
            }
        }
        // validate length        
        if (MetadataUtils.isDataTypeSupportsLength(dataType, boName)) {
            int dataTypeLength = MetadataUtils.getDataTypeLengthFromString(dataType, (String) metadataDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME), (String) metadataDTO.get(DSPDMConstants.BoAttrName.BO_NAME), executionContext);
            int dataTypeDecimalLength = MetadataUtils.getDataTypeDecimalLengthFromString(dataType, (String) metadataDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME), (String) metadataDTO.get(DSPDMConstants.BoAttrName.BO_NAME), executionContext);
            int length = 0;
            int decimalLength = 0;
            if (value instanceof String) {
                length = ((String) value).length();
            } else if (value instanceof Number) {
                length = NumberUtils.getIntegerLength(value, executionContext);
                decimalLength = NumberUtils.getDecimalLength(value, executionContext);
            }else if(value instanceof byte[]){
                length = NumberUtils.getIntegerLength(value, executionContext);
            } else {
                length = value.toString().length();
            }
            if (length > (dataTypeLength - dataTypeDecimalLength)) {
                throw new DSPDMException("Invalid input for {} : Value '{}' exceeds in integer length for bo : '{}' and attribute : '{}'", executionContext.getExecutorLocale(), operationName, value, boName, boAttrName);
            }
            if (decimalLength > dataTypeDecimalLength) {
                throw new DSPDMException("Invalid input for {} : Value '{}' exceeds in decimal length for bo : '{}' and attribute : '{}'", executionContext.getExecutorLocale(), operationName, value, boName, boAttrName);
            }
        }

        return value;
    }

    public static void convertDataTypeForFilters(BusinessObjectInfo businessObjectInfo, Map<String, DynamicDTO> metadataMap,
                                                 String operationName, ExecutionContext executionContext, String userName,
                                                 IDynamicReadService dynamicReadService) {

        convertDataTypeForFilters(businessObjectInfo, metadataMap, operationName, dynamicReadService, executionContext);
    }

    private static void convertDataTypeForFilters(BusinessObjectInfo businessObjectInfo, Map<String, DynamicDTO> metadataMap,
                                                  String operationName,
                                                  IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        if (businessObjectInfo.getFilterList() != null) {
            // check criteria filters
            if(CollectionUtils.hasValue(businessObjectInfo.getFilterList().getFilters())) {
                convertDataTypeForCriteriaFilters(businessObjectInfo.getBusinessObjectType(), businessObjectInfo.getFilterList().getFilters(),
                        metadataMap, operationName, executionContext);
            }
            // check having filters do not put in else block
            if(CollectionUtils.hasValue(businessObjectInfo.getFilterList().getHavingFilters())) {
                convertDataTypeForHavingFilters(businessObjectInfo.getBusinessObjectType(),
                        businessObjectInfo.getFilterList().getHavingFilters(), metadataMap, operationName,  executionContext);
            }
        }
        // now apply same for joins
        if (businessObjectInfo.isJoiningRead()) {
            if (businessObjectInfo.hasSimpleJoinsToRead()) {
                List<SimpleJoinClause> simpleJoins = businessObjectInfo.getSimpleJoins();
                for (SimpleJoinClause simpleJoin : simpleJoins) {
                    if (simpleJoin.getFilterList() != null) {
                        convertDataTypeForSimpleJoin(simpleJoin, operationName, dynamicReadService, executionContext);
                    }
                }
            }
            if (businessObjectInfo.hasDynamicJoinsToRead()) {
                List<DynamicJoinClause> dynamicJoins = businessObjectInfo.getDynamicJoins();
                for (DynamicJoinClause dynamicJoin : dynamicJoins) {
                    List<DynamicTable> dynamicTables = dynamicJoin.getDynamicTables();
                    for (DynamicTable dynamicTable : dynamicTables) {
                        if (dynamicTable.getFilterList() != null) {
                            convertDataTypeForDynamicTable(dynamicTable, operationName, dynamicReadService, executionContext);
                        }
                    }
                }
            }
        }
    }

    private static void convertDataTypeForDynamicTable(DynamicTable dynamicTable, String operationName,
                                                       IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        Map<String, DynamicDTO> metadataMap = dynamicReadService.readMetadataMapForBOName(dynamicTable.getBoName(), executionContext);
        // check criteria filters
        if (CollectionUtils.hasValue(dynamicTable.getFilterList().getFilters())) {
            convertDataTypeForCriteriaFilters(dynamicTable.getBoName(), dynamicTable.getFilterList().getFilters(), metadataMap,
                    operationName, executionContext);
        }
        // check having filters do not put in else block
        if (CollectionUtils.hasValue(dynamicTable.getFilterList().getHavingFilters())) {
            convertDataTypeForHavingFilters(dynamicTable.getBoName(),
                    dynamicTable.getFilterList().getHavingFilters(), metadataMap, operationName, executionContext);
        }
    }

    private static void convertDataTypeForSimpleJoin(SimpleJoinClause simpleJoin, String operationName,
                                                     IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        Map<String, DynamicDTO> metadataMap = dynamicReadService.readMetadataMapForBOName(simpleJoin.getBoName(), executionContext);
        // check criteria filters
        if (CollectionUtils.hasValue(simpleJoin.getFilterList().getFilters())) {
            convertDataTypeForCriteriaFilters(simpleJoin.getBoName(), simpleJoin.getFilterList().getFilters(), metadataMap,
                    operationName, executionContext);
        }
        // check having filters do not put in else block
        if (CollectionUtils.hasValue(simpleJoin.getFilterList().getHavingFilters())) {
            convertDataTypeForHavingFilters(simpleJoin.getBoName(), simpleJoin.getFilterList().getHavingFilters(), metadataMap,
                    operationName, executionContext);
        }
    }

    private static void convertDataTypeForCriteriaFilters(String boName, List<CriteriaFilter> filters, Map<String, DynamicDTO> metadataMap,
                                                  String operationName, ExecutionContext executionContext) {
        for (CriteriaFilter filter : filters) {
            convertDataTypeForCriteriaFilter(boName, filter, metadataMap, operationName, executionContext);
        }
    }

    private static void convertDataTypeForCriteriaFilter(String boName, CriteriaFilter filter, Map<String, DynamicDTO> metadataMap,
                                                  String operationName, ExecutionContext executionContext) {
        // upper case bo attr name
        DynamicDTO metadataDTO = metadataMap.get(filter.getColumnName());
        if ((metadataDTO == null) && (filter.getColumnName().lastIndexOf(DSPDMConstants.DOT) > 0)) {
            int indexDot = filter.getColumnName().lastIndexOf(DSPDMConstants.DOT) + 1;
            metadataDTO = metadataMap.get(filter.getColumnName().substring(indexDot));
        }
        if (metadataDTO != null) {
            if (CollectionUtils.hasValue(filter.getValues())) {
                int filterValueIndex = 0;
                for (Object value : filter.getValues()) {
                    // convert value from string to actual data type as per in metadata
                    if (value != null) {
                        value = validateAndConvertDataType(value, metadataDTO, operationName, executionContext);
                        // update new value in filter
                        filter.getValues()[filterValueIndex] = value;
                    }
                    filterValueIndex++;
                }
            }
        } else {
            logger.info("*********** BO Attribute Name '{}' not found in metadata for BO '{}'", filter.getColumnName(), boName);
        }
    }

    private static void convertDataTypeForHavingFilters(String boName, List<HavingFilter> filters, Map<String, DynamicDTO> metadataMap,
                                                  String operationName, ExecutionContext executionContext) {
        for (HavingFilter filter : filters) {
            convertDataTypeForHavingFilter(boName, filter, metadataMap, operationName, executionContext);
        }
    }

    private static void convertDataTypeForHavingFilter(String boName, HavingFilter filter, Map<String, DynamicDTO> metadataMap,
                                                  String operationName, ExecutionContext executionContext) {
        DynamicDTO metadataDTO = metadataMap.get(filter.getAggregateColumn().getBoAttrName());
        if ((metadataDTO == null) && (filter.getAggregateColumn().getBoAttrName().lastIndexOf(DSPDMConstants.DOT) > 0)) {
            int indexDot = filter.getAggregateColumn().getBoAttrName().lastIndexOf(DSPDMConstants.DOT) + 1;
            metadataDTO = metadataMap.get(filter.getAggregateColumn().getBoAttrName().substring(indexDot));
        }
        if (metadataDTO != null) {
            if (CollectionUtils.hasValue(filter.getValues())) {
                int filterValueIndex = 0;
                for (Object value : filter.getValues()) {
                    // convert value from string to actual data type as per in metadata
                    if ((value != null) && (value instanceof String)) {
                        switch (filter.getAggregateColumn().getAggregateFunction()) {
                            case COUNT:
                            case AVG:
                            case SUM:
                                value = MetadataUtils.convertValueToJavaDataTypeFromString((String) value, java.lang.Double.class, boName,
                                        filter.getAggregateColumn().getBoAttrName(), executionContext);
                                break;
                            case MIN:
                            case MAX:
                                value = validateAndConvertDataType(value, metadataDTO, operationName, executionContext);
                                break;
                            default:
                        }
                        // update new value in filter
                        filter.getValues()[filterValueIndex] = value;
                    }
                    filterValueIndex++;
                }
            }
        } else {
            logger.info("*********** BO Attribute Name '{}' not found in metadata for BO '{}'",
                    filter.getAggregateColumn().getBoAttrName(), boName);
        }
    }
}