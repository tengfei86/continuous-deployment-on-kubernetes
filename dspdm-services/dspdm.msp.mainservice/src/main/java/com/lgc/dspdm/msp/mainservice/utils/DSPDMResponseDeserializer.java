package com.lgc.dspdm.msp.mainservice.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.lgc.dspdm.core.common.data.common.DSPDMMessage;
import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.DateTimeUtils;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.service.common.dynamic.read.IDynamicReadService;
import com.lgc.dspdm.msp.mainservice.model.DSPDMResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

public class DSPDMResponseDeserializer extends JsonDeserializer<DSPDMResponse> {
    private static DSPDMLogger logger = new DSPDMLogger(DSPDMResponseDeserializer.class);
    //@Inject
    IDynamicReadService dynamicReadService;

    @Override
    public DSPDMResponse deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectCodec codec = p.getCodec();
        JsonNode rootNode = codec.readTree(p);
        // EXECUTION CONTEXT
        JsonNode executionContextNode = rootNode.get(DSPDMConstants.DSPDM_RESPONSE.EXECUTION_CONTEXT_KEY);
        ExecutionContext executionContext = parseExecutionContext(executionContextNode);
        // STATUS
        JsonNode statusNode = rootNode.get(DSPDMConstants.DSPDM_RESPONSE.STATUS_KEY);
        DSPDMConstants.Status status = parseStatus(statusNode, executionContext);
        // MESSAGES
        JsonNode messagesNode = rootNode.get(DSPDMConstants.DSPDM_RESPONSE.MESSAGES_KEY);
        List<DSPDMMessage> messages = parseMessages(messagesNode, executionContext);
        // EXCEPTION
        JsonNode exceptionNode = rootNode.get(DSPDMConstants.DSPDM_RESPONSE.EXCEPTION_KEY);
        Throwable exception = parseException(exceptionNode, executionContext);
        // DATA
        JsonNode dataNode = rootNode.get(DSPDMConstants.DSPDM_RESPONSE.DATA_KEY);
        Map<String, Object> data = parseData(dataNode, executionContext);

        DSPDMResponse dspdmResponse = null;
        if (exception != null) {
            dspdmResponse = new DSPDMResponse(exception, executionContext);
        } else {
            dspdmResponse = new DSPDMResponse(status, messages, data, executionContext);
        }
        return dspdmResponse;
    }

    private ExecutionContext parseExecutionContext(JsonNode executionContextNode) throws IOException {
        ExecutionContext executionContext = ExecutionContext.getEmptyExecutionContext();
        if ((executionContextNode != null) && (executionContextNode.size() > 0)) {
            executionContext.setExecutorName(executionContextNode.get(DSPDMConstants.DSPDM_RESPONSE.EXECUTOR_NAME_KEY).asText());
            executionContext.setExecutorLocale(Locale.forLanguageTag(executionContextNode.get(DSPDMConstants.DSPDM_RESPONSE.EXECUTOR_LOCALE_KEY).asText()));
            executionContext.setExecutorTimeZone(ZoneId.of(executionContextNode.get(DSPDMConstants.DSPDM_RESPONSE.EXECUTOR_TIMEZONE_KEY).asText()));
            executionContext.setTransactionStarted(executionContextNode.get(DSPDMConstants.DSPDM_RESPONSE.TRANSACTION_STARTED_KEY).booleanValue());
            executionContext.setTransactionCommitted(executionContextNode.get(DSPDMConstants.DSPDM_RESPONSE.TRANSACTION_COMMITTED_KEY).booleanValue());
            executionContext.setTransactionRollbacked(executionContextNode.get(DSPDMConstants.DSPDM_RESPONSE.TRANSACTION_ROLL_BACKED_KEY).booleanValue());
            executionContext.setUnitTestCall(executionContextNode.get(DSPDMConstants.DSPDM_RESPONSE.UNIT_TEST_CALL_KEY).booleanValue());
            executionContext.setProcessingStartTime(executionContextNode.get(DSPDMConstants.DSPDM_RESPONSE.PROCESSING_START_TIME_KEY).longValue());
            executionContext.setTotalTimeTakenByDB(executionContextNode.get(DSPDMConstants.DSPDM_RESPONSE.TOTAL_TIME_TAKEN_BY_DB_KEY).longValue());
            executionContext.setReadBack(executionContextNode.get(DSPDMConstants.DSPDM_RESPONSE.READ_BACK_KEY).booleanValue());
        }
        return executionContext;
    }

    private DSPDMConstants.Status parseStatus(JsonNode statusNode, ExecutionContext executionContext) throws IOException {
        DSPDMConstants.Status result = null;
        if ((statusNode != null) && (statusNode.size() > 0)) {
            int code = statusNode.get(DSPDMConstants.DSPDM_RESPONSE.STATUS_CODE_KEY).intValue();
            for (DSPDMConstants.Status status : DSPDMConstants.Status.values()) {
                if (status.getCode() == code) {
                    result = status;
                    break;
                }
            }
        }
        return result;
    }

    private List<DSPDMMessage> parseMessages(JsonNode messagesNode, ExecutionContext executionContext) throws IOException {
        List<DSPDMMessage> messages = null;
        if ((messagesNode != null) && (messagesNode.size() > 0) && (messagesNode.isArray())) {
            messages = new ArrayList<>(messagesNode.size());
            for (JsonNode messageNode : messagesNode) {
                messages.add(parseMessage(messageNode, executionContext));
            }
        }
        return messages;
    }

    private DSPDMMessage parseMessage(JsonNode messagesNode, ExecutionContext executionContext) throws IOException {
        DSPDMMessage dspdmMessage = null;
        if ((messagesNode != null) && (messagesNode.size() > 0)) {
            // MESSAGE
            String message = messagesNode.get(DSPDMConstants.DSPDM_RESPONSE.MESSAGE_KEY).asText();
            // MESSAGE STATUS
            JsonNode statusNode = messagesNode.get(DSPDMConstants.DSPDM_RESPONSE.STATUS_KEY);
            DSPDMConstants.Status status = parseStatus(statusNode, executionContext);
            dspdmMessage = new DSPDMMessage(status, message);
        }
        return dspdmMessage;
    }

    private Throwable parseException(JsonNode exceptionNode, ExecutionContext executionContext) throws IOException {
        DSPDMException exception = null;
        if ((exceptionNode != null) && (exceptionNode.size() > 0)) {
            // MESSAGE
            String message = exceptionNode.get(DSPDMConstants.DSPDM_RESPONSE.MESSAGE_KEY).asText();
            // STACK TRACE
            String stackTrace = exceptionNode.get(DSPDMConstants.DSPDM_RESPONSE.STACK_TRACE_KEY).asText();
            exception = new DSPDMException(message, executionContext.getExecutorLocale());
            exception.setStackTrace(StringUtils.parseExceptionStackTrace(stackTrace));
        }
        return exception;
    }

    private Map<String, Object> parseData(JsonNode dataNode, ExecutionContext executionContext) throws IOException {
        Map<String, Object> data = null;
        if ((dataNode != null) && (dataNode.size() > 0) && (dataNode.isObject())) {
            Iterator<String> fieldNames = dataNode.fieldNames();
            data = new LinkedHashMap<>(dataNode.size());
            String boName = null;
            JsonNode pagedListNode = null;
            while (fieldNames.hasNext()) {
                boName = fieldNames.next();
                pagedListNode = dataNode.get(boName);
				if (boName.equals(DSPDMConstants.Units.UNITS)) {//dspdmUnits is Array type
					data.put(boName, pagedListNode);
				} else {
					data.put(boName, parsePagedList(pagedListNode, executionContext));
				}
            }
        }
        return data;
    }

    private PagedList<DynamicDTO> parsePagedList(JsonNode pagedListNode, ExecutionContext executionContext) throws IOException {
        PagedList<DynamicDTO> pagedList = null;
        if ((pagedListNode != null) && (pagedListNode.size() > 0) && (pagedListNode.isObject())) {
            // TOTAL RECORDS
            int totalRecords = pagedListNode.get(DSPDMConstants.DSPDM_RESPONSE.TOTAL_RECORDS_KEY).intValue();
            // LIST
            JsonNode listNode = pagedListNode.get(DSPDMConstants.DSPDM_RESPONSE.LIST_KEY);
            List<DynamicDTO> list = parseDynamicDTOList(listNode, executionContext);
            pagedList = new PagedList<>(totalRecords, list);
        }
        return pagedList;
    }

    private List<DynamicDTO> parseDynamicDTOList(JsonNode listNode, ExecutionContext executionContext) throws IOException {
        List<DynamicDTO> list = null;
        if ((listNode != null) && (listNode.isArray())) {
            list = new ArrayList<>(listNode.size());
            for (JsonNode dynamicDTONode : listNode) {
                list.add(parseDynamicDTO(dynamicDTONode, executionContext));
            }
        }
        return list;
    }

    private DynamicDTO parseDynamicDTO(JsonNode dynamicDTONode, ExecutionContext executionContext) throws IOException {
        DynamicDTO dynamicDTO = null;
        if ((dynamicDTONode != null) && (dynamicDTONode.size() > 0) && (dynamicDTONode.isObject())) {
            JsonNode boNameNode = dynamicDTONode.get(DSPDMConstants.DSPDM_RESPONSE.TYPE_KEY);
            String boName = (boNameNode == null) ? null : boNameNode.asText();
            dynamicDTO = new DynamicDTO(boName, null, executionContext);
            Iterator<String> fieldNames = dynamicDTONode.fieldNames();
            String boAttrName = null;
            JsonNode valueNode = null;
            while (fieldNames.hasNext()) {
                boAttrName = fieldNames.next();
                valueNode = dynamicDTONode.get(boAttrName);
                if ((dynamicDTONode != null) && (dynamicDTONode.size() > 0)) {
                    if (dynamicDTONode.isArray()) {
                        dynamicDTO.put(boAttrName, parseDynamicDTOList(valueNode, executionContext));
                    } else if (dynamicDTONode.isObject()) {
                        dynamicDTO.put(boAttrName, parseObject(valueNode, executionContext));
                    } else {
                        dynamicDTO.put(boAttrName, parseBasic(valueNode, executionContext));
                    }
                }
            }
        }
        return dynamicDTO;
    }

    private Map<String, Object> parseMap(JsonNode mapNode, ExecutionContext executionContext) throws IOException {
        Map<String, Object> map = null;
        if ((mapNode != null) && (mapNode.size() > 0) && (mapNode.isObject())) {
            Iterator<String> fieldNames = mapNode.fieldNames();
            map = new LinkedHashMap<>(mapNode.size());
            String mapKey = null;
            while (fieldNames.hasNext()) {
                mapKey = fieldNames.next();
                map.put(mapKey, parseObject(mapNode.get(mapKey), executionContext));
            }
        }
        return map;
    }

    private Object parseObject(JsonNode objectNode, ExecutionContext executionContext) throws IOException {
        Object object = null;
        if ((objectNode != null) && (!(objectNode.isNull()))) {
            if (objectNode.isArray()) {
                object = parseArray(objectNode, executionContext);
            } else if (objectNode.isObject()) {
                object = parseMap(objectNode, executionContext);
            } else {
                object = parseBasic(objectNode, executionContext);
            }
        }
        return object;
    }

    private List<Object> parseArray(JsonNode arrayNode, ExecutionContext executionContext) throws IOException {
        List<Object> list = null;
        if ((arrayNode != null) && (arrayNode.isArray())) {
            list = new ArrayList<>(arrayNode.size());
            for (JsonNode objectNode : arrayNode) {
                list.add(parseObject(objectNode, executionContext));
            }
        }
        return list;
    }

    private Object parseBasic(JsonNode basicNode, ExecutionContext executionContext) throws IOException {
        Object object = null;
        if ((basicNode != null) && (!(basicNode.isNull()))) {
            JsonNodeType nodeType = basicNode.getNodeType();
            switch (nodeType) {
                case MISSING:
                    object = null;
                    break;
                case BOOLEAN:
                    object = basicNode.booleanValue();
                    break;
                case STRING:
                    String text = basicNode.asText();
                    if (text.length() >= 8) {
                        try {
                            ZonedDateTime zonedDateTime = DateTimeUtils.parse(text, executionContext);
                            object = DateTimeUtils.getTimestamp(zonedDateTime);
                        } catch (Exception e) {
                            object = text;
                        }
                    } else {
                        object = text;
                    }
                    break;
                case NUMBER:
                    if (basicNode.isBigDecimal()) {
                        object = BigDecimal.valueOf(basicNode.doubleValue());
                    } else if (basicNode.isInt()) {
                        object = basicNode.intValue();
                    } else if (basicNode.isDouble()) {
                        object = basicNode.doubleValue();
                    } else if (basicNode.isLong()) {
                        object = basicNode.longValue();
                    } else if (basicNode.isShort()) {
                        object = basicNode.shortValue();
                    } else if (basicNode.isFloat()) {
                        object = basicNode.floatValue();
                    } else if (basicNode.isBigInteger()) {
                        object = basicNode.bigIntegerValue();
                    }
                    break;
                case BINARY:
                    object = basicNode.binaryValue();
                    break;
                case POJO:
                    object = parseObject(basicNode, executionContext);
                    break;
                case ARRAY:
                    object = parseArray(basicNode, executionContext);
                    break;
                case OBJECT:
                    object = parseObject(basicNode, executionContext);
                    break;
                default:
            }
        }
        return object;
    }
}