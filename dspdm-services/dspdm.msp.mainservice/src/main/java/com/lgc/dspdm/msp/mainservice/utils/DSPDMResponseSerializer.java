package com.lgc.dspdm.msp.mainservice.utils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.data.common.BulkImportInvalidRecordReasonDTO;
import com.lgc.dspdm.core.common.data.common.BulkImportResponseDTO;
import com.lgc.dspdm.core.common.data.common.DSPDMMessage;
import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.criteria.DSPDMUnit;
import com.lgc.dspdm.core.common.data.criteria.SQLExpression;
import com.lgc.dspdm.core.common.data.criteria.Unit;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicPK;
import com.lgc.dspdm.core.common.exception.DSPDMBatchException;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.DateTimeUtils;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.msp.mainservice.model.DSPDMResponse;
import com.lgc.dspdm.service.common.dynamic.read.DynamicReadService;
import com.lgc.dspdm.service.common.dynamic.read.IDynamicReadService;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.*;

public class DSPDMResponseSerializer extends JsonSerializer<DSPDMResponse> {
    private static DSPDMLogger logger = new DSPDMLogger(DSPDMResponseSerializer.class);
    //@Inject
    IDynamicReadService dynamicReadService;

    @Override
    public void serialize(DSPDMResponse dspdmResponse, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        // whether write null values
        boolean writeNullValues = dspdmResponse.getExecutionContext().isWriteNullValues();

        jsonGenerator.setPrettyPrinter(new DefaultPrettyPrinter());
        // START RESPONSE OBJECT
        jsonGenerator.writeStartObject();
        // STATUS
        jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.STATUS_KEY);
        writeStatus(jsonGenerator, dspdmResponse.getStatus(), dspdmResponse.getExecutionContext());
        // MESSAGES
        jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.MESSAGES_KEY);
        writeMessages(jsonGenerator, dspdmResponse.getMessages(), dspdmResponse.getExecutionContext());
        // EXCEPTION
        jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.EXCEPTION_KEY);
        writeException(jsonGenerator, dspdmResponse.getException(), dspdmResponse.getExecutionContext());
        // DATA
        jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.DATA_KEY);
        writeMap(jsonGenerator, dspdmResponse.getData(), writeNullValues, dspdmResponse.getExecutionContext());
        // EXECUTION CONTEXT write only in case of unit test calls
        if (dspdmResponse.getExecutionContext().isUnitTestCall()) {
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.EXECUTION_CONTEXT_KEY);
            writeExecutionContext(jsonGenerator, writeNullValues, dspdmResponse.getExecutionContext());
        }
        // OTHER INTERNAL STANDARD FIELDS
        // Application Version
        jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.VERSION_KEY, ConfigProperties.getInstance().version.getPropertyValue());
        // THREAD NAME
        jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.THREAD_NAME_KEY, Thread.currentThread().getName());
        // REQUEST TIME
        Timestamp requestTime = DateTimeUtils.getCurrentTimestampUTC();
        requestTime.setTime(dspdmResponse.getExecutionContext().getProcessingStartTime());
        requestTime = DateTimeUtils.convertTimezoneFromUTCToClient(requestTime, dspdmResponse.getExecutionContext());
        jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.REQUEST_TIME_KEY, DateTimeUtils.getString(DateTimeUtils.DISPLAY_PATTERNS.TIMESTAMP_FORMAT_WITH_TIMEZONE.getPattern(), requestTime, dspdmResponse.getExecutionContext().getExecutorLocale(), dspdmResponse.getExecutionContext().getExecutorTimeZone()));
        // RESPONSE TIME
        Timestamp responseTime = DateTimeUtils.getCurrentTimestampUTC();
        responseTime = DateTimeUtils.convertTimezoneFromUTCToClient(responseTime, dspdmResponse.getExecutionContext());
        jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.RESPONSE_TIME_KEY, DateTimeUtils.getString(DateTimeUtils.DISPLAY_PATTERNS.TIMESTAMP_FORMAT_WITH_TIMEZONE.getPattern(), responseTime, dspdmResponse.getExecutionContext().getExecutorLocale(), dspdmResponse.getExecutionContext().getExecutorTimeZone()));
        // SHOW SQL STATS
        if ((dspdmResponse.getExecutionContext().isCollectSQLStats()) && (dspdmResponse.getExecutionContext().getSqlStats() != null)) {
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.SQL_STATS_KEY);
            writeSQLStats(jsonGenerator, writeNullValues, dspdmResponse.getExecutionContext());
        }
        // COLLECT SQL SCRIPT
        if (dspdmResponse.getExecutionContext().getCollectSQLScriptOptions().isEnabled()) {
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.SQL_SCRIPTS_KEY);
            writeSQLScript(jsonGenerator, writeNullValues, dspdmResponse.getExecutionContext());
        }
        // END RESPONSE OBJECT    
        jsonGenerator.writeEndObject();
    }

    private void writeStatus(JsonGenerator jsonGenerator, DSPDMConstants.Status status, ExecutionContext executionContext) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeNumberField(DSPDMConstants.DSPDM_RESPONSE.STATUS_CODE_KEY, status.getCode());
        jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.STATUS_LABEL_KEY, status.getLabel());
        jsonGenerator.writeEndObject();
    }

    private void writeMessages(JsonGenerator jsonGenerator, List<DSPDMMessage> dspdmMessages, ExecutionContext executionContext) throws IOException {
        jsonGenerator.writeStartArray();
        if (CollectionUtils.hasValue(dspdmMessages)) {
            for (DSPDMMessage dspdmMessage : dspdmMessages) {
                writeMessage(jsonGenerator, dspdmMessage, executionContext);
            }
        }
        jsonGenerator.writeEndArray();
    }

    private void writeMessage(JsonGenerator jsonGenerator, DSPDMMessage dspdmMessage, ExecutionContext executionContext) throws IOException {
        jsonGenerator.writeStartObject();
        // MESSAGE TEXT
        jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.MESSAGE_KEY, dspdmMessage.getMessage());
        // MESSAGE STATUS
        jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.STATUS_KEY);
        writeStatus(jsonGenerator, dspdmMessage.getStatus(), executionContext);
        jsonGenerator.writeEndObject();
    }

    private void writeMap(JsonGenerator jsonGenerator, Map<String, Object> map, boolean writeNullValues, ExecutionContext executionContext) throws IOException {
        jsonGenerator.writeStartObject();
        if (CollectionUtils.hasValue(map)) {
            // Most probably key will be a BO Name
            for (String key : map.keySet()) {
                Object object = map.get(key);
                // do not write null values in json response. If something is not in response it means it is null
                if (object != null) {
                    // Write BO Name
                    jsonGenerator.writeFieldName(key);
                    writeObject(jsonGenerator, object, writeNullValues, executionContext);
                } else if (writeNullValues) {
                    // Write BO Name
                    jsonGenerator.writeFieldName(key);
                    jsonGenerator.writeNull();
                }
            }
        }
        jsonGenerator.writeEndObject();
    }

    private void writeObject(JsonGenerator jsonGenerator, Object object, boolean writeNullValues, ExecutionContext executionContext) throws IOException {
        if (object instanceof DynamicDTO) {
            // RECURSIVE CALL TO SAME FUNCTION TO WRITE CHILD
            writeDynamicDTO(jsonGenerator, (DynamicDTO) object, writeNullValues, executionContext);
        } else if (object instanceof PagedList) {
            writePagedList(jsonGenerator, (PagedList) object, writeNullValues, executionContext);
        } else if (object instanceof List) {
            writeList(jsonGenerator, (List) object, writeNullValues, executionContext);
        } else if (object.getClass().isArray()) {
            writeArray(jsonGenerator, object, writeNullValues, executionContext);
        } else if (object instanceof Map) {
            writeMap(jsonGenerator, (Map) object, writeNullValues, executionContext);
        } else if (object instanceof BulkImportInvalidRecordReasonDTO) {
            writeBulkImportInvalidRecordReasonDTO(jsonGenerator, (BulkImportInvalidRecordReasonDTO) object, writeNullValues, executionContext);
        } else if (object instanceof BulkImportResponseDTO) {
            writeBulkImportResponseDTO(jsonGenerator, (BulkImportResponseDTO) object, writeNullValues, executionContext);
        } else if (object instanceof SQLExpression) {
            writeSimpleTypes(jsonGenerator, DSPDMConstants.EMPTY_STRING, writeNullValues, executionContext);
        } else {
            writeSimpleTypes(jsonGenerator, object, writeNullValues, executionContext);
        }
    }

    private void writeBulkImportInvalidRecordReasonDTO(JsonGenerator jsonGenerator, BulkImportInvalidRecordReasonDTO bulkImportInvalidRecordReasonDTO, boolean writeNullValues, ExecutionContext executionContext) throws IOException {
        // START OBJECT
        jsonGenerator.writeStartObject();
        // BO NAME
        jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.BO_NAME_KEY, bulkImportInvalidRecordReasonDTO.getBoName());
        // BO ATTR NAME
        jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.BO_ATTR_NAME_KEY, bulkImportInvalidRecordReasonDTO.getBoAttrName());
        // BO ATTR DISPLAY NAME
        jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.BO_ATTR_DISPLAY_NAME_KEY, bulkImportInvalidRecordReasonDTO.getBoAttrDisplayName());
        // ROW NUMBER
        jsonGenerator.writeNumberField(DSPDMConstants.DSPDM_RESPONSE.ROW_NUMBER_KEY, bulkImportInvalidRecordReasonDTO.getRowNumber());
        // MESSAGE
        jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.MESSAGE_KEY, bulkImportInvalidRecordReasonDTO.getMessage());
        // VALUE
        if (bulkImportInvalidRecordReasonDTO.getValue() != null) {
            // Write BO Name
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.VALUE_KEY);
            writeObject(jsonGenerator, bulkImportInvalidRecordReasonDTO.getValue(), writeNullValues, executionContext);
        } else if (writeNullValues) {
            // Write BO Name
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.VALUE_KEY);
            jsonGenerator.writeNull();
        }
        // END OBJECT
        jsonGenerator.writeEndObject();
    }

    private void writeBulkImportResponseDTO(JsonGenerator jsonGenerator, BulkImportResponseDTO bulkImportResponseDTO, boolean writeNullValues, ExecutionContext executionContext) throws IOException {
		jsonGenerator.writeStartObject();
        // TOTAL RECORDS
        jsonGenerator.writeNumberField(DSPDMConstants.DSPDM_RESPONSE.TOTAL_RECORDS_KEY, bulkImportResponseDTO.getTotalRecords());
        // PARSED VALUES
        if (CollectionUtils.hasValue(bulkImportResponseDTO.getParsedValues())) {
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.PARSED_VALUES_KEY);
            writePagedList(jsonGenerator, bulkImportResponseDTO.getParsedValues(), writeNullValues, executionContext);
        }
        // OLD VALUES
        if (CollectionUtils.hasValue(bulkImportResponseDTO.getOldValues())) {
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.OLD_VALUES_KEY);
            writePagedList(jsonGenerator, bulkImportResponseDTO.getOldValues(), writeNullValues, executionContext);
        }
        // INVALID VALUES
        if (CollectionUtils.hasValue(bulkImportResponseDTO.getInvalidValues())) {
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.INVALID_VALUES_KEY);
            writePagedList(jsonGenerator, bulkImportResponseDTO.getInvalidValues(), writeNullValues, executionContext);
        }
        // DSPDMUNITS VALUES
        if (CollectionUtils.hasValue(bulkImportResponseDTO.getDspdmUnitsValues())) {
            jsonGenerator.writeFieldName(DSPDMConstants.Units.UNITS);
            writeList(jsonGenerator, bulkImportResponseDTO.getDspdmUnitsValues(), writeNullValues, executionContext);
        }
        jsonGenerator.writeEndObject();
    }

    private void writePagedList(JsonGenerator jsonGenerator, PagedList pagedList, boolean writeNullValues, ExecutionContext executionContext) throws IOException {
        jsonGenerator.writeStartObject();
        // TOTAL RECORDS
        jsonGenerator.writeNumberField(DSPDMConstants.DSPDM_RESPONSE.TOTAL_RECORDS_KEY, pagedList.getTotalRecords());
        // LIST
        jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.LIST_KEY);
        writeList(jsonGenerator, pagedList, writeNullValues, executionContext);
        jsonGenerator.writeEndObject();
    }

    private void writeList(JsonGenerator jsonGenerator, List list, boolean writeNullValues, ExecutionContext executionContext) throws IOException {
        // LIST
        jsonGenerator.writeStartArray();
        if (CollectionUtils.hasValue(list)) {
            for (Object object : list) {
                if (object != null) {
                    // object written directly in array without label
                    writeObject(jsonGenerator, object, writeNullValues, executionContext);
                } else if (writeNullValues) {
                    // null written directly in array without label
                    jsonGenerator.writeNull();
                }
            }
        }
        // MESSAGE TEXT
        jsonGenerator.writeEndArray();
    }

    private void writeArray(JsonGenerator jsonGenerator, Object array, boolean writeNullValues, ExecutionContext executionContext) throws IOException {
        // Array
        boolean written = false;
        if ((array != null) && (array.getClass().getComponentType().isPrimitive())) {
            int length = Array.getLength(array);
            if (length > 0) {
                Object object = Array.get(array, 0);
                if (object instanceof Byte) {
                    // String binary = DatatypeConverter.printBase64Binary((byte[]) array);
                    // jsonGenerator.writeString(binary);
                    // no need of explicit base64 conversion. It is being done by json generator
                    jsonGenerator.writeBinary((byte[]) array);
                    written = true;
                } else if (object instanceof Character) {
                    jsonGenerator.writeRaw((char[]) array, 0, length);
                    written = true;
                } else if (object instanceof Integer) {
                    jsonGenerator.writeArray((int[]) array, 0, length);
                    written = true;
                } else if (object instanceof Long) {
                    jsonGenerator.writeArray((long[]) array, 0, length);
                    written = true;
                } else if (object instanceof Double) {
                    jsonGenerator.writeArray((double[]) array, 0, length);
                    written = true;
                }
            }
        }
        if (!written) {
            jsonGenerator.writeStartArray();
            if (array != null) {
                int length = Array.getLength(array);
                Object object = null;
                for (int i = 0; i < length; i++) {
                    object = Array.get(array, i);
                    if (object != null) {
                        // object written directly in array without label
                        writeObject(jsonGenerator, object, writeNullValues, executionContext);
                    } else if (writeNullValues) {
                        // null written directly in array without label
                        jsonGenerator.writeNull();
                    }
                }
            }
            // MESSAGE TEXT
            jsonGenerator.writeEndArray();
        }
    }

    private void writeDynamicDTO(JsonGenerator jsonGenerator, DynamicDTO dynamicDTO, boolean writeNullValues, ExecutionContext executionContext) throws IOException {
        // START DynamicDTO
        jsonGenerator.writeStartObject();
        // TYPE
        jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.TYPE_KEY, dynamicDTO.getType());
        if ((dynamicDTO.getId() != null) && (CollectionUtils.hasValue(dynamicDTO.getId().getPK()) && (dynamicDTO.getId().getPK()[0] != null))) {
            // ID
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.ID_KEY);
            writeDynamicPK(jsonGenerator, dynamicDTO.getId(), writeNullValues, executionContext);
        } else if (writeNullValues) {
            // ID
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.ID_KEY);
            jsonGenerator.writeNull();
        }
        if (dynamicDTO.isInserted()) {
            jsonGenerator.writeBooleanField("isInserted", true);
        } else if (dynamicDTO.isUpdated()) {
            jsonGenerator.writeBooleanField("isUpdated", true);
        } else if (dynamicDTO.isDeleted()) {
            jsonGenerator.writeBooleanField("isDeleted", true);
        }
        if (dynamicReadService == null) {
            dynamicReadService = DynamicReadService.getInstance();
        }
        // create a set of column so that all the keys are written
        Set<String> remainingColumns = new LinkedHashSet<>(dynamicDTO.keySet());
        // do not print internal key values
        if (!(dynamicDTO.getType().equalsIgnoreCase(DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR))) {
            remainingColumns.remove(DSPDMConstants.BoAttrName.JAVA_DATA_TYPE);
            remainingColumns.remove(DSPDMConstants.BoAttrName.SQL_DATA_TYPE);
            remainingColumns.remove(DSPDMConstants.BoAttrName.MAX_ALLOWED_LENGTH);
            remainingColumns.remove(DSPDMConstants.BoAttrName.MAX_ALLOWED_DECIMAL_PLACES);
        }
        if (CollectionUtils.hasValue(dynamicDTO.getColumnNamesInOrder())) {
            // write short list in the given order
            for (String key : dynamicDTO.getColumnNamesInOrder()) {
                Object object = dynamicDTO.get(key);
                if (object != null) {
                    // write property name first and then object
                    jsonGenerator.writeFieldName(key);
                    writeObject(jsonGenerator, object, writeNullValues, executionContext);
                } else if (writeNullValues) {
                    // write property name first and then null
                    jsonGenerator.writeFieldName(key);
                    jsonGenerator.writeNull();
                }
                // any key written should be removed from set
                remainingColumns.remove(key);
            }
//            // when SelectList is not null ,not read all metadata columns
//            if (writeNullValues) {
//                // read all metadata columns, there  might be some column which are not in current dto
//                List<String> boAttrNamesInMetadata = dynamicReadService.readMetadataAttrNamesForBOName(dynamicDTO.getType(), executionContext);
//                // remove all existing columns in the dto
//                boAttrNamesInMetadata.removeAll(dynamicDTO.keySet());
//                if (boAttrNamesInMetadata.size() > 0) {
//                    for (String key : boAttrNamesInMetadata) {
//                        // write property name first and then null
//                        jsonGenerator.writeFieldName(key);
//                        jsonGenerator.writeNull();
//                    }
//                }
//            }
        } else {
            // write full list in metadata order
            List<String> boAttrNamesInSequence = dynamicReadService.readMetadataAttrNamesForBOName(dynamicDTO.getType(), executionContext);
            for (String key : boAttrNamesInSequence) {
                // if null values are not to be written then we do not need to check that the dto contains ley or not
                // below check is to avoid the null value printing of the key which are not present in the dto
                Object object = dynamicDTO.get(key);
                if (object != null) {
                    // write property name first and then object
                    jsonGenerator.writeFieldName(key);
                    writeObject(jsonGenerator, object, writeNullValues, executionContext);
                } else if (writeNullValues) {
                    // write property name first and then null
                    jsonGenerator.writeFieldName(key);
                    jsonGenerator.writeNull();
                }
                // any key written should be removed from set
                remainingColumns.remove(key);
            }
        }
        // now print remaining keys
        if (remainingColumns.size() > 0) {
            for (String key : remainingColumns) {
                Object object = dynamicDTO.get(key);
                if (object != null) {
                    // write property name first and then object
                    jsonGenerator.writeFieldName(key);
                    writeObject(jsonGenerator, object, writeNullValues, executionContext);
                } else if (writeNullValues) {
                    // write property name first and then null
                    jsonGenerator.writeFieldName(key);
                    jsonGenerator.writeNull();
                }
            }
        }
        // END DynamicDTO
        jsonGenerator.writeEndObject();
    }

    private void writeDynamicPK(JsonGenerator jsonGenerator, DynamicPK dynamicPK, boolean writeNullValues, ExecutionContext executionContext) throws IOException {
        if (CollectionUtils.hasValue(dynamicPK.getPK())) {
            Object[] pkValues = dynamicPK.getPK();
            if (pkValues.length == 1) {
                // write only the first value
                jsonGenerator.writeObject(pkValues[0]);
            } else {
                // write in an array
                jsonGenerator.writeStartArray();
                for (Object pkValue : pkValues) {
                    if (pkValue != null) {
                        jsonGenerator.writeObject(pkValue);
                    } else if (writeNullValues) {
                        jsonGenerator.writeNull();
                    }
                }
                jsonGenerator.writeEndArray();
            }
        } else {
            jsonGenerator.writeNull();
        }
    }

    private void writeDSPDMUnit(JsonGenerator jsonGenerator, DSPDMUnit dspdmUnit, boolean writeNullValues, ExecutionContext executionContext) throws IOException {
        jsonGenerator.writeStartObject();
        // bo name
        if (dspdmUnit.getBoName() != null) {
            jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.BO_NAME_KEY, dspdmUnit.getBoName());
        } else if (writeNullValues) {
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.BO_NAME_KEY);
            jsonGenerator.writeNull();
        }
        // bo att name
        if (dspdmUnit.getBoAttrName() != null) {
            jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.BO_ATTR_NAME_KEY, dspdmUnit.getBoAttrName());
        } else if (writeNullValues) {
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.BO_ATTR_NAME_KEY);
            jsonGenerator.writeNull();
        }
        // bo attr id
        if (dspdmUnit.getBoAttrId() != null) {
            jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.BO_ATTR_ID_KEY, dspdmUnit.getBoAttrId());
        } else if (writeNullValues) {
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.BO_ATTR_ID_KEY);
            jsonGenerator.writeNull();
        }
        // bo att alias name
        if (dspdmUnit.getBoAttrAliasName() != null) {
            jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.BO_ATTR_ALIAS_NAME_KEY, dspdmUnit.getBoAttrAliasName());
        } else if (writeNullValues) {
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.BO_ATTR_ALIAS_NAME_KEY);
            jsonGenerator.writeNull();
        }
        // source unit
        if (dspdmUnit.getSourceUnit() != null) {
            jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.SOURCE_UNIT_KEY, dspdmUnit.getSourceUnit());
        } else if (writeNullValues) {
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.SOURCE_UNIT_KEY);
            jsonGenerator.writeNull();
        }
        // source unit
        if (dspdmUnit.getTargetUnit() != null) {
            jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.TARGET_UNIT_KEY, dspdmUnit.getTargetUnit());
        } else if (writeNullValues) {
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.TARGET_UNIT_KEY);
            jsonGenerator.writeNull();
        }
        // can convert
        if (CollectionUtils.hasValue(dspdmUnit.getCanConversionUnits())) {
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.CAN_CONVERT_UNITS_KEY);
            writeList(jsonGenerator, dspdmUnit.getCanConversionUnits(), writeNullValues, executionContext);
        } else if (writeNullValues) {
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.CAN_CONVERT_UNITS_KEY);
            jsonGenerator.writeNull();
        }
        jsonGenerator.writeEndObject();
    }

    private void writeUnit(JsonGenerator jsonGenerator, Unit unit, boolean writeNullValues, ExecutionContext executionContext) throws IOException {
        jsonGenerator.writeStartObject();
        // unit id
        if (unit.getUnit_id() != null) {
            jsonGenerator.writeNumberField(DSPDMConstants.DSPDM_RESPONSE.UNIT_ID_KEY, unit.getUnit_id());
        } else if (writeNullValues) {
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.UNIT_ID_KEY);
            jsonGenerator.writeNull();
        }
        // unit name
        if (unit.getUnit_name() != null) {
            jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.UNIT_NAME_KEY, unit.getUnit_name());
        } else if (writeNullValues) {
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.UNIT_NAME_KEY);
            jsonGenerator.writeNull();
        }
        // unit label
        if (unit.getUnit_label() != null) {
            jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.UNIT_LABEL_KEY, unit.getUnit_label());
        } else if (writeNullValues) {
            jsonGenerator.writeFieldName(DSPDMConstants.DSPDM_RESPONSE.UNIT_LABEL_KEY);
            jsonGenerator.writeNull();
        }
        // base type id
        jsonGenerator.writeNumberField(DSPDMConstants.DSPDM_RESPONSE.BASE_UNIT_TYPE_KEY, unit.getBase_type_id());

        jsonGenerator.writeEndObject();
    }

    private void writeSimpleTypes(JsonGenerator jsonGenerator, Object object, boolean writeNullValues, ExecutionContext executionContext) throws IOException {
        if (object instanceof Boolean) {
            jsonGenerator.writeBoolean((Boolean) object);
        } else if (object instanceof Character) {
            jsonGenerator.writeString(String.valueOf((Character) object));
        } else if (object instanceof String) {
            jsonGenerator.writeString((String) object);
        } else if (object instanceof java.util.Date) {
            if (object instanceof java.sql.Timestamp) {
                java.sql.Timestamp timestamp = (java.sql.Timestamp) object;
                // convert timezone to client timezone
                if (ConfigProperties.getInstance().use_client_timezone_to_display.getBooleanValue()) {
                    timestamp = DateTimeUtils.convertTimezoneFromUTCToClient(timestamp, executionContext);
                }
                jsonGenerator.writeString(DateTimeUtils.getStringTimestamp(timestamp, executionContext.getExecutorLocale(), executionContext.getExecutorTimeZone()));
            } else if (object instanceof java.sql.Date) {
                jsonGenerator.writeString(DateTimeUtils.getStringDateOnly((java.sql.Date) object, executionContext.getExecutorLocale()));
            } else if (object instanceof java.sql.Time) {
                jsonGenerator.writeString(DateTimeUtils.getStringTimeOnly((java.sql.Time) object, executionContext.getExecutorLocale(), executionContext.getExecutorTimeZone()));
            } else {
                jsonGenerator.writeString(DateTimeUtils.getString((java.util.Date) object, executionContext.getExecutorLocale(), executionContext.getExecutorTimeZone()));
            }
        } else if (object instanceof Number) {

            if (object instanceof Byte) {
                jsonGenerator.writeNumber((byte) object);
            } else if (object instanceof Short) {
                jsonGenerator.writeNumber((Short) object);
            } else if (object instanceof Integer) {
                jsonGenerator.writeNumber((Integer) object);
            } else if (object instanceof Long) {
                jsonGenerator.writeNumber((Long) object);
            } else if (object instanceof BigInteger) {
                jsonGenerator.writeNumber((BigInteger) object);
            } else if (object instanceof Float) {
                jsonGenerator.writeNumber((Float) object);
            } else if (object instanceof Double) {
                jsonGenerator.writeNumber((Double) object);
            } else if (object instanceof BigDecimal) {
                jsonGenerator.writeNumber((BigDecimal) object);
            }
        } else if (object instanceof Enum) {
            jsonGenerator.writeString(((Enum) object).name());
        } else if (object instanceof Locale) {
            jsonGenerator.writeString(((Locale) object).getDisplayName());
        } else if (object instanceof TimeZone) {
            jsonGenerator.writeString(((TimeZone) object).getID());
        } else if (object instanceof DynamicPK) {
            writeDynamicPK(jsonGenerator, (DynamicPK) object, writeNullValues, executionContext);
        } else if (object instanceof InputStream) {
            InputStream inputStream = (InputStream) object;
            jsonGenerator.writeBinary(inputStream, inputStream.available());
        } else if (object instanceof DSPDMUnit) {
            DSPDMUnit dspdmUnit = (DSPDMUnit) object;
            writeDSPDMUnit(jsonGenerator, dspdmUnit, writeNullValues, executionContext);
        } else if (object instanceof Unit) {
            Unit unit = (Unit) object;
            writeUnit(jsonGenerator, unit, writeNullValues, executionContext);
        } else if (object instanceof java.lang.Class) {
            Class c = (Class) object;
            jsonGenerator.writeString(c.getName());
        } else {
            logger.info("********** Unknown object type {}", object.getClass().getName());
            jsonGenerator.writeObject(object);
        }
    }

    private void writeException(JsonGenerator jsonGenerator, Throwable exception, ExecutionContext executionContext) throws IOException {
        jsonGenerator.writeStartObject();
        if (exception != null) {
            jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.MESSAGE_KEY, exception.getMessage());
            // check if exception stack trace is enabled to be printed in response
            if (ConfigProperties.getInstance().print_exception_stack_trace_in_response.getBooleanValue()) {
                StringWriter stringWriter = new StringWriter(1000);
                PrintWriter printWriter = new PrintWriter(stringWriter);
                exception.printStackTrace(printWriter);
                if (exception instanceof DSPDMBatchException) {
                    DSPDMBatchException batchException = (DSPDMBatchException) exception;
                    if (CollectionUtils.hasValue(batchException.getNestedExceptions())) {
                        for (DSPDMException dspdmException : batchException.getNestedExceptions()) {
                            dspdmException.printStackTrace(printWriter);
                        }
                    }
                }
                jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.STACK_TRACE_KEY, stringWriter.toString());
            }
        }
        jsonGenerator.writeEndObject();
    }

    private void writeExecutionContext(JsonGenerator jsonGenerator, boolean writeNullValues, ExecutionContext executionContext) throws IOException {
        jsonGenerator.writeStartObject();
        // START
        jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.EXECUTOR_NAME_KEY, executionContext.getExecutorName());
        jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.EXECUTOR_LOCALE_KEY, executionContext.getExecutorLocale().getLanguage());
        jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.EXECUTOR_TIMEZONE_KEY, executionContext.getExecutorTimeZone().getId());
        jsonGenerator.writeBooleanField(DSPDMConstants.DSPDM_RESPONSE.TRANSACTION_STARTED_KEY, executionContext.isTransactionStarted());
        jsonGenerator.writeBooleanField(DSPDMConstants.DSPDM_RESPONSE.TRANSACTION_COMMITTED_KEY, executionContext.isTransactionCommitted());
        jsonGenerator.writeBooleanField(DSPDMConstants.DSPDM_RESPONSE.TRANSACTION_ROLL_BACKED_KEY, executionContext.isTransactionRollbacked());
        jsonGenerator.writeBooleanField(DSPDMConstants.DSPDM_RESPONSE.UNIT_TEST_CALL_KEY, executionContext.isUnitTestCall());
        jsonGenerator.writeNumberField(DSPDMConstants.DSPDM_RESPONSE.PROCESSING_START_TIME_KEY, executionContext.getProcessingStartTime());
        jsonGenerator.writeNumberField(DSPDMConstants.DSPDM_RESPONSE.TOTAL_TIME_TAKEN_BY_DB_KEY, executionContext.getTotalTimeTakenByDB());
        jsonGenerator.writeBooleanField(DSPDMConstants.DSPDM_RESPONSE.READ_BACK_KEY, executionContext.isReadBack());
        jsonGenerator.writeBooleanField(DSPDMConstants.DSPDM_RESPONSE.SHOW_SQL_STATS_KEY, executionContext.isCollectSQLStats());
        // END
        jsonGenerator.writeEndObject();
    }

    private void writeSQLStats(JsonGenerator jsonGenerator, boolean writeNullValues, ExecutionContext executionContext) throws IOException {
        jsonGenerator.writeStartArray();
        // START
        Map<String, Integer[]> sqlStats = executionContext.getSqlStats();
        for (Map.Entry<String, Integer[]> entry : sqlStats.entrySet()) {
            jsonGenerator.writeStartObject();
            // sql
            jsonGenerator.writeStringField(DSPDMConstants.DSPDM_RESPONSE.SQL_KEY, entry.getKey());
            // execution count
            jsonGenerator.writeNumberField(DSPDMConstants.DSPDM_RESPONSE.EXECUTION_COUNT_KEY, entry.getValue()[0]);
            // total time taken
            jsonGenerator.writeNumberField(DSPDMConstants.DSPDM_RESPONSE.TOTAL_TIME_IN_MILLIS_KEY, entry.getValue()[1]);
            jsonGenerator.writeEndObject();
        }
        // END
        jsonGenerator.writeEndArray();
    }

    private void writeSQLScript(JsonGenerator jsonGenerator, boolean writeNullValues, ExecutionContext executionContext) throws IOException {
        // START
        List<String> sqlScripts = executionContext.getSqlScripts();
        if (CollectionUtils.hasValue(sqlScripts)) {
            StringBuilder finalScript = new StringBuilder(sqlScripts.size() * 200);
            for (String sql : sqlScripts) {
                finalScript.append(sql).append("; ");
            }
            jsonGenerator.writeString(finalScript.toString());
        } else {
            jsonGenerator.writeString("");
        }
        // END
    }
}