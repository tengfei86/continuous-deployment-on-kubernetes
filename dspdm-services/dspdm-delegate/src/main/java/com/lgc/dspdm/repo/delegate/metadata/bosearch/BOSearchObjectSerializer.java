package com.lgc.dspdm.repo.delegate.metadata.bosearch;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.data.criteria.SQLExpression;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicPK;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.DateTimeUtils;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * Special json serializer for BO Search Only. Cannot be used for other purposes.
 * s
 *
 * @author rao.alikhan
 * @since 03-Apr-2020$s
 */
public class BOSearchObjectSerializer extends JsonSerializer<DynamicDTO> {
    private static DSPDMLogger logger = new DSPDMLogger(BOSearchObjectSerializer.class);

    @Override
    public void serialize(DynamicDTO dynamicDTO, JsonGenerator jsonGenerator, SerializerProvider serializers) throws IOException {
        // do not write null values by default
        boolean writeNullValues = false;
        writeMap(jsonGenerator, dynamicDTO, writeNullValues, dynamicDTO.getExecutionContext());
    }

    private void writeList(JsonGenerator jsonGenerator, Collection<Object> values, boolean writeNullValues, ExecutionContext executionContext) throws IOException {
        // LIST
        jsonGenerator.writeStartArray();
        if (CollectionUtils.hasValue(values)) {
            for (Object object : values) {
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
                    // jsonGenerator.writeBinary((byte[]) array);
                    // do not serialize the binary inside json for search
                    jsonGenerator.writeStartArray();
                    jsonGenerator.writeEndArray();
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
            jsonGenerator.writeEndArray();
        }
    }

    private void writeObject(JsonGenerator jsonGenerator, Object object, boolean writeNullValues, ExecutionContext executionContext) throws IOException {
        if (object instanceof DynamicDTO) {
            // RECURSIVE CALL TO SAME FUNCTION TO WRITE CHILD
            // writeDynamicDTO(jsonGenerator, (DynamicDTO) object, writeNullValues, executionContext);
        } else if (object instanceof List) {
            writeList(jsonGenerator, (List) object, writeNullValues, executionContext);
        } else if (object.getClass().isArray()) {
            writeArray(jsonGenerator, object, writeNullValues, executionContext);
        } else if (object instanceof Map) {
            writeMap(jsonGenerator, (Map<String, Object>) object, writeNullValues, executionContext);
        } else if (object instanceof SQLExpression) {
            writeSimpleTypes(jsonGenerator, DSPDMConstants.EMPTY_STRING, writeNullValues, executionContext);
        } else {
            writeSimpleTypes(jsonGenerator, object, writeNullValues, executionContext);
        }
    }

    private void writeDynamicPK(JsonGenerator jsonGenerator, DynamicPK dynamicPK, boolean writeNullValues, ExecutionContext executionContext) throws IOException {
        if (CollectionUtils.hasValue(dynamicPK.getPK())) {
            Object[] pkValues = dynamicPK.getPK();
            if (pkValues.length == 1) {
                // write only the first value
                if (pkValues[0] != null) {
                    jsonGenerator.writeString(String.valueOf(pkValues[0]));
                } else if (writeNullValues) {
                    jsonGenerator.writeNull();
                }
            } else {
                // write in an array
                jsonGenerator.writeStartArray();
                for (Object pkValue : pkValues) {
                    if (pkValue != null) {
                        jsonGenerator.writeString(String.valueOf(pkValue));
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
                jsonGenerator.writeNumber((Byte) object);
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
            // do not serialize the binary inside json for search
            jsonGenerator.writeStartArray();
            jsonGenerator.writeEndArray();
        } else {
            logger.info("********** Unknown object type {}", object.getClass().getName());
            jsonGenerator.writeObject(object);
        }
    }
}
