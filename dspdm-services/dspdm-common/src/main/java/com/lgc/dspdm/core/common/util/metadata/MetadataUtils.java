package com.lgc.dspdm.core.common.util.metadata;

import com.lgc.dspdm.core.common.config.ConnectionProperties;
import com.lgc.dspdm.core.common.data.annotation.AnnotationProcessor;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.dto.IBaseDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.*;

import javax.xml.bind.DatatypeConverter;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZonedDateTime;
import java.util.Base64;

public class MetadataUtils {

    private  static DSPDMLogger logger = new DSPDMLogger(MetadataUtils.class);
    private static String boAttributeNameForFieldBOName = null;
    private static String boAttributeNameForFieldIsActive = null;
    private static String boAttributeNameForFieldIsHidden = null;
    private static String boAttributeNameForFieldSequenceNumber = null;
    private static String boAttributeNameForFieldReferenceTable = null;
    private static String boAttributeNameForFieldReferenceBOAttrValue = null;
    private static String boAttributeNameForFieldReferenceBOAttrLabel = null;

    private static boolean isMSSQLServerDialect(String boName) {
        boolean flag = false;
        if (CollectionUtils.containsIgnoreCase(DSPDMConstants.SERVICE_DB_BO_NAMES, boName)) {
            flag = ConnectionProperties.isMSSQLServerMetadataDialect();
        } else {
            flag = ConnectionProperties.isMSSQLServerDialect();
        }
        return flag;
    }

    public static Class getJavaDataTypeFromString(String dataType, String attributeName, String boName, ExecutionContext executionContext) {
        Class c = DSPDMConstants.DataTypes.getJavaDataTypeFromString(dataType, isMSSQLServerDialect(boName));
        if (c == null) {
            throw new DSPDMException("Invalid datatype '{}' for attribute '{}' and BO '{}'", executionContext.getExecutorLocale(), dataType, attributeName, boName);
        }
        return c;
    }

    public static Object convertValueToJavaDataTypeFromString(String value, Class javaDataType, String boName, String boAttrName, ExecutionContext executionContext) {
        Object newValue = value;
        ZonedDateTime zonedDateTime = null;

        try {
            value = value.trim();
            value = StringUtils.removeFromStartAndEnd(value, "(", ")");
            if (javaDataType == Boolean.class) {
                if ((value.equalsIgnoreCase("true"))
                        || (value.equalsIgnoreCase("false"))) {
                    newValue = Boolean.valueOf(value);
                } else if ((value.equals("1")) || (value.equalsIgnoreCase("Y"))) {
                    newValue = Boolean.TRUE;
                } else if ((value.equals("0")) || (value.equalsIgnoreCase("N"))) {
                    newValue = Boolean.FALSE;
                } else {
                    throw new java.lang.IllegalArgumentException("Value '" + value + "' is illegal to be considered as true or false predicate");
                }
            } else if (javaDataType == String.class) {
                newValue = value;
            } else if (javaDataType == Character.class) {
                newValue = value.trim().charAt(0);
            } else if (javaDataType == java.time.ZonedDateTime.class) {
                zonedDateTime = DateTimeUtils.parse(value, executionContext);
                newValue = zonedDateTime;
            } else if (javaDataType == java.time.OffsetDateTime.class) {
                zonedDateTime = DateTimeUtils.parse(value, executionContext);
                newValue = zonedDateTime.toOffsetDateTime();
            } else if (javaDataType == java.sql.Timestamp.class) {
                zonedDateTime = DateTimeUtils.parse(value, executionContext);
                newValue = DateTimeUtils.getTimestamp(zonedDateTime);
            } else if (javaDataType == java.sql.Date.class) {
                zonedDateTime = DateTimeUtils.parse(value, executionContext);
                newValue = DateTimeUtils.getSQLDate(zonedDateTime);
            } else if (javaDataType == java.sql.Time.class) {
                zonedDateTime = DateTimeUtils.parse(value, executionContext);
                newValue = DateTimeUtils.getTime(zonedDateTime);
            } else if ((javaDataType == BigDecimal.class)
                    || (javaDataType == BigInteger.class)
                    || (javaDataType == Double.class)
                    || (javaDataType == Float.class)
                    || (javaDataType == Long.class)
                    || (javaDataType == Integer.class)
                    || (javaDataType == Short.class)) {
                // convert to type if not already
                newValue = NumberUtils.convertTo(value, javaDataType, executionContext);
            } else if (javaDataType == Byte.class) {
                if (StringUtils.hasValue(value)) {
                    if (value.length() == 1) {
                        newValue = Byte.valueOf(value);
                    } else {
                        // need of explicit base64 reverse conversion.
                        byte[] binary = null;
                        try {
                            binary = DatatypeConverter.parseBase64Binary(value);
                        } catch (Exception ex) {
                            try {
                                binary = Base64.getDecoder().decode(value);
                            } catch (Exception exc) {
                                binary = value.getBytes(DSPDMConstants.UTF_8);
                            }
                        }
                        newValue = binary;
                    }
                }
            } else {
                throw new DSPDMException("Invalid java data type '{}' for string value '{}' for bo '{}' and bo attribute '{}'", executionContext.getExecutorLocale(), javaDataType.getName(), value, boName, boAttrName);
            }
        } catch (DSPDMException e) {
            throw e;
        } catch (Exception e) {
            throw new DSPDMException("Unable to convert string value '{}' to the java data type '{}' for bo '{}' and bo attribute '{}'. Actual error is '{}'", e, executionContext.getExecutorLocale(), value, javaDataType.getName(), boName, boAttrName, e.getMessage());
        }
        return newValue;
    }

    public static String convertValueToStringFromJavaDataType(Object value, ExecutionContext executionContext) {
        String newValue = null;
        if (value != null) {
            try {
                if (value instanceof String) {
                    newValue = (String) value;
                } else if (value instanceof Boolean) {
                    newValue = String.valueOf((Boolean) value);
                } else if (value instanceof Character) {
                    newValue = String.valueOf((Character) value);
                } else if (value instanceof java.sql.Timestamp) {
                    Timestamp timestamp = (Timestamp) value;
                    newValue = DateTimeUtils.getStringTimestamp(timestamp, executionContext.getExecutorLocale(), DateTimeUtils.ZONE_UTC);
                } else if (value instanceof java.sql.Date) {
                    java.sql.Date sqlDate = (java.sql.Date) value;
                    newValue = DateTimeUtils.getStringDateOnly(sqlDate, executionContext.getExecutorLocale());
                } else if (value instanceof java.util.Date) {
                    java.util.Date utilDate = (java.util.Date) value;
                    newValue = DateTimeUtils.getStringDateTime(utilDate, executionContext.getExecutorLocale(), DateTimeUtils.ZONE_UTC);
                } else if (value instanceof java.sql.Time) {
                    java.sql.Time sqlTime = (java.sql.Time) value;
                    newValue = DateTimeUtils.getStringTimeOnly(DateTimeUtils.DISPLAY_PATTERNS.TIME_ONLY_FORMAT_WITH_MILLIS.getPattern(), sqlTime, executionContext.getExecutorLocale(), DateTimeUtils.ZONE_UTC);
                } else if (value instanceof Byte) {
                    if (value.getClass().isArray()) {
                        // need of explicit base64 reverse conversion.
                        byte[] binary = (byte[]) value;
                        try {
                            newValue = DatatypeConverter.printBase64Binary(binary);
                        } catch (Exception ex) {
                            try {
                                newValue = Base64.getEncoder().encodeToString(binary);
                            } catch (Exception exc) {
                                newValue = new String(binary, DSPDMConstants.UTF_8);
                            }
                        }
                    } else {
                        newValue = String.valueOf((Byte) value);
                    }
                } else if ((value instanceof Number)) {
                    newValue = String.valueOf(value);
                } else {
                    logger.info("Invalid java data type '{}' for string value '{}'", executionContext.getExecutorLocale(), value.getClass().getName(), value);
                    newValue = value.toString();
                }
            } catch (Exception e) {
                logger.info("Invalid java data type '{}' for string value '{}'", executionContext.getExecutorLocale(), value.getClass().getName(), value);
                newValue = value.toString();
            }
        }
        return newValue;
    }

    public static int getSQLDataTypeFromString(String dataType, String attributeName, String boName, ExecutionContext executionContext) {
        int sqlDataType = DSPDMConstants.DataTypes.getSQLDataTypeFromString(dataType, isMSSQLServerDialect(boName));
        if (sqlDataType == Types.NULL) {
            throw new DSPDMException("Invalid data type '{}' for attribute '{}' and BO '{}'", executionContext.getExecutorLocale(), dataType, attributeName, boName);
        }
        return sqlDataType;
    }

    public static Object ConvertValueToSQLDataTypeFromString(String boAttributeName, String value, int sqlDataType, BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        Object newValue = null;

        ZonedDateTime zonedDateTime = null;

        switch (sqlDataType) {
            case Types.BOOLEAN:
                newValue = java.lang.Boolean.valueOf(value);
                break;
            case Types.VARCHAR:
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
                newValue = value;
                break;
            case Types.CHAR:
                newValue = value.trim().charAt(0);
                break;
            case Types.TIMESTAMP_WITH_TIMEZONE:
                zonedDateTime = DateTimeUtils.parse(value, executionContext);
                // newValue = zonedDateTime.toOffsetDateTime();
                newValue = DateTimeUtils.getTimestamp(zonedDateTime);
                break;
            case Types.TIMESTAMP:
                zonedDateTime = DateTimeUtils.parse(value, executionContext);
                newValue = DateTimeUtils.getTimestamp(zonedDateTime);
                break;
            case Types.DATE:
                zonedDateTime = DateTimeUtils.parse(value, executionContext);
                newValue = DateTimeUtils.getSQLDate(zonedDateTime);
                break;
            case Types.DOUBLE:
                newValue = java.lang.Double.valueOf(value);
                break;
            case Types.INTEGER:
                newValue = java.lang.Integer.valueOf(value);
                break;
            case Types.LONGVARBINARY:
            case Types.BLOB:
                newValue = value;
                break;
            default:
                throw new DSPDMException("Invalid sql datatype '{}' for attribute '{}' and BO '{}'", executionContext.getExecutorLocale(), sqlDataType, boAttributeName, businessObjectInfo.getBusinessObjectType());
        }
        return newValue;
    }

    public static int getDataTypeLengthFromString(String dataType, String attributeName, String boName, ExecutionContext executionContext) {
        int length = 0;
        boolean isMSSQLServerDialect = isMSSQLServerDialect(boName);
        DSPDMConstants.DataTypes dataTypeEnum = DSPDMConstants.DataTypes.fromAttributeDataType(dataType, isMSSQLServerDialect);
        if (dataTypeEnum == null) {
            throw new DSPDMException("Invalid data type '{}' for attribute '{}'", executionContext.getExecutorLocale(), dataType, attributeName);
        } else {
            switch (dataTypeEnum) {
                case CHARACTER:
                case CHARACTER_VARYING:
                case N_CHARACTER:
                case N_CHARACTER_VARYING:
                    // int startIndex = (dataTypeEnum.getAttributeDataType(isMSSQLServerDialect) + "(").length();
                    int startIndex = dataType.indexOf("(");
                    if(startIndex != -1) {
                        startIndex = startIndex + 1;
                    }
                    int endIndex = dataType.indexOf(")");
                    if ((startIndex != -1) && (endIndex != -1)) {
                        try {
                            length = Integer.valueOf(dataType.substring(startIndex, endIndex));
                        } catch (NumberFormatException e) {
                            throw new DSPDMException("Error in reading length '{}' for attribute '{}' and BO '{}' from metadata", executionContext.getExecutorLocale(), dataType.substring(startIndex, endIndex), attributeName, boName);
                        }
                    }
                    break;
                case NUMERIC:
                case DECIMAL:
                    // startIndex = (dataTypeEnum.getAttributeDataType(isMSSQLServerDialect) + "(").length();
                    startIndex = dataType.indexOf("(");
                    if(startIndex != -1) {
                        startIndex = startIndex + 1;
                    }
                    endIndex = dataType.indexOf(",");
                    if (endIndex == -1) {
                        endIndex = dataType.indexOf(")");
                    }
                    if ((startIndex != -1) && (endIndex != -1)) {
                        try {
                            length = Integer.valueOf(dataType.substring(startIndex, endIndex));
                        } catch (NumberFormatException e) {
                            throw new DSPDMException("Error in reading length '{}' for attribute '{}' from metadata", executionContext.getExecutorLocale(), dataType.substring(startIndex, endIndex), attributeName);
                        }
                    }
                    break;
                case DOUBLE_PRECISION:
                    length = Double.valueOf(Double.MAX_VALUE).toString().length();
                    break;
                case FLOAT:
                case REAL:
                    length = Float.valueOf(Float.MAX_VALUE).toString().length();
                    break;
                case BIG_INT:
                    length = Long.valueOf(Long.MAX_VALUE).toString().length();
                    break;
                case INTEGER:
                    length = Integer.valueOf(Integer.MAX_VALUE).toString().length();
                    break;
                case SMALL_INT:
                    length = Integer.valueOf(Short.MAX_VALUE).toString().length();
                    break;
                case TINY_INT:
                    length = Integer.valueOf(Byte.MAX_VALUE).toString().length();
                    break;
                case TEXT:
                    length = 1024 * 1024 * 1024; // 1 GB
                    break;
                case N_TEXT:
                    length = 1024 * 1024 * 1024 * 2; // 2 GB
                    break;
                case JSONB:
                case IMAGE:
                case BYTE:
                    length = Integer.MAX_VALUE;
                    break;
                case BYTEA:
                case BINARY:
                    length = dataTypeEnum.getMaxLength();
                    break;
                case BOOLEAN:
                case TIMESTAMP:
                case DATE:
                case TIMESTAMP_WITHOUT_TIMEZONE:
                case TIMESTAMP_WITH_TIMEZONE:
                case TIME:
                case DATETIME2:
                case SMALL_DATETIME:
                default:
                    length = 0;
                    break;
            }
        }
        return length;
    }

    public static int getDataTypeDecimalLengthFromString(String dataType, String attributeName, String boName, ExecutionContext executionContext) {
        int length = 0;
        DSPDMConstants.DataTypes dataTypeEnum = DSPDMConstants.DataTypes.fromAttributeDataType(dataType, isMSSQLServerDialect(boName));
        if (dataTypeEnum.isFloatingDataType()) {
            int startIndex = dataType.indexOf(",");
            int endIndex = dataType.indexOf(")");
            if ((startIndex != -1) && (endIndex != -1)) {
                try {
                    length = Integer.valueOf(dataType.substring(startIndex + 1, endIndex));
                } catch (NumberFormatException e) {
                    throw new DSPDMException("Error in reading decimal length '{}' for attribute '{}' and BO '{}' from metadata", executionContext.getExecutorLocale(), dataType.substring(startIndex, endIndex), attributeName, boName);
                }
            }
        }
        return length;
    }

    public static <T extends IBaseDTO> String getBONameForDTO(Class<T> c, ExecutionContext executionContext) {
        return AnnotationProcessor.getBOName(c, executionContext);
    }

    public static <T extends IBaseDTO> String getBOAttributeNameForFieldBOName(Class<T> c, String boNameFieldName, ExecutionContext executionContext) {
        if (boAttributeNameForFieldBOName == null) {
            boAttributeNameForFieldBOName = getBOAttributeNameForField(c, boNameFieldName, executionContext);
        }
        return boAttributeNameForFieldBOName;
    }

    public static <T extends IBaseDTO> String getBOAttributeNameForFieldIsActive(Class<T> c, String isActiveFieldName, ExecutionContext executionContext) {
        if (boAttributeNameForFieldIsActive == null) {
            boAttributeNameForFieldIsActive = getBOAttributeNameForField(c, isActiveFieldName, executionContext);
        }
        return boAttributeNameForFieldIsActive;
    }

    public static <T extends IBaseDTO> String getBOAttributeNameForFieldIsHidden(Class<T> c, String isHiddenFieldName, ExecutionContext executionContext) {
        if (boAttributeNameForFieldIsHidden == null) {
            boAttributeNameForFieldIsHidden = getBOAttributeNameForField(c, isHiddenFieldName, executionContext);
        }
        return boAttributeNameForFieldIsHidden;
    }

    public static <T extends IBaseDTO> String getBOAttributeNameForFieldReferenceTable(Class<T> c, String referenceTableFieldName, ExecutionContext executionContext) {
        if (boAttributeNameForFieldReferenceTable == null) {
            boAttributeNameForFieldReferenceTable = getBOAttributeNameForField(c, referenceTableFieldName, executionContext);
        }
        return boAttributeNameForFieldReferenceTable;
    }

    public static <T extends IBaseDTO> String getBoAttributeNameForFieldReferenceBOAttrValue(Class<T> c, String referenceBOAttrValueFieldName, ExecutionContext executionContext) {
        if (boAttributeNameForFieldReferenceBOAttrValue == null) {
            boAttributeNameForFieldReferenceBOAttrValue = getBOAttributeNameForField(c, referenceBOAttrValueFieldName, executionContext);
        }
        return boAttributeNameForFieldReferenceBOAttrValue;
    }

    public static <T extends IBaseDTO> String getBoAttributeNameForFieldReferenceBOAttrLabel(Class<T> c, String referenceBOAttrLabelFieldName, ExecutionContext executionContext) {
        if (boAttributeNameForFieldReferenceBOAttrLabel == null) {
            boAttributeNameForFieldReferenceBOAttrLabel = getBOAttributeNameForField(c, referenceBOAttrLabelFieldName, executionContext);
        }
        return boAttributeNameForFieldReferenceBOAttrLabel;
    }

    public static <T extends IBaseDTO> String getBoAttributeNameForFieldSequenceNumber(Class<T> c, String sequenceNumberFieldName, ExecutionContext executionContext) {
        if (boAttributeNameForFieldSequenceNumber == null) {
            boAttributeNameForFieldSequenceNumber = getBOAttributeNameForField(c, sequenceNumberFieldName, executionContext);
        }
        return boAttributeNameForFieldSequenceNumber;
    }

    public static <T extends IBaseDTO> String getBOAttributeNameForField(Class<T> c, String fieldName, ExecutionContext executionContext) {
        String boAttributeNameForGivenFieldName = null;
        Field field = null;
        try {
            field = c.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            throw new DSPDMException("Field '{}' not found in class '{}'", executionContext.getExecutorLocale(), fieldName, c.getSimpleName());
        }
        boAttributeNameForGivenFieldName = AnnotationProcessor.getBOAttributeName(field);
        if (StringUtils.isNullOrEmpty(boAttributeNameForGivenFieldName)) {
            throw new DSPDMException("Annotation boAttrName is not found on field '{}' of class '{}'", executionContext.getExecutorLocale(), fieldName, c.getSimpleName());
        }
        return boAttributeNameForGivenFieldName;
    }

    public static boolean isDataTypeSupportsLength(String dataType, String boName) {
        return isNumberDataType(dataType, boName) || isStringDataType(dataType, boName) || isBinaryDataType(dataType, boName);
    }

    public static boolean isNumberDataType(String dataType, String boName) {
        boolean isMSSQLServerDialect = isMSSQLServerDialect(boName);
        return (dataType.startsWith(DSPDMConstants.DataTypes.BYTE.getAttributeDataType(isMSSQLServerDialect)))
                || (dataType.startsWith(DSPDMConstants.DataTypes.TINY_INT.getAttributeDataType(isMSSQLServerDialect)))
                || (dataType.startsWith(DSPDMConstants.DataTypes.SMALL_INT.getAttributeDataType(isMSSQLServerDialect)))
                || (dataType.startsWith(DSPDMConstants.DataTypes.INTEGER.getAttributeDataType(isMSSQLServerDialect)))
                || (dataType.startsWith(DSPDMConstants.DataTypes.BIG_INT.getAttributeDataType(isMSSQLServerDialect)))
                || (dataType.startsWith(DSPDMConstants.DataTypes.DECIMAL.getAttributeDataType(isMSSQLServerDialect)))
                || (dataType.startsWith(DSPDMConstants.DataTypes.NUMERIC.getAttributeDataType(isMSSQLServerDialect)))
                || (dataType.startsWith(DSPDMConstants.DataTypes.REAL.getAttributeDataType(isMSSQLServerDialect)))
                || (dataType.startsWith(DSPDMConstants.DataTypes.FLOAT.getAttributeDataType(isMSSQLServerDialect)))
                || (dataType.startsWith(DSPDMConstants.DataTypes.DOUBLE_PRECISION.getAttributeDataType(isMSSQLServerDialect)));
    }

    public static boolean isStringDataType(String dataType, String boName) {
        boolean isMSSQLServerDialect = isMSSQLServerDialect(boName);
        return (dataType.startsWith(DSPDMConstants.DataTypes.CHARACTER_VARYING.getAttributeDataType(isMSSQLServerDialect)))
                || (dataType.startsWith(DSPDMConstants.DataTypes.CHARACTER.getAttributeDataType(isMSSQLServerDialect)))
                || (dataType.startsWith(DSPDMConstants.DataTypes.TEXT.getAttributeDataType(isMSSQLServerDialect)))
                || (dataType.startsWith(DSPDMConstants.DataTypes.N_CHARACTER_VARYING.getAttributeDataType(isMSSQLServerDialect)))
                || (dataType.startsWith(DSPDMConstants.DataTypes.N_CHARACTER.getAttributeDataType(isMSSQLServerDialect)))
                || (dataType.startsWith(DSPDMConstants.DataTypes.N_TEXT.getAttributeDataType(isMSSQLServerDialect)));
    }

    public static boolean isBinaryDataType(String dataType, String boName) {
        boolean isMSSQLServerDialect = isMSSQLServerDialect(boName);
        return ((dataType.startsWith(DSPDMConstants.DataTypes.BINARY.getAttributeDataType(isMSSQLServerDialect))))
                || (dataType.startsWith(DSPDMConstants.DataTypes.BYTEA.getAttributeDataType(isMSSQLServerDialect)));
    }

    public static boolean isBooleanDataType(String dataType, String boName) {
        boolean isMSSQLServerDialect = isMSSQLServerDialect(boName);
        return (dataType.startsWith(DSPDMConstants.DataTypes.BOOLEAN.getAttributeDataType(isMSSQLServerDialect)));
    }

    public static boolean isDateTimeDataType(String dataType, String boName) {
        boolean isMSSQLServerDialect = isMSSQLServerDialect(boName);
        return (dataType.startsWith(DSPDMConstants.DataTypes.TIMESTAMP_WITHOUT_TIMEZONE.getAttributeDataType(isMSSQLServerDialect)))
                || (dataType.startsWith(DSPDMConstants.DataTypes.TIMESTAMP_WITH_TIMEZONE.getAttributeDataType(isMSSQLServerDialect)))
                || (dataType.startsWith(DSPDMConstants.DataTypes.TIMESTAMP.getAttributeDataType(isMSSQLServerDialect)))
                || (dataType.startsWith(DSPDMConstants.DataTypes.DATE.getAttributeDataType(isMSSQLServerDialect)))
                || (dataType.startsWith(DSPDMConstants.DataTypes.TIME.getAttributeDataType(isMSSQLServerDialect)));
    }
}
