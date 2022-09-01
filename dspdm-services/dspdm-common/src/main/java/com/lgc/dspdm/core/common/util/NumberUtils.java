package com.lgc.dspdm.core.common.util;

import com.lgc.dspdm.core.common.exception.DSPDMException;

import java.math.BigDecimal;
import java.math.BigInteger;

public class NumberUtils {

    public static Object convertTo(Object value, Class javaDataType, ExecutionContext executionContext) {
        Object number = null;
        if (value != null) {
            if (javaDataType == String.class) {
                if (value instanceof String) {
                    number = value;
                } else {
                    number = value.toString();
                }
            } else if (javaDataType == BigDecimal.class) {
                number = NumberUtils.convertToBigDecimal(value, executionContext);
            } else if (javaDataType == BigInteger.class) {
                number = NumberUtils.convertToBigInteger(value, executionContext);
            } else if (javaDataType == Double.class) {
                number = NumberUtils.convertToDouble(value, executionContext);
            } else if (javaDataType == Float.class) {
                number = NumberUtils.convertToFloat(value, executionContext);
            } else if (javaDataType == Long.class) {
                number = NumberUtils.convertToLong(value, executionContext);
            } else if (javaDataType == Integer.class) {
                number = NumberUtils.convertToInteger(value, executionContext);
            } else if (javaDataType == Short.class) {
                number = NumberUtils.convertToShort(value, executionContext);
            } else if (javaDataType == Byte.class) {
                number = NumberUtils.convertToShort(value, executionContext);
            }
        }
        return number;
    }

    public static int getIntegerLength(Object value, ExecutionContext executionContext) {
        int actualLength = 0;
        if (value != null) {
            if (value instanceof BigDecimal) {
                actualLength = Long.toString(((BigDecimal) value).longValue()).length();
            } else if (value instanceof BigInteger) {
                actualLength = Long.toString(((BigInteger) value).longValue()).length();
            } else if (value instanceof Double) {
                actualLength = Long.toString(((Double) value).longValue()).length();
            } else if (value instanceof Float) {
                actualLength = Long.toString(((Float) value).longValue()).length();
            } else if (value instanceof Long) {
                actualLength = ((Long) value).toString().length();
            } else if (value instanceof Integer) {
                actualLength = ((Integer) value).toString().length();
            } else if (value instanceof Short) {
                actualLength = ((Short) value).toString().length();
            } else if (value instanceof byte[]) {
                actualLength = ((byte[]) value).length;
            }
        }
        return actualLength;
    }

    public static int getDecimalLength(Object value, ExecutionContext executionContext) {
		int decimalLength = 0;
		if (value != null) {
			String strValue = new BigDecimal(String.valueOf(value)).stripTrailingZeros().toPlainString();
			int integerPlaces = strValue.indexOf('.');
			decimalLength = integerPlaces < 0 ? 0 : (strValue.length() - integerPlaces - 1);
		}
		return decimalLength;
    }
    
    public static BigDecimal convertToBigDecimal(Object value, ExecutionContext executionContext) {
        BigDecimal bigDecimalValue = null;
        if (value != null) {
            if (value instanceof String) {
                bigDecimalValue = BigDecimal.valueOf(Double.valueOf((String) value));
            } else if (value instanceof BigDecimal) {
                bigDecimalValue = (BigDecimal) value;
            } else if (value instanceof java.lang.Double) {
                bigDecimalValue = new BigDecimal(value.toString());
            } else if (value instanceof java.math.BigInteger) {
                bigDecimalValue = BigDecimal.valueOf(((java.math.BigInteger) value).longValue());
            } else if (value instanceof java.lang.Integer) {
                bigDecimalValue = BigDecimal.valueOf(((java.lang.Integer) value).longValue());
            } else if (value instanceof java.lang.Long) {
                bigDecimalValue = BigDecimal.valueOf(((java.lang.Long) value).longValue());
            } else if (value instanceof java.lang.Float) {
                bigDecimalValue = new BigDecimal(value.toString());
            } else if (value instanceof java.lang.Short) {
                bigDecimalValue = BigDecimal.valueOf(((java.lang.Short) value).longValue());
            } else {
                throw new DSPDMException("Value '{}' with type '{}' cannot be converted to double", executionContext.getExecutorLocale(), value, value.getClass().getName());
            }
        }
        return bigDecimalValue;
    }

    public static BigInteger convertToBigInteger(Object value, ExecutionContext executionContext) {
        BigInteger bigIntValue = null;
        if (value != null) {
            if (value instanceof String) {
                bigIntValue = BigInteger.valueOf(Long.valueOf((String) value));
            } else if (value instanceof BigInteger) {
                bigIntValue = (BigInteger) value;
            } else if (value instanceof java.lang.Double) {
                bigIntValue = BigInteger.valueOf(((java.lang.Double) value).longValue());
            } else if (value instanceof java.math.BigDecimal) {
                bigIntValue = BigInteger.valueOf(((java.math.BigDecimal) value).longValue());
            } else if (value instanceof java.lang.Integer) {
                bigIntValue = BigInteger.valueOf(((java.lang.Integer) value).longValue());
            } else if (value instanceof java.lang.Long) {
                bigIntValue = BigInteger.valueOf(((java.lang.Long) value).longValue());
            } else if (value instanceof java.lang.Float) {
                bigIntValue = BigInteger.valueOf(((java.lang.Float) value).longValue());
            } else if (value instanceof java.lang.Short) {
                bigIntValue = BigInteger.valueOf(((java.lang.Short) value).longValue());
            } else {
                throw new DSPDMException("Value '{}' with type '{}' cannot be converted to double", executionContext.getExecutorLocale(), value, value.getClass().getName());
            }
        }
        return bigIntValue;
    }

    public static Double convertToDouble(Object value, ExecutionContext executionContext) {
        Double doubleValue = null;
        if (value != null) {
            if (value instanceof String) {
                doubleValue = Double.valueOf((String) value);
            } else if (value instanceof java.lang.Double) {
                doubleValue = (Double) value;
            } else if (value instanceof java.math.BigDecimal) {
                doubleValue = ((java.math.BigDecimal) value).doubleValue();
            } else if (value instanceof java.math.BigInteger) {
                doubleValue = ((java.math.BigInteger) value).doubleValue();
            } else if (value instanceof java.lang.Integer) {
                doubleValue = ((java.lang.Integer) value).doubleValue();
            } else if (value instanceof java.lang.Long) {
                doubleValue = ((java.lang.Long) value).doubleValue();
            } else if (value instanceof java.lang.Float) {
                doubleValue = Double.valueOf(value.toString());
            } else if (value instanceof java.lang.Short) {
                doubleValue = ((java.lang.Short) value).doubleValue();
            } else {
                throw new DSPDMException("Value '{}' with type '{}' cannot be converted to double", executionContext.getExecutorLocale(), value, value.getClass().getName());
            }
        }
        return doubleValue;
    }

    public static Float convertToFloat(Object value, ExecutionContext executionContext) {
        Float floatValue = null;
        if (value != null) {
            if (value instanceof String) {
                floatValue = Float.valueOf((String) value);
            } else if (value instanceof java.lang.Float) {
                floatValue = (Float) value;
            } else if (value instanceof java.math.BigDecimal) {
                floatValue = ((java.math.BigDecimal) value).floatValue();
            } else if (value instanceof java.math.BigInteger) {
                floatValue = ((java.math.BigInteger) value).floatValue();
            } else if (value instanceof java.lang.Integer) {
                floatValue = ((java.lang.Integer) value).floatValue();
            } else if (value instanceof java.lang.Long) {
                floatValue = ((java.lang.Long) value).floatValue();
            } else if (value instanceof java.lang.Double) {
                floatValue = ((java.lang.Double) value).floatValue();
            } else if (value instanceof java.lang.Short) {
                floatValue = ((java.lang.Short) value).floatValue();
            } else {
                throw new DSPDMException("Value '{}' with type '{}' cannot be converted to float", executionContext.getExecutorLocale(), value, value.getClass().getName());
            }
        }
        return floatValue;
    }

    public static Integer convertToInteger(Object value, ExecutionContext executionContext) {
        Integer intValue = null;
        if (value != null) {
            if (value instanceof String) {
                intValue = Integer.valueOf((String) value);
            } else if (value instanceof java.lang.Integer) {
                intValue = (Integer) value;
            } else if (value instanceof java.math.BigDecimal) {
                intValue = ((java.math.BigDecimal) value).intValue();
            } else if (value instanceof java.math.BigInteger) {
                intValue = ((java.math.BigInteger) value).intValue();
            } else if (value instanceof java.lang.Double) {
                intValue = ((java.lang.Double) value).intValue();
            } else if (value instanceof java.lang.Long) {
                intValue = ((java.lang.Long) value).intValue();
            } else if (value instanceof java.lang.Float) {
                intValue = ((java.lang.Float) value).intValue();
            } else if (value instanceof java.lang.Short) {
                intValue = ((java.lang.Short) value).intValue();
            } else {
                throw new DSPDMException("Value '{}' with type '{}' cannot be converted to int", executionContext.getExecutorLocale(), value, value.getClass().getName());
            }
        }
        return intValue;
    }

    public static Long convertToLong(Object value, ExecutionContext executionContext) {
        Long longValue = null;
        if (value != null) {
            if (value instanceof String) {
                longValue = Long.valueOf((String) value);
            } else if (value instanceof java.lang.Long) {
                longValue = (Long) value;
            } else if (value instanceof java.math.BigDecimal) {
                longValue = ((java.math.BigDecimal) value).longValue();
            } else if (value instanceof java.math.BigInteger) {
                longValue = ((java.math.BigInteger) value).longValue();
            } else if (value instanceof java.lang.Double) {
                longValue = ((java.lang.Double) value).longValue();
            } else if (value instanceof java.lang.Integer) {
                longValue = ((java.lang.Integer) value).longValue();
            } else if (value instanceof java.lang.Float) {
                longValue = ((java.lang.Float) value).longValue();
            } else if (value instanceof java.lang.Short) {
                longValue = ((java.lang.Short) value).longValue();
            } else {
                throw new DSPDMException("Value '{}' with type '{}' cannot be converted to long", executionContext.getExecutorLocale(), value, value.getClass().getName());
            }
        }
        return longValue;
    }

    public static Short convertToShort(Object value, ExecutionContext executionContext) {
        Short shortValue = null;
        if (value != null) {
            if (value instanceof String) {
                shortValue = Short.valueOf((String) value);
            } else if (value instanceof java.lang.Short) {
                shortValue = (Short) value;
            } else if (value instanceof java.math.BigDecimal) {
                shortValue = ((java.math.BigDecimal) value).shortValue();
            } else if (value instanceof java.math.BigInteger) {
                shortValue = ((java.math.BigInteger) value).shortValue();
            } else if (value instanceof java.lang.Double) {
                shortValue = ((java.lang.Double) value).shortValue();
            } else if (value instanceof java.lang.Integer) {
                shortValue = ((java.lang.Integer) value).shortValue();
            } else if (value instanceof java.lang.Float) {
                shortValue = ((java.lang.Float) value).shortValue();
            } else if (value instanceof java.lang.Long) {
                shortValue = ((java.lang.Long) value).shortValue();
            } else {
                throw new DSPDMException("Value '{}' with type '{}' cannot be converted to short", executionContext.getExecutorLocale(), value, value.getClass().getName());
            }
        }
        return shortValue;
    }

    public static Byte convertToByte(Object value, ExecutionContext executionContext) {
        Byte byteValue = null;
        if (value != null) {
            if (value instanceof String) {
                byteValue = Byte.valueOf((String) value);
            } else if (value instanceof java.lang.Byte) {
                byteValue = (Byte) value;
            } else if (value instanceof java.lang.Short) {
                byteValue = ((Short) value).byteValue();
            } else if (value instanceof java.math.BigDecimal) {
                byteValue = ((java.math.BigDecimal) value).byteValue();
            } else if (value instanceof java.math.BigInteger) {
                byteValue = ((java.math.BigInteger) value).byteValue();
            } else if (value instanceof java.lang.Double) {
                byteValue = ((java.lang.Double) value).byteValue();
            } else if (value instanceof java.lang.Integer) {
                byteValue = ((java.lang.Integer) value).byteValue();
            } else if (value instanceof java.lang.Float) {
                byteValue = ((java.lang.Float) value).byteValue();
            } else if (value instanceof java.lang.Long) {
                byteValue = ((java.lang.Long) value).byteValue();
            } else {
                throw new DSPDMException("Value '{}' with type '{}' cannot be converted to byte", executionContext.getExecutorLocale(), value, value.getClass().getName());
            }
        }
        return byteValue;
    }

    /**
     * It returns a new number which is an exact multiple of second number. The returned number is greater or equal to the first number
     *
     * @param firstNumber
     * @param secondNumber
     * @return
     * @author Muhammad Imran Asnari
     * @since 22-Mar-2021
     */
    public static int convertFirstNumberToMultipleOfSecond(int firstNumber, int secondNumber) {
        final int remainder = firstNumber % secondNumber;
        // get the number of records from max page size. Subtract the modulus number from max page size.
        // This number is the number to be add in total records in order to make the number as multiple of max page size
        final int difference = secondNumber - remainder;
        if ((firstNumber > secondNumber) && (difference != secondNumber)) {
            // add the number in total records to read for rounding up for making the number in multiple of max page size
            firstNumber += difference;
        }
        return firstNumber;
    }
}
