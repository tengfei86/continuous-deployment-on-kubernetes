package com.lgc.dspdm.core.common.util;

import com.lgc.dspdm.core.common.exception.DSPDMException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Objects;

/**
 * @Author rao.alikhan
 */

public class ObjectUtils {


    /**
     * Returns a deep copy of the passes java object
     *
     * @param object
     * @param executionContext
     * @param <T>
     * @return
     * @author rao.alikhan
     */
    public static <T> T deepCopy(T object, ExecutionContext executionContext) {
        T t = null;
        try (
                ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
                ObjectOutputStream objOutputStream = new ObjectOutputStream(baOutputStream);
        ) {
            objOutputStream.writeObject(object);
            try (
                    ByteArrayInputStream baInputStream = new ByteArrayInputStream(baOutputStream.toByteArray());
                    ObjectInputStream objInputStream = new ObjectInputStream(baInputStream);
            ) {
                t = (T) objInputStream.readObject();
            } catch (Throwable e) {
                DSPDMException.throwException(e, executionContext);
            }
        } catch (Throwable e) {
            DSPDMException.throwException(e, executionContext);
        }

        return t;
    }

    /**
     * copares both objects and returns true if both are same otherwise false
     *
     * @param obj1
     * @param obj2
     * @return
     * @author Muhammad Imran Ansari
     */
    public static boolean areTwoValuesSame(Object obj1, Object obj2) {
        boolean same = true;
        if ((obj1 != null) && (obj1 instanceof java.sql.Timestamp) && (obj2 != null) && (obj2 instanceof java.sql.Timestamp)) {
            // if comparing timestamp objects then first make their nano seconds to zero
            obj1 = new java.sql.Timestamp(((java.sql.Timestamp) obj1).getTime());
            ((java.sql.Timestamp) obj1).setNanos(0);

            obj2 = new java.sql.Timestamp(((java.sql.Timestamp) obj2).getTime());
            ((java.sql.Timestamp) obj2).setNanos(0);
        }
        if (!Objects.equals(obj1, obj2)) {
            if ((obj1 != null) && (obj1 instanceof java.math.BigDecimal) && (obj2 != null) && (obj2 instanceof java.math.BigDecimal)) {
                // both values are not same using big decimal compare to function so it means they are really not same otherwise same
                // in big decimal value "2.0" is not equal to "2.00" but using compare both are same
                if (((BigDecimal) obj1).compareTo((BigDecimal) obj2) != 0) {
                    same = false;
                }
            } else if ((obj1 != null) && (obj2 != null) && (obj1.getClass().isArray()) && (obj2.getClass().isArray())) {
                int length1 = Array.getLength(obj1);
                int length2 = Array.getLength(obj2);
                if (length1 != length2) {
                    same = false;
                } else {
                    if (length1 > 0) {
                        if (length1 > 10000) {
                            same = false;
                        } else {
                            if ((obj1.getClass().getComponentType().isPrimitive()) && (obj2.getClass().getComponentType().isPrimitive())) {
                                Object byte1 = Array.get(obj1, 0);
                                Object byte2 = Array.get(obj2, 0);
                                if ((byte1 instanceof Byte) && (byte2 instanceof Byte)) {
                                    if (!(Arrays.equals((byte[]) obj1, (byte[]) obj2))) {
                                        same = false;
                                    }
                                } else {
                                    same = false;
                                }
                            } else {
                                same = false;
                            }
                        }
                    }
                }
            } else {
                same = false;
            }
        }
        return same;
    }
}
