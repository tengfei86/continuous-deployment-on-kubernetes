package com.lgc.dspdm.core.common.util;

import java.util.*;

/**
 * A utility class to hold the order of the properties as it is defined in the property file
 *
 * @author muhammadimran.ansari
 * @since 14-May-2020
 */
public class OrderedProperties extends Properties {

    private Set<Object> keySet = new LinkedHashSet<Object>(100);

    @Override
    public Enumeration<Object> keys() {
        return Collections.enumeration(keySet);
    }

    @Override
    public Set<Object> keySet() {
        return keySet;
    }

    @Override
    public synchronized Object put(Object key, Object value) {
        if (!keySet.contains(key)) {
            keySet.add(key);
        }
        return super.put(key, value);
    }

    @Override
    public synchronized Object remove(Object key) {
        keySet.remove(key);
        return super.remove(key);
    }

    @Override
    public synchronized void putAll(Map values) {
        for (Object key : values.keySet()) {
            if (!containsKey(key)) {
                keySet.add(key);
            }
        }
        super.putAll(values);
    }
}