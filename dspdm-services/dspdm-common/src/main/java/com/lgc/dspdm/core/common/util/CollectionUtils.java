package com.lgc.dspdm.core.common.util;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;

import java.util.*;

public class CollectionUtils {

    public static boolean isNullOrEmpty(Collection collection) {
        return ((collection == null) || (collection.size() == 0));
    }

    public static boolean hasValue(Collection collection) {
        return !isNullOrEmpty(collection);
    }

    public static boolean isNullOrEmpty(Map map) {
        return ((map == null) || (map.size() == 0));
    }

    public static boolean hasValue(Map map) {
        return !isNullOrEmpty(map);
    }

    public static boolean isNullOrEmpty(Object[] array) {
        return ((array == null) || (array.length == 0));
    }

    public static boolean hasValue(Object[] array) {
        return !isNullOrEmpty(array);
    }

    public static String getCommaSeparated(List<? extends Object> list) {
        return joinWith(", ", list.toArray(new Object[]{}));
    }

    public static String getCommaSeparated(Set<? extends Object> set) {
        return joinWith(", ", set.toArray(new Object[]{}));
    }

    public static String getCommaSeparated(Collection<? extends Object> collection) {
        return joinWith(", ", collection.toArray(new Object[]{}));
    }

    public static String getCommaSeparated(Object... array) {
        return joinWith(", ", array);
    }

    public static String getCommaSeparated(Map<? extends Object, ? extends Object> map) {
        return joinWith(", ", map);
    }

    public static String joinWithAnd(List<? extends Object> list) {
        return joinWith(" AND ", list.toArray(new Object[]{}));
    }

    public static String joinWithAnd(Object... array) {
        return joinWith(" AND ", array);
    }

    public static String joinWithOr(List<? extends Object> list) {
        return joinWith(" OR ", list.toArray(new Object[]{}));
    }

    public static String joinWithOr(Object... array) {
        return joinWith(" OR ", array);
    }

    public static String joinWith(String joiner, Object... array) {
        return joinWith(1, joiner, array);
    }

    public static String joinWith(int joinElementCount, String joiner, Object... array) {
        String result = DSPDMConstants.EMPTY_STRING;
        if (hasValue(array)) {
            for (int i = 0; i < array.length; ) {
                // do not add comma at the start and not at the end
                if ((i != 0) && (i != array.length)) {
                    result += joiner;
                }
                for (int j = 0; ((j < joinElementCount) && ((i + j) < array.length)); j++) {
                    if ((j != 0) && (j != joinElementCount)) {
                        result += DSPDMConstants.SPACE;
                    }
                    result += array[i + j];
                }
                i += joinElementCount;
            }
        }
        return result;
    }

    public static String joinWith(String joiner, Map<? extends Object, ? extends Object> map) {
        return joinWith(1, joiner, map);
    }

    public static String joinWith(int joinElementCount, String joiner, Map<? extends Object, ? extends Object> map) {
        String result = DSPDMConstants.EMPTY_STRING;
        if (hasValue(map)) {
            int i = 0;
            for (Object key : map.keySet()) {
                // do not add comma at the start and not at the end
                if ((i != 0) && (i != map.size())) {
                    result += joiner;
                }
                for (int j = 0; ((j < joinElementCount) && ((i + j) < map.size())); j++) {
                    if ((i != 0) && (i != joinElementCount)) {
                        result += DSPDMConstants.SPACE;
                    }
                    result += key + " " + map.get(key);
                }
                i += joinElementCount;
            }
        }
        return result;
    }

    public static boolean containsIgnoreCase(List<String> list, String value) {
        boolean flag = false;
        if ((list != null) && (value != null)) {
            for (String key : list) {
                if (value.equalsIgnoreCase(key)) {
                    flag = true;
                    break;
                }
            }
        }
        return flag;
    }

    public static boolean contains(int[] array, int value) {
        boolean flag = false;
        if ((array != null) ) {
            for (int key : array) {
                if (value == key) {
                    flag = true;
                    break;
                }
            }
        }
        return flag;
    }

    public static boolean containsKeyIgnoreCase(Map<String, ? extends Object> map, String key) {
        boolean flag = false;
        if ((map != null) && (key != null)) {
            for (String _key : map.keySet()) {
                if (key.equalsIgnoreCase(_key)) {
                    flag = true;
                    break;
                }
            }
        }
        return flag;
    }

    public static String removeIgnoreCase(List<String> list, String value) {
        String removed = null;
        if ((list != null) && (value != null)) {
            for (String key : list) {
                if (value.equalsIgnoreCase(key)) {
                    list.remove(key);
                    removed = key;
                    break;
                }
            }
        }
        return removed;
    }

    public static int indexOfIgnoreCase(List<String> list, String value) {
        int index = -1;
        if ((list != null) && (value != null)) {
            for (String key : list) {
                if (value.equalsIgnoreCase(key)) {
                    index = list.indexOf(key);
                    break;
                }
            }
        }
        return index;
    }

    public static List<Object> getValuesFromListOfMap(Collection<? extends Map> collection, Object key) {
        List<Object> list = new ArrayList<>(collection.size());
        for (Map map : collection) {
            list.add(map.get(key));
        }
        return list;
    }

    public static <T> Set<T> getValuesSetFromList(Collection<? extends Map> collection, Object key) {
        Set<T> set = new LinkedHashSet<>(collection.size());
        String value = null;
        for (Map map : collection) {
            set.add((T) map.get(key));
        }
        return set;
    }

    public static List<Object> getValuesFromList(Collection<? extends Map> collection, Object key) {
        List<Object> list = new ArrayList<>(collection.size());
        String value = null;
        for (Map map : collection) {
            list.add(map.get(key));
        }
        return list;
    }

    public static List<String> getStringValuesFromList(Collection<? extends Map> collection, Object key) {
        List<String> list = new ArrayList<>(collection.size());
        String value = null;
        for (Map map : collection) {
            list.add((String) map.get(key));
        }
        return list;
    }

    public static Set<Integer> getIntegerValuesFromList(Collection<? extends Map> collection, Object key) {
        Set<Integer> set = new LinkedHashSet<>(collection.size());
        Object value = null;
        for (Map map : collection) {
            value = map.get(key);
            if ((value == null) || (value instanceof Integer)) {
                set.add((Integer) value);
            } else if (value instanceof String) {
                try {
                    set.add(Integer.valueOf((String) value));
                } catch (NumberFormatException e) {
                }
            }
        }
        return set;
    }

    public static List<String> getUpperCaseValuesFromListOfMap(Collection<? extends Map> collection, Object key) {
        List<String> list = new ArrayList<>(collection.size());
        String value = null;
        for (Map map : collection) {
            value = (String) map.get(key);
            list.add((value == null) ? null : value.toUpperCase());
        }
        return list;
    }

    /**
     * It will prepare a map with primary column name as its value and whole DynamicDTO as its value
     *
     * @param businessObjectList
     * @param key
     * @return
     */
    public static <T> Map<T, DynamicDTO> prepareMapFromValuesOfKey(List<DynamicDTO> businessObjectList, String key) {
        Map<T, DynamicDTO> map = new HashMap<>();
        for (DynamicDTO businessObject : businessObjectList) {
            T value = (T) businessObject.get(key);
            map.put(value, businessObject);
        }
        return map;
    }

    /**
     * It will prepare a map with primary column name as its value and whole DynamicDTO as its value
     *
     * @param businessObjectList
     * @param key
     * @return
     */
    public static Map<String, DynamicDTO> prepareIgnoreCaseMapFromStringValuesOfKey(List<DynamicDTO> businessObjectList, String key) {
        Map<String, DynamicDTO> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (DynamicDTO businessObject : businessObjectList) {
            String value = (String) businessObject.get(key);
            map.put(value, businessObject);
        }
        return map;
    }

    /**
     * It will prepare a map with key as value of given attribute name and values will be list of objects
     *
     * @param businessObjectList
     * @param key
     * @return
     */
    public static Map<String, List<DynamicDTO>> prepareIgnoreCaseMapOfListFromStringValuesOfKey(List<DynamicDTO> businessObjectList, String key) {
        Map<String, List<DynamicDTO>> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (DynamicDTO businessObject : businessObjectList) {
            String value = (String) businessObject.get(key);
            if (value != null) {
                List<DynamicDTO> list = map.get(value);
                if (list == null) {
                    list = new ArrayList<>();
                    map.put(value, list);
                }
                list.add(businessObject);
            }
        }
        return map;
    }

    /**
     * It will prepare a map with key as value of given attribute name and values will be list of objects
     *
     * @param businessObjectList
     * @param key
     * @param <T>
     * @return
     */
    public static <T> Map<T, List<DynamicDTO>> prepareMapOfListFromValuesOfKey(List<DynamicDTO> businessObjectList, String key) {
        Map<T, List<DynamicDTO>> map = new HashMap<>();
        for (DynamicDTO businessObject : businessObjectList) {
            T value = (T) businessObject.get(key);
            if (value != null) {
                List<DynamicDTO> list = map.get(value);
                if (list == null) {
                    list = new ArrayList<>();
                    map.put(value, list);
                }
                list.add(businessObject);
            }
        }
        return map;
    }

    /**
     * Filter DynamicDTO list by property name and value. It iterates over the list and returns the filtered list of
     * only those objects which have the same property value for the given property name
     *
     * @param dynamicDTOList List of dynamicDTO containing results
     * @param propertyName   Property Name which is used to get information
     * @param propertyValues  Given property values which is used for comparison
     * @return returns filtered list of dynamiDTO containing matching results
     * @author rao.alikhan
     */
    public static List<DynamicDTO> filterDynamicDTOByPropertyNameAndPropertyValue(List<DynamicDTO> dynamicDTOList, String propertyName, Object ... propertyValues) {
        List<DynamicDTO> filteredList = null;
        if (CollectionUtils.hasValue(dynamicDTOList)) {
            filteredList = new ArrayList<>(dynamicDTOList.size());
            for (DynamicDTO dynamicDTO : dynamicDTOList) {
                for(Object propertyValue : propertyValues) {
                    if (dynamicDTO.get(propertyName).equals(propertyValue)) {
                        filteredList.add(dynamicDTO);
                        break;
                    }
                }
            }
        }
        return filteredList;
    }

    public static void setPropertyNameAndPropertyValue(List<DynamicDTO> dynamicDTOList, String propertyName, Object  propertyValues) {
        if (CollectionUtils.hasValue(dynamicDTOList)) {
            for (DynamicDTO dynamicDTO : dynamicDTOList) {
                dynamicDTO.put(propertyName,propertyValues);
            }
        }
    }

    /**
     * Filter DynamicDTO list by property name and value. It iterates over the list and returns the first filtered dto of
     * only those objects which have the same property value for the given property name. If nthing is filtered then null is returned.
     *
     * @param dynamicDTOList
     * @param propertyName
     * @param propertyValues
     * @return
     */
    public static DynamicDTO filterFirstDynamicDTOByPropertyNameAndPropertyValue(List<DynamicDTO> dynamicDTOList, String propertyName, Object ... propertyValues) {
        DynamicDTO filteredDynamicDTO = null;
        if (CollectionUtils.hasValue(dynamicDTOList)) {
            for (DynamicDTO dynamicDTO : dynamicDTOList) {
                for(Object propertyValue: propertyValues) {
                    if (dynamicDTO.get(propertyName).equals(propertyValue)) {
                        filteredDynamicDTO = dynamicDTO;
                        return filteredDynamicDTO;
                    }
                }
            }
        }
        return filteredDynamicDTO;
    }

    /**
     * Filter DynamicDTO map by property name and value. It iterates over the map and returns the filtered map of
     * only those objects which have the same property value for the given property name
     *
     * @param dynamicDTOMap Map of dynamicDTO containing results
     * @param propertyName   Property Name which is used to get information
     * @param propertyValues  Given property value which is used for comparison
     * @return returns filtered map of dynamiDTO containing matching results
     * @author rao.alikhan
     */
    public static Map<String, DynamicDTO> filterDynamicDTOByPropertyNameAndPropertyValue(Map<String, DynamicDTO> dynamicDTOMap, String propertyName, Object ... propertyValues) {
        Map<String, DynamicDTO> filteredMap = null;
        if (CollectionUtils.hasValue(dynamicDTOMap)) {
            filteredMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (Map.Entry<String, DynamicDTO> entry : dynamicDTOMap.entrySet()) {
                for(Object propertyValue : propertyValues) {
                    if (entry.getValue().get(propertyName).equals(propertyValue)) {
                        filteredMap.put(entry.getKey(), entry.getValue());
                        break;
                    }
                }
            }
        }
        return filteredMap;
    }

    /**
     * This method groups the objects of the given list on the basis of the value of the given property name
     *
     * @param dynamicDTOList DynamicDTO List
     * @return Returns a map which contains grouped Business objects with given property's value
     * @author rao.alikhan
     */
    public static <T> Map<T, List<DynamicDTO>> groupDynamicDTOByPropertyValue(List<DynamicDTO> dynamicDTOList, String propertyName) {
        return groupDynamicDTOByPropertyValue(dynamicDTOList, propertyName, -1);
    }

    /**
     * This method groups the objects of the given list on the basis of the value of the given property name
     * value of the given property must be of type string and the new map will work with ignore case keys
     *
     * @param dynamicDTOList
     * @param propertyName
     * @return
     * @author Muhammad Imran Ansari
     */
    public static Map<String, List<DynamicDTO>> groupDynamicDTOByPropertyValueIgnoreCase(final List<DynamicDTO> dynamicDTOList, String propertyName) {
        final Map<String, List<DynamicDTO>> groupedMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        return groupDynamicDTOByPropertyValue(dynamicDTOList, propertyName, -1, groupedMap);
    }

    public static <T> Map<T, List<DynamicDTO>> groupDynamicDTOByPropertyValue(final List<DynamicDTO> dynamicDTOList, String propertyName, int maxGroupSize) {
        final Map<T, List<DynamicDTO>> groupedMap = new HashMap<>();
        return groupDynamicDTOByPropertyValue(dynamicDTOList, propertyName, maxGroupSize, groupedMap);
    }

    private static <T> Map<T, List<DynamicDTO>> groupDynamicDTOByPropertyValue(final List<DynamicDTO> dynamicDTOList, String propertyName, int maxGroupSize, Map<T, List<DynamicDTO>> groupedMap) {
        List<DynamicDTO> oneGroupList = null;
        T propertyValue = null;
        for (DynamicDTO dynamicDTO : dynamicDTOList) {
            propertyValue = (T) dynamicDTO.get(propertyName);
            oneGroupList = groupedMap.get(propertyValue);
            if (oneGroupList != null) {
                // if group size is less then max allowed group size then add.
                if ((maxGroupSize < 0) || (oneGroupList.size() < maxGroupSize)) {
                    oneGroupList.add(dynamicDTO);
                }
            } else {
                oneGroupList = new ArrayList<>(1);
                oneGroupList.add(dynamicDTO);
                groupedMap.put(propertyValue, oneGroupList);
            }
        }
        return groupedMap;
    }

    /**
     * Getting the property values from the given map
     *
     * @param map              Contains the key value pair of dataSet
     * @param keys             Property Names which require to fetch values from map
     * @param executionContext
     * @param <T>              Map can include any given type
     * @return Will return list of objects which fulfills the given criteria
     */
    public static <T extends Map> List<Object> getMapValuesForKeys(T map, List<?> keys, ExecutionContext executionContext) {
        List<Object> values = new ArrayList<>(keys.size());
        if (hasValue(map) && hasValue(keys)) {
            for (Object key : keys) {
                values.add(map.get(key));
            }
        } else {
            throw new DSPDMException("Cannot filter or object because properties are null", executionContext.getExecutorLocale());
        }
        return values;
    }

    /**
     * Converts an array to a list. It is good to convert an array to a list before it is passed to a var args method
     *
     * @param array
     * @param <T>
     * @return
     */
    public static <T> List<T> getListFromArray(T[] array) {
        List<T> list = null;
        if (array != null) {
            list = new ArrayList<>(array.length);
            for (T t : array) {
                list.add(t);
            }
        }
        return list;
    }

    /**
     * It merges all the given collections i a single list
     *
     * @param collections
     * @param <T>
     * @return
     */
    public static <T> List<T> mergeLists(Collection<? extends Collection<T>> collections) {
        List<T> list = new ArrayList<>();
        for (Collection<T> collection : collections) {
            list.addAll(collection);
        }
        return list;
    }

    public static boolean startWithEqualIgnoreCase(String value, String regex) {
        boolean flag = false;
        if ((value != null) && (regex != null)) {
            if (value.trim().toLowerCase().startsWith(regex.trim().toLowerCase())) {
                flag = true;
            }
        }
        return flag;
    }

}
