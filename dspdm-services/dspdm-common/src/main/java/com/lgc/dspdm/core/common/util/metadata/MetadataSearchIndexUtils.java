package com.lgc.dspdm.core.common.util.metadata;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;

import java.util.List;
import java.util.Map;

/**
 * @author Muhammad Suleman Tanveer
 * @since 24-Dec-2021
 */
public class MetadataSearchIndexUtils {
    /**
     * Returns true if search index is found for the given attributes
     *
     * @param searchIndexes
     * @param boAttrNames
     * @return
     */
    public static boolean searchIndexExistsForAttributes(List<DynamicDTO> searchIndexes, List<String> boAttrNames) {
        return searchIndexExistsForAttributes(searchIndexes, boAttrNames.toArray(new String[0]));
    }

    /**
     * Returns true if search index is found for the given attributes
     *
     * @param searchIndexes
     * @param boAttrNames
     * @return
     */
    public static boolean searchIndexExistsForAttributes(List<DynamicDTO> searchIndexes, String... boAttrNames) {
        boolean unique = false;
        // group all unique constraints by their name
        Map<String, List<DynamicDTO>> searchIndexByName = CollectionUtils.groupDynamicDTOByPropertyValueIgnoreCase(searchIndexes, DSPDMConstants.BoAttrName.INDEX_NAME);
        // now iterate on the constraints
        for (List<DynamicDTO> indexes : searchIndexByName.values()) {
            if (indexes.size() == boAttrNames.length) {
                List<DynamicDTO> filteredIndexes = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(indexes, DSPDMConstants.BoAttrName.BO_ATTR_NAME, boAttrNames);
                if (filteredIndexes.size() == boAttrNames.length) {
                    unique = true;
                    break;
                }
            }
        }
        return unique;
    }
}
