package com.lgc.dspdm.core.common.util.metadata;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;

import java.util.List;
import java.util.Map;

/**
 * @author Muhammad Imran Ansari
 * @since 12-Jan-2021
 */
public class MetadataUniqueConstraintUtils {
    /**
     * Returns true if unique constraint is found for the given attributes
     *
     * @param uniqueConstraints
     * @param boAttrNames
     * @return
     */
    public static boolean uniqueConstraintExistsForAttributes(List<DynamicDTO> uniqueConstraints, List<String> boAttrNames) {
        return uniqueConstraintExistsForAttributes(uniqueConstraints, boAttrNames.toArray(new String[0]));
    }

    /**
     * Returns true if unique constraint is found for the given attributes
     *
     * @param uniqueConstraints
     * @param boAttrNames
     * @return
     */
    public static boolean uniqueConstraintExistsForAttributes(List<DynamicDTO> uniqueConstraints, String... boAttrNames) {
        boolean unique = false;
        // group all unique constraints by their name
        Map<String, List<DynamicDTO>> constraintsByName = CollectionUtils.groupDynamicDTOByPropertyValueIgnoreCase(uniqueConstraints, DSPDMConstants.BoAttrName.CONSTRAINT_NAME);
        // now iterate on the constraints
        for (List<DynamicDTO> constraints : constraintsByName.values()) {
            if (constraints.size() == boAttrNames.length) {
                List<DynamicDTO> filteredConstraints = CollectionUtils.filterDynamicDTOByPropertyNameAndPropertyValue(constraints, DSPDMConstants.BoAttrName.BO_ATTR_NAME, boAttrNames);
                if (filteredConstraints.size() == boAttrNames.length) {
                    unique = true;
                    break;
                }
            }
        }
        return unique;
    }
}
