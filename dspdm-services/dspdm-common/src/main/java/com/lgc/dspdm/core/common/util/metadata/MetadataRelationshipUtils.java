package com.lgc.dspdm.core.common.util.metadata;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MetadataRelationshipUtils {
    /**
     * @param allChildRelationshipsGroupsByNameMap Having information of composite foreign keys
     * @param childBoName                          The requested business object that requires from relationship map
     * @return returns the List of Dynamic DTO of foreign keys
     * @author rao.alikhan
     */
    public static List<DynamicDTO> findBestRelationshipWithCurrentChild(Map<String, List<DynamicDTO>> allChildRelationshipsGroupsByNameMap, String childBoName) {
        List<DynamicDTO> simplePrimaryKeyDynamicDTOList = null;
        List<DynamicDTO> simpleNonPrimaryKeyDynamicDTOList = null;
        List<DynamicDTO> compositePrimaryKeyDynamicDTOList = null;
        List<DynamicDTO> compositeNonPrimaryKeyDynamicDTOList = null;

        String relationshipName = null;
        List<DynamicDTO> relationShipList = null;
        DynamicDTO relationshipDTO = null;
        for (Map.Entry<String, List<DynamicDTO>> e : allChildRelationshipsGroupsByNameMap.entrySet()) {
            relationshipName = e.getKey(); // just for readability and understandability of code
            relationShipList = e.getValue();
            relationshipDTO = relationShipList.get(0);
            if (childBoName.equalsIgnoreCase((String) relationshipDTO.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME))) {
                if (relationShipList.size() == 1) {
                    if (Boolean.TRUE.equals(relationshipDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP))) {
                        simplePrimaryKeyDynamicDTOList = relationShipList;
                        // break immediately. no need to look for more relationships
                        break;
                    } else {
                        // do not break, continue till we find a better more exact primary key based simple relationship
                        simpleNonPrimaryKeyDynamicDTOList = relationShipList;
                    }
                } else {
                    if (Boolean.TRUE.equals(relationshipDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP))) {
                        // do not break, continue till we find a better more exact primary key based simple relationship
                        compositePrimaryKeyDynamicDTOList = relationShipList;
                    } else {
                        // do not break, continue till we find a better more exact primary key based simple relationship
                        compositeNonPrimaryKeyDynamicDTOList = relationShipList;
                    }
                }
            }
        }

        List<DynamicDTO> finalList = null;
        if (CollectionUtils.hasValue(simplePrimaryKeyDynamicDTOList)) {
            finalList = simplePrimaryKeyDynamicDTOList;
        } else if (CollectionUtils.hasValue(simpleNonPrimaryKeyDynamicDTOList)) {
            finalList = simpleNonPrimaryKeyDynamicDTOList;
        } else if (CollectionUtils.hasValue(compositePrimaryKeyDynamicDTOList)) {
            finalList = compositePrimaryKeyDynamicDTOList;
        } else if (CollectionUtils.hasValue(compositeNonPrimaryKeyDynamicDTOList)) {
            finalList = compositeNonPrimaryKeyDynamicDTOList;
        }
        return finalList;
    }

    /**
     * verifies that the given list does not have any relationship with the given parent and child business object attribute names
     *
     * @param allRelationships
     * @param parentBoAttrName
     * @param childBoAttrName
     * @return
     */
    public static boolean relationshipExistsForAttributes(List<DynamicDTO> allRelationships, String parentBoAttrName, String childBoAttrName) {
        return relationshipExistsForAttributes(allRelationships, Arrays.asList(parentBoAttrName), Arrays.asList(childBoAttrName));
    }

    /**
     * verifies that the given list does not have the relationships with the given same parent and child business object attribute names
     *
     * @param allRelationships
     * @param parentBoAttrNames
     * @param childBoAttrNames
     * @return
     */
    public static boolean relationshipExistsForAttributes(List<DynamicDTO> allRelationships, List<String> parentBoAttrNames, List<String> childBoAttrNames) {
        boolean exists = false;
        // group all relationships by their name
        Map<String, List<DynamicDTO>> relationshipsByName = CollectionUtils.groupDynamicDTOByPropertyValueIgnoreCase(allRelationships, DSPDMConstants.BoAttrName.BUS_OBJ_RELATIONSHIP_NAME);
        // now iterate on the relationships
        for (List<DynamicDTO> relationships : relationshipsByName.values()) {
            if ((relationships.size() == parentBoAttrNames.size()) && (parentBoAttrNames.size() == childBoAttrNames.size())) {
                int index = 0;
                for (index = 0; index < relationships.size(); index++) {
                    // parent or child attribute not necessarily be in same order in the relationships so we filter the whole list
                    DynamicDTO relationship = CollectionUtils.filterFirstDynamicDTOByPropertyNameAndPropertyValue(relationships, DSPDMConstants.BoAttrName.PARENT_BO_ATTR_NAME, parentBoAttrNames.get(index));
                    if ((relationship == null) || (!(relationship.get(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME).equals(childBoAttrNames.get(index))))) {
                        // either relationship is null (means parent bo attr not found in the list) or the child bo attr name is different then break the loop early
                        break;
                    }
                }
                if (index == relationships.size()) {
                    // it means loop executed full length and all parents and child are found in the relationship
                    exists = true;
                    break;
                }
            }
        }
        return exists;
    }

    public static List<DynamicDTO> filterRelationshipsForSameParentAndChild(List<DynamicDTO> allRelationships, String parentBoName, String childBoName, Boolean isPKRelationship) {
        List<DynamicDTO> filteredList = new ArrayList<>(allRelationships.size());
        // now iterate on the relationships
        for (DynamicDTO relationship : allRelationships) {
            if ((parentBoName.equalsIgnoreCase((String) relationship.get(DSPDMConstants.BoAttrName.PARENT_BO_NAME)))
                    && (childBoName.equalsIgnoreCase((String) relationship.get(DSPDMConstants.BoAttrName.CHILD_BO_NAME)))) {
                // either flag is null or same
                if ((isPKRelationship == null) || (isPKRelationship.equals(relationship.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP)))) {
                    filteredList.add(relationship);
                }
            }
        }
        return filteredList;
    }
}
