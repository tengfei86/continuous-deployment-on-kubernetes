package com.lgc.dspdm.msp.mainservice.utils.metadata;

import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.service.common.dynamic.read.IDynamicReadService;

import java.util.List;
import java.util.Map;

public class MetadataAttributeChangeRestServiceHelper {

    public static void validateAddCustomAttributes(List<DynamicDTO> boListToSave, ExecutionContext executionContext) {
        for (DynamicDTO dynamicDTO : boListToSave) {
            if (StringUtils.isNullOrEmpty((String) dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME))) {
                throw new DSPDMException("ATTRIBUTE DISPLAY NAME is mandatory to add new custom attribute", executionContext.getExecutorLocale());
            }
            if (StringUtils.isNullOrEmpty((String) dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE))) {
                throw new DSPDMException("ATTRIBUTE DATA TYPE is mandatory to add new custom attribute", executionContext.getExecutorLocale());
            }
            if (StringUtils.hasValue((String) dynamicDTO.get(DSPDMConstants.BoAttrName.UNIT))) {
                String unit = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.UNIT);
                if (unit.length() > DSPDMConstants.Units.UNIT_MAX_LENGTH)
                    throw new DSPDMException("DEFAULT UNIT cannot be more than '{}' characters long", executionContext.getExecutorLocale(), DSPDMConstants.Units.UNIT_MAX_LENGTH);
            }
        }
    }

    public static void validateUpdateAttributes(List<DynamicDTO> boListToUpdate, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {

        for (DynamicDTO dynamicDTOToUpdate : boListToUpdate) {
            if (StringUtils.isNullOrEmpty((String) dynamicDTOToUpdate.get(DSPDMConstants.BoAttrName.BO_NAME))) {
                throw new DSPDMException("BO NAME is mandatory to update attribute", executionContext.getExecutorLocale());
            }
            if (StringUtils.isNullOrEmpty((String) dynamicDTOToUpdate.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME))) {
                throw new DSPDMException("BO ATTR NAME is mandatory to update attribute", executionContext.getExecutorLocale());
            }
            if (!StringUtils.isNullOrEmpty((String) dynamicDTOToUpdate.get(DSPDMConstants.BoAttrName.ENTITY))) {
                throw new DSPDMException("Update Entity is not allowed", executionContext.getExecutorLocale());
            }
            if (!StringUtils.isNullOrEmpty((String) dynamicDTOToUpdate.get(DSPDMConstants.BoAttrName.ATTRIBUTE))) {
                throw new DSPDMException("Update Attribute is not allowed", executionContext.getExecutorLocale());
            }
            if (dynamicDTOToUpdate.get(DSPDMConstants.BoAttrName.IS_REFERENCE_IND) != null) {
                // it means w'v a request to update the reference field (reference indicator). Now we should expect the other reference columns as well like
                // reference_bo_name, reference_bo_attr_value, reference_bo_attr_label
                Boolean isReferenceIndicator = (Boolean) dynamicDTOToUpdate.get(DSPDMConstants.BoAttrName.IS_REFERENCE_IND);
                if (isReferenceIndicator) {
                    if (!checkIsAllReferenceFieldsAreAvailable(dynamicDTOToUpdate, dynamicReadService, executionContext)) {
                        throw new DSPDMException("If IS_REFERENCE_IND is updated, then the following all fields are mandatory: " + DSPDMConstants.BoAttrName.REFERENCE_BO_NAME
                                + " , " + DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_VALUE + " , " + DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_LABEL, executionContext.getExecutorLocale());
                    }
                } else {
                    // if is_reference_ind is false, we need to make sure we dont receive value for any other reference column
                    if (checkIsAnyReferenceFieldValuePresent(dynamicDTOToUpdate)) {
                        throw new DSPDMException("If IS_REFERENCE_IND is updated, then the following all fields should be empty or null: " + DSPDMConstants.BoAttrName.REFERENCE_BO_NAME
                                + " , " + DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_VALUE + " , " + DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_LABEL, executionContext.getExecutorLocale());
                    }
                }
            }
            if (!StringUtils.isNullOrEmpty((String) dynamicDTOToUpdate.get(DSPDMConstants.BoAttrName.REFERENCE_BO_NAME)) ||
                    !StringUtils.isNullOrEmpty((String) dynamicDTOToUpdate.get(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_VALUE)) ||
                    !StringUtils.isNullOrEmpty((String) dynamicDTOToUpdate.get(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_LABEL))) {
                // if we get an update request to any of the reference field, we gotta make sure we've all the required fields then i.e., reference_bo_name, reference_bo_attr_value, reference_bo_attr_label
                if (!checkIsAllReferenceFieldsAreAvailable(dynamicDTOToUpdate, dynamicReadService, executionContext)) {
                    throw new DSPDMException("If any of the reference field is updated, the following all fields are mandatory: " + DSPDMConstants.BoAttrName.REFERENCE_BO_NAME
                            + " , " + DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_VALUE + " , " + DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_LABEL, executionContext.getExecutorLocale());
                }
            }
            if (!StringUtils.isNullOrEmpty((String) dynamicDTOToUpdate.get(DSPDMConstants.BoAttrName.RELATED_BO_ATTR_NAME))) {
                // in case of related_bo_attr_name update make sure that related_bo_attr_name already exists in the current/same business object.
                checkRelatedBoAttrAlreadyExistsInCurrentBO(dynamicDTOToUpdate, dynamicReadService, executionContext);
            }
        }
    }

    private static Boolean checkIsAllReferenceFieldsAreAvailable(DynamicDTO dynamicDTO, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        if (!StringUtils.isNullOrEmpty((String) dynamicDTO.get(DSPDMConstants.BoAttrName.REFERENCE_BO_NAME)) &&
                !StringUtils.isNullOrEmpty((String) dynamicDTO.get(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_VALUE)) &&
                !StringUtils.isNullOrEmpty((String) dynamicDTO.get(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_LABEL))) {
            //get metadata for that bo and check for both attributes here not later
            String boName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.REFERENCE_BO_NAME);
            String refBoAttrValue = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_VALUE);
            String referenceBoAttrLabel = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_LABEL);
            Map<String, DynamicDTO> boAttrNameMetadataMap = dynamicReadService.readMetadataMapForBOName(boName, executionContext);
            if (boAttrNameMetadataMap != null) {
                if ((!CollectionUtils.containsKeyIgnoreCase(boAttrNameMetadataMap, refBoAttrValue)) ||
                        (!CollectionUtils.containsKeyIgnoreCase(boAttrNameMetadataMap, referenceBoAttrLabel))) {
                    throw new DSPDMException("Reference bo attribute(s) not found against the business object '{}'", executionContext.getExecutorLocale(), boName);
                }
            } else {
                throw new DSPDMException("No reference business object found with name '{}'", executionContext.getExecutorLocale(), boName);
            }
            return true;
        }
        return false;
    }

    private static Boolean checkIsAnyReferenceFieldValuePresent(DynamicDTO dynamicDTO){
        if(StringUtils.isNullOrEmpty((String) dynamicDTO.get(DSPDMConstants.BoAttrName.REFERENCE_BO_NAME)) &&
                StringUtils.isNullOrEmpty((String) dynamicDTO.get(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_VALUE)) &&
                StringUtils.isNullOrEmpty((String) dynamicDTO.get(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_LABEL))){
            // setting reference values to null just in case they are of string and are empty
            dynamicDTO.put(DSPDMConstants.BoAttrName.REFERENCE_BO_NAME,null);
            dynamicDTO.put(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_VALUE,null);
            dynamicDTO.put(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_LABEL,null);
            dynamicDTO.put(DSPDMConstants.BoAttrName.RELATED_BO_ATTR_NAME,null);
            return false;
        }
        return true;
    }

    private static void checkRelatedBoAttrAlreadyExistsInCurrentBO(DynamicDTO dynamicDTO, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        String boName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        Map<String, DynamicDTO> metadataMap = dynamicReadService.readMetadataMapForBOName(boName, executionContext);
        if (metadataMap != null) {
            if (!(CollectionUtils.containsKeyIgnoreCase(metadataMap, (String) dynamicDTO.get(DSPDMConstants.BoAttrName.RELATED_BO_ATTR_NAME)))) {
                throw new DSPDMException("Related bo attribute not found against the business object '{}'", executionContext.getExecutorLocale(), boName);
            }
        } else {
            throw new DSPDMException("No business object found with name '{}'", executionContext.getExecutorLocale(), boName);
        }
    }

    public static void validateDeleteCustomAttributes(List<DynamicDTO> boListToSave, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        for (DynamicDTO dynamicDTO : boListToSave) {
            if (StringUtils.isNullOrEmpty((String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME))) {
                throw new DSPDMException("BO_NAME is mandatory to delete a custom attribute", executionContext.getExecutorLocale());
            }
            if (StringUtils.isNullOrEmpty((String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME))) {
                throw new DSPDMException("BO_ATTR_NAME is mandatory to delete a custom attribute", executionContext.getExecutorLocale());
            }
            if(DSPDMConstants.RESTRICT_USER_TO_DELETE_DATA_FIRST_TO_DELETE_CUSTOM_ATTR_FLAG){
                // Restricting user to delete custom attribute if it has data inside it.
                BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo((String)dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME), executionContext);
                businessObjectInfo.addColumnsToSelect((String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME));
                businessObjectInfo.addFilter((String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME), Operator.NOT_EQUALS, null);
                businessObjectInfo.setReadFirst(Boolean.TRUE);
                if(CollectionUtils.hasValue(dynamicReadService.readSimple(businessObjectInfo, executionContext))){
                    throw new DSPDMException("Cannot delete custom attribute '{}' from business object '{}'. Please delete data inside it first",
                            executionContext.getExecutorLocale(), dynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME),
                            dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME));
                }
            }
        }
    }
}
