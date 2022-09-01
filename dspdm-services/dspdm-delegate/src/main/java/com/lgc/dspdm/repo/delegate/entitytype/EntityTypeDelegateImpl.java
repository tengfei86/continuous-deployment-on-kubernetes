package com.lgc.dspdm.repo.delegate.entitytype;

import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAO;
import com.lgc.dspdm.core.dao.dynamic.businessobject.DynamicDAOFactory;
import com.lgc.dspdm.repo.delegate.common.write.config.REntityTypeConfig;

import java.util.List;
import java.util.Map;

/**
 * This class will perform validations on the provided data. These validation rules are stored in R ENTITY TYPE table
 *
 * @author muhammadimran.ansari, changmin sun
 * @since  02-Feb-2021
 *
 */
public class EntityTypeDelegateImpl implements IEntityTypeDelegate {
    private static DSPDMLogger logger = new DSPDMLogger(EntityTypeDelegateImpl.class);
    private static IEntityTypeDelegate singleton = null;

    private EntityTypeDelegateImpl() {

    }

    public static IEntityTypeDelegate getInstance() {
        if (singleton == null) {
            singleton = new EntityTypeDelegateImpl();
        }
        return singleton;
    }

    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */

    @Override
    public void validateREntityTypeForSave(String boName, List<DynamicDTO> boListToSave, ExecutionContext executionContext) {
        if (StringUtils.isNullOrEmpty(boName) || CollectionUtils.isNullOrEmpty(boListToSave)) {
            return;
        }
        Map<String, List<REntityTypeConfig.Node>> configMap = REntityTypeConfig.getInstance().getConfigMap();
        if (CollectionUtils.isNullOrEmpty(configMap)) {
            return;
        }
        List<REntityTypeConfig.Node> nodeList = configMap.get(boName);
        if (CollectionUtils.isNullOrEmpty(nodeList)) {
            return;
        }
        IDynamicDAO rEntityTypeDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(DSPDMConstants.BoName.R_ENTITY_TYPE, executionContext);

        for (REntityTypeConfig.Node node : nodeList) {
            if (node == null) {
                continue;
            }
            for (DynamicDTO dynamicDTO : boListToSave) {
                String entityType = (String) dynamicDTO.get(node.getSourceBoAttrName());
                // entity type will be WELL or FACILITY it is BO NAME
                String entityName = (String) dynamicDTO.get(node.getTargetBoAttrName());
                // entity name is a UWI or it a facility name
                // proceed with validations only if entity type is not null
                if (StringUtils.hasValue(entityType)) {
                    if (StringUtils.isNullOrEmpty(entityName)) {
                        String entityNameDisplayName = DSPDMConstants.BoAttrName.ENTITY_NAME;
                        String entityTypeDisplayName = DSPDMConstants.BoAttrName.ENTITY_TYPE;
                        IDynamicDAO currentBoDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(boName, executionContext);
                        Map<String, DynamicDTO> currentBoMetadataMap = currentBoDAO.readMetadataMap(executionContext);
                        if (currentBoMetadataMap.containsKey(entityNameDisplayName)) {
                            entityNameDisplayName = (String) currentBoMetadataMap.get(entityNameDisplayName).get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME);
                        }
                        if (currentBoMetadataMap.containsKey(entityTypeDisplayName)) {
                            entityTypeDisplayName = (String) currentBoMetadataMap.get(entityTypeDisplayName).get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME);
                        }
                        throw new DSPDMException("Unable to process the records. No {} provided for the given {} '{}'",
                                executionContext.getExecutorLocale(), entityNameDisplayName, entityTypeDisplayName, entityType);
                    }
                    // 1. Get entity's metadata: bo_name and default_attribute_name
                    // eg: select bo_name,default_attribute_name from r_entity_type where entity_type = #{entityType}
                    BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.R_ENTITY_TYPE, executionContext);
                    businessObjectInfo.addColumnsToSelect(DSPDMConstants.BoAttrName.BO_NAME, DSPDMConstants.BoAttrName.DEFAULT_ATTRIBUTE_NAME);
                    businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.ENTITY_TYPE, Operator.EQUALS, entityType);
                    // read first record only
                    businessObjectInfo.setReadFirst(true);
                    List<DynamicDTO> rEntityTypeDTOList = rEntityTypeDAO.read(businessObjectInfo, executionContext);
                    if (CollectionUtils.isNullOrEmpty(rEntityTypeDTOList)) {
                        String entityTypeDisplayName = DSPDMConstants.BoAttrName.ENTITY_TYPE;
                        Map<String, DynamicDTO> rEntityTypeMetadataMap = rEntityTypeDAO.readMetadataMap(executionContext);
                        if (rEntityTypeMetadataMap.containsKey(entityTypeDisplayName)) {
                            entityTypeDisplayName = (String) rEntityTypeMetadataMap.get(entityTypeDisplayName).get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME);
                        }
                        String rEntityTypeBoDisplayName = (String) rEntityTypeDAO.readMetadataBusinessObject(executionContext)
                                .get(DSPDMConstants.BoAttrName.BO_DISPLAY_NAME);
                        throw new DSPDMException("Unable to process the records. {} '{}' does not exist in business object {}",
                                executionContext.getExecutorLocale(), entityTypeDisplayName, entityType, rEntityTypeBoDisplayName);
                    }
                    // 2. Query whether entityName exists.
                    // eg: select count(1) from #{entityBoName} where #{entityBoAttrName}= #{entityName}
                    String entityBoName = (String) rEntityTypeDTOList.get(0).get(DSPDMConstants.BoAttrName.BO_NAME);
                    String entityBoAttrName = (String) rEntityTypeDTOList.get(0).get(DSPDMConstants.BoAttrName.DEFAULT_ATTRIBUTE_NAME);
                    if ((StringUtils.hasValue(entityBoName)) && (StringUtils.hasValue(entityBoAttrName))) {
                        businessObjectInfo = new BusinessObjectInfo(entityBoName, executionContext);
                        businessObjectInfo.addFilter(entityBoAttrName, Operator.EQUALS, entityName);
                        IDynamicDAO entityTypeValueDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(entityBoName, executionContext);
                        int entityNameCount = entityTypeValueDAO
                                .count(businessObjectInfo, executionContext);
                        if (entityNameCount <= 0) {
                            String entityTypeValueBoDisplayName = (String) entityTypeValueDAO.readMetadataBusinessObject(executionContext)
                                    .get(DSPDMConstants.BoAttrName.BO_DISPLAY_NAME);
                            String entityBoAttrDisplayName = entityBoAttrName;
                            Map<String, DynamicDTO> entityTypeValueMetadataMap = entityTypeValueDAO.readMetadataMap(executionContext);
                            if (entityTypeValueMetadataMap.containsKey(entityBoAttrName)) {
                                entityBoAttrDisplayName = (String) entityTypeValueMetadataMap.get(entityBoAttrName).get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME);
                            }
                            throw new DSPDMException("Unable to process the records. {} '{}' does not exist in business object '{}'",
                                    executionContext.getExecutorLocale(), entityBoAttrDisplayName, entityName, entityTypeValueBoDisplayName);
                        }
                    }
                } else if (StringUtils.hasValue(entityName)) {
                    dynamicDTO.put(node.getTargetBoAttrName(), null);
                }
            }
        }
    }
}
