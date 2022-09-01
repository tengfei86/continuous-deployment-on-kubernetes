package com.lgc.dspdm.repo.delegate.metadata.uniqueconstraints.read;

import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.core.dao.dynamic.businessobject.DynamicDAOFactory;
import com.lgc.dspdm.repo.delegate.metadata.boattr.read.MetadataBOAttrReadDelegateImpl;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MetadataConstraintsDelegateImpl implements IMetadataConstraintsDelegate {
    private static DSPDMLogger logger = new DSPDMLogger(MetadataConstraintsDelegateImpl.class);
    private static IMetadataConstraintsDelegate singleton = null;
    
    private MetadataConstraintsDelegateImpl() {
        
    }
    
    public static IMetadataConstraintsDelegate getInstance() {
        if (singleton == null) {
            singleton = new MetadataConstraintsDelegateImpl();
        }
        return singleton;
    }
    
    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */
    
    @Override
    public List<DynamicDTO> read(String boName, ExecutionContext executionContext) {
//        BusinessObjectInfo metadataConstraintsBusinessObjectInfo = new BusinessObjectInfo(MetadataUtils.getBONameForDTO(BusinessObjectAttributeConstraintsDTO.class));
//        // where bo name is {businessObjectInfo.getBusinessObjectType()}
//        metadataConstraintsBusinessObjectInfo.addFilter(MetadataUtils.getBOAttributeNameForFieldBOName(BusinessObjectAttributeConstraintsDTO.class, BusinessObjectAttributeConstraintsDTO.properties.boName.name()), boName);
//        // where is_active = true
//        metadataConstraintsBusinessObjectInfo.addFilter(MetadataUtils.getBOAttributeNameForFieldIsActive(BusinessObjectAttributeConstraintsDTO.class, BusinessObjectAttributeConstraintsDTO.properties.isActive.name()), Boolean.TRUE);
//        // read first 100 records of metadata table
//        metadataConstraintsBusinessObjectInfo.setRecordsPerPage(100);
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(boName, executionContext).readActiveMetadataConstraints(executionContext);
    }

    @Override
    public Map<String, Object> readParentConstraints(String boName, ExecutionContext executionContext) {
        return readParentConstraints(boName, null, executionContext);
    }

    @Override
    public Map<String, Object> readParentConstraints(String boName, List<DynamicDTO> metadataList, ExecutionContext executionContext) {
        Map<String, Object> finalMap = null;
        // read metadata first if not already provided
        if (CollectionUtils.isNullOrEmpty(metadataList)) {
            metadataList = MetadataBOAttrReadDelegateImpl.getInstance().read(boName, executionContext);
        }
        Set<String> referenceBONames = CollectionUtils.getValuesSetFromList(metadataList, DSPDMConstants.BoAttrName.REFERENCE_BO_NAME);
        if (CollectionUtils.hasValue(referenceBONames)) {
            finalMap = new LinkedHashMap<>(referenceBONames.size());
            for (String referenceBOName : referenceBONames) {
                // could be one null in a set
                if (StringUtils.hasValue(referenceBOName)) {
                    logger.info("Going to read parent business object metadata constraints.");
                    List<DynamicDTO> constraints = this.read(referenceBOName, executionContext);
                    if (CollectionUtils.hasValue(constraints)) {
                        finalMap.put(referenceBOName, new PagedList<>(constraints.size(), constraints));
                    }
                }
            }
        }
        return finalMap;
    }
}
