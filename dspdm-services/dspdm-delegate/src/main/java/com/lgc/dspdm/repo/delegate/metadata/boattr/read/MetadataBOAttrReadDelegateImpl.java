package com.lgc.dspdm.repo.delegate.metadata.boattr.read;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.dao.dynamic.businessobject.DynamicDAOFactory;

import java.util.List;
import java.util.Map;

/**
 * @author Muhammad Imran Ansari
 */
public class MetadataBOAttrReadDelegateImpl implements IMetadataBOAttrReadDelegate {
    private static IMetadataBOAttrReadDelegate singleton = null;
    
    private MetadataBOAttrReadDelegateImpl() {
        
    }
    
    public static IMetadataBOAttrReadDelegate getInstance() {
        if (singleton == null) {
            singleton = new MetadataBOAttrReadDelegateImpl();
        }
        return singleton;
    }
    
    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */
        
    @Override
    public DynamicDTO readMetadataBusinessObject(String boName, ExecutionContext executionContext) {
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(boName, executionContext).readMetadataBusinessObject(executionContext);
    }
    
    @Override
    public List<String> readBOAttrNames(String boName, ExecutionContext executionContext) {
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(boName, executionContext).readMetadataBOAttrNames(executionContext);
    }

    @Override
    public Map<String, Object> getDefaultValuesMap(String boName, ExecutionContext executionContext) {
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(boName, executionContext).getDefaultValuesMap(executionContext);
    }

    @Override
    public List<DynamicDTO> read(String boName, ExecutionContext executionContext) {
//        BusinessObjectInfo metadataBusinessObjectInfo = new BusinessObjectInfo(AnnotationProcessor.getBOName(BusinessObjectAttributeDTO.class));
//        // where bo name is {businessObjectInfo.getBusinessObjectType()}
//        metadataBusinessObjectInfo.addFilter(MetadataUtils.getBOAttributeNameForFieldBOName(BusinessObjectAttributeDTO.class, BusinessObjectAttributeDTO.properties.boName.name()), boName);
//        // where is_active = true       
//        metadataBusinessObjectInfo.addFilter(MetadataUtils.getBOAttributeNameForFieldIsActive(BusinessObjectAttributeDTO.class, BusinessObjectAttributeDTO.properties.isActive.name()), Boolean.TRUE);
//        // read first 100 records of metadata table
//        metadataBusinessObjectInfo.setRecordsPerPage(10000);
//        metadataBusinessObjectInfo.addOrderByAsc(MetadataUtils.getBoAttributeNameForFieldSequenceNumber(BusinessObjectAttributeDTO.class, BusinessObjectAttributeDTO.properties.sequenceNumber.name()));
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(boName, executionContext).readMetadata(executionContext);
    }
    
    @Override
    public Map<String, DynamicDTO> readMap(String boName, ExecutionContext executionContext) {
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(boName, executionContext).readMetadataMap(executionContext);
    }
}
