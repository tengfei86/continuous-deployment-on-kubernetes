package com.lgc.dspdm.repo.delegate.metadata.bo.read;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.dao.dynamic.businessobject.DynamicDAOFactory;

import java.util.List;

/**
 * @author Muhammad Imran Ansari
 */
public class MetadataBODelegateImpl implements IMetadataBODelegate {
    private static IMetadataBODelegate singleton = null;
    
    private MetadataBODelegateImpl() {
        
    }
    
    public static IMetadataBODelegate getInstance() {
        if (singleton == null) {
            singleton = new MetadataBODelegateImpl();
        }
        return singleton;
    }
    
    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */
    
    @Override
    public List<DynamicDTO> readMetadataAllBusinessObjects(ExecutionContext executionContext) {
        return DynamicDAOFactory.getInstance(executionContext).getBusinessObjects(executionContext);
    }
}
