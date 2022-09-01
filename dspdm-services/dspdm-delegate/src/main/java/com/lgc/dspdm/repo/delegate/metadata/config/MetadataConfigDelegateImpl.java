package com.lgc.dspdm.repo.delegate.metadata.config;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.dao.dynamic.businessobject.DynamicDAOFactory;
import com.lgc.dspdm.core.dao.dynamic.businessobject.MetadataValidationDAO;

import java.util.List;
import java.util.Map;

/**
 * @author Muhammad Imran Ansari
 */
public class MetadataConfigDelegateImpl implements IMetadataConfigDelegate {
    private static IMetadataConfigDelegate singleton = null;
    
    private MetadataConfigDelegateImpl() {
        
    }
    
    public static IMetadataConfigDelegate getInstance() {
        if (singleton == null) {
            singleton = new MetadataConfigDelegateImpl();
        }
        return singleton;
    }
    
    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */
    
    @Override
    public void refreshMetadata(ExecutionContext executionContext) {
        DynamicDAOFactory.getInstance(executionContext).rebuildDAOFactoryInParalell(executionContext);
    }

    @Override
    public Map<String, List<String>> validateMetadata(DynamicDTO businessObject, ExecutionContext executionContext) {
       return MetadataValidationDAO.getInstance().validateMetadata(businessObject, executionContext);
    }
}
