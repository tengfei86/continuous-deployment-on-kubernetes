package com.lgc.dspdm.repo.delegate.metadata.config;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.repo.delegate.BaseDelegate;

import java.util.List;
import java.util.Map;

/**
 * @author Muhammad Imran Ansari
 */
public class MetadataConfigDelegate extends BaseDelegate implements IMetadataConfigDelegate {
    private static IMetadataConfigDelegate singleton = null;
    
    private MetadataConfigDelegate(ExecutionContext executionContext) {
        super(executionContext);
    }
    
    public static IMetadataConfigDelegate getInstance(ExecutionContext executionContext) {
        if (singleton == null) {
            singleton = new MetadataConfigDelegate(executionContext);
        }
        return singleton;
    }
    
    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */
    
    @Override
    public void refreshMetadata(ExecutionContext executionContext) {
        try {
            MetadataConfigDelegateImpl.getInstance().refreshMetadata(executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
    }

    @Override
    public Map<String, List<String>> validateMetadata(DynamicDTO businessObject, ExecutionContext executionContext) {
        try {
          return MetadataConfigDelegateImpl.getInstance().validateMetadata(businessObject, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return null;
    }
}
