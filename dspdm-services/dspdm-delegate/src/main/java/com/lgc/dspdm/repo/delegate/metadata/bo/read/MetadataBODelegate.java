package com.lgc.dspdm.repo.delegate.metadata.bo.read;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.repo.delegate.BaseDelegate;

import java.util.List;

/**
 * @author Muhammad Imran Ansari
 */
public class MetadataBODelegate extends BaseDelegate implements IMetadataBODelegate {
    private static IMetadataBODelegate singleton = null;
    
    private MetadataBODelegate(ExecutionContext executionContext) {
        super(executionContext);
    }
    
    public static IMetadataBODelegate getInstance(ExecutionContext executionContext) {
        if (singleton == null) {
            singleton = new MetadataBODelegate(executionContext);
        }
        return singleton;
    }
    
    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */
    
    @Override
    public List<DynamicDTO> readMetadataAllBusinessObjects(ExecutionContext executionContext) {
        List<DynamicDTO> list = null;
        try {
            list = MetadataBODelegateImpl.getInstance().readMetadataAllBusinessObjects(executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return list;
    }
}
