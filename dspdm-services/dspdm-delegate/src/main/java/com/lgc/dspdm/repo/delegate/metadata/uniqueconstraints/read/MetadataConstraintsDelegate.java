package com.lgc.dspdm.repo.delegate.metadata.uniqueconstraints.read;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.repo.delegate.BaseDelegate;

import java.util.List;
import java.util.Map;

public class MetadataConstraintsDelegate extends BaseDelegate implements IMetadataConstraintsDelegate {
    private static IMetadataConstraintsDelegate singleton = null;
    
    private MetadataConstraintsDelegate(ExecutionContext executionContext) {
        super(executionContext);
    }
    
    public static IMetadataConstraintsDelegate getInstance(ExecutionContext executionContext) {
        if (singleton == null) {
            singleton = new MetadataConstraintsDelegate(executionContext);
        }
        return singleton;
    }
    
    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */
    
    @Override
    public List<DynamicDTO> read(String boName, ExecutionContext executionContext) {
        List<DynamicDTO> list = null;
        try {
            list = MetadataConstraintsDelegateImpl.getInstance().read(boName, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return list;
    }

    @Override
    public Map<String, Object> readParentConstraints(String boName, ExecutionContext executionContext) {
        Map<String, Object> map = null;
        try {
            map = MetadataConstraintsDelegateImpl.getInstance().readParentConstraints(boName, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return map;
    }

    @Override
    public Map<String, Object> readParentConstraints(String boName, List<DynamicDTO> metadataList, ExecutionContext executionContext) {
        Map<String, Object> map = null;
        try {
            map = MetadataConstraintsDelegateImpl.getInstance().readParentConstraints(boName, metadataList, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return map;
    }
}
