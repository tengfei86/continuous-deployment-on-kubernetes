package com.lgc.dspdm.repo.delegate.metadata.relationships.read;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.repo.delegate.BaseDelegate;

import java.util.List;
import java.util.Map;

public class MetadataRelationshipsDelegate extends BaseDelegate implements IMetadataRelationshipsDelegate {
    private static IMetadataRelationshipsDelegate singleton = null;
    
    private MetadataRelationshipsDelegate(ExecutionContext executionContext) {
        super(executionContext);
    }
    
    public static IMetadataRelationshipsDelegate getInstance(ExecutionContext executionContext) {
        if (singleton == null) {
            singleton = new MetadataRelationshipsDelegate(executionContext);
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
            list = MetadataRelationshipsDelegateImpl.getInstance().read(boName, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return list;
    }

    @Override
    public Map<String, List<DynamicDTO>> readMetadataChildRelationshipsMap(String boName, ExecutionContext executionContext) {
        Map<String, List<DynamicDTO>> map = null;
        try {
            map = MetadataRelationshipsDelegateImpl.getInstance().readMetadataChildRelationshipsMap(boName, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return map;
    }

    @Override
    public Map<String, List<DynamicDTO>> readMetadataParentRelationshipsMap(String boName, ExecutionContext executionContext) {
        Map<String, List<DynamicDTO>> map = null;
        try {
            map = MetadataRelationshipsDelegateImpl.getInstance().readMetadataParentRelationshipsMap(boName, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return map;
    }

    @Override
    public DynamicDTO readBoHierarchy(DynamicDTO dynamicDTO, ExecutionContext executionContext) {
        DynamicDTO boHierarchy = null;
        try {
            boHierarchy = MetadataRelationshipsDelegateImpl.getInstance().readBoHierarchy(dynamicDTO, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return boHierarchy;
    }
}
