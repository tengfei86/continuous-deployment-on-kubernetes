package com.lgc.dspdm.repo.delegate.metadata.boattr.read;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.repo.delegate.BaseDelegate;

import java.util.List;
import java.util.Map;

/**
 * @author Muhammad Imran Ansari
 */
public class MetadataBOAttrReadDelegate extends BaseDelegate implements IMetadataBOAttrReadDelegate {
    private static IMetadataBOAttrReadDelegate singleton = null;
    
    private MetadataBOAttrReadDelegate(ExecutionContext executionContext) {
        super(executionContext);
    }
    
    public static IMetadataBOAttrReadDelegate getInstance(ExecutionContext executionContext) {
        if (singleton == null) {
            singleton = new MetadataBOAttrReadDelegate(executionContext);
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
            list = MetadataBOAttrReadDelegateImpl.getInstance().read(boName, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return list;
    }
    
    @Override
    public Map<String, DynamicDTO> readMap(String boName, ExecutionContext executionContext) {
        Map<String, DynamicDTO> map = null;
        try {
            map = MetadataBOAttrReadDelegateImpl.getInstance().readMap(boName, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return map;
    }
    
    @Override
    public List<String> readBOAttrNames(String boName, ExecutionContext executionContext) {
        List<String> list = null;
        try {
            list = MetadataBOAttrReadDelegateImpl.getInstance().readBOAttrNames(boName, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return list;
    }

    @Override
    public Map<String, Object> getDefaultValuesMap(String boName, ExecutionContext executionContext) {
        Map<String, Object> map = null;
        try {
            map = MetadataBOAttrReadDelegateImpl.getInstance().getDefaultValuesMap(boName, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return map;
    }
        
    @Override
    public DynamicDTO readMetadataBusinessObject(String boName, ExecutionContext executionContext) {
        DynamicDTO dynamicDTO = null;
        try {
            dynamicDTO = MetadataBOAttrReadDelegateImpl.getInstance().readMetadataBusinessObject(boName, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return dynamicDTO;
    }
}
