package com.lgc.dspdm.repo.delegate.referencedata.read;

import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.repo.delegate.BaseDelegate;

import java.util.List;
import java.util.Map;

public class ReferenceDataReadDelegate extends BaseDelegate implements IReferenceDataReadDelegate {
    
    private static IReferenceDataReadDelegate singleton = null;
    
    private ReferenceDataReadDelegate(ExecutionContext executionContext) {
        super(executionContext);
    }
    
    public static IReferenceDataReadDelegate getInstance(ExecutionContext executionContext) {
        if (singleton == null) {
            singleton = new ReferenceDataReadDelegate(executionContext);
        }
        return singleton;
    }
    
    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */
    
    @Override
    public Map<String, PagedList<DynamicDTO>> read(String boName, ExecutionContext executionContext) {
        Map<String, PagedList<DynamicDTO>> map = null;
        try {
            map = ReferenceDataReadDelegateImpl.getInstance().read(boName, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        } 
        return map;
    }
    
    @Override
    public Map<String, PagedList<DynamicDTO>> read(String boName, List<DynamicDTO> metadataList, ExecutionContext executionContext) {
        Map<String, PagedList<DynamicDTO>> map = null;
        try {
            map = ReferenceDataReadDelegateImpl.getInstance().read(boName, metadataList, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        } 
        return map;
    }

    @Override
    public Map<String, PagedList<DynamicDTO>> readFromCurrent(String boName, ExecutionContext executionContext) {
        Map<String, PagedList<DynamicDTO>> map = null;
        try {
            map = ReferenceDataReadDelegateImpl.getInstance().readFromCurrent(boName, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return map;
    }

    @Override
    public Map<String, PagedList<DynamicDTO>> readFromCurrent(String boName, List<DynamicDTO> metadataList, ExecutionContext executionContext) {
        Map<String, PagedList<DynamicDTO>> map = null;
        try {
            map = ReferenceDataReadDelegateImpl.getInstance().readFromCurrent(boName, metadataList, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return map;
    }
}
