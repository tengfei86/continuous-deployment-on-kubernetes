package com.lgc.dspdm.service.common.dynamic.write;

import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.repo.delegate.BusinessDelegateFactory;
import com.lgc.dspdm.service.common.dynamic.BaseDynamicService;

import java.util.List;
import java.util.Map;


public class DynamicWriteService extends BaseDynamicService implements IDynamicWriteService {
    private static DynamicWriteService singleton = null;
    
    
    private DynamicWriteService() {
        
    }
    
    public static IDynamicWriteService getInstance() {
        if (singleton == null) {
            singleton = new DynamicWriteService();
        }
        return singleton;
    }
    
    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */
    
    @Override
    public SaveResultDTO saveOrUpdate(Map<String, List<DynamicDTO>> mapToSaveOrUpdate, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getBusinessObjectWriteDelegate(executionContext).saveOrUpdate(mapToSaveOrUpdate, executionContext);
    }
    
    @Override
    public SaveResultDTO delete(Map<String, List<DynamicDTO>> mapToDelete, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getBusinessObjectWriteDelegate(executionContext).delete(mapToDelete, executionContext);
    }
    
    @Override
    public SaveResultDTO softDelete(Map<String, List<DynamicDTO>> mapToDelete, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getBusinessObjectWriteDelegate(executionContext).softDelete(mapToDelete, executionContext);
    }
    
    @Override
	public SaveResultDTO deleteAllSearchIndexes(ExecutionContext executionContext) {
		return BusinessDelegateFactory.getBOSearchDelegate(executionContext).deleteAllSearchIndexes(executionContext);
	}

    @Override
    public SaveResultDTO deleteAllSearchIndexesForBoName(String boName, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getBOSearchDelegate(executionContext).deleteAllSearchIndexesForBoName(boName, executionContext);
    }

    @Override
	public SaveResultDTO createSearchIndexForAllBusinessObjects(ExecutionContext executionContext) {
		return BusinessDelegateFactory.getBOSearchDelegate(executionContext).createSearchIndexForAllBusinessObjects(executionContext);
	}

    @Override
    public SaveResultDTO createSearchIndexForBoName(String boName, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getBOSearchDelegate(executionContext).createSearchIndexForBoName(boName, executionContext);
    }
}
