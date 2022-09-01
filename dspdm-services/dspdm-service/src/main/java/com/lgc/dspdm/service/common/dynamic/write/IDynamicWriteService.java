package com.lgc.dspdm.service.common.dynamic.write;

import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.List;
import java.util.Map;

public interface IDynamicWriteService {
    public SaveResultDTO saveOrUpdate(Map<String, List<DynamicDTO>> mapToSaveOrUpdate, ExecutionContext executionContext);
    
    public SaveResultDTO delete(Map<String, List<DynamicDTO>> mapToDelete, ExecutionContext executionContext);
    
    public SaveResultDTO softDelete(Map<String, List<DynamicDTO>> mapToDelete, ExecutionContext executionContext);
    
	public SaveResultDTO deleteAllSearchIndexes(ExecutionContext executionContext);

    public SaveResultDTO deleteAllSearchIndexesForBoName(String boName, ExecutionContext executionContext);

	public SaveResultDTO createSearchIndexForAllBusinessObjects(ExecutionContext executionContext);

    public SaveResultDTO createSearchIndexForBoName(String boName, ExecutionContext executionContext);
}
