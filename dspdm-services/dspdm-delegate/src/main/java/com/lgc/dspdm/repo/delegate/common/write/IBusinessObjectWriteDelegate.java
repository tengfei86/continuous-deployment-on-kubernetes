package com.lgc.dspdm.repo.delegate.common.write;

import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.List;
import java.util.Map;

public interface IBusinessObjectWriteDelegate {

    public Integer[] getNextFromSequence(String boName, String sequenceName, int count, ExecutionContext executionContext);

    public Integer[] getNextFromSequenceFromDataModelDB(String boName, String sequenceName, int count, ExecutionContext executionContext);
    /**
     * save or updates the records in database
     * 
     * @param mapToSaveOrUpdate
     * @return
     */
    public SaveResultDTO saveOrUpdate(Map<String, List<DynamicDTO>> mapToSaveOrUpdate, ExecutionContext executionContext);
    
    public SaveResultDTO delete(Map<String, List<DynamicDTO>> mapToDelete, ExecutionContext executionContext);
    
    public SaveResultDTO softDelete(Map<String, List<DynamicDTO>> mapToDelete, ExecutionContext executionContext);
}
