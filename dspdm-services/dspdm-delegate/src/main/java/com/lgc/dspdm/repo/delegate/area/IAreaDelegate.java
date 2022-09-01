package com.lgc.dspdm.repo.delegate.area;

import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.List;
import java.util.Map;

/**
 * @author rao.alikhan
 */
public interface IAreaDelegate {
    
    public Map<Object, Integer> saveAreaLevel(String boName, List<DynamicDTO> boListToSave, SaveResultDTO rootSaveResultDTO, ExecutionContext executionContext);
    
    public SaveResultDTO saveArea(String boName, List<DynamicDTO> boListToSave, Map<Object, Integer> areaLevelIdsMap, ExecutionContext executionContext);
}
