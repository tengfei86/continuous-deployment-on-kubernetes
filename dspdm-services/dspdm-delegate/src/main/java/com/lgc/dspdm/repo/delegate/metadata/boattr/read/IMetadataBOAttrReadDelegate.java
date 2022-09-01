package com.lgc.dspdm.repo.delegate.metadata.boattr.read;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.List;
import java.util.Map;

public interface IMetadataBOAttrReadDelegate {
    
    public DynamicDTO readMetadataBusinessObject(String boName, ExecutionContext executionContext);
    
    public List<String> readBOAttrNames(String boName, ExecutionContext executionContext);

    public Map<String, Object> getDefaultValuesMap(String boName, ExecutionContext executionContext);
    
    public List<DynamicDTO> read(String boName, ExecutionContext executionContext);
    
    public Map<String, DynamicDTO> readMap(String boName, ExecutionContext executionContext);
}
