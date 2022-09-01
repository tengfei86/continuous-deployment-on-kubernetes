package com.lgc.dspdm.repo.delegate.metadata.config;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.List;
import java.util.Map;

public interface IMetadataConfigDelegate {
    
    public void refreshMetadata(ExecutionContext executionContext);

    public Map<String, List<String>> validateMetadata(DynamicDTO businessObject, ExecutionContext executionContext);
}
