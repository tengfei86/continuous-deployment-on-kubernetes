package com.lgc.dspdm.repo.delegate.metadata.uniqueconstraints.read;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.List;
import java.util.Map;

public interface IMetadataConstraintsDelegate {
    public List<DynamicDTO> read(String boName, ExecutionContext executionContext);

    public Map<String, Object> readParentConstraints(String boName, ExecutionContext executionContext);

    public Map<String, Object> readParentConstraints(String boName, List<DynamicDTO> metadataList, ExecutionContext executionContext);
}
