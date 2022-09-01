package com.lgc.dspdm.repo.delegate.metadata.bo.read;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.List;

public interface IMetadataBODelegate {
        
    public List<DynamicDTO> readMetadataAllBusinessObjects(ExecutionContext executionContext);
}
