package com.lgc.dspdm.repo.delegate.metadata.searchindexes.read.read;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.List;
import java.util.Map;

public interface IMetadataSearchIndexesDelegate {
    public List<DynamicDTO> read(String boName, ExecutionContext executionContext);
}
