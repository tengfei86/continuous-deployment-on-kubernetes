package com.lgc.dspdm.repo.delegate.referencedata.read;

import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.List;
import java.util.Map;

public interface IReferenceDataReadDelegate {
    public Map<String, PagedList<DynamicDTO>> read(String boName, ExecutionContext executionContext);

    public Map<String, PagedList<DynamicDTO>> read(String boName, List<DynamicDTO> metadataList, ExecutionContext executionContext);

    public Map<String, PagedList<DynamicDTO>> readFromCurrent(String boName, ExecutionContext executionContext);

    public Map<String, PagedList<DynamicDTO>> readFromCurrent(String boName, List<DynamicDTO> metadataList, ExecutionContext executionContext);
}
