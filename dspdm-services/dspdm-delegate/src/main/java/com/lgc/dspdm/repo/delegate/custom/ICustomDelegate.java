package com.lgc.dspdm.repo.delegate.custom;

import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;

public interface ICustomDelegate {
    public PagedList<DynamicDTO> readBOEVolumeData(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext);
}
