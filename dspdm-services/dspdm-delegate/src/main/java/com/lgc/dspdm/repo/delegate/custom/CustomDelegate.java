package com.lgc.dspdm.repo.delegate.custom;

import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicPK;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.repo.delegate.BaseDelegate;
import com.lgc.dspdm.repo.delegate.common.read.BusinessObjectReadDelegateImpl;
import com.lgc.dspdm.repo.delegate.common.read.IBusinessObjectReadDelegate;

import java.util.List;
import java.util.Map;

public class CustomDelegate extends BaseDelegate implements ICustomDelegate {

    private static ICustomDelegate singleton = null;

    private CustomDelegate(ExecutionContext executionContext) {
        super(executionContext);
    }

    public static ICustomDelegate getInstance(ExecutionContext executionContext) {
        if (singleton == null) {
            singleton = new CustomDelegate(executionContext);
        }
        return singleton;
    }

    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */

    @Override
    public PagedList<DynamicDTO> readBOEVolumeData(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        PagedList<DynamicDTO> list = null;
        try {
            list = CustomDelegateImpl.getInstance().readBOEVolumeData(businessObjectInfo, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return list;
    }
}
