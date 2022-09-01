package com.lgc.dspdm.repo.delegate.custom;

import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateColumn;
import com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateFunction;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.repo.delegate.common.read.BusinessObjectReadDelegateImpl;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomDelegateImpl implements ICustomDelegate {
    private static DSPDMLogger logger = new DSPDMLogger(CustomDelegateImpl.class);
    private static ICustomDelegate singleton = null;

    private CustomDelegateImpl() {

    }

    public static ICustomDelegate getInstance() {
        if (singleton == null) {
            singleton = new CustomDelegateImpl();
        }
        return singleton;
    }

    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */

    @Override
    public PagedList<DynamicDTO> readBOEVolumeData(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        // read max volume date from whole table
        Timestamp maxVolumeDate = null;
        BusinessObjectInfo tempBusinessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.WELL_VOL_DAILY, executionContext);
        tempBusinessObjectInfo.addAggregateColumnsToSelect(new AggregateColumn(AggregateFunction.MAX, DSPDMConstants.BoAttrName.VOLUME_DATE, "MAX_VOLUME_DATE"));
        tempBusinessObjectInfo.setReadFirst(true);
        List<DynamicDTO> result = BusinessObjectReadDelegateImpl.getInstance().readSimple(tempBusinessObjectInfo, executionContext);
        if (CollectionUtils.hasValue(result)) {
            maxVolumeDate = (Timestamp) result.get(0).get("MAX_VOLUME_DATE");
        }
        // read boe_volume and boe_volume_ouom for all UWI
        Map<String, DynamicDTO> boeVolumeMap = new HashMap<>();
        tempBusinessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.WELL_VOL_DAILY, executionContext);
        tempBusinessObjectInfo.addColumnsToSelect(DSPDMConstants.BoAttrName.UWI, DSPDMConstants.BoAttrName.BOE_VOLUME);
        tempBusinessObjectInfo.addFilter(DSPDMConstants.BoAttrName.VOLUME_DATE, maxVolumeDate);
        tempBusinessObjectInfo.setReadAllRecords(true);
        result = BusinessObjectReadDelegateImpl.getInstance().readSimple(tempBusinessObjectInfo, executionContext);
        if (CollectionUtils.hasValue(result)) {
            for (DynamicDTO dynamicDTO : result) {
                boeVolumeMap.put((String) dynamicDTO.get(DSPDMConstants.BoAttrName.UWI), dynamicDTO);
            }
        }
        // read all unique well_id and UWI
        tempBusinessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.WELL_VOL_DAILY, executionContext);
        tempBusinessObjectInfo.addColumnsToSelect(DSPDMConstants.BoAttrName.WELL_ID, DSPDMConstants.BoAttrName.UWI);
        tempBusinessObjectInfo.setReadWithDistinct(true);
        tempBusinessObjectInfo.setReadAllRecords(true);
        tempBusinessObjectInfo.addOrderByAsc(DSPDMConstants.BoAttrName.UWI);
        result = BusinessObjectReadDelegateImpl.getInstance().readSimple(tempBusinessObjectInfo, executionContext);
        // now iterate on unique records and add max volume data and boe volume data
        if (CollectionUtils.hasValue(result)) {
            DynamicDTO boeVolumeDTO = null;
            String uwi = null;
            for (DynamicDTO dynamicDTO : result) {
                dynamicDTO.put("MAX_VOLUME_DATE", maxVolumeDate);
                uwi = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.UWI);
                boeVolumeDTO = boeVolumeMap.get(uwi);
                if (boeVolumeDTO != null) {
                    dynamicDTO.put(DSPDMConstants.BoAttrName.BOE_VOLUME, boeVolumeDTO.get(DSPDMConstants.BoAttrName.BOE_VOLUME));
                }
            }
        }
        return new PagedList<>(result.size(), result);
    }
}
