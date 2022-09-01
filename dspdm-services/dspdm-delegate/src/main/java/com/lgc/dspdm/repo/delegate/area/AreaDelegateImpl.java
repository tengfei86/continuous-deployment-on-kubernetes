package com.lgc.dspdm.repo.delegate.area;

import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.dao.dynamic.businessobject.DynamicDAOFactory;
import com.lgc.dspdm.repo.delegate.BaseDelegate;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AreaDelegateImpl extends BaseDelegate implements IAreaDelegate {
    
    private static IAreaDelegate singleton = null;
    
    private AreaDelegateImpl(ExecutionContext executionContext) {
        super(executionContext);
    }
    
    public static IAreaDelegate getInstance(ExecutionContext executionContext) {
        if (singleton == null) {
            singleton = new AreaDelegateImpl(executionContext);
        }
        return singleton;
    }

    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */
    
    @Override
    public Map<Object, Integer> saveAreaLevel(String boName, List<DynamicDTO> boListToSave, SaveResultDTO rootSaveResultDTO, ExecutionContext executionContext) {
        Map<Object, Integer> idMap = new LinkedHashMap<>(boListToSave.size());
        for (DynamicDTO dynamicDTO : boListToSave) {
            Object id = dynamicDTO.get(DSPDMConstants.BoAttrName.R_AREA_LEVEL_ID);
            if (id != null) {
                if (id instanceof String) {
                    // make primary key column to null so that it is a candidate for insert
                    dynamicDTO.put(DSPDMConstants.BoAttrName.R_AREA_LEVEL_ID, null);
                }
                idMap.put(id, null);
            }
        }
        rootSaveResultDTO.addResult(DynamicDAOFactory.getInstance(executionContext)
                .getDynamicDAO(boName, executionContext).saveOrUpdate(boListToSave, executionContext));
        int index = 0;
        for (Object key : idMap.keySet()) {
            idMap.replace(key, (Integer) boListToSave.get(index).get(DSPDMConstants.BoAttrName.R_AREA_LEVEL_ID));
            index++;
        }
        return idMap;
    }
    
    @Override
    public SaveResultDTO saveArea(String boName, List<DynamicDTO> boListToSave, Map<Object, Integer> areaLevelIdsMap, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // 1. fix area level ids in area objects to be saved
        for (DynamicDTO dynamicDTO : boListToSave) {
            Object areaLevelId = dynamicDTO.get(DSPDMConstants.BoAttrName.R_AREA_LEVEL_ID);
            Object areaId = dynamicDTO.get(DSPDMConstants.BoAttrName.AREA_ID);
            if (areaLevelId instanceof String && areaLevelIdsMap != null && areaLevelIdsMap.size() > 0) {
                dynamicDTO.put(DSPDMConstants.BoAttrName.R_AREA_LEVEL_ID, areaLevelIdsMap.get(areaLevelId));
            }
            if (areaId instanceof String) {
                dynamicDTO.put(DSPDMConstants.BoAttrName.AREA_ID, null);
            }
        }
        // 2. save area list
        saveResultDTO.addResult(DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(boName, executionContext).saveOrUpdate(boListToSave, executionContext));
        
        // 3. save area children if any
        for (DynamicDTO dynamicDTO : boListToSave) {
            if (dynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY) != null) {
                Map<String, DynamicDTO> children = (Map<String, DynamicDTO>) dynamicDTO.get(DSPDMConstants.DSPDM_REQUEST.CHILDREN_KEY);
                if (children.size() > 0) {
                    // 4. set parent id in all the children
                    List<DynamicDTO> childrenDTO = new ArrayList<>(boListToSave.size());
                    for (String key : children.keySet()) {
                        childrenDTO = (List<DynamicDTO>) children.get(key);
                        for (DynamicDTO childDynamicDTO : childrenDTO) {
                            childDynamicDTO.put(DSPDMConstants.BoAttrName.PARENT_AREA_ID, dynamicDTO.get(DSPDMConstants.BoAttrName.AREA_ID));
                        }
                    }
                    // 5. save children recursive call
                    saveResultDTO.addResult(saveArea(boName, childrenDTO, areaLevelIdsMap, executionContext));
                }
            }
        }
        return saveResultDTO;
    }
}
