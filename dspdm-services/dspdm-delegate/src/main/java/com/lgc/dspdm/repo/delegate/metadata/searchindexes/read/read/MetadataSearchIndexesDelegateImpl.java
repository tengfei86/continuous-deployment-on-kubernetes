package com.lgc.dspdm.repo.delegate.metadata.searchindexes.read.read;

import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.core.dao.dynamic.businessobject.DynamicDAOFactory;
import com.lgc.dspdm.repo.delegate.metadata.boattr.read.MetadataBOAttrReadDelegateImpl;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MetadataSearchIndexesDelegateImpl implements IMetadataSearchIndexesDelegate {
    private static DSPDMLogger logger = new DSPDMLogger(MetadataSearchIndexesDelegateImpl.class);
    private static IMetadataSearchIndexesDelegate singleton = null;
    
    private MetadataSearchIndexesDelegateImpl() {
        
    }
    
    public static IMetadataSearchIndexesDelegate getInstance() {
        if (singleton == null) {
            singleton = new MetadataSearchIndexesDelegateImpl();
        }
        return singleton;
    }
    
    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */
    
    @Override
    public List<DynamicDTO> read(String boName, ExecutionContext executionContext) {
        return DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(boName, executionContext).readActiveMetadataSearchIndexes(executionContext);
    }
}
