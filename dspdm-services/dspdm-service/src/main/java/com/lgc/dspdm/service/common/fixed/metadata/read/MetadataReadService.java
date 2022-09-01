package com.lgc.dspdm.service.common.fixed.metadata.read;

import com.lgc.dspdm.service.common.fixed.metadata.BaseMetadataService;

/**
 * Metadata class to provide metadata reading functionality
 * This class will deal only with the fixed structure metadata classes. It will not deal with dynamic classes
 *
 * @author Muhammad Imran Ansari
 * @since 09-Jan-2020
 */
public class MetadataReadService extends BaseMetadataService implements IMetadataReadService {
    private static MetadataReadService singleton = null;
    
    
    private MetadataReadService() {
        
    }
    
    public static IMetadataReadService getInstance() {
        if (singleton == null) {
            singleton = new MetadataReadService();
        }
        return singleton;
    }
    
    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */
}
