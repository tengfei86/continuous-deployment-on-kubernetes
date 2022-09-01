package com.lgc.dspdm.service.common.fixed.metadata.write;

import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.repo.delegate.BusinessDelegateFactory;
import com.lgc.dspdm.service.common.fixed.metadata.BaseMetadataService;

import java.util.List;
import java.util.Set;

/**
 * Metadata class to provide metadata writing functionality
 * This class will deal only with the fixed structure metadata classes. It will not deal with dynamic classes
 *
 * @author Muhammad Imran Ansari
 * @since 09-Jan-2020
 */
public class MetadataWriteService extends BaseMetadataService implements IMetadataWriteService {
    private static MetadataWriteService singleton = null;
    
    
    private MetadataWriteService() {
        
    }
    
    public static MetadataWriteService getInstance() {
        if (singleton == null) {
            singleton = new MetadataWriteService();
        }
        return singleton;
    }
    
    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */
    
    @Override
    public void refreshMetadata(ExecutionContext executionContext) {
        BusinessDelegateFactory.getMetadataReadDelegate(executionContext).refreshMetadata(executionContext);
    }
    
    @Override
    public SaveResultDTO addCustomAttributes(List<DynamicDTO> boListToSave, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataBOAttrWriteDelegate(executionContext).addCustomAttributes(boListToSave, executionContext);
    }

    @Override
    public SaveResultDTO updateCustomAttributes(List<DynamicDTO> boListToUpdate, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataBOAttrWriteDelegate(executionContext).updateCustomAttributes(boListToUpdate, executionContext);
    }

    @Override
    public SaveResultDTO deleteCustomAttributes(List<DynamicDTO> boListToSave, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataBOAttrWriteDelegate(executionContext).deleteCustomAttributes(boListToSave, executionContext);
    }

    @Override
    public SaveResultDTO introduceNewBusinessObjects(List<DynamicDTO> boListToSave, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataBOAttrWriteDelegate(executionContext).introduceNewBusinessObjects(boListToSave, executionContext);
    }

    @Override
    public SaveResultDTO dropBusinessObjects(List<DynamicDTO> boListToSave, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataBOAttrWriteDelegate(executionContext).dropBusinessObjects(boListToSave, executionContext);
    }

    @Override
    public SaveResultDTO dropBusinessObjectRelationships(List<DynamicDTO> busObjRelationshipsToDrop, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataBOAttrWriteDelegate(executionContext).dropBusinessObjectRelationships(busObjRelationshipsToDrop, executionContext);
    }

    @Override
    public SaveResultDTO addBusinessObjectRelationships(List<DynamicDTO> busObjRelationshipsToCreate, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataBOAttrWriteDelegate(executionContext).addBusinessObjectRelationships(busObjRelationshipsToCreate, executionContext);
    }

    @Override
    public SaveResultDTO addUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeCreated, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataBOAttrWriteDelegate(executionContext).addUniqueConstraints(busObjAttrUniqueConstraintsToBeCreated, executionContext);
    }

    @Override
    public SaveResultDTO addSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeCreated, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataBOAttrWriteDelegate(executionContext).addSearchIndexes(busObjAttrSearchIndexesToBeCreated, executionContext);
    }

    @Override
    public SaveResultDTO dropUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeDropped, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataBOAttrWriteDelegate(executionContext).dropUniqueConstraints(busObjAttrUniqueConstraintsToBeDropped, executionContext);
    }

    @Override
    public SaveResultDTO dropSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeDropped, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataBOAttrWriteDelegate(executionContext).dropSearchIndexes(busObjAttrSearchIndexesToBeDropped, executionContext);
    }

    @Override
    public SaveResultDTO generateMetadataForExistingTable(List<DynamicDTO> boListToGenerateMetadata, Set<String> exclusionList, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataBOAttrWriteDelegate(executionContext).generateMetadataForExistingTable(boListToGenerateMetadata, exclusionList, executionContext);
    }

    @Override
    public SaveResultDTO deleteMetadataForExistingTable(List<DynamicDTO> boListToDeleteMetadata, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataBOAttrWriteDelegate(executionContext).deleteMetadataForExistingTable(boListToDeleteMetadata, executionContext);
    }
    
    @Override
    public SaveResultDTO addBusinessObjectGroups(List<DynamicDTO> busObjGroupsToBeCreated, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataBOAttrWriteDelegate(executionContext).addBusinessObjectGroups(busObjGroupsToBeCreated, executionContext);
    }
}
