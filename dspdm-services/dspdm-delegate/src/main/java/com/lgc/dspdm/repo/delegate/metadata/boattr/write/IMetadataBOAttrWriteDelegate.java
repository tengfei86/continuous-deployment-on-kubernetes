package com.lgc.dspdm.repo.delegate.metadata.boattr.write;

import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.List;
import java.util.Set;

public interface IMetadataBOAttrWriteDelegate {
    public SaveResultDTO addCustomAttributes(List<DynamicDTO> boListToSave, ExecutionContext executionContext);
    public SaveResultDTO updateCustomAttributes(List<DynamicDTO> boListToUpdate, ExecutionContext executionContext);
    public SaveResultDTO deleteCustomAttributes(List<DynamicDTO> boListToDelete, ExecutionContext executionContext);
    public SaveResultDTO introduceNewBusinessObjects(List<DynamicDTO> boListToSave, ExecutionContext executionContext);
    public SaveResultDTO dropBusinessObjects(List<DynamicDTO> businessObjectsToBeDropped, ExecutionContext executionContext);
    public SaveResultDTO dropBusinessObjectRelationships(List<DynamicDTO> businessObjectsRelationshipsToBeDropped, ExecutionContext executionContext);
    public SaveResultDTO addBusinessObjectRelationships(List<DynamicDTO> busObjRelationshipsToCreate, ExecutionContext executionContext);
    public SaveResultDTO addUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeCreated, ExecutionContext executionContext);
    public SaveResultDTO addSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeCreated, ExecutionContext executionContext);
    public SaveResultDTO dropUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeDropped, ExecutionContext executionContext);
    public SaveResultDTO dropSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeDropped, ExecutionContext executionContext);
    public SaveResultDTO generateMetadataForExistingTable(List<DynamicDTO> boListToGenerateMetadata, Set<String> exclusionList, ExecutionContext executionContext);
    public SaveResultDTO deleteMetadataForExistingTable(List<DynamicDTO> boListToDeleteMetadata, ExecutionContext executionContext);
    public SaveResultDTO addBusinessObjectGroups(List<DynamicDTO> busObjGroupsToBeCreated, ExecutionContext executionContext);
}
