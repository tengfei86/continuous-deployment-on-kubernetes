package com.lgc.dspdm.service.common.fixed.metadata.write;

import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.List;
import java.util.Set;

/**
 * Business interface to define common business methods
 *
 * @author Muhammad Imran Ansari
 * @since 09-Jan-2020
 */
public interface IMetadataWriteService {

    public void refreshMetadata(ExecutionContext executionContext);
    
    public SaveResultDTO addCustomAttributes(List<DynamicDTO> boListToSave, ExecutionContext executionContext);

    public SaveResultDTO updateCustomAttributes(List<DynamicDTO> boListToUpdate, ExecutionContext executionContext);

    public SaveResultDTO deleteCustomAttributes(List<DynamicDTO> boListToSave, ExecutionContext executionContext);

    public SaveResultDTO introduceNewBusinessObjects(List<DynamicDTO> boListToSave, ExecutionContext executionContext);

    public SaveResultDTO dropBusinessObjects(List<DynamicDTO> boListToSave, ExecutionContext executionContext);

    public SaveResultDTO dropBusinessObjectRelationships(List<DynamicDTO> busObjRelationshipsToDrop, ExecutionContext executionContext);

    public SaveResultDTO addBusinessObjectRelationships(List<DynamicDTO> busObjRelationshipsToCreate, ExecutionContext executionContext);

    public SaveResultDTO addUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeCreated, ExecutionContext executionContext);

    public SaveResultDTO addSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeCreated, ExecutionContext executionContext);

    public SaveResultDTO dropUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeDropped, ExecutionContext executionContext);

    public SaveResultDTO dropSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeDropped, ExecutionContext executionContext);

    public SaveResultDTO generateMetadataForExistingTable(List<DynamicDTO> boListToGenerateMetadata, Set<String> exclusionList, ExecutionContext executionContext);

    public SaveResultDTO deleteMetadataForExistingTable(List<DynamicDTO> boListToDeleteMetadata,  ExecutionContext executionContext);
    
    public SaveResultDTO addBusinessObjectGroups(List<DynamicDTO> busObjGroupsToBeCreated, ExecutionContext executionContext);

}
