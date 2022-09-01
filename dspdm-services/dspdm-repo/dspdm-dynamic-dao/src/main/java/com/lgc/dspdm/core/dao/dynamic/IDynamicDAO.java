package com.lgc.dspdm.core.dao.dynamic;

import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.join.BaseJoinClause;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicPK;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Muhammad Imran Ansari
 */
public interface IDynamicDAO {

    public List<String> getPrimaryKeyColumnNames();

    public int count(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext);

    public List<DynamicDTO> read(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext);

    public List<DynamicDTO> readCopy(List<DynamicDTO> businessObjectsToRead, ExecutionContext executionContext);

    public List<DynamicDTO> readCopyRetainChildren(List<DynamicDTO> businessObjectsToRead, ExecutionContext executionContext);

    public Map<DynamicPK, DynamicDTO> readCopyAsMap(List<DynamicDTO> businessObjectsToRead, ExecutionContext executionContext);

    public List<DynamicDTO> readCopyWithCustomFilter(List<DynamicDTO> boListToReadCopy, ExecutionContext executionContext, String... boAttrNamesToVerify);

    public List<DynamicDTO> readLongRangeUsingInClause(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter, String boAttrNameForInClause, Object[] uniqueValuesForInClause, Operator operator, ExecutionContext executionContext);

    public List<DynamicDTO> readLongRangeUsingInClauseAndExistingFilters(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter, String boAttrNameForInClause, Object[] uniqueValuesForInClause, Operator operator, ExecutionContext executionContext);

    public List<DynamicDTO> readLongRangeUsingCompositeORClause(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter, List<String> uniqueBoAttrNames, Collection<DynamicDTO> uniqueBoAttrNamesValues, ExecutionContext executionContext);

    public DynamicDTO readMetadataBusinessObject(ExecutionContext executionContext);

    public List<String> readMetadataBOAttrNames(ExecutionContext executionContext);

    public Map<String, Object> getDefaultValuesMap(ExecutionContext executionContext);

    public List<DynamicDTO> readMetadata(ExecutionContext executionContext);

    public Map<String, DynamicDTO> readMetadataMap(ExecutionContext executionContext);

    public List<DynamicDTO> readActiveMetadata(ExecutionContext executionContext);

    public List<DynamicDTO> readMetadataConstraints(ExecutionContext executionContext);

    public List<DynamicDTO> readActiveMetadataConstraints(ExecutionContext executionContext);

    public List<DynamicDTO> readActiveMetadataSearchIndexes(ExecutionContext executionContext);

    public List<DynamicDTO> readMetadataRelationships(ExecutionContext executionContext);

    public Map<String, List<DynamicDTO>> readMetadataParentRelationships(ExecutionContext executionContext);

    public Map<String, List<DynamicDTO>> readMetadataChildRelationships(ExecutionContext executionContext);

    public DynamicDTO readOne(DynamicPK dynamicPK, ExecutionContext executionContext);

    public SaveResultDTO saveOrUpdate(List<DynamicDTO> businessObjectsToSave, ExecutionContext executionContext);

    public Integer[] getNextFromSequence(String boName, String sequenceName, int count, ExecutionContext executionContext);

    public Integer[] getNextFromSequenceFromDataModelDB(String boName, String sequenceName, int count, ExecutionContext executionContext);

    public SaveResultDTO delete(List<DynamicDTO> businessObjectsToDelete, ExecutionContext executionContext);
    
    public SaveResultDTO deleteAll(ExecutionContext executionContext);

    public SaveResultDTO softDelete(List<DynamicDTO> businessObjectsToDelete, ExecutionContext executionContext);

    public SaveResultDTO addCustomAttributes(List<DynamicDTO> businessObjectsToSave, ExecutionContext executionContext);

    public SaveResultDTO updateCustomAttributes(List<DynamicDTO> businessObjectsToUpdate, ExecutionContext executionContext);

    public SaveResultDTO deleteCustomAttributes(List<DynamicDTO> businessObjectsToSave, ExecutionContext executionContext);

    public SaveResultDTO introduceNewBusinessObjects(List<DynamicDTO> businessObjectsToSave, ExecutionContext executionContext);

    public SaveResultDTO dropBusinessObjects(List<DynamicDTO> businessObjectsToBeDropped, ExecutionContext executionContext);

    public SaveResultDTO dropBusinessObjectRelationships(List<DynamicDTO> businessObjectsRelationshipsToBeDropped, ExecutionContext executionContext);

    public SaveResultDTO addBusinessObjectRelationships(List<DynamicDTO> busObjRelationshipsToCreate, ExecutionContext executionContext);

    public SaveResultDTO addUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeCreated, ExecutionContext executionContext);

    public SaveResultDTO addSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeCreated, ExecutionContext executionContext);

    public SaveResultDTO dropUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeDropped, ExecutionContext executionContext);

    public SaveResultDTO dropSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeDropped, ExecutionContext executionContext);

    public SaveResultDTO generateMetadataForExistingTable(List<DynamicDTO> boListToGenerateMetadata, Set<String> exclusionList, ExecutionContext executionContext);

    public SaveResultDTO deleteMetadataForExistingTable(List<DynamicDTO> boListToDeleteMetadata, ExecutionContext executionContext);

    public SaveResultDTO addBusinessObjectGroup(List<DynamicDTO> busObjGroupsToBeCreated, ExecutionContext executionContext);

    public List<String> getAllEntityNamesListOfConnectedDB(DynamicDTO dynamicDTO, Set<String> exclusionList, ExecutionContext executionContext);

    public boolean isMSSQLServerDialect(String type);

    public String getCurrentUserDBSchemaName(ExecutionContext executionContext);

    public String getCurrentUserDBName(ExecutionContext executionContext);
}
