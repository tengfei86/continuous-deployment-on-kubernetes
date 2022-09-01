package com.lgc.dspdm.service.common.dynamic.read;

import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.join.BaseJoinClause;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicPK;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.repo.delegate.BusinessDelegateFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IDynamicReadService {
    
    public void refreshMetadata(ExecutionContext executionContext);

    public Map<String, List<String>> validateMetadata(DynamicDTO businessObject, ExecutionContext executionContext);
    
    public int count(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext);
    
    public DynamicDTO readOne(DynamicPK dynamicPK, ExecutionContext executionContext);
    
    public List<DynamicDTO> read(String boName, ExecutionContext executionContext);
    
    public List<DynamicDTO> readSimple(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext);

    public List<DynamicDTO> readLongRangeUsingInClause(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter, String boAttrNameForInClause, Object[] uniqueValuesForInClause, Operator operator, ExecutionContext executionContext);

    public List<DynamicDTO> readLongRangeUsingInClauseAndExistingFilters(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter, String boAttrNameForInClause, Object[] uniqueValuesForInClause, Operator operator, ExecutionContext executionContext);

    public List<DynamicDTO> readLongRangeUsingCompositeORClause(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter, List<String> uniqueBoAttrNames, Collection<DynamicDTO> uniqueBoAttrNamesValues, ExecutionContext executionContext);

    public Map<String, Object> readWithDetails(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext);
    
    public List<DynamicDTO> readMetadataAllBusinessObjects(ExecutionContext executionContext);
    
    public DynamicDTO readMetadataBusinessObject(String boName, ExecutionContext executionContext);
    
    /**
     * Returns metadata bo attributes names in sequence number order and these are case sensitive
     * @param boName
     * @param executionContext
     * @return
     */
    public List<String> readMetadataAttrNamesForBOName(String boName, ExecutionContext executionContext);

    public Map<String, Object> getDefaultValuesMap(String boName, ExecutionContext executionContext);
    
    public List<DynamicDTO> readMetadataForBOName(String boName, ExecutionContext executionContext);
    
    public Map<String, DynamicDTO> readMetadataMapForBOName(String boName, ExecutionContext executionContext);
    
    public List<DynamicDTO> readMetadataConstraintsForBOName(String boName, ExecutionContext executionContext);

    public List<DynamicDTO> readMetadataSearchIndexesForBOName(String boName, ExecutionContext executionContext);

    public Map<String, List<DynamicDTO>> readMetadataChildRelationshipsMap(String boName, ExecutionContext executionContext);

    public Map<String, List<DynamicDTO>> readMetadataParentRelationshipsMap(String boName, ExecutionContext executionContext);
    
    public Map<String, PagedList<DynamicDTO>> readReferenceDataForBOName(String boName, ExecutionContext executionContext);
    
    public Map<String, PagedList<DynamicDTO>> readReferenceDataForBOName(String boName, List<DynamicDTO> metadataList, ExecutionContext executionContext);

    public PagedList<DynamicDTO> search(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext);

    public PagedList<DynamicDTO> readBOEVolumeData(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext);

    public void validateREntityTypeForSave(String boName, List<DynamicDTO> boListToSave, ExecutionContext executionContext);

    public Integer[] getNextFromSequence(String boName, String sequenceName, int count, ExecutionContext executionContext);

    public Integer[] getNextFromSequenceFromDataModelDB(String boName, String sequenceName, int count, ExecutionContext executionContext);

    public String getCurrentUserDBName(ExecutionContext executionContext);

    public String getCurrentUserDBSchemaName(ExecutionContext executionContext);

    public List<String> getAllEntityNamesListOfConnectedDB(DynamicDTO dynamicDTO, Set<String> exclusionList, ExecutionContext executionContext);

    public DynamicDTO getBoHierarchyForBusinessObject(DynamicDTO dynamicDTO, ExecutionContext executionContext);
}
