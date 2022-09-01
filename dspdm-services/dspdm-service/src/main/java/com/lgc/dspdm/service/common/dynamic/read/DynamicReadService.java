package com.lgc.dspdm.service.common.dynamic.read;

import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.join.BaseJoinClause;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicPK;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.repo.delegate.BusinessDelegateFactory;
import com.lgc.dspdm.service.common.dynamic.BaseDynamicService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class DynamicReadService extends BaseDynamicService implements IDynamicReadService {
    private static DynamicReadService singleton = null;


    private DynamicReadService() {

    }

    public static IDynamicReadService getInstance() {
        if (singleton == null) {
            singleton = new DynamicReadService();
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
    public Map<String, List<String>> validateMetadata(DynamicDTO businessObject, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataReadDelegate(executionContext).validateMetadata(businessObject, executionContext);
    }

    @Override
    public DynamicDTO readOne(DynamicPK dynamicPK, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getBusinessObjectReadDelegate(executionContext).readOne(dynamicPK, executionContext);
    }

    @Override
    public int count(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getBusinessObjectReadDelegate(executionContext).count(businessObjectInfo, executionContext);
    }

    @Override
    public List<DynamicDTO> read(String boName, ExecutionContext executionContext) {
        BusinessObjectInfo referenceBusinessObjectInfo = new BusinessObjectInfo(boName, executionContext);
        // read all records of metadata table
        referenceBusinessObjectInfo.setReadAllRecords(false);
        return readSimple(referenceBusinessObjectInfo, executionContext);
    }

    @Override
    public List<DynamicDTO> readSimple(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getBusinessObjectReadDelegate(executionContext).readSimple(businessObjectInfo, executionContext);
    }

    @Override
    public List<DynamicDTO> readLongRangeUsingInClause(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter,
                                                       String boAttrNameForInClause, Object[] uniqueValuesForInClause,
                                                       Operator operator, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getBusinessObjectReadDelegate(executionContext)
                .readLongRangeUsingInClause(businessObjectInfo, joinClauseToApplyFilter, boAttrNameForInClause,
                        uniqueValuesForInClause, operator, executionContext);
    }

    @Override
    public List<DynamicDTO> readLongRangeUsingInClauseAndExistingFilters(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter,
                                                                         String boAttrNameForInClause, Object[] uniqueValuesForInClause,
                                                                         Operator operator, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getBusinessObjectReadDelegate(executionContext)
                .readLongRangeUsingInClauseAndExistingFilters(businessObjectInfo, joinClauseToApplyFilter, boAttrNameForInClause,
                        uniqueValuesForInClause, operator, executionContext);
    }

    @Override
    public List<DynamicDTO> readLongRangeUsingCompositeORClause(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter,
                                                                List<String> uniqueBoAttrNames, Collection<DynamicDTO> uniqueBoAttrNamesValues,
                                                                ExecutionContext executionContext) {
        return BusinessDelegateFactory.getBusinessObjectReadDelegate(executionContext)
                .readLongRangeUsingCompositeORClause(businessObjectInfo, joinClauseToApplyFilter, uniqueBoAttrNames,
                        uniqueBoAttrNamesValues, executionContext);
    }

    @Override
    public List<DynamicDTO> readMetadataAllBusinessObjects(ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataBODelegate(executionContext).readMetadataAllBusinessObjects(executionContext);
    }

    @Override
    public DynamicDTO readMetadataBusinessObject(String boName, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataBOAttrReadDelegate(executionContext).readMetadataBusinessObject(boName, executionContext);
    }

    @Override
    public List<String> readMetadataAttrNamesForBOName(String boName, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataBOAttrReadDelegate(executionContext).readBOAttrNames(boName, executionContext);
    }

    @Override
    public Map<String, Object> getDefaultValuesMap(String boName, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataBOAttrReadDelegate(executionContext).getDefaultValuesMap(boName, executionContext);
    }

    @Override
    public Map<String, Object> readWithDetails(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getBusinessObjectReadDelegate(executionContext).readComplex(businessObjectInfo, executionContext);
    }

    @Override
    public List<DynamicDTO> readMetadataForBOName(String boName, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataBOAttrReadDelegate(executionContext).read(boName, executionContext);
    }

    @Override
    public Map<String, DynamicDTO> readMetadataMapForBOName(String boName, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataBOAttrReadDelegate(executionContext).readMap(boName, executionContext);
    }

    @Override
    public List<DynamicDTO> readMetadataConstraintsForBOName(String boName, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataConstraintsReadDelegate(executionContext).read(boName, executionContext);
    }

    @Override
    public List<DynamicDTO> readMetadataSearchIndexesForBOName(String boName, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataSearchIndexesReadDelegate(executionContext).read(boName, executionContext);
    }

    @Override
    public Map<String, List<DynamicDTO>> readMetadataChildRelationshipsMap(String boName, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataRelationshipsDelegate(executionContext).readMetadataChildRelationshipsMap(boName, executionContext);
    }

    @Override
    public Map<String, List<DynamicDTO>> readMetadataParentRelationshipsMap(String boName, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataRelationshipsDelegate(executionContext).readMetadataParentRelationshipsMap(boName, executionContext);
    }

    @Override
    public Map<String, PagedList<DynamicDTO>> readReferenceDataForBOName(String boName, ExecutionContext executionContext) {
        return readReferenceDataForBOName(boName, null, executionContext);
    }

    @Override
    public Map<String, PagedList<DynamicDTO>> readReferenceDataForBOName(String boName, List<DynamicDTO> metadataList, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getReferenceDataReadDelegate(executionContext).read(boName, metadataList, executionContext);
    }

    @Override
    public PagedList<DynamicDTO> search(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getBOSearchDelegate(executionContext).search(businessObjectInfo, executionContext);
    }

    @Override
    public PagedList<DynamicDTO> readBOEVolumeData(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getCustomDelegate(executionContext).readBOEVolumeData(businessObjectInfo, executionContext);
    }

    @Override
    public void validateREntityTypeForSave(String boName, List<DynamicDTO> boListToSave, ExecutionContext executionContext) {
        BusinessDelegateFactory.getEntityTypeDelegate(executionContext).validateREntityTypeForSave(boName, boListToSave, executionContext);
    }

    @Override
    public Integer[] getNextFromSequence(String boName, String sequenceName, int count, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getBusinessObjectWriteDelegate(executionContext).getNextFromSequence(boName, sequenceName, count, executionContext);
    }

    @Override
    public Integer[] getNextFromSequenceFromDataModelDB(String boName, String sequenceName, int count, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getBusinessObjectWriteDelegate(executionContext).getNextFromSequenceFromDataModelDB(boName, sequenceName, count, executionContext);
    }

    @Override
    public String getCurrentUserDBName(ExecutionContext executionContext) {
        return BusinessDelegateFactory.getBusinessObjectReadDelegate(executionContext).getCurrentUserDBName(executionContext);
    }

    @Override
    public String getCurrentUserDBSchemaName(ExecutionContext executionContext) {
        return BusinessDelegateFactory.getBusinessObjectReadDelegate(executionContext).getCurrentUserDBSchemaName(executionContext);
    }

    @Override
    public List<String> getAllEntityNamesListOfConnectedDB(DynamicDTO dynamicDTO, Set<String> exclusionList, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getBusinessObjectReadDelegate(executionContext).getAllEntityNamesListOfConnectedDB(dynamicDTO, exclusionList, executionContext);
    }

    @Override
    public DynamicDTO getBoHierarchyForBusinessObject(DynamicDTO dynamicDTO, ExecutionContext executionContext) {
        return BusinessDelegateFactory.getMetadataRelationshipsDelegate(executionContext).readBoHierarchy(dynamicDTO, executionContext);
    }
}
