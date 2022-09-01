package com.lgc.dspdm.core.dao.dynamic;

import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.join.BaseJoinClause;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicPK;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Muhammad Imran Ansari
 */
public interface IDynamicDAOImpl {

    public List<String> getPrimaryKeyColumnNames();

    public int count(BusinessObjectInfo businessObjectInfo, boolean hasSQLExpression, Connection connection, ExecutionContext executionContext);
    
    public List<DynamicDTO> read(BusinessObjectInfo businessObjectInfo, boolean hasSQLExpression, Connection connection, ExecutionContext executionContext);
    
    public List<DynamicDTO> readCopy(List<DynamicDTO> businessObjectsToRead, Connection connection, ExecutionContext executionContext);

    public List<DynamicDTO> readCopyRetainChildren(List<DynamicDTO> businessObjectsToRead, Connection connection, ExecutionContext executionContext);

    public List<DynamicDTO> readCopyWithCustomBoAttrNamesFilter(List<DynamicDTO> businessObjectsToRead, Connection connection, ExecutionContext executionContext, String... boAttrNames);

    public List<DynamicDTO> readCopyAndVerifyBoAttrNamesExists(List<DynamicDTO> boListToDelete, Connection connection, ExecutionContext executionContext, String... boAttrNamesToVerify);

    public List<DynamicDTO> readCopyAndVerifyIdExists(List<DynamicDTO> boListToDelete, Connection connection, ExecutionContext executionContext);

    public List<DynamicDTO> readLongRangeUsingInClause(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter, String boAttrNameForInClause, Object[] uniqueValuesForInClause, Operator operator, Connection connection, ExecutionContext executionContext);

    public List<DynamicDTO> readLongRangeUsingInClauseAndExistingFilters(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter, String boAttrNameForInClause, Object[] uniqueValuesForInClause, Operator operator, Connection connection, ExecutionContext executionContext);

    public List<DynamicDTO> readLongRangeUsingCompositeORClause(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter, List<String> uniqueBoAttrNames, Collection<DynamicDTO> values, Connection connection, ExecutionContext executionContext);

    public Map<DynamicPK, DynamicDTO> readCopyAsMap(List<DynamicDTO> businessObjectsToRead, Connection connection, ExecutionContext executionContext);
    
    public DynamicDTO readOne(DynamicPK dynamicPK, Connection connection, ExecutionContext executionContext);
    
    public SaveResultDTO saveOrUpdate(List<DynamicDTO> businessObjectsToSave, boolean hasSQLExpression, Connection connection, ExecutionContext executionContext);
    
    public SaveResultDTO delete(List<DynamicDTO> businessObjectsToDelete, boolean hasSQLExpression, Connection connection, ExecutionContext executionContext);
    
    public SaveResultDTO deleteAll(Connection connection, ExecutionContext executionContext);
    
    public SaveResultDTO softDelete(List<DynamicDTO> businessObjectsToDelete, boolean hasSQLExpression, Connection connection, ExecutionContext executionContext);

    public Integer[] getNextFromSequence(String boName, String sequenceName, int count, Connection connection, ExecutionContext executionContext);
    
    public SaveResultDTO addCustomAttributes(List<DynamicDTO> boListToSave, Connection serviceDBConnection, Connection dataModelDBConnection, ExecutionContext executionContext);

    public SaveResultDTO updateCustomAttributes(List<DynamicDTO> boListToUpdate, Connection dataModelDBConnection, Connection serviceDBConnection, ExecutionContext executionContext);
    
    public SaveResultDTO deleteCustomAttributes(List<DynamicDTO> boListToDelete, Connection dataModelDBConnection, Connection serviceDBConnection, ExecutionContext executionContext);

    public SaveResultDTO introduceNewBusinessObjects(List<DynamicDTO> boListToSave, Connection dataModelDBConnection, Connection serviceDBConnection, ExecutionContext executionContext);

    public SaveResultDTO dropBusinessObjects(List<DynamicDTO> boListToDelete, Connection dataModelDBConnection, Connection serviceDBConnection, ExecutionContext executionContext);

    public SaveResultDTO dropBusinessObjectRelationships(List<DynamicDTO> businessObjectsRelationshipsToBeDropped, Connection dataModelDBConnection, Connection serviceDBConnection, ExecutionContext executionContext);

    public SaveResultDTO addBusinessObjectRelationships(List<DynamicDTO> busObjRelationshipsToBeCreated, Connection dataModelDBConnection, Connection serviceDBConnection, ExecutionContext executionContext);

    public SaveResultDTO addUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeCreated, Connection serviceDBConnection, Connection dataModelDBConnection, ExecutionContext executionContext);

    public SaveResultDTO addSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeCreated, Connection serviceDBConnection, Connection dataModelDBConnection, ExecutionContext executionContext);

    public SaveResultDTO generateMetadataForExistingTable(List<DynamicDTO> boListToGenerateMetadata, Set<String> exclusionList, Connection serviceDBConnection, Connection dataModelDBConnection, ExecutionContext executionContext);

    public SaveResultDTO deleteMetadataForExistingTable(List<DynamicDTO> boListToDeleteMetadata, Connection serviceDBConnection, ExecutionContext executionContext);

    public SaveResultDTO addBusinessObjectGroup(List<DynamicDTO> busObjGroupsToBeCreated, Connection serviceDBConnection, ExecutionContext executionContext);

    public  List<String> getAllEntityNamesListOfConnectedDB(DynamicDTO dynamicDTO, Set<String> exclusionList, Connection dataModelDBConnection, ExecutionContext executionContext);

    public List<String[]> getAttributeUnitMapping(String boName, Connection serviceDBConnection, ExecutionContext executionContext);

    public List<DynamicDTO> restoreBusinessObjectGroupFromHistory(List<DynamicDTO> boList, Connection serviceDBConnection, ExecutionContext executionContext);


}

