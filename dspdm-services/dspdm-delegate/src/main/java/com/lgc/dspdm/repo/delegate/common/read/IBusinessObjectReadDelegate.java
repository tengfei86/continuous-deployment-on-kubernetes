package com.lgc.dspdm.repo.delegate.common.read;

import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.join.BaseJoinClause;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicPK;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IBusinessObjectReadDelegate {

    public int count(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext);

    public DynamicDTO readOne(DynamicPK dynamicPK, ExecutionContext executionContext);

    public List<DynamicDTO> readSimple(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext);

    public Map<String, Object> readComplex(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext);

    public List<DynamicDTO> readLongRangeUsingInClause(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter, String boAttrNameForInClause, Object[] uniqueValuesForInClause, Operator operator, ExecutionContext executionContext);

    public List<DynamicDTO> readLongRangeUsingInClauseAndExistingFilters(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter, String boAttrNameForInClause, Object[] uniqueValuesForInClause, Operator operator, ExecutionContext executionContext);

    public List<DynamicDTO> readLongRangeUsingCompositeORClause(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter, List<String> uniqueBoAttrNames, Collection<DynamicDTO> uniqueBoAttrNamesValues, ExecutionContext executionContext);

    public String getCurrentUserDBSchemaName(ExecutionContext executionContext);

    public String getCurrentUserDBName(ExecutionContext executionContext);

    public List<String> getAllEntityNamesListOfConnectedDB(DynamicDTO dynamicDTO, Set<String> exclusionList, ExecutionContext executionContext);
}
