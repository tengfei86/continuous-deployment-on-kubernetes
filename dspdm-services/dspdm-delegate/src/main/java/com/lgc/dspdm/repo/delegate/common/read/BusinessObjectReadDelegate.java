package com.lgc.dspdm.repo.delegate.common.read;

import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.criteria.join.BaseJoinClause;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicPK;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.repo.delegate.BaseDelegate;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BusinessObjectReadDelegate extends BaseDelegate implements IBusinessObjectReadDelegate {

    private static IBusinessObjectReadDelegate singleton = null;

    private BusinessObjectReadDelegate(ExecutionContext executionContext) {
        super(executionContext);
    }

    public static IBusinessObjectReadDelegate getInstance(ExecutionContext executionContext) {
        if (singleton == null) {
            singleton = new BusinessObjectReadDelegate(executionContext);
        }
        return singleton;
    }

    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */

    @Override
    public int count(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        int count = 0;
        try {
            count = BusinessObjectReadDelegateImpl.getInstance().count(businessObjectInfo, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return count;
    }

    @Override
    public DynamicDTO readOne(DynamicPK dynamicPK, ExecutionContext executionContext) {
        DynamicDTO dynamicDTO = null;
        try {
            dynamicDTO = BusinessObjectReadDelegateImpl.getInstance().readOne(dynamicPK, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return dynamicDTO;
    }

    @Override
    public List<DynamicDTO> readSimple(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        List<DynamicDTO> list = null;
        try {
            list = BusinessObjectReadDelegateImpl.getInstance().readSimple(businessObjectInfo, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return list;
    }

    @Override
    public Map<String, Object> readComplex(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        Map<String, Object> map = null;
        try {
            map = BusinessObjectReadDelegateImpl.getInstance().readComplex(businessObjectInfo, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return map;
    }

    @Override
    public List<DynamicDTO> readLongRangeUsingInClause(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter,
                                                       String boAttrNameForInClause, Object[] uniqueValuesForInClause,
                                                       Operator operator, ExecutionContext executionContext) {
        List<DynamicDTO> list = null;
        try {
            list = BusinessObjectReadDelegateImpl.getInstance().readLongRangeUsingInClause(businessObjectInfo, joinClauseToApplyFilter,
                    boAttrNameForInClause, uniqueValuesForInClause, operator, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return list;
    }

    @Override
    public List<DynamicDTO> readLongRangeUsingInClauseAndExistingFilters(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter,
                                                                         String boAttrNameForInClause, Object[] uniqueValuesForInClause,
                                                                         Operator operator, ExecutionContext executionContext) {
        List<DynamicDTO> list = null;
        try {
            list = BusinessObjectReadDelegateImpl.getInstance().readLongRangeUsingInClauseAndExistingFilters(businessObjectInfo, joinClauseToApplyFilter,
                    boAttrNameForInClause, uniqueValuesForInClause, operator, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return list;
    }

    @Override
    public List<DynamicDTO> readLongRangeUsingCompositeORClause(BusinessObjectInfo businessObjectInfo, BaseJoinClause joinClauseToApplyFilter,
                                                                List<String> uniqueBoAttrNames, Collection<DynamicDTO> uniqueBoAttrNamesValues,
                                                                ExecutionContext executionContext) {
        List<DynamicDTO> list = null;
        try {
            list = BusinessObjectReadDelegateImpl.getInstance().readLongRangeUsingCompositeORClause(businessObjectInfo,
                    joinClauseToApplyFilter, uniqueBoAttrNames, uniqueBoAttrNamesValues, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return list;
    }

    @Override
    public String getCurrentUserDBSchemaName(ExecutionContext executionContext) {
        String dbSchemaName = null;
        try {
            dbSchemaName = BusinessObjectReadDelegateImpl.getInstance().getCurrentUserDBSchemaName(executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return dbSchemaName;
    }

    @Override
    public String getCurrentUserDBName(ExecutionContext executionContext) {
        String dbName = null;
        try {
            dbName = BusinessObjectReadDelegateImpl.getInstance().getCurrentUserDBName(executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return dbName;
    }

    @Override
    public List<String> getAllEntityNamesListOfConnectedDB(DynamicDTO dynamicDTO, Set<String> exclusionList, ExecutionContext executionContext) {
        List<String> entityNamesList = null;
        try {
            entityNamesList = BusinessObjectReadDelegateImpl.getInstance().getAllEntityNamesListOfConnectedDB(dynamicDTO, exclusionList, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return entityNamesList;
    }
}
