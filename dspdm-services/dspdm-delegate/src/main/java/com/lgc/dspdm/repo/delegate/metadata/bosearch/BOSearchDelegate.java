package com.lgc.dspdm.repo.delegate.metadata.bosearch;

import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.repo.delegate.BaseTransactionalDelegate;

import java.util.List;

/**
 * @author rao.alikhan
 */
public class BOSearchDelegate extends BaseTransactionalDelegate implements IBOSearchDelegate {

    private static IBOSearchDelegate singleton = null;

    private BOSearchDelegate(ExecutionContext executionContext) {
        super(executionContext);
    }

    public static IBOSearchDelegate getInstance(ExecutionContext executionContext) {
        if (singleton == null) {
            singleton = new BOSearchDelegate(executionContext);
        }
        return singleton;
    }

    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */

    @Override
    public SaveResultDTO createSearchIndexForBusinessObjects(List<DynamicDTO> businessObjectList, String boName, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL SAVE OR UPDATE for all the dynamic dto objects in the map
            saveResultDTO = BOSearchDelegateImpl.getInstance(executionContext).createSearchIndexForBusinessObjects(businessObjectList, boName, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO createSearchIndexForAllBusinessObjects(ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL SAVE OR UPDATE for all the dynamic dto objects in the map
            saveResultDTO = BOSearchDelegateImpl.getInstance(executionContext).createSearchIndexForAllBusinessObjects(executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO createSearchIndexForBoName(String boName, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL SAVE OR UPDATE for all the dynamic dto objects in the map
            saveResultDTO = BOSearchDelegateImpl.getInstance(executionContext).createSearchIndexForBoName(boName, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO delete(List<DynamicDTO> businessObjectList, String boName, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL DELETE for all the dynamic dto objects in the map
            saveResultDTO = BOSearchDelegateImpl.getInstance(executionContext).delete(businessObjectList, boName, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO deleteAllSearchIndexes(ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL DELETE for all the dynamic dto objects in the map
            saveResultDTO = BOSearchDelegateImpl.getInstance(executionContext).deleteAllSearchIndexes(executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO deleteAllSearchIndexesForBoName(String boName, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL DELETE for all the dynamic dto objects in the map
            saveResultDTO = BOSearchDelegateImpl.getInstance(executionContext).deleteAllSearchIndexesForBoName(boName, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO softDelete(List<DynamicDTO> businessObjectList, String boName, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL DELETE for all the dynamic dto objects in the map
            saveResultDTO = BOSearchDelegateImpl.getInstance(executionContext).softDelete(businessObjectList, boName, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public PagedList<DynamicDTO> search(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        PagedList<DynamicDTO> boSearchDynamicDTOList = null;
        try {
            boSearchDynamicDTOList = BOSearchDelegateImpl.getInstance(executionContext).search(businessObjectInfo, executionContext);
        } catch (Throwable e) {
            DSPDMException.throwException(e, executionContext);
        }
        return boSearchDynamicDTOList;
    }
}
