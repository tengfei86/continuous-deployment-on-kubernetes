package com.lgc.dspdm.repo.delegate.common.write;

import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.repo.delegate.BaseTransactionalDelegate;

import java.util.List;
import java.util.Map;

public class BusinessObjectWriteDelegate extends BaseTransactionalDelegate implements IBusinessObjectWriteDelegate {
    
    private static IBusinessObjectWriteDelegate singleton = null;
    
    private BusinessObjectWriteDelegate(ExecutionContext executionContext) {
        super(executionContext);
    }
    
    public static IBusinessObjectWriteDelegate getInstance(ExecutionContext executionContext) {
        if (singleton == null) {
            singleton = new BusinessObjectWriteDelegate(executionContext);
        }
        return singleton;
    }
    
    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */

    @Override
    public Integer[] getNextFromSequence(String boName, String sequenceName, int count, ExecutionContext executionContext) {
        Integer[] sequenceNumbers = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL the DB
            sequenceNumbers = BusinessObjectWriteDelegateImpl.getInstance().getNextFromSequence(boName, sequenceName, count, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        return sequenceNumbers;
    }

    @Override
    public Integer[] getNextFromSequenceFromDataModelDB(String boName, String sequenceName, int count, ExecutionContext executionContext) {
        Integer[] sequenceNumbers = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL the DB
            sequenceNumbers = BusinessObjectWriteDelegateImpl.getInstance().getNextFromSequenceFromDataModelDB(boName, sequenceName, count, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        return sequenceNumbers;
    }
    @Override
    public SaveResultDTO saveOrUpdate(Map<String, List<DynamicDTO>> mapToSave, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL SAVE OR UPDATE for all the dynamic dto objects in the map
            saveResultDTO = BusinessObjectWriteDelegateImpl.getInstance().saveOrUpdate(mapToSave, executionContext);
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
    public SaveResultDTO delete(Map<String, List<DynamicDTO>> mapToDelete, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL DELETE for all the dynamic dto objects in the map
            saveResultDTO = BusinessObjectWriteDelegateImpl.getInstance().delete(mapToDelete, executionContext);
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
    public SaveResultDTO softDelete(Map<String, List<DynamicDTO>> mapToDelete, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL DELETE for all the dynamic dto objects in the map
            saveResultDTO = BusinessObjectWriteDelegateImpl.getInstance().softDelete(mapToDelete, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        return saveResultDTO;
    }
}
