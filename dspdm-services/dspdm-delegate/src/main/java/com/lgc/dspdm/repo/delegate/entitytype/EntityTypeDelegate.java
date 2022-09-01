package com.lgc.dspdm.repo.delegate.entitytype;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.repo.delegate.BaseDelegate;

import java.util.List;

/**
 * This class will perform validations on the provided data. These validation rules are stored in R ENTITY TYPE table
 *
 * @author muhammadimran.ansari, changmin sun
 * @since 02-Feb-2021
 */
public class EntityTypeDelegate extends BaseDelegate implements IEntityTypeDelegate {

    private static IEntityTypeDelegate singleton = null;

    private EntityTypeDelegate(ExecutionContext executionContext) {
        super(executionContext);
    }

    public static IEntityTypeDelegate getInstance(ExecutionContext executionContext) {
        if (singleton == null) {
            singleton = new EntityTypeDelegate(executionContext);
        }
        return singleton;
    }

    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */

    @Override
    public void validateREntityTypeForSave(String boName, List<DynamicDTO> boListToSave, ExecutionContext executionContext) {
        try {
            EntityTypeDelegateImpl.getInstance().validateREntityTypeForSave(boName, boListToSave, executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
    }
}
