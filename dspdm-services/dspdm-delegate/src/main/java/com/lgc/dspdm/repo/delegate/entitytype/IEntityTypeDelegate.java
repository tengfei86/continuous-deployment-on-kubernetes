package com.lgc.dspdm.repo.delegate.entitytype;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.List;

/**
 * This class will perform validations on the provided data. These validation rules are stored in R ENTITY TYPE table
 *
 * @author muhammadimran.ansari, changmin sun
 * @since  02-Feb-2021
 */
public interface IEntityTypeDelegate {
    public void validateREntityTypeForSave(String boName, List<DynamicDTO> boListToSave, ExecutionContext executionContext);
}
