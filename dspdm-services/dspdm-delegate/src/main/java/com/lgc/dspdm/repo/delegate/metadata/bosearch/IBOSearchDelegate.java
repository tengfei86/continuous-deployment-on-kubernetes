package com.lgc.dspdm.repo.delegate.metadata.bosearch;

import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.List;

/**
 * @author rao.alikhan
 */

public interface IBOSearchDelegate {
    /**
     * save or updates the records in database
     *
     * @return
     */
    public SaveResultDTO createSearchIndexForBusinessObjects(List<DynamicDTO> businessObjectList, String boName, ExecutionContext executionContext);

    public SaveResultDTO createSearchIndexForAllBusinessObjects(ExecutionContext executionContext);

    public SaveResultDTO createSearchIndexForBoName(String boName, ExecutionContext executionContext);

    public SaveResultDTO delete(List<DynamicDTO> businessObjectList, String boName, ExecutionContext executionContext);

    public SaveResultDTO softDelete(List<DynamicDTO> businessObjectList, String boName, ExecutionContext executionContext);

    public PagedList<DynamicDTO> search(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext);

	public SaveResultDTO deleteAllSearchIndexes(ExecutionContext executionContext);

    public SaveResultDTO deleteAllSearchIndexesForBoName(String boName, ExecutionContext executionContext);
}
