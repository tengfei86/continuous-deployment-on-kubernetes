package com.lgc.dspdm.msp.mainservice.utils;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.criteria.CriteriaFilter;
import com.lgc.dspdm.core.common.data.criteria.DSPDMUnit;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.*;
import com.lgc.dspdm.msp.mainservice.model.BOQuery;
import com.lgc.dspdm.service.common.dynamic.read.IDynamicReadService;
import com.lgc.dspdm.service.utils.excel.ExcelExportUtil;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author rao.alikhan
 */
public class ExportHelper {

    /**
     * This will execute read operation and write in excel sheet. If requested records are more than the expected then it will execute the read operations in chunks
     *
     * @param businessObjectInfo Business object info to read
     * @param metadata           Metadata for requested business object
     * @param searchApplied      flag whether the postgres search is applied or not
     * @param dynamicReadService Dynamic read service for read business objects
     * @param executionContext   Execution context
     * @return Will return sheet data in byte array
     * @author rao.alikhan
     * @since 28-09-2020
     */
    public static void readAndConvertToExcelBinary(BusinessObjectInfo businessObjectInfo, Workbook workbook, String sheetName,
                                                     List<DynamicDTO> metadata, boolean searchApplied, IDynamicReadService dynamicReadService,
                                                     ExecutionContext executionContext) {
        int totalRecords = 0;
        if (searchApplied) {
            // get business object info BO_SEARCH
            BusinessObjectInfo businessObjectInfoForBOSearch = getBusinessObjectInfoForBOSearch(businessObjectInfo, executionContext);
            totalRecords = dynamicReadService.count(businessObjectInfoForBOSearch, executionContext);
        } else {
            totalRecords = dynamicReadService.count(businessObjectInfo, executionContext);
        }
        if (totalRecords > 0) {
            // fetch the total records to read
            int totalRecordsToRead = businessObjectInfo.isReadAllRecords() ? totalRecords :
                    (totalRecords < businessObjectInfo.getPagination().getRecordsToRead()) ? totalRecords : businessObjectInfo.getPagination().getRecordsToRead();

            if (totalRecordsToRead > totalRecords) {
                totalRecordsToRead = totalRecords;
            }
            if (totalRecordsToRead > ConfigProperties.getInstance().max_records_to_export.getIntegerValue()) {
                throw new DSPDMException("Cannot read '{}' records. More than '{}' records are not allowed to export",
                        executionContext.getExecutorLocale(), totalRecordsToRead, ConfigProperties.getInstance().max_records_to_export.getIntegerValue());
            }
            // !Important
            // Following block will make the variable totalRecordsToRead the exact multiple of the max page size.
            // it will get the remainder and add the upper part of remainder to the total records
            // so that it becomes the exact multiple of max page size
            totalRecordsToRead = NumberUtils.convertFirstNumberToMultipleOfSecond(totalRecordsToRead, ConfigProperties.getInstance().max_page_size.getIntegerValue());
            // if total records is greater than max page size then export in a loop
            if (totalRecordsToRead >= ConfigProperties.getInstance().max_page_size.getIntegerValue()) {
                exportExcelFilesInChunks(totalRecordsToRead, businessObjectInfo, workbook, sheetName, metadata, searchApplied, dynamicReadService, executionContext);

            } else {
                // get the business objects data of requested business object
                List<DynamicDTO> businessObjects = readBusinessObjects(businessObjectInfo, searchApplied, dynamicReadService, executionContext);
                convertBusinessObjectsToExcelBinary(businessObjects, businessObjectInfo, workbook, sheetName, metadata, dynamicReadService, executionContext);
            }
        } else {
            List<DynamicDTO> businessObjects = new ArrayList<>(0);
            convertBusinessObjectsToExcelBinary(businessObjects, businessObjectInfo, workbook, sheetName, metadata, dynamicReadService, executionContext);
        }
    }

    /**
	 * @param businessObjects
	 * @param businessObjectInfo
	 * @param metadata
	 * @param dynamicReadService
	 * @param executionContext
	 * @return
	 */
	private static void convertBusinessObjectsToExcelBinary(List<DynamicDTO> businessObjects,
			BusinessObjectInfo businessObjectInfo, Workbook workbook, String sheetName,
			List<DynamicDTO> metadata, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        // covert data to the file
        sheetName= StringUtils.hasValue(sheetName)? sheetName:businessObjectInfo.getBusinessObjectType();
        ExcelExportUtil.exportToExcelSingleSheetStream(workbook, sheetName, metadata,
                businessObjects, executionContext);
    }

    /**
     * This will construct business object info for business object 'BO_SEARCH'
     *
     * @param businessObjectInfo Business object info required for constructing business object for 'BO_SEARCH'
     * @param executionContext   Execution Context
     * @return Will return business object info for Business object 'BO_SEARCH'
     * @author rao.alikhan
     * @since 28-09-2020
     */
    private static BusinessObjectInfo getBusinessObjectInfoForBOSearch(BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext) {
        // prepare business object info for bo search and get total count
        BusinessObjectInfo boSearchBusinessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BO_SEARCH, executionContext);
        boSearchBusinessObjectInfo.addColumnsToSelect(DSPDMConstants.BoAttrName.PK_BO_ATTR_NAMES, DSPDMConstants.BoAttrName.PK_BO_ATTR_VALUES);
        boSearchBusinessObjectInfo.addFilter(DSPDMConstants.BoAttrName.BO_NAME, businessObjectInfo.getBusinessObjectType());
        // find search keywords from original business object info
        Object[] searchKeywordsExact = (businessObjectInfo.getFilterList() == null) ? null : businessObjectInfo.getFilterList().getValuesWithOperator(Operator.JSONB_FIND_EXACT);
        Object[] searchKeywordsLike = (businessObjectInfo.getFilterList() == null) ? null : businessObjectInfo.getFilterList().getValuesWithOperator(Operator.JSONB_FIND_LIKE);
        Object[] searchKeywords = null;
        Operator jsonbExactOrLikeOperator = null;
        if ((CollectionUtils.hasValue(searchKeywordsExact)) && (CollectionUtils.hasValue(searchKeywordsLike))) {
            throw new DSPDMException("Invalid search criteria filter found with operator {}", executionContext.getExecutorLocale(), Operator.JSONB_FIND_LIKE.name() + " and " + Operator.JSONB_FIND_EXACT);
        } else if (CollectionUtils.hasValue(searchKeywordsExact)) {
            // search keywords are already converted to string with lower case
            jsonbExactOrLikeOperator = Operator.JSONB_FIND_EXACT;
            searchKeywords = searchKeywordsExact;
        } else if (CollectionUtils.hasValue(searchKeywordsLike)) {
            // search keywords are already converted to string with lower case
            jsonbExactOrLikeOperator = Operator.JSONB_FIND_LIKE;
            searchKeywords = searchKeywordsLike;
        } else {
            throw new DSPDMException("Unable to perform search. No search criteria filter found with operator {}", executionContext.getExecutorLocale(), Operator.JSONB_FIND_LIKE.name() + " or " + Operator.JSONB_FIND_EXACT);
        }
        boSearchBusinessObjectInfo.addFilter(DSPDMConstants.BoAttrName.SEARCH_JSONB, jsonbExactOrLikeOperator, searchKeywords);
        // now propagate all remaining filters to the search business object info but exclude the filter which has been already added
        if (businessObjectInfo.getFilterList() != null) {
            for (CriteriaFilter filter : businessObjectInfo.getFilterList().getFilters()) {
                if (jsonbExactOrLikeOperator != filter.getOperator()) {
                    boSearchBusinessObjectInfo.addFilter(DSPDMConstants.BoAttrName.OBJECT_JSONB + DSPDMConstants.SPACE + Operator.JSONB_DOT_FOR_TEXT.getOperator() + " '" + filter.getColumnName() + "' ", filter.getOperator(), filter.getValues());
                }
            }
        }
        return boSearchBusinessObjectInfo;
    }

    /**
     * export excel file in chunks streaming
     *
     * @param totalRecordsToRead
     * @param businessObjectInfo
     * @param metadata
     * @param searchApplied
     * @param dynamicReadService
     * @param executionContext
     * @return
     */
    private static void exportExcelFilesInChunks(int totalRecordsToRead, BusinessObjectInfo businessObjectInfo, Workbook workbook, String sheetName,
                                                   List<DynamicDTO> metadata, boolean searchApplied,
                                                   IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        // get the read chunk pages according to the maximum records limit
            double readRecordsChunkCount = Math.ceil(Double.valueOf(totalRecordsToRead)/Double.valueOf(ConfigProperties.getInstance().max_page_size.getIntegerValue()));
        // Excell custome sheet name, if sheet name is empty, use business object name
        sheetName= StringUtils.hasValue(sheetName)? sheetName:businessObjectInfo.getBusinessObjectType();
        // for reading data as chunk need to set read all records to false
        try {
            businessObjectInfo.setReadAllRecords(false);
            // get the user provided staring index to read from that index,
            // it must be outside the loop. Because business object info will change the starting index when pagination updates.
            int readFromIndex = businessObjectInfo.getPagination().getReadFromIndex();
            for (int i = 1; i <= readRecordsChunkCount; i++) {
                // changing the page size and read from index to skip the pages
                businessObjectInfo.getPagination().setPagesToRead(executionContext,
                            i+(Double.valueOf(Math.floor(Double.valueOf(readFromIndex/ConfigProperties.getInstance().max_page_size.getIntegerValue())))).intValue());
                // need to set records per page for every chunk to read
                    businessObjectInfo.setRecordsPerPage((totalRecordsToRead > ConfigProperties.getInstance().max_page_size.getIntegerValue())
                            ? ConfigProperties.getInstance().max_page_size.getIntegerValue() : totalRecordsToRead, executionContext);
                // get the business objects data of requested business object
                List<DynamicDTO> businessObjects = readBusinessObjects(businessObjectInfo, searchApplied, dynamicReadService, executionContext);
                if (i == 1) {
                    // get the workbook instance
                    ExcelExportUtil.createWorkBookSingleSheet(workbook, sheetName, metadata, businessObjects,  executionContext);
                } else {
                    // append the business object data to existing workbook sheet
                    ExcelExportUtil.appendDataToExcelSingleSheet(sheetName, workbook, metadata, businessObjects, executionContext);
                }
                    if (totalRecordsToRead > ConfigProperties.getInstance().max_page_size.getIntegerValue()) {
                        totalRecordsToRead = totalRecordsToRead - ConfigProperties.getInstance().max_page_size.getIntegerValue();
                }
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
    }

    /**
     * This will read the business object from data store. If the search is applied then it will search the data and return business objects
     *
     * @param businessObjectInfo Business object information to execute read operation
     * @param searchApplied      flag whether the postgres search is applied or not
     * @param dynamicReadService Dynamic read service to read business objects
     * @param executionContext   Execution Context
     * @return Will return the business objects of requested business object
     * @author rao.alikhan
     * @since 28-09-2020
     */
    private static List<DynamicDTO> readBusinessObjects(BusinessObjectInfo businessObjectInfo, boolean searchApplied, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        List<DynamicDTO> businessObjects = null;
        if (searchApplied) {
            // search data from search db and fetch list from data store
            businessObjects = dynamicReadService.search(businessObjectInfo, executionContext);
        } else {
            // FETCH LIST FROM DATA STORE
            businessObjects = dynamicReadService.readSimple(businessObjectInfo, executionContext);
        }
        return businessObjects;
    }

}
