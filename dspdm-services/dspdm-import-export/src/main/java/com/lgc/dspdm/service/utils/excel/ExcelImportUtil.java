package com.lgc.dspdm.service.utils.excel;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.data.common.BulkImportInvalidRecordReasonDTO;
import com.lgc.dspdm.core.common.data.common.BulkImportResponseDTO;
import com.lgc.dspdm.core.common.data.common.PagedList;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.*;
import com.lgc.dspdm.core.common.util.metadata.MetadataUtils;
import com.lgc.dspdm.repo.delegate.common.write.config.REntityTypeConfig;
import com.lgc.dspdm.service.common.dynamic.read.IDynamicReadService;
import org.apache.poi.ss.usermodel.*;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.NumberFormat;
import java.util.*;
import java.util.stream.Collectors;

public class ExcelImportUtil {

    private static final int MIN_OFFSET = 0;
    private static final int DEFAULT_OFFSET = 0;
    private static final int MIN_LIMIT = 1;
    private static final int DEFAULT_LIMIT = 100;

    private static DSPDMLogger logger = new DSPDMLogger(ExcelImportUtil.class);
    private static NumberFormat numericCellValueToStringFormat = null;

    private static NumberFormat getNumericCellValueToStringFormat() {
        if (numericCellValueToStringFormat == null) {
            numericCellValueToStringFormat = NumberFormat.getInstance();
            // sets the maximum allowed number of decimal places in the decimal part of the number to avoid decimal places being rounded
            numericCellValueToStringFormat.setMaximumFractionDigits(15);
            // sets the minimum number of digits allowed in the decimal part of a number to avoid excess zeros in the decimal place
            numericCellValueToStringFormat.setMinimumFractionDigits(0);
            // remove scientific notation display
            numericCellValueToStringFormat.setGroupingUsed(false);
        }
        return numericCellValueToStringFormat;
    }

    /**
     * Read all available sheet names along with their row counts from the given excel file
     *
     * @param input
     * @param executionContext
     * @return
     * @throws Exception
     * @author Muhammad Imran Ansari
     * @since 11-Nov-2019
     */
    public static Map<String, Integer> getSheetNamesAndRecordsCount(InputStream input, ExecutionContext executionContext) throws Exception {
        return MonitorJBLUtils.getSheetNamesAndRecordsCount(input, executionContext);
    }

    /**
     * Parses the excel file according to the given bo name in params
     * boName coming in params is equal to the sheet name inside excel file case insensitive
     * first row of each sheet is the header row and all header names are string/text fields only
     * limit and offset inside param work like paging inside the excel file data.
     * offset is the 1-based index of first record to be read
     * limit is the total number of records to be read for a particular bo
     * sample params for bo name WELL are below
     * {
     * "WELL": {
     * "language": "en",
     * "timezone": "GMT+08:00",
     * "readBack": true,
     * "offset": 1,
     * "limit": 1000
     * }
     * }
     *
     * @param params              boName as the key and the language, timezone, limit and offset as the map key values
     * @param input               input stream to read excel file
     * @param readOldValuesFromDB a flag true or false to read old (db) values. It will be true in case of bulk import and false in case of simple import
     * @param dynamicReadService  service instance to read metadata and other business object data if needed
     * @param executionContext    current user execution context
     * @return a hash map of the boNames (same as coming from param if found in excel file) and the parsed data as BulkImportResponseDTO
     * @throws Exception
     * @Author Yuan Tian, Muhammad Imran Ansari
     * @since 11-Nov-2019
     */
    public static Map<String, BulkImportResponseDTO> read(Map<String, Object> params, InputStream input,
                                                          boolean readOldValuesFromDB,
                                                          IDynamicReadService dynamicReadService,
                                                          ExecutionContext executionContext) throws Exception {

        // prepare entity type validation data
        Map<String, List<REntityTypeConfig.Node>> entityTypeConfigurationsMap = REntityTypeConfig.getInstance().getConfigMap();
        // prepare final result
        Map<String, BulkImportResponseDTO> result = new LinkedHashMap<>();
        Workbook workbook = null;
        try {
            workbook = MonitorJBLUtils.getWorkbook(input, executionContext);
            for (String boName : params.keySet()) {
                // SET OFFSET AND LIMIT
                Integer offset = ((offset = (Integer) ((Map<String, Object>) params.get(boName)).get("offset")) != null) ? offset : DEFAULT_OFFSET;
                if (offset < MIN_OFFSET) {
                    throw new DSPDMException("Offset for bo name '{}' must be greater or equal to '{}'", executionContext.getExecutorLocale(), boName, MIN_OFFSET);
                }
                Integer limit = ((limit = (Integer) ((Map<String, Object>) params.get(boName)).get("limit")) != null) ? limit : DEFAULT_LIMIT;
                if (limit < MIN_LIMIT) {
                    throw new DSPDMException("Limit for bo name '{}' must be greater or equal to '{}'", executionContext.getExecutorLocale(), boName, MIN_LIMIT);
                } else if (limit > ConfigProperties.getInstance().max_records_to_import.getIntegerValue()) {
                    throw new DSPDMException("Limit for bo name '{}' cannot be greater than '{}'", executionContext.getExecutorLocale(), boName, ConfigProperties.getInstance().max_records_to_import.getIntegerValue());
                }

                Sheet sheet = MonitorJBLUtils.getSheet(workbook, boName, executionContext);
                // get rows iterator
                Iterator<Row> rowIterator = sheet.rowIterator();
                if (!rowIterator.hasNext()) {
                    throw new DSPDMException("Invalid input : Excel sheet for business object name '{}' has no data", executionContext.getExecutorLocale(), boName);
                }
                // Read metadata for the current business object being processed
                Map<String, DynamicDTO> metadataByboAttrName = dynamicReadService.readMetadataMapForBOName(boName, executionContext);
                // Create a map for mandatory fields to be used in data validation, key is the bo_attr_name and the value is display name to be used in error message
                Map<String, String> mandatoryFieldsForExcelSheet = getMandatoryFieldsForExcelSheet(metadataByboAttrName, dynamicReadService, executionContext);
                // Get primary key column names to be set in dynamic dto to generate proper id
                List<String> primaryKeyColumnNames = getPrimaryKeyColumnNames(metadataByboAttrName);
                // identify columns which are not to be read from excel
                List<String> foreignKeyColumnNames = getForeignKeyColumnNames(boName, dynamicReadService, executionContext);
                // first row is the header row
                Row headerRow = rowIterator.next();
                // read column names from header row
                Map<Integer, String> boAttrNamesByCellNumber = getCellNumberToBoAttrNameMapping(boName, headerRow, metadataByboAttrName, mandatoryFieldsForExcelSheet, dynamicReadService, executionContext);
                // create a newor a copy list of all cell numbers. It will be used to remove all the auto genenated columns to be removed from reading list from excel
                List<Integer> headerRowCellNumbers = new ArrayList<>(boAttrNamesByCellNumber.keySet());
                // create a hash map to contain all the data in the forms of columns.
                // the key in the following hash map is the bo attr name and value is another map. Internal value map has integer row index
                // as the key and the value is of type object. Value is the actual cell value that is to be read from the excel file
                Map<String, Map<Integer, Object>> columnValuesByBoAttrName = new LinkedHashMap<>();
                String boAttrName = null;
                for (Integer headerRowCellNumber : headerRowCellNumbers) {
                    boAttrName = boAttrNamesByCellNumber.get(headerRowCellNumber);
                    if ((CollectionUtils.containsIgnoreCase(primaryKeyColumnNames, boAttrName)) || (CollectionUtils.containsIgnoreCase(foreignKeyColumnNames, boAttrName))) {
                        // remove primary key and foreign key columns in ready data from excel
                        // remove column from reading data because this is an id column and it will be auto generated
                        boAttrNamesByCellNumber.remove(headerRowCellNumber);
                    } else {
                        columnValuesByBoAttrName.put(boAttrName, new LinkedHashMap<Integer, Object>());
                    }
                }

                // set unitDynamicDTO
                List<Map<String, String>> unitDynamicDTO = null;
                // get unit row
                Integer unitRow = ((unitRow = (Integer) ((Map<String, Object>) params.get(boName))
                        .get(DSPDMConstants.Units.UNITROW)) != null) ? unitRow : 0;
                // unitRow
                int unitRowIndex = 2;// unit row index is 2 default
                // move row iterator to the next row index that is unitRow
                while ((unitRowIndex < unitRow) && (rowIterator.hasNext())) {
                    rowIterator.next();
                    unitRowIndex++;
                }
                if (unitRow > 0 && rowIterator.hasNext()) {
                    // second row is the unit row
                    Row rowUnit = rowIterator.next();
                    // check whether unit row
                    unitDynamicDTO = readUnitMap(rowUnit, boAttrNamesByCellNumber);
                }

                // create an empty map to hold all invalid records which fail the criteria of mandatory fields or wrong data type or invalid value
                Map<Integer, DynamicDTO> invalidRecordsByRowNumber = new LinkedHashMap<>();
                // PROCESSING OFFSET(distance between unit row and data row
                int offsetRowCount = offset;
                // move row iterator to the next row index that is offset
                while ((offsetRowCount > MIN_OFFSET) && (rowIterator.hasNext())) {
                    rowIterator.next();
                    offsetRowCount--;
                }

                // prepare entity type validation data
                boolean validateEntityTypeConfigurations = CollectionUtils.hasValue(((entityTypeConfigurationsMap == null) ? null : entityTypeConfigurationsMap.get(boName)));
                long startTime = System.currentTimeMillis();
                // read data
                for (int i = 0; i < limit; i++) {
                    if (rowIterator.hasNext()) {
                        Row row = rowIterator.next();
                        // IMPORT Reading row data from excel
                        readRowDataAndPutInColumnDataMap(boName, row, metadataByboAttrName, mandatoryFieldsForExcelSheet, boAttrNamesByCellNumber, columnValuesByBoAttrName, primaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
                        // validate Entity Type and Entity Name data after reading a full row
                        if (validateEntityTypeConfigurations) {
                            // read entity type configurations for current bo name
                            List<REntityTypeConfig.Node> configurations = entityTypeConfigurationsMap.get(boName);
                            for (REntityTypeConfig.Node configuration : configurations) {
                                // this data can have more than one pair for same bo name so iterate over all pairs
                                String entityType = configuration.getSourceBoAttrName();
                                String entityName = configuration.getTargetBoAttrName();
                                if ((StringUtils.hasValue(entityType)) && (StringUtils.hasValue(entityName))) {
                                    // continue only if there is some data for the given columns
                                    if ((columnValuesByBoAttrName.get(entityType) != null) && (columnValuesByBoAttrName.get(entityName) != null)) {
                                        Object valueForEntityType = columnValuesByBoAttrName.get(entityType).get(row.getRowNum());
                                        Object valueForEntityName = columnValuesByBoAttrName.get(entityName).get(row.getRowNum());
                                        if ((valueForEntityType != null) && (valueForEntityName != null)) {
                                            // prepare a dummy dynamic dto to call the validation service on it
                                            DynamicDTO dynamicDTO = new DynamicDTO(boName, primaryKeyColumnNames, executionContext);
                                            dynamicDTO.put(entityType, valueForEntityType);
                                            dynamicDTO.put(entityName, valueForEntityName);
                                            try {
                                                // call the service to validate the record
                                                dynamicReadService.validateREntityTypeForSave(boName, Arrays.asList(dynamicDTO), executionContext);
                                            } catch (DSPDMException e) {
                                                // eat the exception suppressed
                                                logger.error(e.getMessage(), e);
                                                String entityNameBoAttrName = entityName; // ENTITY_NAME
                                                String entityNameBoAttrDisplayName = (String) metadataByboAttrName.get(entityName).get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME);
                                                String reasonForInvalid = e.getMessage();
                                                Object cellValueAfterReadFromExcel = valueForEntityName; // WELL_NAME
                                                if(reasonForInvalid.contains(DSPDMConstants.BoAttrName.ENTITY_TYPE)) {
                                                    // error is related to entity type that entity type does not exist
                                                    cellValueAfterReadFromExcel = valueForEntityType;
                                                }
                                                addInvalidRecordForEntityTypeConfigurationMisMatched(boName, entityNameBoAttrName,
                                                        entityNameBoAttrDisplayName, reasonForInvalid, cellValueAfterReadFromExcel,
                                                        row.getRowNum(), primaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        // data finished.
                        break;
                    }
                }
                logger.info("Total time taken by Read All Data Rows From Excel file for bo name {} : {} millis", boName, (System.currentTimeMillis() - startTime));
                // if readOldValuesFromDB flag is true then create an empty list as place holder and internal methods will add records to this list
                List<DynamicDTO> oldValuesFromDB = (readOldValuesFromDB) ? new ArrayList<>() : null;
                // ITERATING ALL MAPPED
                // COLUMN TO ADD MISSING COLUMN
                Set<String> idBoAttrNamesAlreadyConverted = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                NameToIdConversionUtils.processNameToIdConversion(boName, metadataByboAttrName, boAttrNamesByCellNumber,
                        columnValuesByBoAttrName, oldValuesFromDB, primaryKeyColumnNames, idBoAttrNamesAlreadyConverted,
                        invalidRecordsByRowNumber, dynamicReadService, executionContext);
                // Now start MERGE COLUMNS
                // create a final map whose key is a row number and value is a row data
                Map<Integer, DynamicDTO> validRecordsByRowNumber = new LinkedHashMap<>();
                Integer rowNumber = null;
                DynamicDTO rowData = null;
                // iterate over all columns
                for (Map.Entry<String, Map<Integer, Object>> boAttrNameEntry : columnValuesByBoAttrName.entrySet()) {
                    // iterate over all rows within the particular current column
                    for (Map.Entry<Integer, Object> rowIdEntry : boAttrNameEntry.getValue().entrySet()) {
                        rowNumber = rowIdEntry.getKey();
                        rowData = invalidRecordsByRowNumber.get(rowNumber);
                        if (rowData == null) {
                            // not found in invalid records it means it is a valid record
                            rowData = validRecordsByRowNumber.get(rowNumber);
                            if (rowData == null) {
                                rowData = new DynamicDTO(boName, primaryKeyColumnNames, executionContext);
                                validRecordsByRowNumber.put(rowNumber, rowData);
                            }
                        }
                        rowData.put(boAttrNameEntry.getKey(), rowIdEntry.getValue());
                    }
                }
                // fill default values to the parsed values if the there is no parsed value for the default value attribute
                if (ConfigProperties.getInstance().fill_default_values_from_metadata.getBooleanValue()) {
                    Map<String, Object> defaultValuesMap = dynamicReadService.getDefaultValuesMap(boName, executionContext);
                    for (DynamicDTO validDTO : validRecordsByRowNumber.values()) {
                        for (Map.Entry<String, Object> defaultValueEntry : defaultValuesMap.entrySet()) {
                            if (validDTO.get(defaultValueEntry.getKey()) == null) {
                                validDTO.put(defaultValueEntry.getKey(), defaultValueEntry.getValue());
                            }
                        }
                    }
                }

                // PUT in final result for the business object
                PagedList<DynamicDTO> parsedValues = new PagedList<DynamicDTO>(validRecordsByRowNumber.size(), validRecordsByRowNumber.values());
				PagedList<DynamicDTO> oldValues = (readOldValuesFromDB && (oldValuesFromDB != null))
						? new PagedList<>(oldValuesFromDB.size(), oldValuesFromDB)
						: new PagedList<>(0, new ArrayList<>());
                PagedList<DynamicDTO> invalidValues = new PagedList<>(invalidRecordsByRowNumber.size(), invalidRecordsByRowNumber.values());
                // the number of rows that need reduce
                int reduceRowNum = (sheet.getLastRowNum() > 0 && unitRow > 0) ? 1 : 0;
                // create a bulk response dto
                BulkImportResponseDTO bulkImportResponseDTO = new BulkImportResponseDTO(sheet.getLastRowNum() - reduceRowNum, parsedValues, oldValues, invalidValues, unitDynamicDTO);
                result.put(boName, bulkImportResponseDTO);
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        } finally {
            if (workbook != null) {
                workbook.close();
            }
        }
        return result;
    }

    /**
     * Returns the hash map of mandatory fields excluding id fields because they will be converted from their corresponding name fields
     * returned map key is bo_attr_name and the value is the display_name
     *
     * @param metadataByboAttrName mata data map for the current business object
     * @param dynamicReadService   service instance to read reference metadata if required
     * @param executionContext     current user execution context
     * @return
     * @Author Muhammad Imran Ansari
     * @since 30-Nov-2019
     */
    private static Map<String, String> getMandatoryFieldsForExcelSheet(Map<String, DynamicDTO> metadataByboAttrName, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        // create a map of mandatory fields to make sure that all mandatory columns exist in excel file
        Map<String, String> mandatoryFields = new LinkedHashMap<>();
        for (DynamicDTO dynamicDTO : metadataByboAttrName.values()) {
            // check if a field is mandatory true and does not have a default value defined for it
            if ((dynamicDTO.get(DSPDMConstants.BoAttrName.IS_MANDATORY) != null) && ((Boolean) dynamicDTO.get(DSPDMConstants.BoAttrName.IS_MANDATORY))
                    && (StringUtils.isNullOrEmpty((String) dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DEFAULT)))) {
                // do not add id fields either from primary key or from reference key
                // do not add if it is a primary key based reference. Add only name based reference fields
                if ((dynamicDTO.get(DSPDMConstants.BoAttrName.IS_REFERENCE_IND) != null) && ((Boolean) dynamicDTO.get(DSPDMConstants.BoAttrName.IS_REFERENCE_IND))) {
                    String referenceBoName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.REFERENCE_BO_NAME);
                    String referenceBoAttrValue = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_VALUE);
                    if (StringUtils.hasValue(referenceBoName) && (StringUtils.hasValue(referenceBoAttrValue))) {
                        Map<String, DynamicDTO> metadataMapForRefBOName = dynamicReadService.readMetadataMapForBOName(referenceBoName, executionContext);
                        DynamicDTO refDynamicDTO = metadataMapForRefBOName.get(referenceBoAttrValue);
                        if (refDynamicDTO != null) {
                            if ((refDynamicDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY) == null) || (!((Boolean) refDynamicDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY)))) {
                                // add to mandatory fields because reference key is not a primary key in its original table
                                // add original bo attr name not the ref bo attr name
                                String displayName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME);
                                if (StringUtils.isNullOrEmpty(displayName)) {
                                    displayName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                                }
                                mandatoryFields.put((String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME), displayName);
                            }
                        }
                    }
                } else if ((dynamicDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY) == null) || (!((Boolean) dynamicDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY)))) {
                    // if not primary key then add
                    String displayName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME);
                    if (StringUtils.isNullOrEmpty(displayName)) {
                        displayName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME);
                    }
                    mandatoryFields.put((String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME), displayName);
                }
            }
        }
        return mandatoryFields;
    }

    /**
     * Takes an excel header row as a param and returns a hash map of key as column index (zero-based) and bo attr name as value
     * It will be a column index map and will be used to read all the remaining rows from the same sheet.
     * It will also validate and throw error in case if a mandatory column is missing for the given bo name
     *
     * @param boName
     * @param headerRow
     * @param metadataByboAttrName
     * @param dynamicReadService
     * @param executionContext
     * @return
     * @Author Yuan Tian, Muhammad Imran Ansari
     * @since 11-Nov-2019
     */
    private static Map<Integer, String> getCellNumberToBoAttrNameMapping(String boName, Row headerRow, Map<String, DynamicDTO> metadataByboAttrName, Map<String, String> mandatoryFieldsForExcelSheet, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        long startTime = System.currentTimeMillis();
        // clone the mandatory fields map because we will remove the records from the cloned copy
        // it will not effect the original mandatory fields map which is coming as a parameter to this function.
        mandatoryFieldsForExcelSheet = new LinkedHashMap<>(mandatoryFieldsForExcelSheet);
        // create a map of attribute display name as the key
        Map<String, DynamicDTO> metadataByDisplayName = null;
        try {
            metadataByDisplayName = metadataByboAttrName.values().stream().collect(
                    Collectors.toMap(dynamicDTO ->
                                    dynamicDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME).toString().toUpperCase().replace(' ', '_'),
                            dynamicDTO -> dynamicDTO));
        } catch (IllegalStateException e) {
            throw new DSPDMException("Attribute Display Name must be unique with in the business object '{}'", e,
                    executionContext.getExecutorLocale(), metadataByboAttrName.values().stream().findFirst().get().get(DSPDMConstants.BoAttrName.BO_NAME));
        }
        // start reading data from header row
        Map<Integer, String> colIndexToBoAttrNameMapping = new LinkedHashMap<>();
        for (int cellIndex = headerRow.getFirstCellNum(); cellIndex <= headerRow.getLastCellNum(); cellIndex++) {
            Cell cell = headerRow.getCell(cellIndex);
            if (cell != null) {
                if (cell.getCellType() == CellType.STRING) {
                    String columnName = cell.getStringCellValue().trim().toUpperCase().replace(' ', '_');
                    // CHECK IF HEADER IS A BO ATTR NAME DIRECTLY
                    if (metadataByboAttrName.get(columnName) != null) {
                        String boAttrName = (String) metadataByboAttrName.get(columnName).get("BO_ATTR_NAME");
                        colIndexToBoAttrNameMapping.put(cellIndex, boAttrName);
                        mandatoryFieldsForExcelSheet.remove(boAttrName);
                        // CHECK IF HEADER IS DISPLAY NAME
                    } else if (metadataByDisplayName.get(columnName) != null) {
                        String boAttrName = (String) metadataByDisplayName.get(columnName).get("BO_ATTR_NAME");
                        colIndexToBoAttrNameMapping.put(cellIndex, boAttrName);
                        mandatoryFieldsForExcelSheet.remove(boAttrName);
                    } else {
                        logger.info("Column name '{}' is not a registered attribute for business object '{}'", columnName, boName);
                    }
                } else {
                    logger.info("Invalid cell type found '{}' for header row with cell number '{}'. Header must be a text.", cell.getCellType().name(), cellIndex);
                }
            }
        }
        if (mandatoryFieldsForExcelSheet.size() > 0) {
            throw new DSPDMException("Unable to find mandatory field(s) '{}' in the excel sheet for bo name '{}'", executionContext.getExecutorLocale(), CollectionUtils.getCommaSeparated(mandatoryFieldsForExcelSheet.values()), boName);
        }
        logger.info("Total time taken by Read Header Row From Excel file for bo name {} : {} millis", boName, (System.currentTimeMillis() - startTime));
        return colIndexToBoAttrNameMapping;
    }

    /**
     * This method reads a row data from excel file and put the row data in columnToRowsDataMap (COMING AS PARAM) for each given column in colIndexToBoAttrNameMapping
     * During read if the cell is empty it skips. If a value is founf then it validates the data type and convert according to the type defined in metadata
     *
     * @param row                      param to read excel data from this read. current row being read
     * @param metadataByboAttrName     map of metadata
     * @param boAttrNamesByCellIndex   column index and bo attr name mapping to be used when a column is read from the row then we know that it is which bo attr name
     * @param columnValuesByBoAttrName map to put data inside it after reading. Key is the column index and value is the map of data for all the rows for this column
     * @param executionContext         current user execution context
     * @Author Yuan Tian, Muhammad Imran Ansari
     * @since 11-Nov-2019
     */
    private static void readRowDataAndPutInColumnDataMap(String boName, Row row, Map<String, DynamicDTO> metadataByboAttrName, Map<String, String> mandatoryFieldsForExcelSheet,
                                                         Map<Integer, String> boAttrNamesByCellIndex, Map<String, Map<Integer, Object>> columnValuesByBoAttrName,
                                                         List<String> primaryKeyColumnNames, Map<Integer, DynamicDTO> invalidRecordsByRowNumber,
                                                         ExecutionContext executionContext) {
        // iterate on all columns using column index
        for (Map.Entry<Integer, String> entry : boAttrNamesByCellIndex.entrySet()) {
            Integer cellIndex = entry.getKey();
            // get the attribute name for the current column index
            String boAttrName = entry.getValue();
            // get the cell at the current column index
            Cell cell = row.getCell(cellIndex);
            boolean isInvalidRecord = false;
            if (cell == null) {
                String boAttrDisplayName = mandatoryFieldsForExcelSheet.get(boAttrName);
                if (boAttrDisplayName != null) {
                    // found that a mandatory field value is missing add the current row  dto to invalid records
                    addInvalidRecordForMandatoryValueNotFound(boName, boAttrName, boAttrDisplayName, row.getRowNum(), primaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
                } else {
                    // get map from matrix for particular column bo_Attr_name to put valid records
                    Map<Integer, Object> cellValuesByRowIndex = columnValuesByBoAttrName.get(boAttrName);
                    // going to put null value in the map with the key for the DTO to be written null to db in case of replace some existing value with null
                    cellValuesByRowIndex.put(row.getRowNum(), null);
                }
            } else {
                Object cellValueAfterReadFromExcel = getCellValue(cell);
                if (cellValueAfterReadFromExcel == null) {
                    // get map from matrix for particular column bo_Attr_name to put valid records
                    Map<Integer, Object> cellValuesByRowIndex = columnValuesByBoAttrName.get(boAttrName);
                    // going to put null value in the map with the key for the DTO to be written null to db in case of replace some existing value with null
                    cellValuesByRowIndex.put(row.getRowNum(), null);
                    // now check if the missing values is for mandatory field then ad the record to invalid records
                    String boAttrDisplayName = mandatoryFieldsForExcelSheet.get(boAttrName);
                    if (boAttrDisplayName != null) {
                        // found that a mandatory field value is missing add the current row  dto to invalid records
                        addInvalidRecordForMandatoryValueNotFound(boName, boAttrName, boAttrDisplayName, row.getRowNum(), primaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
                    }
                } else {
                    // get map from matrix for particular column bo_Attr_name to put valid records
                    Map<Integer, Object> cellValuesByRowIndex = columnValuesByBoAttrName.get(boAttrName);
                    // convert to metadata data type and put in appropriate map
                    // if record is invalid then add to invalid records
                    // if record is valid then put in rowIndexToValueMapping
                    convertDataTypeFromExcelToMetadataAndAddToMap(boAttrName, boName, metadataByboAttrName.get(boAttrName),
                            cellValueAfterReadFromExcel, row.getRowNum(), cellValuesByRowIndex, primaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
                }
            }
        }
    }

    private static List<Map<String, String>> readUnitMap(Row row, Map<Integer, String> boAttrNamesByCellIndex) {
        List unitMaps = new ArrayList();
        // iterate on all columns using column index
        for (Map.Entry<Integer, String> entry : boAttrNamesByCellIndex.entrySet()) {
            Integer cellIndex = entry.getKey();
            // get the attribute name for the current column index
            String boAttrName = entry.getValue();
            // get the cell at the current column index
            Cell cell = row.getCell(cellIndex);
            if (cell != null) {
                Object cellValueAfterReadFromExcel = getCellValue(cell);
                if (cellValueAfterReadFromExcel != null) {
                    String unit = cellValueAfterReadFromExcel.toString();
                    if (!unit.trim().isEmpty()) {
                        Map<String, String> unitMap = new HashMap<String, String>();
                        unitMap.put(DSPDMConstants.DSPDM_RESPONSE.BO_ATTR_NAME_KEY, boAttrName);
                        unitMap.put(DSPDMConstants.BoAttrName.SOURCEUNIT, unit);
                        unitMaps.add(unitMap);
                    }

                }
            }
        }
        return unitMaps;
    }

    private static Object getCellValue(Cell cell) {
        Object cellValueAfterReadFromExcel = null;
        switch (cell.getCellType()) {
            case STRING:
                cellValueAfterReadFromExcel = cell.getStringCellValue();
                break;
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    cellValueAfterReadFromExcel = cell.getDateCellValue();
                } else {
                    cellValueAfterReadFromExcel = cell.getNumericCellValue();
                }
                break;
            case BOOLEAN:
                cellValueAfterReadFromExcel = cell.getBooleanCellValue();
                break;
            case BLANK:
                cellValueAfterReadFromExcel = null;
                break;
            default:
                cellValueAfterReadFromExcel = null;
        }
        return cellValueAfterReadFromExcel;
    }

    private static void convertDataTypeFromExcelToMetadataAndAddToMap(String boAttrName, String boName, DynamicDTO metadataDTO,
                                                                      Object cellValueAfterReadFromExcel, int rowNumber, Map<Integer, Object> cellValuesByRowIndex,
                                                                      List<String> primaryKeyColumnNames, Map<Integer, DynamicDTO> invalidRecordsByRowNumber,
                                                                      ExecutionContext executionContext) {
        String attributeDataType = (String) metadataDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DATATYPE);
        String boAttrDisplayName = (String) metadataDTO.get(DSPDMConstants.BoAttrName.ATTRIBUTE_DISPLAYNAME);
        Integer maxAllowedLength = (Integer) metadataDTO.get(DSPDMConstants.BoAttrName.MAX_ALLOWED_LENGTH);
        Integer maxAllowedDecimalPlaces = (Integer) metadataDTO.get(DSPDMConstants.BoAttrName.MAX_ALLOWED_DECIMAL_PLACES);
        Class javaDataTypeFromMetadata = (Class) metadataDTO.get(DSPDMConstants.BoAttrName.JAVA_DATA_TYPE);
        boolean valueAddedToInvalid = false;
        try {
            if (cellValueAfterReadFromExcel instanceof String) {
                if (javaDataTypeFromMetadata != String.class) {
                    cellValueAfterReadFromExcel = MetadataUtils.convertValueToJavaDataTypeFromString((String) cellValueAfterReadFromExcel, javaDataTypeFromMetadata, boName, boAttrName, executionContext);
                } else {
                    // validate max length
                    int actualLength = ((String) cellValueAfterReadFromExcel).trim().length();
                    if (actualLength > maxAllowedLength) {
                        addInvalidRecordForExceedingLength(boName, boAttrName, boAttrDisplayName, cellValueAfterReadFromExcel, maxAllowedLength, actualLength, rowNumber, primaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
                        valueAddedToInvalid = true;
                    }
                }
            } else {
                if (javaDataTypeFromMetadata == java.sql.Timestamp.class) {
                    if (cellValueAfterReadFromExcel instanceof java.sql.Timestamp) {
                        if (ConfigProperties.getInstance().use_utc_timezone_to_save.getBooleanValue()) {
                            cellValueAfterReadFromExcel = DateTimeUtils.convertTimezoneFromClientToUTC((java.sql.Timestamp) cellValueAfterReadFromExcel, executionContext);
                        }
                    } else if (cellValueAfterReadFromExcel instanceof java.sql.Date) {
                        cellValueAfterReadFromExcel = new java.sql.Timestamp(((java.sql.Date) cellValueAfterReadFromExcel).getTime());
                    } else if (cellValueAfterReadFromExcel instanceof java.util.Date) {
                        if (ConfigProperties.getInstance().use_utc_timezone_to_save.getBooleanValue()) {
                            cellValueAfterReadFromExcel = DateTimeUtils.convertTimezoneFromClientToUTC(new java.sql.Timestamp(((java.util.Date) cellValueAfterReadFromExcel).getTime()), executionContext);
                        } else {
                            cellValueAfterReadFromExcel = new java.sql.Timestamp(((java.util.Date) cellValueAfterReadFromExcel).getTime());
                        }
                    }
                } else if (javaDataTypeFromMetadata == java.sql.Date.class) {
                    if (cellValueAfterReadFromExcel instanceof java.sql.Timestamp) {
                        if (ConfigProperties.getInstance().use_utc_timezone_to_save.getBooleanValue()) {
                            cellValueAfterReadFromExcel = new java.sql.Date(DateTimeUtils.convertTimezoneFromClientToUTC((java.sql.Timestamp) cellValueAfterReadFromExcel, executionContext).getTime());
                        } else {
                            cellValueAfterReadFromExcel = new java.sql.Date(((java.sql.Timestamp) cellValueAfterReadFromExcel).getTime());
                        }
                    } else if (cellValueAfterReadFromExcel instanceof java.util.Date) {
                        if (ConfigProperties.getInstance().use_utc_timezone_to_save.getBooleanValue()) {
                            cellValueAfterReadFromExcel = new java.sql.Date(DateTimeUtils.convertTimezoneFromClientToUTC(new java.sql.Timestamp(((java.util.Date) cellValueAfterReadFromExcel).getTime()), executionContext).getTime());
                        } else {
                            cellValueAfterReadFromExcel = new java.sql.Date(((java.util.Date) cellValueAfterReadFromExcel).getTime());
                        }
                    }
                } else if (javaDataTypeFromMetadata == String.class) {
                    if (cellValueAfterReadFromExcel instanceof Double) {
                        cellValueAfterReadFromExcel = getNumericCellValueToStringFormat().format(cellValueAfterReadFromExcel);
                    } else {
                        cellValueAfterReadFromExcel = cellValueAfterReadFromExcel.toString();
                    }
                    // validate max length
                    int actualLength = ((String) cellValueAfterReadFromExcel).length();
                    if (actualLength > maxAllowedLength) {
                        addInvalidRecordForExceedingLength(boName, boAttrName, boAttrDisplayName, cellValueAfterReadFromExcel, maxAllowedLength, actualLength, rowNumber, primaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
                        valueAddedToInvalid = true;
                    }
                } else if (javaDataTypeFromMetadata == Boolean.class) {
                    if (!(cellValueAfterReadFromExcel instanceof Boolean)) {
                        addInvalidRecordForDataTypeMisMatch(boName, boAttrName, boAttrDisplayName, cellValueAfterReadFromExcel, javaDataTypeFromMetadata, rowNumber, primaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
                        valueAddedToInvalid = true;
                    }
                } else if ((javaDataTypeFromMetadata == BigDecimal.class)
                        || (javaDataTypeFromMetadata == BigInteger.class)
                        || (javaDataTypeFromMetadata == Double.class)
                        || (javaDataTypeFromMetadata == Float.class)
                        || (javaDataTypeFromMetadata == Long.class)
                        || (javaDataTypeFromMetadata == Integer.class)
                        || (javaDataTypeFromMetadata == Short.class)) {
                    try {
                        // convert to double if not already
                        cellValueAfterReadFromExcel = NumberUtils.convertTo(cellValueAfterReadFromExcel, javaDataTypeFromMetadata, executionContext);
                        int actualLength = NumberUtils.getIntegerLength(cellValueAfterReadFromExcel, executionContext);

                        // validate max length
                        if (actualLength > maxAllowedLength) {
                            addInvalidRecordForExceedingLength(boName, boAttrName, boAttrDisplayName, cellValueAfterReadFromExcel, maxAllowedLength, actualLength, rowNumber, primaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
                            valueAddedToInvalid = true;
                        }
                    } catch (DSPDMException e) {
                        addInvalidRecordForDataTypeMisMatch(boName, boAttrName, boAttrDisplayName, cellValueAfterReadFromExcel, javaDataTypeFromMetadata, rowNumber, primaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
                        valueAddedToInvalid = true;
                    }
                } else {
                    DynamicDTO dynamicDTOForRowNumber = getDynamicDTOForRowNumber(boName, rowNumber, primaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
                    dynamicDTOForRowNumber.put(boAttrName, cellValueAfterReadFromExcel);
                    // found that the field value is not in correct data type add the current row  dto to invalid records
                    String reasonForInvalid = StringUtils.formatMessage("Java data type '{}' read from metadata is not registered in import/parse process while reading from excel for attribute '{}' and bo name '{}' at row index '{}'",
                            javaDataTypeFromMetadata.getName(), boAttrName, boName, (rowNumber + 1));
                    BulkImportInvalidRecordReasonDTO bulkImportInvalidRecordReasonDTO = new BulkImportInvalidRecordReasonDTO(boName, boAttrName, boAttrDisplayName, rowNumber, cellValueAfterReadFromExcel, reasonForInvalid);
                    // add to invalid records
                    addInvalidRecord(bulkImportInvalidRecordReasonDTO, primaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
                    valueAddedToInvalid = true;
                }
            }
        } catch (Throwable e) {
            // found that the field value is not in correct data type add the current row  dto to invalid records
            String reasonForInvalid = StringUtils.formatMessage(e.getMessage() + " Expected data type in metadata is '{}'  but data type found in excel is '{}' for attribute '{}' and bo name '{}' at row index '{}' and value '{}'",
                    javaDataTypeFromMetadata.getName(), cellValueAfterReadFromExcel.getClass().getName(), boAttrDisplayName, boName, (rowNumber + 1), cellValueAfterReadFromExcel);
            BulkImportInvalidRecordReasonDTO bulkImportInvalidRecordReasonDTO = new BulkImportInvalidRecordReasonDTO(boName, boAttrName, boAttrDisplayName, rowNumber, cellValueAfterReadFromExcel, reasonForInvalid);
            // add to invalid records
            addInvalidRecord(bulkImportInvalidRecordReasonDTO, primaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
            valueAddedToInvalid = true;
            logger.error(e.getMessage(), e);
        }

        if (!valueAddedToInvalid) {
            // put value against row index in the current column map
            cellValuesByRowIndex.put(rowNumber, cellValueAfterReadFromExcel);
        }
    }

    private static void addInvalidRecordForMandatoryValueNotFound(String boName, String boAttrName, String boAttrDisplayName,
                                                                  Integer rowNumber, List<String> primaryKeyColumnNames, Map<Integer, DynamicDTO> invalidRecordsByRowNumber, ExecutionContext executionContext) {
        // found that the field value Llength is greater than the allowed length
        String reasonForInvalid = StringUtils.formatMessage("No value found for a mandatory attribute '{}' in the row number '{}' for business object '{}'", boAttrDisplayName, (rowNumber + 1), boName);
        Object cellValueAfterReadFromExcel = null;
        BulkImportInvalidRecordReasonDTO bulkImportInvalidRecordReasonDTO = new BulkImportInvalidRecordReasonDTO(boName, boAttrName, boAttrDisplayName, rowNumber, cellValueAfterReadFromExcel, reasonForInvalid);
        // add to invalid records
        DynamicDTO dynamicDTO = addInvalidRecord(bulkImportInvalidRecordReasonDTO, primaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
        // no value to be added to the DTO
    }

    private static void addInvalidRecordForExceedingLength(String boName, String boAttrName, String boAttrDisplayName,
                                                           Object cellValueAfterReadFromExcel, Integer maxAllowedLength, Integer actualLength, Integer rowNumber,
                                                           List<String> primaryKeyColumnNames, Map<Integer, DynamicDTO> invalidRecordsByRowNumber, ExecutionContext executionContext) {
        // found that the field value length is greater than the allowed length
        String reasonForInvalid = StringUtils.formatMessage("Max allowed length for cell value is '{}' but length found in excel is '{}' for attribute '{}' and bo name '{}' at row index '{}'",
                maxAllowedLength, actualLength, boAttrDisplayName, boName, (rowNumber + 1));
        BulkImportInvalidRecordReasonDTO bulkImportInvalidRecordReasonDTO = new BulkImportInvalidRecordReasonDTO(boName, boAttrName, boAttrDisplayName, rowNumber, cellValueAfterReadFromExcel, reasonForInvalid);
        // add to invalid records
        DynamicDTO dynamicDTO = addInvalidRecord(bulkImportInvalidRecordReasonDTO, primaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
        // add value to the DTO
        dynamicDTO.put(boAttrName, cellValueAfterReadFromExcel);
    }

    private static void addInvalidRecordForDataTypeMisMatch(String boName, String boAttrName, String boAttrDisplayName,
                                                            Object cellValueAfterReadFromExcel, Class javaDataTypeFromString, Integer rowNumber,
                                                            List<String> primaryKeyColumnNames, Map<Integer, DynamicDTO> invalidRecordsByRowNumber, ExecutionContext executionContext) {
        // found that the field value is not in correct data type add the current row  dto to invalid records
        String reasonForInvalid = StringUtils.formatMessage("Expected data type in metadata is '{}'  but data type found in excel is '{}' for attribute '{}' and bo name '{}' at row index '{}'",
                javaDataTypeFromString.getName(), cellValueAfterReadFromExcel.getClass().getName(), boAttrDisplayName, boName, (rowNumber + 1));
        BulkImportInvalidRecordReasonDTO bulkImportInvalidRecordReasonDTO = new BulkImportInvalidRecordReasonDTO(boName, boAttrName, boAttrDisplayName, rowNumber, cellValueAfterReadFromExcel, reasonForInvalid);
        // add to invalid records
        DynamicDTO dynamicDTO = addInvalidRecord(bulkImportInvalidRecordReasonDTO, primaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
        // add value to the DTO
        dynamicDTO.put(boAttrName, cellValueAfterReadFromExcel);
    }

    public static void addInvalidRecordForReferenceIdNotFound(String boName, String boAttrName, String boAttrDisplayName,
                                                              Object cellValueAfterReadFromExcel, String refBoName, String refBoAttrLabel,
                                                              Integer rowNumber, List<String> primaryKeyColumnNames, Map<Integer, DynamicDTO> invalidRecordsByRowNumber, ExecutionContext executionContext) {
        // found that the field value is not in correct data type add the current row  dto to invalid records
        String reasonForInvalid = StringUtils.formatMessage("Reference value '{}' not found in reference table '{}' with reference column name '{}' for attribute '{}' and bo name '{}' at row index '{}'",
                cellValueAfterReadFromExcel, refBoName, refBoAttrLabel, boAttrDisplayName, boName, (rowNumber + 1));
        BulkImportInvalidRecordReasonDTO bulkImportInvalidRecordReasonDTO = new BulkImportInvalidRecordReasonDTO(boName, boAttrName, boAttrDisplayName, rowNumber, cellValueAfterReadFromExcel, reasonForInvalid);
        // add to invalid records
        DynamicDTO dynamicDTO = addInvalidRecord(bulkImportInvalidRecordReasonDTO, primaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
        // add value to the DTO
        dynamicDTO.put(boAttrName, cellValueAfterReadFromExcel);
    }

    public static void addInvalidRecordForUniqueIdNotFound(String boName, String boAttrName, String boAttrDisplayName,
                                                           Object cellValueAfterReadFromExcel, Integer rowNumber, List<String> primaryKeyColumnNames,
                                                           Map<Integer, DynamicDTO> invalidRecordsByRowNumber, ExecutionContext executionContext) {
        // found that the field value is not in correct data type add the current row  dto to invalid records
        String reasonForInvalid = StringUtils.formatMessage("Unable to find a business object of type '{}' with unique column name '{}' and unique value '{}' found at row index '{}'",
                boName, boAttrDisplayName, cellValueAfterReadFromExcel, (rowNumber + 1));
        BulkImportInvalidRecordReasonDTO bulkImportInvalidRecordReasonDTO = new BulkImportInvalidRecordReasonDTO(boName, boAttrName, boAttrDisplayName, rowNumber, cellValueAfterReadFromExcel, reasonForInvalid);
        // add to invalid records
        DynamicDTO dynamicDTO = addInvalidRecord(bulkImportInvalidRecordReasonDTO, primaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
        // add value to the DTO
        dynamicDTO.put(boAttrName, cellValueAfterReadFromExcel);
    }

    public static void addInvalidRecordForForeignKeyUniqueIdNotFound(String currentBoName, List<String> currentBoAttrDisplayNamesBeingLookedUp, String foreignKeyBoName, List<String> foreignTableBoAttrNames, List<String> foreignTableBoAttrDisplayNames,
                                                                     List<Object> cellValuesAfterReadFromExcel, Integer rowNumber, List<String> currentBoPrimaryKeyColumnNames,
                                                                     Map<Integer, DynamicDTO> invalidRecordsByRowNumber, ExecutionContext executionContext) {
        // make comma separated attr names and values to form the error message
        String currentBoAttrDisplayNameBeingLookedUp = CollectionUtils.getCommaSeparated(currentBoAttrDisplayNamesBeingLookedUp);
        String foreignTableBoAttrName = CollectionUtils.getCommaSeparated(foreignTableBoAttrNames);
        String foreignTableBoAttrDisplayName = CollectionUtils.getCommaSeparated(foreignTableBoAttrDisplayNames);
        String cellValueAfterReadFromExcel = CollectionUtils.getCommaSeparated(cellValuesAfterReadFromExcel);
        // found that the field value is not in correct data type add the current row  dto to invalid records
        String reasonForInvalid = StringUtils.formatMessage("Reference value '{}' not found in reference table '{}' with reference column name '{}' for attribute '{}' and bo name '{}' at row index '{}'",
                cellValueAfterReadFromExcel, foreignKeyBoName, foreignTableBoAttrDisplayName, currentBoAttrDisplayNameBeingLookedUp, currentBoName, (rowNumber + 1));


        BulkImportInvalidRecordReasonDTO bulkImportInvalidRecordReasonDTO = new BulkImportInvalidRecordReasonDTO(currentBoName, foreignTableBoAttrName, foreignTableBoAttrDisplayName, rowNumber, cellValueAfterReadFromExcel, reasonForInvalid);
        // add to invalid records
        DynamicDTO dynamicDTO = addInvalidRecord(bulkImportInvalidRecordReasonDTO, currentBoPrimaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
        // add values to the DTO
        for (int i = 0; i < foreignTableBoAttrNames.size(); i++) {
            dynamicDTO.put(foreignTableBoAttrNames.get(i), cellValuesAfterReadFromExcel.get(i));
        }
    }

    private static void addInvalidRecordForEntityTypeConfigurationMisMatched(String currentBoName, String entityNameBoAttrName, String entityNameBoAttrDisplayName, String reasonForInvalid,
                                                                           Object cellValueAfterReadFromExcel, Integer rowNumber, List<String> currentBoPrimaryKeyColumnNames,
                                                                           Map<Integer, DynamicDTO> invalidRecordsByRowNumber, ExecutionContext executionContext) {
        BulkImportInvalidRecordReasonDTO bulkImportInvalidRecordReasonDTO = new BulkImportInvalidRecordReasonDTO(currentBoName, entityNameBoAttrName, entityNameBoAttrDisplayName, rowNumber, cellValueAfterReadFromExcel, reasonForInvalid);
        // add to invalid records
        DynamicDTO dynamicDTO = addInvalidRecord(bulkImportInvalidRecordReasonDTO, currentBoPrimaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
        // add value to the DTO
        dynamicDTO.put(entityNameBoAttrName, cellValueAfterReadFromExcel);
    }

    private static DynamicDTO addInvalidRecord(BulkImportInvalidRecordReasonDTO bulkImportInvalidRecordReasonDTO, List<String> primaryKeyColumnNames, Map<Integer, DynamicDTO> invalidRecordsByRowNumber, ExecutionContext executionContext) {
        // a mandatory field value is missing for the current row
        // check weather the row number already exists in invalid records due to already declared invalid for some other column before current column
        DynamicDTO dynamicDTO = getDynamicDTOForRowNumber(bulkImportInvalidRecordReasonDTO.getBoName(), bulkImportInvalidRecordReasonDTO.getRowNumber(), primaryKeyColumnNames, invalidRecordsByRowNumber, executionContext);
        // now add the reason of invalidity into the messages inside the dynamic dto
        addMessageToDynamicDTO(bulkImportInvalidRecordReasonDTO, dynamicDTO);
        return dynamicDTO;
    }

    private static DynamicDTO getDynamicDTOForRowNumber(String boName, Integer rowNumber, List<String> primaryKeyColumnNames, Map<Integer, DynamicDTO> invalidRecordsByRowNumber, ExecutionContext executionContext) {
        // check weather the row number already exists in invalid records due to already declared invalid for some other column before current column
        DynamicDTO dynamicDTO = invalidRecordsByRowNumber.get(rowNumber);
        if (dynamicDTO == null) {
            dynamicDTO = new DynamicDTO(boName, primaryKeyColumnNames, executionContext);
            // put dynamic dto inside the invalid records map
            invalidRecordsByRowNumber.put(rowNumber, dynamicDTO);
        }
        return dynamicDTO;
    }

    private static void addMessageToDynamicDTO(BulkImportInvalidRecordReasonDTO bulkImportInvalidRecordReasonDTO, DynamicDTO dynamicDTO) {
        // now add the reason of invalidity into the messages inside the dynamic dto
        List<BulkImportInvalidRecordReasonDTO> messages = (List<BulkImportInvalidRecordReasonDTO>) dynamicDTO.get(DSPDMConstants.DSPDM_RESPONSE.MESSAGES_KEY);
        if (CollectionUtils.isNullOrEmpty(messages)) {
            // if no messages already added then create an empty list and add it to the dynamic dto
            messages = new ArrayList<>();
            // put list of messages inside the dynamic dto
            dynamicDTO.put(DSPDMConstants.DSPDM_RESPONSE.MESSAGES_KEY, messages);
        }
        // increment to row number to make it one based not zero based
        bulkImportInvalidRecordReasonDTO.setRowNumber(bulkImportInvalidRecordReasonDTO.getRowNumber() + 1);
        messages.add(bulkImportInvalidRecordReasonDTO);
    }

    private static List<String> getPrimaryKeyColumnNames(Map<String, DynamicDTO> metadataByboAttrName) {
        // Get primary key column names to be set in dynamic dto to generate proper id
        List<String> primaryKeyColumnNames = new ArrayList<>();
        for (DynamicDTO metadataDTO : metadataByboAttrName.values()) {
            if (Boolean.TRUE.equals(metadataDTO.get(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY))) {
                primaryKeyColumnNames.add((String) metadataDTO.get(DSPDMConstants.BoAttrName.BO_ATTR_NAME));
            }
        }
        return primaryKeyColumnNames;
    }

    private static List<String> getForeignKeyColumnNames(String boName, IDynamicReadService dynamicReadService, ExecutionContext executionContext) {
        List<String> foreignKeyColumnNames = null;
        BusinessObjectInfo businessObjectInfo = new BusinessObjectInfo(DSPDMConstants.BoName.BUS_OBJ_RELATIONSHIP, executionContext);
        businessObjectInfo.addColumnsToSelect(DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
        businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.CHILD_BO_NAME, boName);
        businessObjectInfo.addFilter(DSPDMConstants.BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, true);
        // read all relationships
        businessObjectInfo.setReadAllRecords(true);
        List<DynamicDTO> relationships = dynamicReadService.readSimple(businessObjectInfo, executionContext);
        if (CollectionUtils.hasValue(relationships)) {
            foreignKeyColumnNames = CollectionUtils.getStringValuesFromList(relationships, DSPDMConstants.BoAttrName.CHILD_BO_ATTR_NAME);
        } else {
            foreignKeyColumnNames = new ArrayList<>();
        }
        return foreignKeyColumnNames;
    }
}
