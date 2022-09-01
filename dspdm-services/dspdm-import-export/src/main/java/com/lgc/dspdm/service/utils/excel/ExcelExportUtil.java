package com.lgc.dspdm.service.utils.excel;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.config.ConnectionProperties;
import com.lgc.dspdm.core.common.data.criteria.DSPDMUnit;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.DateTimeUtils;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.NumberUtils;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.temporal.Temporal;
import java.util.*;

/**
 * For excel, it use Windows system locale settings, so this utils doesn't require custom locale parameter
 */
public class ExcelExportUtil {

    private static DSPDMLogger logger = new DSPDMLogger(ExcelExportUtil.class);

    /**
     * create the workbook
     */
    public static Workbook createWorkBook(ExecutionContext executionContext) {
        int streamWindowSize = ConfigProperties.getInstance().export_streaming_rows_window_size.getIntegerValue();
        Workbook workbook = null;
        try {
            workbook = (streamWindowSize > 0) ? new SXSSFWorkbook(streamWindowSize) : new XSSFWorkbook();
            if (workbook instanceof SXSSFWorkbook) {
                ((SXSSFWorkbook) workbook).setCompressTempFiles(true); // temp files will be gzipped
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return workbook;
    }
    
    /**
     * dispose the workbook
     */
    public static void disposeWorkBook(Workbook workbook) {
    	if (workbook != null) {
            try {
                workbook.close();
                if (workbook instanceof SXSSFWorkbook) {
                    ((SXSSFWorkbook) workbook).dispose();
                }
            } catch (IOException e) {
            }
        }
    }
    
    public static void exportToExcelSingleSheetStream(Workbook workbook, String sheetName, List<DynamicDTO> metadata,
                                                      Iterable<DynamicDTO> listDTO,
                                                       ExecutionContext executionContext) {
        try {
            createWorkBookSingleSheet(workbook, sheetName, metadata, listDTO,  executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
    }

    /**
     * This will convert workbook data to byte array
     *
     * @param workbook         Excel workbook instance is required
     * @param executionContext Execution Context
     * @return Will return byte array of workbook
     * @author rao.alikhan
     * @since 28-09-2020
     */
    public static byte[] convertExcelWorkbookToByteArray(Workbook workbook, ExecutionContext executionContext) {
        byte[] byteArray = null;
        ByteArrayOutputStream baos = null;
        try {
            try {
                baos = new ByteArrayOutputStream();
                workbook.write(baos);
                byteArray = baos.toByteArray();
            } finally {
                if (baos != null) {
                    baos.close();
                }
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return byteArray;
    }

    /**
     * This will append business objects data into existing workbook sheet
     *
     * @param sheetName        Sheet name is required to get the sheet instance
     * @param workbook         Workbook instance is required to append data
     * @param metadata         Business object metadata
     * @param listDTO          Business object list
     * @param executionContext Execution Context
     * @author rao.alikhan
     * @since 28-09-2020
     */
    public static void appendDataToExcelSingleSheet(String sheetName, Workbook workbook, List<DynamicDTO> metadata,
                                                    Iterable<DynamicDTO> listDTO, ExecutionContext executionContext) {
        Sheet sheet = workbook.getSheet(sheetName);
        int lastRowNum = sheet.getLastRowNum() + 1;
        Map<String, Integer> businessObjectAttributeColumnIndexMap = getBusinessObjectAttributeColumnIndexMap(metadata);
        // map for numeric style
        Map<String, CellStyle> numericStyleMap = new HashMap<>(5);
        CreationHelper createHelper = workbook.getCreationHelper();
        // date only style
        CellStyle dateStyle = workbook.getCellStyleAt(1);
        // date time style
        CellStyle dateTimeStyle = workbook.getCellStyleAt(2);
        // int style
        CellStyle intStyle = workbook.getCellStyleAt(3);
        // binary style
        CellStyle binaryStyle = workbook.getCellStyleAt(4);
        // append data to workbook sheet
        appendDataToSheet(metadata, businessObjectAttributeColumnIndexMap, listDTO, workbook, sheet,
                lastRowNum, numericStyleMap, intStyle, dateStyle, dateTimeStyle, binaryStyle,
                createHelper, executionContext);
    }

    /**
     * create worksheet data from dtos.
     */
    private static XSSFWorkbook createWorkBookMultiSheet(Map<String, List<DynamicDTO>> metadata,
                                                         Map<String, Object> listDTO, ExecutionContext executionContext) {
        XSSFWorkbook workbook = null;
        try {
            workbook = new XSSFWorkbook();
            for (Map.Entry<String, List<DynamicDTO>> entry : metadata.entrySet()) {
                String sheetName = entry.getKey();
                List<DynamicDTO> metadataList = entry.getValue();
                // create sheet for each entry
                createSheetForWorkbook(workbook, sheetName, metadataList,
                        (Iterable<DynamicDTO>) listDTO.get(sheetName), executionContext);
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return workbook;
    }
    
    public static Workbook createWorkBookSingleSheet(Workbook workbook, String sheetName, List<DynamicDTO> metadata,
                                                     Iterable<DynamicDTO> listDTO,
                                                    ExecutionContext executionContext) {
        try {
            // create one sheet
            createSheetForWorkbook(workbook, sheetName, metadata, listDTO,  executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return workbook;
    }

    private static void addCommonCellStyleFormat(CellStyle cellStyle) {
        cellStyle.setShrinkToFit(false);
        cellStyle.setAlignment(HorizontalAlignment.CENTER);
        cellStyle.setVerticalAlignment(VerticalAlignment.CENTER);
    }

    private static void createSheetForWorkbook(Workbook workbook, String sheetName, List<DynamicDTO> metadata,
                                               Iterable<DynamicDTO> listDTO,  ExecutionContext executionContext) {
        boolean isMSSQLServerDialect = ConnectionProperties.isMSSQLServerDialect();
        // create header row font
        final Font headerFont = workbook.createFont();
        headerFont.setFontName("Arial");
        headerFont.setBold(true);
        headerFont.setColor(IndexedColors.AUTOMATIC.index);
        // create sheet
        Sheet sheet = workbook.createSheet(sheetName);
        CreationHelper createHelper = workbook.getCreationHelper();
        // date only style
        CellStyle dateStyle = workbook.createCellStyle();
        dateStyle.setDataFormat(
                createHelper.createDataFormat().getFormat("yyyy-mm-dd"));
        addCommonCellStyleFormat(dateStyle);
        // date time style
        CellStyle dateTimeStyle = workbook.createCellStyle();
        dateTimeStyle.setDataFormat(
                createHelper.createDataFormat().getFormat("yyyy-mm-dd hh:mm:ss"));
        addCommonCellStyleFormat(dateTimeStyle);
        // int style
        CellStyle intStyle = workbook.createCellStyle();
        intStyle.setDataFormat(
                createHelper.createDataFormat().getFormat("0"));
        addCommonCellStyleFormat(intStyle);
        // binary style
        CellStyle binaryStyle = workbook.createCellStyle();
        binaryStyle.setWrapText(false);
        addCommonCellStyleFormat(binaryStyle);
        // header style
        CellStyle headerStyle = workbook.createCellStyle();
        // do not et any background color
        //headerStyle.setFillBackgroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
        headerStyle.setFillForegroundColor(IndexedColors.GREY_40_PERCENT.getIndex());
        headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        headerStyle.setFont(headerFont);
        // border
        headerStyle.setBorderBottom(BorderStyle.THIN);
        headerStyle.setBottomBorderColor(IndexedColors.BLACK.getIndex());
        headerStyle.setBorderLeft(BorderStyle.THIN);
        headerStyle.setLeftBorderColor(IndexedColors.BLACK.getIndex());
        headerStyle.setBorderRight(BorderStyle.THIN);
        headerStyle.setRightBorderColor(IndexedColors.BLACK.getIndex());
        headerStyle.setBorderTop(BorderStyle.THIN);
        headerStyle.setTopBorderColor(IndexedColors.BLACK.getIndex());
        addCommonCellStyleFormat(headerStyle);
        // map for numeric style
        Map<String, CellStyle> numericStyleMap = new HashMap<>(5);

        // create header
        int rowNum = 0;
        int colNum = 0;
        Row headerRow = sheet.createRow(rowNum++);
        String boAttrName = null;
        Map<String, Integer> boAttrNameToColumnIndexMap = new HashMap<>();
        for (DynamicDTO entry : metadata) {
            boAttrName = (entry.get(DSPDMConstants.DYNAMICALALIAS) != null) ? (String) entry.get(DSPDMConstants.DYNAMICALALIAS) : (String) entry.get("BO_ATTR_NAME");
            boAttrNameToColumnIndexMap.put(boAttrName, colNum);
            Cell cell = headerRow.createCell(colNum);
            String type = (String) entry.get("ATTRIBUTE_DATATYPE");
            DSPDMConstants.DataTypes dataType = DSPDMConstants.DataTypes.fromAttributeDataType(type, isMSSQLServerDialect);
            sheet.setColumnWidth(colNum, (dataType != null) ? dataType.getColumnWidthInExcel() : 6000);
            cell.setCellValue((entry.get(DSPDMConstants.DYNAMICALALIAS) != null) ? (String) entry.get(DSPDMConstants.DYNAMICALALIAS) : (String) entry.get("ATTRIBUTE_DISPLAYNAME"));
            cell.setCellStyle(headerStyle);
            colNum++;
        }
        // append business object list to sheet
        appendDataToSheet(metadata, boAttrNameToColumnIndexMap, listDTO, workbook, sheet, rowNum, numericStyleMap,
                intStyle, dateStyle, dateTimeStyle, binaryStyle, createHelper, executionContext);
    }

    /**
     * This will get business object attribute map according to column in excel sheet
     *
     * @param metadata Metadata is required to prepare map
     * @return Will return business object attribute column map
     * @author rao.alikhan
     * @since 28-09-2020
     */
    private static Map<String, Integer> getBusinessObjectAttributeColumnIndexMap(List<DynamicDTO> metadata) {
        Map<String, Integer> boAttrNameToColumnIndexMap = new HashMap<>();
        int colNum = 0;
        for (DynamicDTO entry : metadata) {
            boAttrNameToColumnIndexMap.put((String) entry.get("BO_ATTR_NAME"), colNum);
            colNum++;
        }
        return boAttrNameToColumnIndexMap;
    }

    /**
     * This wll append business object list to given workbook sheet
     *
     * @param metadata                   Metadata is required to get the business object attribute properties
     * @param boAttrNameToColumnIndexMap Business object attribute column indexing in excel sheet
     * @param listDTO                    Business object list to write/append in sheet
     * @param workbook                   Workbook instance is required to create double cell style if required
     * @param sheet                      Sheet instance is required to append data
     * @param rowNum                     Row number is number after where the data should be written
     * @param numericStyleMap            Numeric Style Map
     * @param intStyle                   Integer Cell style
     * @param dateStyle                  Date Cell Style
     * @param dateTimeStyle              Date Time Cell Style
     * @param binaryStyle                Binary Cell Style
     * @param creatorHelper              Workbook creator helper instance
     * @param executionContext           Execution Context
     * @author rao.alikhan
     * @since 28-09-2020
     */
    private static void appendDataToSheet(List<DynamicDTO> metadata, Map<String, Integer> boAttrNameToColumnIndexMap, Iterable<DynamicDTO> listDTO,
                                          Workbook workbook, Sheet sheet, int rowNum,
                                          Map<String, CellStyle> numericStyleMap,
                                          CellStyle intStyle, CellStyle dateStyle, CellStyle dateTimeStyle, CellStyle binaryStyle,
                                          CreationHelper creatorHelper,
                                          ExecutionContext executionContext) {
        // append each DTP object to sheet
        for (DynamicDTO dto : listDTO) {
            // create row for data
            Row entry = sheet.createRow(rowNum++);
            for (Map.Entry<String, Object> e : dto.entrySet()) {
                String boAttrName = e.getKey();
                Object value = e.getValue();
                if (boAttrNameToColumnIndexMap.containsKey(boAttrName)) {
                    Cell cell = entry.createCell(boAttrNameToColumnIndexMap.get(boAttrName));
                    if (value != null) {
                        if (value instanceof Boolean) {
                            cell.setCellValue((Boolean) value);
                        } else if (value instanceof String) {
                            cell.setCellValue((String) value);
                        } else if ((value instanceof BigInteger)
                                || (value instanceof Long)
                                || (value instanceof Integer)
                                || (value instanceof Short)) {
                            cell.setCellValue(NumberUtils.convertToLong(value, executionContext));
                            cell.setCellStyle(intStyle);
                        } else if ((value instanceof BigDecimal)
                                || (value instanceof Double)
                                || (value instanceof Float)) {
                            Double doubleValue = NumberUtils.convertToDouble(value, executionContext);
                            DynamicDTO definition = metadata.get((boAttrNameToColumnIndexMap.get(boAttrName)));
                            String length = (String) definition.get("ATTRIBUTE_DATATYPE");
                            Integer l = 8;
                            if(length.contains(",")) {
                                length = length.substring(length.indexOf(",") + 1);
                                length = length.substring(0, length.indexOf(")"));
                                l = Integer.parseInt(length);
                            }
                            StringBuffer outputBuffer = new StringBuffer(l);
                            // the following loop must start from 1 not from zero, because one zero is already appended in the format
                            for (int i = 1; i < l; i++) {
                                outputBuffer.append("#");
                                // variable length with no extra zeros
                            }
                            // must show at least one zero before and after decimal point.
                            String format = "0.0" + outputBuffer.toString();
                            CellStyle doubleStyle = null;
                            if (numericStyleMap.containsKey(format)) {
                                doubleStyle = numericStyleMap.get(format);
                            } else {
                                doubleStyle = workbook.createCellStyle();
                                doubleStyle.setDataFormat(creatorHelper.createDataFormat().getFormat(format));
                                // common style settings
                                addCommonCellStyleFormat(doubleStyle);
                                numericStyleMap.put(format, doubleStyle);
                            }
                            cell.setCellValue(doubleValue);
                            cell.setCellStyle(doubleStyle);
                        } else if ((value instanceof java.util.Date)
                                        || (value instanceof Temporal)) {
                            if (value instanceof java.sql.Timestamp) {
                                java.sql.Timestamp timestamp = (java.sql.Timestamp) value;
                                // convert timezone to client timezone
                                if (ConfigProperties.getInstance().use_client_timezone_to_display.getBooleanValue()) {
                                    value = DateTimeUtils.convertTimezoneFromUTCToClient(timestamp, executionContext);
                                }
                            } else if (value instanceof java.time.OffsetDateTime) {
                                java.time.OffsetDateTime offsetDateTime = (java.time.OffsetDateTime) value;
                                java.sql.Timestamp timestamp = Timestamp.from(offsetDateTime.toInstant());
                                value = timestamp;
                                // convert timezone to client timezone
                                if (ConfigProperties.getInstance().use_client_timezone_to_display.getBooleanValue()) {
                                    value = DateTimeUtils.convertTimezoneFromUTCToClient(timestamp, executionContext);
                                }
                            } else if (value instanceof java.time.ZonedDateTime) {
                                java.time.ZonedDateTime zonedDateTime = (java.time.ZonedDateTime) value;
                                java.sql.Timestamp timestamp = Timestamp.from(zonedDateTime.toInstant());
                                value = timestamp;
                                // convert timezone to client timezone
                                if (ConfigProperties.getInstance().use_client_timezone_to_display.getBooleanValue()) {
                                    value = DateTimeUtils.convertTimezoneFromUTCToClient(timestamp, executionContext);
                                }
                            }
                            cell.setCellValue((Date) value);
                            String controlTypeFromMetadata = (String) (metadata.get(boAttrNameToColumnIndexMap.get(boAttrName))).get(DSPDMConstants.BoAttrName.CONTROL_TYPE);
                            if (DSPDMConstants.ControlTypes.CALENDAR_WITH_DATE.equalsIgnoreCase(controlTypeFromMetadata)) {
                                cell.setCellStyle(dateStyle);
                            } else {
                                cell.setCellStyle(dateTimeStyle);
                            }
                        } else {
                            if (value.getClass().isArray()) {
                                if (value.getClass().getComponentType().isPrimitive()) {
                                    int length = Array.getLength(value);
                                    if (length > 0) {
                                        Object object = Array.get(value, 0);
                                        if (object instanceof Byte) {
                                            // need of explicit base64 conversion.
                                            String binary = null;
                                            try {
                                                binary = DatatypeConverter.printBase64Binary((byte[]) value);
                                            } catch (Exception ex) {
                                                try {
                                                    byte[] encoded = Base64.getEncoder().encode((byte[]) value);
                                                    binary = new String(encoded);
                                                } catch (Exception exc) {
                                                    binary = new String((byte[]) value, DSPDMConstants.UTF_8);
                                                }
                                            }
                                            cell.setCellValue(binary);
                                            cell.setCellStyle(binaryStyle);
                                        } else if (object instanceof Character) {
                                            cell.setCellValue(String.valueOf((char[]) value));
                                        } else {
                                            // skip
                                        }
                                    }
                                }
                            } else {
                                throw new DSPDMException("Cannot write a cell value in excel. Unknown data type encountered '{}' for attribute name '{}'", executionContext.getExecutorLocale(),
                                        value.getClass().getName(), boAttrName);
                            }
                        }
                    }
                }
            }
        }
    }
}
