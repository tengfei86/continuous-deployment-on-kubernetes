package com.lgc.dspdm.service.utils.excel;

import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.monitorjbl.xlsx.StreamingReader;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

public class MonitorJBLUtils {
    
    public static Map<String, Integer> getSheetNamesAndRecordsCount(InputStream input, ExecutionContext executionContext) throws Exception {
        Map<String, Integer> sheetNamesAndRecordsCountMap = null;
        Workbook workbook = null;
        try {
            workbook = StreamingReader.builder()
                    .rowCacheSize(5)
                    .bufferSize(4096)
                    .open(input);
            int numberOfSheets = workbook.getNumberOfSheets();
            sheetNamesAndRecordsCountMap = new LinkedHashMap<>(numberOfSheets);
            Sheet sheet = null;
            for (int i = 0; i < numberOfSheets; i++) {
                sheet = workbook.getSheetAt(i);
                sheetNamesAndRecordsCountMap.put(sheet.getSheetName().trim(), sheet.getLastRowNum());
            }
        } finally {
            if (workbook != null) {
                workbook.close();
            }
        }
        return sheetNamesAndRecordsCountMap;
    }
    
    public static Workbook getWorkbook(InputStream inputStream, ExecutionContext executionContext) {
        return StreamingReader.builder()
                // number of rows to keep in memory (defaults to 10)
                .rowCacheSize(100)
                // buffer size to use when reading InputStream to file (defaults to 1024)
                .bufferSize(4096)
                .open(inputStream);
    }
    
    public static Sheet getSheet(Workbook workbook, String boName, ExecutionContext executionContext) {
        Sheet sheet = null;
        int sheetCount = workbook.getNumberOfSheets();
        int index = 0;
        while (index < sheetCount) {
            sheet = workbook.getSheetAt(index);
            if (sheet.getSheetName().trim().equalsIgnoreCase(boName.trim())) {
                break;
            }
            index++;
        }
        if (index == sheetCount) {
            throw new DSPDMException("Invalid input : No excel sheet found for business object name '{}'", executionContext.getExecutorLocale(), boName);
        }
        return sheet;
    }
}
