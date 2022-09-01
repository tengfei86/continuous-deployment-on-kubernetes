package com.lgc.dspdm.core.common.data.common;

/**
 * @author Muhammad Imran Ansari
 * @since 02-Jan-2020
 */
public class BulkImportInvalidRecordReasonDTO {    
    private String boName = null;
    private String boAttrName = null;
    private String boAttrDisplayName = null;
    private Integer rowNumber = null;
    private Object value = null;
    private String message = null;
    
    public BulkImportInvalidRecordReasonDTO(String boName, String boAttrName, String boAttrDisplayName, Integer rowNumber, Object value, String message) {
        this.boName = boName;
        this.boAttrName = boAttrName;
        this.boAttrDisplayName = boAttrDisplayName;
        this.rowNumber = rowNumber;
        this.value = value;
        this.message = message;
    }
    
    public String getBoName() {
        return boName;
    }
    
    public String getBoAttrName() {
        return boAttrName;
    }
    
    public String getBoAttrDisplayName() {
        return boAttrDisplayName;
    }
    
    public Integer getRowNumber() {
        return rowNumber;
    }
    
    public void setRowNumber(Integer rowNumber) {
        this.rowNumber = rowNumber;
    }
    
    public Object getValue() {
        return value;
    }
    
    public String getMessage() {
        return message;
    }
}
