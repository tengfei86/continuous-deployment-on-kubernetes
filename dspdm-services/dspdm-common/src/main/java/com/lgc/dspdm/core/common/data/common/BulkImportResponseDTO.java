package com.lgc.dspdm.core.common.data.common;

import java.util.List;
import java.util.Map;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;

public class BulkImportResponseDTO {
    private Integer totalRecords = null;
    private PagedList<DynamicDTO> parsedValues = null;
    private PagedList<DynamicDTO> oldValues = null;
    private PagedList<DynamicDTO> invalidValues = null;
    private List<Map<String,String>> dspdmUnits=null;
    
    public BulkImportResponseDTO(Integer totalRecords, PagedList<DynamicDTO> parsedValues, PagedList<DynamicDTO> oldValues, PagedList<DynamicDTO> invalidValues) {
        this.totalRecords = totalRecords;
        this.parsedValues = parsedValues;
        this.oldValues = oldValues;
        this.invalidValues = invalidValues;
    }
    public BulkImportResponseDTO(Integer totalRecords, PagedList<DynamicDTO> parsedValues, PagedList<DynamicDTO> oldValues, PagedList<DynamicDTO> invalidValues,List<Map<String,String>> dspdmUnits) {
        this.totalRecords = totalRecords;
        this.parsedValues = parsedValues;
        this.oldValues = oldValues;
        this.invalidValues = invalidValues;
		this.dspdmUnits = dspdmUnits;
    }
    
    public Integer getTotalRecords() {
        return totalRecords;
    }
    
    public PagedList<DynamicDTO> getParsedValues() {
        return parsedValues;
    }
    
    public PagedList<DynamicDTO> getOldValues() {
        return oldValues;
    }
    
    public PagedList<DynamicDTO> getInvalidValues() {
        return invalidValues;
    }
    
    public List<Map<String,String>> getDspdmUnitsValues() {
        return dspdmUnits;
    }
}
