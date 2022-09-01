package com.lgc.dspdm.core.common.exception;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class DSPDMBatchException extends DSPDMException {
    private List<DSPDMException> nestedExceptions = null;
    
    public DSPDMBatchException(String sqlState, Integer sqlErrorCode, String message, Throwable cause, Locale locale) {
        super(sqlState, sqlErrorCode, message, cause, locale);
        this.nestedExceptions = null;
    }
    
    public void addException(DSPDMException e) {
        if (nestedExceptions == null) {
            nestedExceptions = new ArrayList<>();
        }
        nestedExceptions.add(e);
    }
    
    public List<DSPDMException> getNestedExceptions() {
        return nestedExceptions;
    }
}
