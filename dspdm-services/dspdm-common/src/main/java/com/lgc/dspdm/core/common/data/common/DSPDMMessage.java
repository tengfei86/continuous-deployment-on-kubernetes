package com.lgc.dspdm.core.common.data.common;

import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.StringUtils;

import java.io.Serializable;

/**
 * @author Muhammad Imran Ansari
 */
public class DSPDMMessage implements Serializable {
    private String message = null;
    private DSPDMConstants.Status status = null;
    
    public DSPDMMessage(DSPDMConstants.Status status, String message, Object... args) {
        this.message = StringUtils.formatMessage(message, args);
        this.status = status;
    }
    
    public String getMessage() {
        return message;
    }
    
    public DSPDMConstants.Status getStatus() {
        return status;
    }
    
    public int getStatusCode() {
        return status.getCode();
    }
    
    public String getStatusLabel() {
        return status.getLabel();
    }
}
