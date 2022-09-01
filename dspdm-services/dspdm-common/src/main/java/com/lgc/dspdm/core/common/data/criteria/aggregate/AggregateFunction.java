package com.lgc.dspdm.core.common.data.criteria.aggregate;

public enum AggregateFunction {
    /**
     * return the average value.
     */
    AVG("AVG"),
    /**
     * return the number of values.
     */
    COUNT("COUNT"),
    /**
     * return the maximum value.
     */
    MAX("MAX"),
    /**
     * return the minimum value.
     */
    MIN("MIN"),
    /**
     * return the sum of all or distinct values.
     */
    SUM("SUM");
    
    AggregateFunction(String functionName) {
        this.functionName = functionName;
    }
    
    private String functionName = null;
    
    public String getFunctionName() {
        return functionName;
    }
}
