package com.lgc.dspdm.core.common.data.criteria;

/**
 * @author Muhammad Imran Ansari
 * @since 17-Jan-2020
 */
public class SQLExpression {
    private String expression = null;
    private Object[] values = null;
    private int[] valuesSqlDataType = null;
    private int paramAddedCount = 0;
    
    public SQLExpression(String expression, int paramsCount) {
        this.expression = expression;
        if (!(this.expression.startsWith("("))) {
            this.expression = "(" + this.expression + ")";
        }
        if (paramsCount > 0) {
            values = new Object[paramsCount];
            valuesSqlDataType = new int[paramsCount];
        }
    }
    
    public SQLExpression addParam(Object value, int sqlDataType) {
        values[paramAddedCount] = value;
        valuesSqlDataType[paramAddedCount] = sqlDataType;
        paramAddedCount++;
        return this;
    }
    
    public String getExpression() {
        return expression;
    }
    
    public Object[] getValues() {
        return values;
    }
    
    public int[] getValuesSqlDataType() {
        return valuesSqlDataType;
    }
    
    public int getParamAddedCount() {
        return paramAddedCount;
    }
}
