package com.lgc.dspdm.core.common.data.criteria;

import java.io.Serializable;
import java.sql.Types;

/**
 * Base class for filters of where clause and Having clause
 */
public class BaseFilter implements Serializable {
    private Operator operator;
    private Object[] values = null;
    private Object oldValue = null;
    // java sql data type to be set from metadata later otherwise default will be java.lang.String
    private int sqlDataType = Types.NULL;
    
    public BaseFilter(Operator operator, Object[] values, Object oldValue, int sqlDataType) {
        this.operator = operator;
        this.values = values;
        this.oldValue = oldValue;
        this.sqlDataType = sqlDataType;
        if ((values != null) && (values.length == 1)) {
            Object value = values[0];
            if ((value != null) && (value instanceof java.util.Collection)) {
                this.values = ((java.util.Collection) value).toArray();
            }
        }
    }
    
    public Object replaceValue(Object value) {
        this.oldValue = values;
        this.values = new Object[]{value};
        return this.oldValue;
    }
    
    public Object replaceValues(Object[] values) {
        this.oldValue = values;
        this.values = values;
        return this.oldValue;
    }
    
    public Operator getOperator() {
        return operator;
    }
    
    public Object[] getValues() {
        return values;
    }
    
    public Object getOldValue() {
        return oldValue;
    }
    
    public int getSqlDataType() {
        return sqlDataType;
    }
    
    public void setSqlDataType(int sqlDataType) {
        this.sqlDataType = sqlDataType;
    }
}
