package com.lgc.dspdm.core.common.data.criteria;

import java.sql.Types;

/**
 * Class to be used for SQL where clause filters
 * @author Muhammad Imran Ansari
 * @since 16-Jan-2020
 */
public class CriteriaFilter extends BaseFilter {
    
    private String columnName = null;
        
    public CriteriaFilter(String columnName, Operator operator, Object[] values) {
        super(operator, values, null, Types.NULL);
        this.columnName = columnName;
    }
    
    public String getColumnName() {
        return columnName;
    }
}
