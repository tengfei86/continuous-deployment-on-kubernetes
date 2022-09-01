package com.lgc.dspdm.core.common.data.criteria.aggregate;

import com.lgc.dspdm.core.common.data.criteria.BaseFilter;
import com.lgc.dspdm.core.common.data.criteria.Operator;

import java.sql.Types;

/**
 * This class is used to add sql having clause filters. to be applied on aggregated data.
 *
 * @author Muhammad Imran Ansari
 * @since 16-Jan-2020
 */
public class HavingFilter extends BaseFilter {
    private AggregateColumn aggregateColumn = null;
    
    public HavingFilter(AggregateColumn aggregateColumn, Operator operator, Object[] values) {
        super(operator, values, null, Types.NULL);
        this.aggregateColumn = aggregateColumn;
    }
    
    public AggregateColumn getAggregateColumn() {
        return aggregateColumn;
    }
}
