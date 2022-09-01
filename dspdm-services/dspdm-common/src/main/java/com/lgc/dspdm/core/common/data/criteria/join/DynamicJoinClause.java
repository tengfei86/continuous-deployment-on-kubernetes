package com.lgc.dspdm.core.common.data.criteria.join;

import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Class is to be used for creating a read request for reading from multiple non-existing tables using sql join clause
 *
 * @author muhammadimran.ansari
 * @since 30-Apr-2020
 */
public class DynamicJoinClause extends BaseJoinClause {
    private List<DynamicTable> dynamicTables;

    public DynamicJoinClause(String joinAlias, JoinType joinType) {
        super(joinAlias, joinType);
    }

    public List<DynamicTable> getDynamicTables() {
        return dynamicTables;
    }

    public void setDynamicTables(List<DynamicTable> dynamicTables) {
        this.dynamicTables = dynamicTables;
    }

    /**
     * Returns a new list of containing all the  dynamic tables according to their join  order.
     * If no join order is specified then first come first serve order will work
     *
     * @return
     * @author Muhammad Imran Ansari
     * @since 30-Jul-2020
     */
    public List<DynamicTable> getDynamicTablesInTheirJoinOrder() {
        List<BaseJoinClause> orderedTables = new LinkedList<>();
        if (CollectionUtils.hasValue(dynamicTables)) {
            BaseJoinClause.addJoinsToListAsPerTheirOrder(orderedTables, dynamicTables);
        }
        return Arrays.asList(orderedTables.toArray(new DynamicTable[0]));
    }

    public DynamicJoinClause clone(ExecutionContext executionContext) {
        DynamicJoinClause dynamicJoinClause = (DynamicJoinClause) super.clone(executionContext);
        if (this.dynamicTables != null) {
            dynamicJoinClause.dynamicTables = new ArrayList<>(this.dynamicTables.size());
            for (DynamicTable dynamicTable : this.dynamicTables) {
                dynamicJoinClause.dynamicTables.add(dynamicTable.clone(executionContext));
            }
        }
        return dynamicJoinClause;
    }
}