package com.lgc.dspdm.core.common.data.criteria.join;

import com.lgc.dspdm.core.common.data.criteria.SelectList;
import com.lgc.dspdm.core.common.util.ExecutionContext;

/**
 * Class is to be used for creating a read request for reading from multiple tables using sql join clause
 *
 * @author muhammadimran.ansari
 * @since 30-Apr-2020
 */
public class DynamicTable extends BaseJoinClause {
    private static final long serialVersionUID = 1L;
    private String boName;

    public DynamicTable(String boName, SelectList selectList) {
        super(selectList);
        this.boName = boName;
    }

    public String getBoName() {
        return boName;
    }

    public void setBoName(String boName) {
        this.boName = boName;
    }

    public DynamicTable clone(ExecutionContext executionContext) {
        DynamicTable dynamicTable = (DynamicTable) super.clone(executionContext);
        return dynamicTable;
    }
}
