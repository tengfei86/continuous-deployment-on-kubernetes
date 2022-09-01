package com.lgc.dspdm.core.common.data.criteria;

import com.lgc.dspdm.core.common.data.criteria.aggregate.AggregateColumn;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SelectList implements Serializable, Cloneable {
    private List<String> columnsToSelect = null;
    private List<AggregateColumn> aggregateColumnsToSelect = null;

    public SelectList() {
    }

    public SelectList clone(ExecutionContext executionContext) {
        SelectList selectList = new SelectList();
        if (this.columnsToSelect != null) {
            selectList.columnsToSelect = new ArrayList<>(this.columnsToSelect);
        }
        if (this.aggregateColumnsToSelect != null) {
            selectList.aggregateColumnsToSelect = new ArrayList<>(this.aggregateColumnsToSelect);
        }
        return selectList;
    }

    public SelectList(List<String> columnsToSelect) {
        this.columnsToSelect = columnsToSelect;
    }

    public List<String> getColumnsToSelect() {
        return columnsToSelect;
    }

    public List<AggregateColumn> getAggregateColumnsToSelect() {
        return aggregateColumnsToSelect;
    }

    public SelectList addColumnsToSelect(List<String> columnsToSelect) {
        if (this.columnsToSelect == null) {
            this.columnsToSelect = new ArrayList<>(columnsToSelect.size());
        }
        for (String newColumn : columnsToSelect) {
            if (!(CollectionUtils.containsIgnoreCase(this.columnsToSelect, newColumn))) {
                this.columnsToSelect.add(newColumn);
            }
        }
        return this;
    }

    public SelectList addColumnsToSelect(String... columnsToSelect) {
        if (this.columnsToSelect == null) {
            this.columnsToSelect = new ArrayList<>(columnsToSelect.length);
        }
        //column maybe split by comma, for example:
        //[0] WELL_COMPLETION_ID
        //[1] WELLBORE_UWI,COMPLETION_DATE,COMPLETION_OBS_NO
        for (String column : columnsToSelect) {
            if (column.indexOf(',') > -1) {
                String[] splitColumns = column.split(",");
                for (String splitColumn : splitColumns) {
                    if (!(CollectionUtils.containsIgnoreCase(this.columnsToSelect, splitColumn.trim()))) {
                        this.columnsToSelect.add(splitColumn.trim());
                    }
                }
            } else {
                if (!(CollectionUtils.containsIgnoreCase(this.columnsToSelect, column))) {
                    this.columnsToSelect.add(column);
                }
            }
        }
        return this;
    }

    public SelectList addAggregateColumnsToSelect(AggregateColumn... aggregateColumns) {
        if (this.aggregateColumnsToSelect == null) {
            this.aggregateColumnsToSelect = new ArrayList<>(aggregateColumns.length);
        }
        this.aggregateColumnsToSelect.addAll(Arrays.asList(aggregateColumns));
        return this;
    }

    public SelectList addAggregateColumnsToSelect(List<AggregateColumn> aggregateColumns) {
        if (this.aggregateColumnsToSelect == null) {
            this.aggregateColumnsToSelect = new ArrayList<>(aggregateColumns.size());
        }
        this.aggregateColumnsToSelect.addAll(aggregateColumns);
        return this;
    }

    public boolean contains(String column) {
        return columnsToSelect.contains(column);
    }

    public int getTotalColumnsCount() {
        int count = 0;
        if (this.columnsToSelect != null) {
            count = this.columnsToSelect.size();
        }
        if (this.aggregateColumnsToSelect != null) {
            count = this.aggregateColumnsToSelect.size();
        }
        return count;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SelectList{");
        sb.append("columnsToSelect=").append(CollectionUtils.getCommaSeparated(columnsToSelect));
        sb.append('}');
        return sb.toString();
    }
}
