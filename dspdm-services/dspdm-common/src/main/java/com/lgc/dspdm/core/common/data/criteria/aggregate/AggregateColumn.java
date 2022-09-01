package com.lgc.dspdm.core.common.data.criteria.aggregate;

import java.io.Serializable;
import java.util.Objects;

public class AggregateColumn implements Serializable {
    private AggregateFunction aggregateFunction = null;
    private String boAttrName = null;
    private String columnAlias = null;
    
    public AggregateColumn(AggregateFunction aggregateFunction, String boAttrName) {
        this.aggregateFunction = aggregateFunction;
        this.boAttrName = boAttrName;
    }
    
    public AggregateColumn(AggregateFunction aggregateFunction, String boAttrName, String columnAlias) {
        this.aggregateFunction = aggregateFunction;
        this.boAttrName = boAttrName;
        this.columnAlias = columnAlias;
    }
    
    public AggregateFunction getAggregateFunction() {
        return aggregateFunction;
    }
    
    public String getBoAttrName() {
        return boAttrName;
    }
    
    public String getColumnAlias() {
        return columnAlias;
    }
    
    public void setColumnAlias(String columnAlias) {
        this.columnAlias = columnAlias;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AggregateColumn)) {
            return false;
        }
        AggregateColumn that = (AggregateColumn) o;
        return aggregateFunction == that.aggregateFunction &&
                Objects.equals(boAttrName, that.boAttrName) &&
                Objects.equals(columnAlias, that.columnAlias);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(aggregateFunction, boAttrName, columnAlias);
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AggregateColumn{");
        sb.append("aggregateFunction=").append(aggregateFunction);
        sb.append(", boAttrName=").append(boAttrName);
        sb.append(", columnAlias=").append(columnAlias);
        sb.append('}');
        return sb.toString();
    }
}
