package com.lgc.dspdm.core.common.data.criteria;

import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class OrderBy implements Serializable, Cloneable {
    // map column name and order ascending or descending
    private Map<String, Boolean> orderMap = null;

    public OrderBy() {
        orderMap = new LinkedHashMap<>(3);
    }

    public OrderBy clone(ExecutionContext executionContext) {
        OrderBy orderBy = new OrderBy();
        if (this.orderMap != null) {
            orderBy.orderMap = new LinkedHashMap<>(this.orderMap);
        }
        return orderBy;
    }

    public Map<String, Boolean> getOrderMap() {
        return orderMap;
    }

    public OrderBy addOrderByAsc(String attributeName) {
        orderMap.put(attributeName, true);
        return this;
    }

    public OrderBy addOrderByDesc(String attributeName) {
        orderMap.put(attributeName, false);
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OrderBy{");
        sb.append("orderMap={");
        for (String key : orderMap.keySet()) {
            sb.append(key).append(" ").append((orderMap.get(key)) ? "ASC" : "DESC").append(",");
        }
        sb.append("}}");
        return sb.toString();
    }
}
