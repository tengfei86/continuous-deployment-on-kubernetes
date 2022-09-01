package com.lgc.dspdm.core.common.data.criteria.join;

public enum JoinType {
    INNER("INNER JOIN"),
    LEFT("LEFT OUTER JOIN"),
    RIGHT("RIGHT OUTER JOIN"),
    FULL("FULL OUTER JOIN");

    private String clause = null;

    JoinType(String clause) {
        this.clause = clause;
    }

    public String getClause() {
        return clause;
    }
}
