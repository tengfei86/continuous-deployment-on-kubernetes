package com.lgc.dspdm.core.common.data.criteria;

import java.io.Serializable;

public enum Operator implements Serializable {
    EQUALS(" = "," eq "),
    NOT_EQUALS(" != "," neq "),
    GREATER_THAN(" > "," gt "),
    LESS_THAN(" < "," lt "),
    GREATER_OR_EQUALS(" >= "," gte "),
    LESS_OR_EQUALS(" <= "," lte "),
    BETWEEN(" BETWEEN "," between "),
    NOT_BETWEEN(" NOT BETWEEN "," nbetween "),
    IN(" IN "," in "),
    NOT_IN(" NOT IN "," nin "),
    LIKE(" LIKE "," like "),
    NOT_LIKE(" NOT LIKE "," nlike "),
    ILIKE(" ILIKE "," ilike "),
    NOT_ILIKE(" NOT ILIKE "," nilike "),
    JSONB_FIND_EXACT(" jsonb_exists_any "," jsonb_exists_any "),
    JSONB_FIND_LIKE("::text similar to ","::text similar to"),
    JSONB_DOT(" -> "," -> "),
    JSONB_DOT_FOR_TEXT(" ->> "," ->> "),
    AND(" AND "," and "),
    OR(" OR "," or ");
    
    Operator(String operator,String restOperator) {
        this.operator = operator;
        this.restOperator=restOperator;
    }
    
    private String operator = null;
    private String restOperator = null;
    
    public String getOperator() {
        return operator;
    }
    public String getRestOperator() {
        return restOperator;
    }
    
	public static Operator getOperatorByRest(String restOperator) {
		for (Operator operator : values()) {
			if (operator.getRestOperator().equals(restOperator)) {
				return operator;
			}
		}
		return null;
	}
}
