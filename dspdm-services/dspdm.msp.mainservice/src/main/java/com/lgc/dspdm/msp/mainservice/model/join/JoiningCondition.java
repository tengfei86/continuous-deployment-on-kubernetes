package com.lgc.dspdm.msp.mainservice.model.join;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Class to be used as on condition of the join
 *
 * @author muhammadimran.ansari
 * @since 30-Apr-2020
 */
public class JoiningCondition {

    @JsonProperty("leftSide")
    @Schema(name = "leftSide", description = "Left side of the join on condition")
    private JoiningConditionOperand leftSide;

    @JsonProperty("operator")
    @Schema(name = "operator", description = "operator to be applied to the filter condition. Supported values are EQUALS, NOT_EQUALS, GREATER_THAN, LESS_THAN, GREATER_OR_EQUALS, LESS_OR_EQUALS, BETWEEN, NOT_BETWEEN, IN, NOT_IN, LIKE, NOT_LIKE, ILIKE, NOT_ILIKE, JSONB_FIND_EXACT, JSONB_FIND_LIKE, JSONB_DOT, JSONB_DOT_FOR_TEXT")
    private Operator operator;

    @JsonProperty("rightSide")
    @Schema(name = "rightSide", description = "Right side of the join on condition")
    private JoiningConditionOperand rightSide;

    public JoiningConditionOperand getLeftSide() {
        return leftSide;
    }

    public JoiningCondition setLeftSide(JoiningConditionOperand leftSide) {
        this.leftSide = leftSide;
        return this;
    }

    public Operator getOperator() {
        return operator;
    }

    public JoiningCondition setOperator(Operator operator) {
        this.operator = operator;
        return this;
    }

    public JoiningConditionOperand getRightSide() {
        return rightSide;
    }

    public JoiningCondition setRightSide(JoiningConditionOperand rightSide) {
        this.rightSide = rightSide;
        return this;
    }

    /**
     * Inner class to be used as operands of join operator
     */
    public static class JoiningConditionOperand {
        @JsonProperty("joinAlias")
        @Schema(name = "joinAlias", description = "Alias name which is already used in join with other business objects")
        private String joinAlias;

        @JsonProperty("boAttrName")
        @Schema(name = "boAttrName", description = "BO attribute name to be applied as a filter")
        private String boAttrName;

        public String getJoinAlias() {
            return joinAlias;
        }

        public JoiningConditionOperand setJoinAlias(String joinAlias) {
            this.joinAlias = joinAlias;
            return this;
        }

        public String getBoAttrName() {
            return boAttrName;
        }

        public JoiningConditionOperand setBoAttrName(String boAttrName) {
            this.boAttrName = boAttrName;
            return this;
        }
    }
}
