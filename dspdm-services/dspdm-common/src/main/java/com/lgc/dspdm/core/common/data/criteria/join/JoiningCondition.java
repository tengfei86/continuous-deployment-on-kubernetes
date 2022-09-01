package com.lgc.dspdm.core.common.data.criteria.join;

import com.lgc.dspdm.core.common.data.criteria.Operator;

/**
 * Class to be used as on condition of the join
 *
 * @author muhammadimran.ansari
 * @since 30-Apr-2020
 */
public class JoiningCondition {
    private JoiningConditionOperand leftSide;
    private Operator operator;
    private JoiningConditionOperand rightSide;

    public JoiningCondition() {
    }

    public JoiningCondition(JoiningConditionOperand leftSide, Operator operator, JoiningConditionOperand rightSide) {
        this.leftSide = leftSide;
        this.operator = operator;
        this.rightSide = rightSide;
    }

    public JoiningConditionOperand getLeftSide() {
        return leftSide;
    }

    public void setLeftSide(JoiningConditionOperand leftSide) {
        this.leftSide = leftSide;
    }

    public Operator getOperator() {
        return operator;
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    public JoiningConditionOperand getRightSide() {
        return rightSide;
    }

    public void setRightSide(JoiningConditionOperand rightSide) {
        this.rightSide = rightSide;
    }

    /**
     * Inner class to be used as operands of join operator
     */
    public static class JoiningConditionOperand {
        private String joinAlias;
        private String boAttrName;

        public JoiningConditionOperand() {
        }

        public JoiningConditionOperand(String joinAlias, String boAttrName) {
            this.joinAlias = joinAlias;
            this.boAttrName = boAttrName;
        }

        public String getJoinAlias() {
            return joinAlias;
        }

        public void setJoinAlias(String joinAlias) {
            this.joinAlias = joinAlias;
        }

        public String getBoAttrName() {
            return boAttrName;
        }

        public void setBoAttrName(String boAttrName) {
            this.boAttrName = boAttrName;
        }
    }
}
