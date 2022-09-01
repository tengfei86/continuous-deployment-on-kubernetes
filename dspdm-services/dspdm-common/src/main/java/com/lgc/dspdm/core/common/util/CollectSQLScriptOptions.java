package com.lgc.dspdm.core.common.util;

public enum CollectSQLScriptOptions {
    NO_SCRIPT(0, "No SQL Script At All (Default)"),   // default
    COUNT_ONLY(1, "COUNT Only"),
    READ_ONLY(2, "SELECT ONLY"),
    READ_COUNT_ONLY(3, "SELECT and COUNT"),
    INSERT_ONLY(4, "INSERT Only"),
    UPDATE_ONLY(5, "UPDATE Only"),
    DELETE_ONLY(6, "DELETE Only"),
    INSERT_UPDATE_ONLY(7, "INSERT and UPDATE Only"),
    ALL_DML_ONLY(8, "ALL DML Operations Only"),
    ALL_DDL_ONLY(9, "ALL DDL Operations Only"),
    ALL_DDL_DML_ONLY(10, "ALL DDL and DML Operations Only"),
    ALL(11, "ALL SQL Operations");

    CollectSQLScriptOptions(int value, String label) {
        this.value = value;
        this.label = label;
    }

    private int value = 0;
    private String label = null;
    public static String description = null;

    public int getValue() {
        return value;
    }

    public String getLabel() {
        return label;
    }


    public static CollectSQLScriptOptions getByValue(Integer value) {
        if(value != null) {
            for (CollectSQLScriptOptions option : values()) {
                if (option.getValue() == value) {
                    return option;
                }
            }
        }
        return null;
    }

    static {
        final StringBuilder sb = new StringBuilder("Following are the possible values.        ");
        for (CollectSQLScriptOptions option : values()) {
            sb.append(option.value).append(" : ").append(option.label).append(",       ");
        }
        description = sb.toString();
    }

    public boolean isEnabled() {
        boolean flag = false;
        if (this != NO_SCRIPT) {
            flag = true;
        }
        return flag;
    }

    public boolean isCountEnabled() {
        boolean flag = false;
        switch (this) {
            case COUNT_ONLY:
            case READ_COUNT_ONLY:
            case ALL:
                flag = true;
                break;
            default:
                flag = false;
        }
        return flag;
    }

    public boolean isReadEnabled() {
        boolean flag = false;
        switch (this) {
            case READ_ONLY:
            case READ_COUNT_ONLY:
            case ALL:
                flag = true;
                break;
            default:
                flag = false;
        }
        return flag;
    }

    public boolean isInsertEnabled() {
        boolean flag = false;
        switch (this) {
            case INSERT_ONLY:
            case INSERT_UPDATE_ONLY:
            case ALL_DML_ONLY:
            case ALL_DDL_DML_ONLY:
            case ALL:
                flag = true;
                break;
            default:
                flag = false;
        }
        return flag;
    }

    public boolean isUpdateEnabled() {
        boolean flag = false;
        switch (this) {
            case UPDATE_ONLY:
            case INSERT_UPDATE_ONLY:
            case ALL_DML_ONLY:
            case ALL_DDL_DML_ONLY:
            case ALL:
                flag = true;
                break;
            default:
                flag = false;
        }
        return flag;
    }

    public boolean isDeleteEnabled() {
        boolean flag = false;
        switch (this) {
            case DELETE_ONLY:
            case ALL_DML_ONLY:
            case ALL_DDL_DML_ONLY:
            case ALL:
                flag = true;
                break;
            default:
                flag = false;
        }
        return flag;
    }

    public boolean isDDLEnabled() {
        boolean flag = false;
        switch (this) {
            case ALL_DDL_ONLY:
            case ALL_DDL_DML_ONLY:
            case ALL:
                flag = true;
                break;
            default:
                flag = false;
        }
        return flag;
    }

    public boolean isALLEnabled() {
        boolean flag = false;
        switch (this) {
            case ALL:
                flag = true;
                break;
            default:
                flag = false;
        }
        return flag;
    }
}
