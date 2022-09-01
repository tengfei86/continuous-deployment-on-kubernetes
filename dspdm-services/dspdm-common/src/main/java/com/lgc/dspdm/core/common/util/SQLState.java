package com.lgc.dspdm.core.common.util;

import com.lgc.dspdm.core.common.config.ConnectionProperties;
import com.lgc.dspdm.core.common.exception.DSPDMException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public abstract class SQLState {

    private static String getMessage(String sqlState, int sqlErrorCode, String defaultMessage, Locale locale) {
        String message = null;
        if (ConnectionProperties.isPostgresDialect()) {
            if (StringUtils.hasValue(sqlState)) {
                message = PostgresSQLState.map.get(sqlState);
            } else {
                message = defaultMessage;
            }
        } else if (ConnectionProperties.isMSSQLServerDialect()) {
            // keep sql state here for future use of sql server new versions if supported
            if ((sqlErrorCode != 0) && (MSSQLServerSQLState.sqlErrorCodeMap.containsKey(sqlErrorCode))) {
                message = MSSQLServerSQLState.sqlErrorCodeMap.get(sqlErrorCode);
            } else if (StringUtils.hasValue(sqlState)) {
                message = MSSQLServerSQLState.sqlStatsMap.get(sqlState);
            } else {
                message = defaultMessage;
            }
        } else {
            message = defaultMessage;
        }

        if (message == null) {
            message = defaultMessage;
        }
        return message;
    }

    public static DSPDMException getActualExceptionForAddRelationship(DSPDMException exception, ExecutionContext executionContext) {
        String sqlState = (exception.getSqlState() != null) ? exception.getSqlState() : DSPDMConstants.EMPTY_STRING;
        Integer sqlErrorCode = (exception.getSqlErrorCode() != null) ? exception.getSqlErrorCode() : 0;
        String message = (exception.getMessage() != null) ? exception.getMessage() : DSPDMConstants.EMPTY_STRING;
        if (isUniqueKeyViolatedSQLStateAndCode(sqlState, sqlErrorCode, message)
                || isDuplicateColumnSQLStateAndCode(sqlState, sqlErrorCode)) {
            message = "Relationship(s) with same name already exist.";
        } else if (isNotNullViolatedSQLStateAndCode(sqlState, sqlErrorCode)) {
            message = "Relationship(s) cannot be added due to not null constraint violation. Please check the values provided.";
        } else {
            message = getMessage(sqlState, sqlErrorCode, message, executionContext.getExecutorLocale());
        }
        exception = new DSPDMException(message, exception, executionContext.getExecutorLocale());
        return exception;
    }

    public static DSPDMException getActualExceptionForAddUniqueConstraint(DSPDMException exception, ExecutionContext executionContext) {
        String sqlState = (exception.getSqlState() != null) ? exception.getSqlState() : DSPDMConstants.EMPTY_STRING;
        Integer sqlErrorCode = (exception.getSqlErrorCode() != null) ? exception.getSqlErrorCode() : 0;
        String message = (exception.getMessage() != null) ? exception.getMessage() : DSPDMConstants.EMPTY_STRING;
        if (isUniqueKeyViolatedSQLStateAndCode(sqlState, sqlErrorCode, message)
                || isDuplicateColumnSQLStateAndCode(sqlState, sqlErrorCode)) {
            message = "Unique constraint(s) with same name already exist.";
        } else if (isNotNullViolatedSQLStateAndCode(sqlState, sqlErrorCode)) {
            message = "Unique constraint(s) cannot be added due to not null constraint violation. Please check the values provided.";
        } else {
            message = getMessage(sqlState, sqlErrorCode, message, executionContext.getExecutorLocale());
        }
        exception = new DSPDMException(message, exception, executionContext.getExecutorLocale());
        return exception;
    }

    public static DSPDMException getActualExceptionForAddSearchIndex(DSPDMException exception, ExecutionContext executionContext) {
        String sqlState = (exception.getSqlState() != null) ? exception.getSqlState() : DSPDMConstants.EMPTY_STRING;
        Integer sqlErrorCode = (exception.getSqlErrorCode() != null) ? exception.getSqlErrorCode() : 0;
        String message = (exception.getMessage() != null) ? exception.getMessage() : DSPDMConstants.EMPTY_STRING;
        if (isUniqueKeyViolatedSQLStateAndCode(sqlState, sqlErrorCode, message)
                || isDuplicateColumnSQLStateAndCode(sqlState, sqlErrorCode)) {
            message = "Search index(es) with same name already exist.";
        } else if (isNotNullViolatedSQLStateAndCode(sqlState, sqlErrorCode)) {
            message = "Search index(es) cannot be added due to not null constraint violation. Please check the values provided.";
        } else {
            message = getMessage(sqlState, sqlErrorCode, message, executionContext.getExecutorLocale());
        }
        exception = new DSPDMException(message, exception, executionContext.getExecutorLocale());
        return exception;
    }

    public static DSPDMException getActualExceptionForDeleteBusinessObjectMetadata(DSPDMException exception, ExecutionContext executionContext) {
        String sqlState = (exception.getSqlState() != null) ? exception.getSqlState() : DSPDMConstants.EMPTY_STRING;
        Integer sqlErrorCode = (exception.getSqlErrorCode() != null) ? exception.getSqlErrorCode() : 0;
        String message = (exception.getMessage() != null) ? exception.getMessage() : DSPDMConstants.EMPTY_STRING;
        if (isForeignKeyViolatedSQLStateAndCode(sqlState, sqlErrorCode, message)) {
            message = "Business object(s) metadata cannot be deleted due to dependent business object(s). Please drop dependent business object(s) first and then try again.";
        } else {
            message = getMessage(sqlState, sqlErrorCode, message, executionContext.getExecutorLocale());
        }
        exception = new DSPDMException(message, exception, executionContext.getExecutorLocale());
        return exception;
    }

    public static DSPDMException getActualExceptionForDropBusinessObject(DSPDMException exception, ExecutionContext executionContext) {
        String sqlState = (exception.getSqlState() != null) ? exception.getSqlState() : DSPDMConstants.EMPTY_STRING;
        Integer sqlErrorCode = (exception.getSqlErrorCode() != null) ? exception.getSqlErrorCode() : 0;
        String message = (exception.getMessage() != null) ? exception.getMessage() : DSPDMConstants.EMPTY_STRING;
        if (isForeignKeyViolatedSQLStateAndCode(sqlState, sqlErrorCode, message)) {
            message = "Business object(s) cannot be dropped due to dependent business object(s). Please drop dependent business object(s) first and then try again.";
        } else {
            message = getMessage(sqlState, sqlErrorCode, message, executionContext.getExecutorLocale());
        }
        exception = new DSPDMException(message, exception, executionContext.getExecutorLocale());
        return exception;
    }

    public static DSPDMException getActualExceptionForAddBusinessObject(DSPDMException exception, ExecutionContext executionContext) {
        String sqlState = (exception.getSqlState() != null) ? exception.getSqlState() : DSPDMConstants.EMPTY_STRING;
        Integer sqlErrorCode = (exception.getSqlErrorCode() != null) ? exception.getSqlErrorCode() : 0;
        String message = (exception.getMessage() != null) ? exception.getMessage() : DSPDMConstants.EMPTY_STRING;
        if (isUniqueKeyViolatedSQLStateAndCode(sqlState, sqlErrorCode, message)
                || isDuplicateColumnSQLStateAndCode(sqlState, sqlErrorCode)) {
            message = "Business object(s) with same name already exist.";
        } else if (isNotNullViolatedSQLStateAndCode(sqlState, sqlErrorCode)) {
            message = "Business object(s) cannot be added due to not null constraint violation. Please check the values provided.";
        } else {
            message = getMessage(sqlState, sqlErrorCode, message, executionContext.getExecutorLocale());
        }
        exception = new DSPDMException(message, exception, executionContext.getExecutorLocale());
        return exception;
    }

    public static DSPDMException getActualExceptionForDeleteCustomAttribute(DSPDMException exception, ExecutionContext executionContext) {
        String sqlState = (exception.getSqlState() != null) ? exception.getSqlState() : DSPDMConstants.EMPTY_STRING;
        Integer sqlErrorCode = (exception.getSqlErrorCode() != null) ? exception.getSqlErrorCode() : 0;
        String message = (exception.getMessage() != null) ? exception.getMessage() : DSPDMConstants.EMPTY_STRING;
        if (isForeignKeyViolatedSQLStateAndCode(sqlState, sqlErrorCode, message)) {
            message = "Custom attribute(s) cannot be deleted due to foreign key violation. Please delete dependent attributes first and then try again.";
        } else {
            message = getMessage(sqlState, sqlErrorCode, message, executionContext.getExecutorLocale());
        }
        exception = new DSPDMException(message, exception, executionContext.getExecutorLocale());
        return exception;
    }

    public static DSPDMException getActualExceptionForAddCustomAttribute(DSPDMException exception, ExecutionContext executionContext) {
        String sqlState = (exception.getSqlState() != null) ? exception.getSqlState() : DSPDMConstants.EMPTY_STRING;
        Integer sqlErrorCode = (exception.getSqlErrorCode() != null) ? exception.getSqlErrorCode() : 0;
        String message = (exception.getMessage() != null) ? exception.getMessage() : DSPDMConstants.EMPTY_STRING;
        if (isUniqueKeyViolatedSQLStateAndCode(sqlState, sqlErrorCode, message)
                || isDuplicateColumnSQLStateAndCode(sqlState, sqlErrorCode)) {
            message = "Custom attribute(s) with same name already exist.";
        } else if (isNotNullViolatedSQLStateAndCode(sqlState, sqlErrorCode)) {
            message = "Custom attribute(s) cannot be added due to not null constraint violation. Please check the values provided.";
        } else {
            message = getMessage(sqlState, sqlErrorCode, message, executionContext.getExecutorLocale());
        }
        exception = new DSPDMException(message, exception, executionContext.getExecutorLocale());
        return exception;
    }

    public static DSPDMException getActualExceptionForDelete(DSPDMException exception, ExecutionContext executionContext) {
        String sqlState = (exception.getSqlState() != null) ? exception.getSqlState() : DSPDMConstants.EMPTY_STRING;
        Integer sqlErrorCode = (exception.getSqlErrorCode() != null) ? exception.getSqlErrorCode() : 0;
        String message = (exception.getMessage() != null) ? exception.getMessage() : DSPDMConstants.EMPTY_STRING;
        if (isForeignKeyViolatedSQLStateAndCode(sqlState, sqlErrorCode, message)) {
            message = "These record(s) cannot be deleted due to dependent record(s). Please delete dependent record(s) first and then try again.";
        } else {
            message = getMessage(sqlState, sqlErrorCode, message, executionContext.getExecutorLocale());
        }
        exception = new DSPDMException(message, exception, executionContext.getExecutorLocale());
        return exception;
    }

    public static DSPDMException getActualExceptionForSave(DSPDMException exception, ExecutionContext executionContext) {
        String sqlState = (exception.getSqlState() != null) ? exception.getSqlState() : DSPDMConstants.EMPTY_STRING;
        Integer sqlErrorCode = (exception.getSqlErrorCode() != null) ? exception.getSqlErrorCode() : 0;
        String message = (exception.getMessage() != null) ? exception.getMessage() : DSPDMConstants.EMPTY_STRING;
        if (isForeignKeyViolatedSQLStateAndCode(sqlState, sqlErrorCode, message)) {
            exception = new DSPDMException("These record(s) cannot be inserted or updated due to foreign key violation. Please check the values for reference columns.", exception, executionContext.getExecutorLocale());
        } else if (isCheckConstraintViolatedSQLStateAndCode(sqlState, sqlErrorCode, message)) {
            exception = new DSPDMException("These record(s) cannot be inserted or updated due to check constraint violation. Please check the values provided.", exception, executionContext.getExecutorLocale());
        } else if (isPrimaryKeyViolatedSQLStateAndCode(sqlState, sqlErrorCode, message)) {
            exception = new DSPDMException("These record(s) cannot be inserted or updated due to primary key unique violation. Please check the values provided.", exception, executionContext.getExecutorLocale());
        } else if (isUniqueKeyViolatedSQLStateAndCode(sqlState, sqlErrorCode, message)) {
            exception = new DSPDMException("These record(s) cannot be inserted or updated due to unique constraint violation. Please check the values provided.", exception, executionContext.getExecutorLocale());
        } else if (isDuplicateColumnSQLStateAndCode(sqlState, sqlErrorCode)) {
            exception = new DSPDMException("These record(s) cannot be inserted or updated due to unique constraint violation. Please check the values provided.", exception, executionContext.getExecutorLocale());
        } else if (isNotNullViolatedSQLStateAndCode(sqlState, sqlErrorCode)) {
            exception = new DSPDMException("These record(s) cannot be inserted or updated due to not null constraint violation. Please check the values provided.", exception, executionContext.getExecutorLocale());
        } else if (isZeroLengthStringSQLStateAndCode(sqlState, sqlErrorCode)) {
            exception = new DSPDMException("These record(s) cannot be inserted or updated due to not empty constraint violation. Please check the values provided.", exception, executionContext.getExecutorLocale());
        }
        return exception;
    }

    private static boolean isForeignKeyViolatedSQLStateAndCode(String sqlState, Integer sqlErrorCode, String message) {
        boolean flag = false;
        if (ConnectionProperties.isPostgresDialect()) {
            flag = PostgresSQLState.foreign_key_violation.getSqlState().equalsIgnoreCase(sqlState);
        } else if (ConnectionProperties.isMSSQLServerDialect()) {
            if (MSSQLServerSQLState.sqlErrorCodeMap.containsKey(sqlErrorCode)) {
                if (MSSQLServerSQLState.integrity_constraint_violation_foreign_key_or_check_violation.getSqlErrorCode() == sqlErrorCode) {
                    // check message contains text becuase error code 547 is same for foreign key violation and check constraint violation
                    if ((StringUtils.hasValue(message))
                            && ((message.toUpperCase().contains(" FOREIGN ")) || (message.toUpperCase().contains(" REFERENCE ")))) {
                        flag = true;
                    }
                }
            } else {
                flag = MSSQLServerSQLState.integrity_constraint_violation_foreign_key_or_check_violation.getSqlState().equalsIgnoreCase(sqlState);
            }
        }
        return flag;
    }

    private static boolean isCheckConstraintViolatedSQLStateAndCode(String sqlState, Integer sqlErrorCode, String message) {
        boolean flag = false;
        if (ConnectionProperties.isPostgresDialect()) {
            flag = PostgresSQLState.check_violation.getSqlState().equalsIgnoreCase(sqlState);
        } else if (ConnectionProperties.isMSSQLServerDialect()) {
            if (MSSQLServerSQLState.sqlErrorCodeMap.containsKey(sqlErrorCode)) {
                if (MSSQLServerSQLState.integrity_constraint_violation_foreign_key_or_check_violation.getSqlErrorCode() == sqlErrorCode) {
                    // check message contains text becuase error code 547 is same for foreign key violation and check constraint violation
                    if ((StringUtils.hasValue(message)) && (message.toUpperCase().contains(" CHECK "))) {
                        flag = true;
                    }
                }
            } else {
                flag = MSSQLServerSQLState.integrity_constraint_violation_foreign_key_or_check_violation.getSqlState().equalsIgnoreCase(sqlState);
            }
        }
        return flag;
    }

    private static boolean isPrimaryKeyViolatedSQLStateAndCode(String sqlState, Integer sqlErrorCode, String message) {
        boolean flag = false;
        if (ConnectionProperties.isPostgresDialect()) {
            flag = PostgresSQLState.unique_violation.getSqlState().equalsIgnoreCase(sqlState);
        } else if (ConnectionProperties.isMSSQLServerDialect()) {
            if (MSSQLServerSQLState.sqlErrorCodeMap.containsKey(sqlErrorCode)) {
                if (MSSQLServerSQLState.integrity_constraint_violation_unique_violation_unique_key_primary_key.getSqlErrorCode() == sqlErrorCode) {
                    // check message contains text becuase error code 547 is same for foreign key violation and check constraint violation
                    if ((StringUtils.hasValue(message)) && (message.toUpperCase().contains(" PRIMARY "))) {
                        flag = true;
                    }
                }
            } else if (MSSQLServerSQLState.integrity_constraint_violation_unique_violation_unique_key_primary_key.getSqlState().equalsIgnoreCase(sqlState)) {
                // check message contains text becuase error code 547 is same for foreign key violation and check constraint violation
                if ((StringUtils.hasValue(message)) && (message.toUpperCase().contains(" PRIMARY "))) {
                    flag = true;
                }
            }
        }
        return flag;
    }

    private static boolean isUniqueKeyViolatedSQLStateAndCode(String sqlState, Integer sqlErrorCode, String message) {
        boolean flag = false;
        if (ConnectionProperties.isPostgresDialect()) {
            flag = PostgresSQLState.unique_violation.getSqlState().equalsIgnoreCase(sqlState);
        } else if (ConnectionProperties.isMSSQLServerDialect()) {
            if (MSSQLServerSQLState.sqlErrorCodeMap.containsKey(sqlErrorCode)) {
                if (MSSQLServerSQLState.integrity_constraint_violation_unique_violation_unique_key_primary_key.getSqlErrorCode() == sqlErrorCode) {
                    // check message contains text becuase error code 547 is same for foreign key violation and check constraint violation
                    if ((StringUtils.hasValue(message)) && (message.toUpperCase().contains(" UNIQUE "))) {
                        flag = true;
                    }
                }
            } else {
                flag = MSSQLServerSQLState.integrity_constraint_violation_unique_violation_unique_key_primary_key.getSqlState().equalsIgnoreCase(sqlState);
            }
        }
        return flag;
    }

    private static boolean isDuplicateColumnSQLStateAndCode(String sqlState, Integer sqlErrorCode) {
        boolean flag = false;
        if (ConnectionProperties.isPostgresDialect()) {
            flag = PostgresSQLState.duplicate_column.getSqlState().equalsIgnoreCase(sqlState);
        } else if (ConnectionProperties.isMSSQLServerDialect()) {
            flag = MSSQLServerSQLState.duplicate_column.getSqlState().equalsIgnoreCase(sqlState);
        }
        return flag;
    }

    private static boolean isNotNullViolatedSQLStateAndCode(String sqlState, Integer sqlErrorCode) {
        boolean flag = false;
        if (ConnectionProperties.isPostgresDialect()) {
            flag = PostgresSQLState.not_null_violation.getSqlState().equalsIgnoreCase(sqlState);
        } else if (ConnectionProperties.isMSSQLServerDialect()) {
            if (MSSQLServerSQLState.sqlErrorCodeMap.containsKey(sqlErrorCode)) {
                flag = MSSQLServerSQLState.integrity_constraint_violation_not_null_violation.getSqlErrorCode() == sqlErrorCode;
            } else {
                flag = MSSQLServerSQLState.integrity_constraint_violation_not_null_violation.getSqlState().equalsIgnoreCase(sqlState);
            }
        }
        return flag;
    }

    private static boolean isZeroLengthStringSQLStateAndCode(String sqlState, Integer sqlErrorCode) {
        boolean flag = false;
        if (ConnectionProperties.isPostgresDialect()) {
            flag = PostgresSQLState.zero_length_character_string.getSqlState().equalsIgnoreCase(sqlState);
        } else if (ConnectionProperties.isMSSQLServerDialect()) {
            if (MSSQLServerSQLState.sqlErrorCodeMap.containsKey(sqlErrorCode)) {
                flag = MSSQLServerSQLState.zero_length_character_string.getSqlErrorCode() == sqlErrorCode;
            } else {
                flag = MSSQLServerSQLState.zero_length_character_string.getSqlState().equalsIgnoreCase(sqlState);
            }
        }
        return flag;
    }

    public static Object[] getUniqueConstraintsViolatedSQLState() {
        Object[] objects = null;
        if (ConnectionProperties.isPostgresDialect()) {
            objects = new Object[]{PostgresSQLState.unique_violation.getSqlState(), PostgresSQLState.unique_violation.getSqlErrorCode()};
        } else if (ConnectionProperties.isMSSQLServerDialect()) {
            objects = new Object[]{MSSQLServerSQLState.integrity_constraint_violation_unique_violation_unique_key_primary_key.getSqlState(), MSSQLServerSQLState.integrity_constraint_violation_unique_violation_unique_key_primary_key.getSqlErrorCode()};
        }
        return objects;
    }

    public static Object[] getNotNullViolationSQLState() {
        Object[] objects = null;
        if (ConnectionProperties.isPostgresDialect()) {
            objects = new Object[]{PostgresSQLState.not_null_violation.getSqlState(), PostgresSQLState.not_null_violation.getSqlErrorCode()};
        } else if (ConnectionProperties.isMSSQLServerDialect()) {
            objects = new Object[]{MSSQLServerSQLState.integrity_constraint_violation_not_null_violation.getSqlState(), MSSQLServerSQLState.integrity_constraint_violation_not_null_violation.getSqlErrorCode()};
        }
        return objects;
    }

    public static Object[] getZeroLengthStringSQLState() {
        Object[] objects = null;
        if (ConnectionProperties.isPostgresDialect()) {
            objects = new Object[]{PostgresSQLState.zero_length_character_string.getSqlState(), PostgresSQLState.zero_length_character_string.getSqlErrorCode()};
        } else if (ConnectionProperties.isMSSQLServerDialect()) {
            objects = new Object[]{MSSQLServerSQLState.zero_length_character_string.getSqlState(), MSSQLServerSQLState.zero_length_character_string.getSqlErrorCode()};
        }
        return objects;
    }

    public static enum PostgresSQLState {
        successful_completion("00000", 0, "successful_completion"),
        warning("01000", 0, "warning"),
        dynamic_result_sets_returned("0100C", 0, "dynamic_result_sets_returned"),
        implicit_zero_bit_padding("01008", 0, "implicit_zero_bit_padding"),
        null_value_eliminated_in_set_function("01003", 0, "null_value_eliminated_in_set_function"),
        privilege_not_granted("01007", 0, "privilege_not_granted"),
        privilege_not_revoked("01006", 0, "privilege_not_revoked"),
        string_data_right_truncation("01004", 0, "string_data_right_truncation"),
        deprecated_feature("01P01", 0, "deprecated_feature"),
        // Class 02 — No Data ")this is also a warning Class per the SQL
        no_data("02000", 0, "no_data"),
        no_additional_dynamic_result_sets_returned("02001", 0, "no_additional_dynamic_result_sets_returned"),
        // Class 03 — SQL Statement Not Yet Complete
        sql_statement_not_yet_complete("03000", 0, "sql_statement_not_yet_complete"),
        // Class 08 — Connection Exception
        connection_exception("08000", 0, "connection_exception"),
        connection_does_not_exist("08003", 0, "connection_does_not_exist"),
        connection_failure("08006", 0, "connection_failure"),
        sqlclient_unable_to_establish_sqlconnection("08001", 0, "sqlclient_unable_to_establish_sqlconnection"),
        sqlserver_rejected_establishment_of_sqlconnection("08004", 0, "sqlserver_rejected_establishment_of_sqlconnection"),
        transaction_resolution_unknown("08007", 0, "transaction_resolution_unknown"),
        protocol_violation("08P01", 0, "protocol_violation"),
        // Class 09 — Triggered Action Exception
        triggered_action_exception("09000", 0, "triggered_action_exception"),
        // Class 0A — Feature Not Supported
        feature_not_supported("0A000", 0, "feature_not_supported"),
        // Class 0B — Invalid Transaction Initiation
        invalid_transaction_initiation("0B000", 0, "invalid_transaction_initiation"),
        // Class 0F — Locator Exception
        locator_exception("0F000", 0, "locator_exception"),
        invalid_locator_specification("0F001", 0, "invalid_locator_specification"),
        // Class 0L — Invalid Grantor
        invalid_grantor("0L000", 0, "invalid_grantor"),
        invalid_grant_operation("0LP01", 0, "invalid_grant_operation"),
        // Class 0P — Invalid Role Specification
        invalid_role_specification("0P000", 0, "invalid_role_specification"),
        // Class 0Z — Diagnostics Exception
        diagnostics_exception("0Z000", 0, "diagnostics_exception"),
        stacked_diagnostics_accessed_without_active_handler("0Z002", 0, "stacked_diagnostics_accessed_without_active_handler"),
        // Class 20 — Case Not Found
        case_not_found("20000", 0, ":case_not_found"),
        // Class 21 — Cardinality Violation
        cardinality_violation("21000", 0, "cardinality_violation"),
        // Class 22 — Data Exception
        data_exception("22000", 0, "data_exception"),
        array_subscript_error("2202E", 0, "array_subscript_error"),
        character_not_in_repertoire("22021", 0, "character_not_in_repertoire"),
        datetime_field_overflow("22008", 0, "datetime_field_overflow"),
        division_by_zero("22012", 0, "division_by_zero"),
        error_in_assignment("22005", 0, "error_in_assignment"),
        escape_character_conflict("2200B", 0, "escape_character_conflict"),
        indicator_overflow("22022", 0, "indicator_overflow"),
        interval_field_overflow("22015", 0, "interval_field_overflow"),
        invalid_argument_for_logarithm("2201E", 0, "invalid_argument_for_logarithm"),
        invalid_argument_for_ntile_function("22014", 0, "invalid_argument_for_ntile_function"),
        invalid_argument_for_nth_value_function("22016", 0, "invalid_argument_for_nth_value_function"),
        invalid_argument_for_power_function("2201F", 0, "invalid_argument_for_power_function"),
        invalid_argument_for_width_bucket_function("2201G", 0, "invalid_argument_for_width_bucket_function"),
        invalid_character_value_for_cast("22018", 0, "invalid_character_value_for_cast"),
        invalid_datetime_format("22007", 0, "invalid_datetime_format"),
        invalid_escape_character("22019", 0, "invalid_escape_character"),
        invalid_escape_octet("2200D", 0, "invalid_escape_octet"),
        invalid_escape_sequence("22025", 0, "invalid_escape_sequence"),
        nonstandard_use_of_escape_character("22P06", 0, "nonstandard_use_of_escape_character"),
        invalid_indicator_parameter_value("22010", 0, "invalid_indicator_parameter_value"),
        invalid_parameter_value("22023", 0, "invalid_parameter_value"),
        invalid_preceding_or_following_size("22013", 0, "invalid_preceding_or_following_size"),
        invalid_regular_expression("2201B", 0, "invalid_regular_expression"),
        invalid_row_count_in_limit_clause("2201W", 0, "invalid_row_count_in_limit_clause"),
        invalid_row_count_in_result_offset_clause("2201X", 0, "invalid_row_count_in_result_offset_clause"),
        invalid_tablesample_argument("2202H", 0, "invalid_tablesample_argument"),
        invalid_tablesample_repeat("2202G", 0, "invalid_tablesample_repeat"),
        invalid_time_zone_displacement_value("22009", 0, "invalid_time_zone_displacement_value"),
        invalid_use_of_escape_character("2200C", 0, "invalid_use_of_escape_character"),
        most_specific_type_mismatch("2200G", 0, "most_specific_type_mismatch"),
        null_value_not_allowed("22004", 0, "null_value_not_allowed"),
        null_value_no_indicator_parameter("22002", 0, "null_value_no_indicator_parameter"),
        numeric_value_out_of_range("22003", 0, "numeric_value_out_of_range"),
        sequence_generator_limit_exceeded("2200H", 0, "sequence_generator_limit_exceeded"),
        string_data_length_mismatch("22026", 0, "string_data_length_mismatch"),
        string_data_right_truncation_22001("22001", 0, "string_data_right_truncation_22001"),
        substring_error("22011", 0, "substring_error"),
        trim_error("22027", 0, "trim_error"),
        unterminated_c_string("22024", 0, "unterminated_c_string"),
        zero_length_character_string("2200F", 0, "zero_length_character_string"),
        floating_point_exception("22P01", 0, "floating_point_exception"),
        invalid_text_representation("22P02", 0, "invalid_text_representation"),
        invalid_binary_representation("22P03", 0, "invalid_binary_representation"),
        bad_copy_file_format("22P04", 0, "bad_copy_file_format"),
        untranslatable_character("22P05", 0, "untranslatable_character"),
        not_an_xml_document("2200L", 0, "not_an_xml_document"),
        invalid_xml_document("2200M", 0, "invalid_xml_document"),
        invalid_xml_content("2200N", 0, "invalid_xml_content"),
        invalid_xml_comment("2200S", 0, "invalid_xml_comment"),
        invalid_xml_processing_instruction("2200T", 0, "invalid_xml_processing_instruction"),
        // Class 23 — Integrity Constraint Violation
        integrity_constraint_violation("23000", 0, "integrity_constraint_violation"),
        restrict_violation("23001", 0, "restrict_violation"),
        not_null_violation("23502", 0, "not_null_violation"),
        foreign_key_violation("23503", 0, "foreign_key_violation"),
        unique_violation("23505", 0, "unique_violation"),
        check_violation("23514", 0, "check_violation"),
        exclusion_violation("23P01", 0, "exclusion_violation"),
        // Class 24 — Invalid Cursor State
        invalid_cursor_state("24000", 0, "invalid_cursor_state"),
        // Class 25 — Invalid Transaction state
        invalid_transaction_state("25000", 0, "invalid_transaction_state"),
        active_sql_transaction("25001", 0, "active_sql_transaction"),
        branch_transaction_already_active("25002", 0, "branch_transaction_already_active"),
        held_cursor_requires_same_isolation_level("25008", 0, "held_cursor_requires_same_isolation_level"),
        inappropriate_access_mode_for_branch_transaction("25003", 0, "inappropriate_access_mode_for_branch_transaction"),
        inappropriate_isolation_level_for_branch_transaction("25004", 0, "inappropriate_isolation_level_for_branch_transaction"),
        no_active_sql_transaction_for_branch_transaction("25005", 0, "no_active_sql_transaction_for_branch_transaction"),
        read_only_sql_transaction("25006", 0, "read_only_sql_transaction"),
        schema_and_data_statement_mixing_not_supported("25007", 0, "schema_and_data_statement_mixing_not_supported"),
        no_active_sql_transaction("25P01", 0, "no_active_sql_transaction"),
        in_failed_sql_transaction("25P02", 0, "in_failed_sql_transaction"),
        idle_in_transaction_session_timeout("25P03", 0, "idle_in_transaction_session_timeout"),
        // Class 26 — Invalid SQL Statmeement N  a
        invalid_sql_statement_name("26000", 0, "invalid_sql_statement_name"),
        // Class 27 — Triggered Data Change Violation
        triggered_data_change_violation("27000", 0, "triggered_data_change_violation"),
        // Class 28 — Invalid Authorization Specification
        invalid_authorization_specification("28000", 0, "invalid_authorization_specification"),
        invalid_password("28P01", 0, "invalid_password"),
        // Class 2B — Dependent Privilege Descriptors Still Exist
        dependent_privilege_descriptors_still_exist("2B000", 0, "dependent_privilege_descriptors_still_exist"),
        dependent_objects_still_exist("2BP01", 0, "dependent_objects_still_exist"),
        // Class 2D — Invalid Transaction Termination
        invalid_transaction_termination("2D000", 0, "invalid_transaction_termination"),
        // Class 2F — SQL Routine Exception
        sql_routine_exception("2F000", 0, "sql_routine_exception"),
        function_executed_no_return_statement("2F005", 0, "function_executed_no_return_statement"),
        modifying_sql_data_not_permitted("2F002", 0, "modifying_sql_data_not_permitted"),
        prohibited_sql_statement_attempted("2F003", 0, "prohibited_sql_statement_attempted"),
        reading_sql_data_not_permitted("2F004", 0, "reading_sql_data_not_permitted"),
        // Class 34 — Invalid Cursor Name
        invalid_cursor_name("34000", 0, "invalid_cursor_name"),
        // Class 38 — External Routine Exception
        external_routine_exception("38000", 0, "external_routine_exception"),
        containing_sql_not_permitted("38001", 0, "containing_sql_not_permitted"),
        modifying_sql_data_not_permitted_38002("38002", 0, "modifying_sql_data_not_permitted_38002"),
        prohibited_sql_statement_attempted_38003("38003", 0, "prohibited_sql_statement_attempted_38003"),
        reading_sql_data_not_permitted_38004("38004", 0, "reading_sql_data_not_permitted_38004"),
        // Class 39 — External Routine Invocation Exception
        external_routine_invocation_exception("39000", 0, "external_routine_invocation_exception"),
        invalid_sqlstate_returned("39001", 0, "invalid_sqlstate_returned"),
        null_value_not_allowed_39004("39004", 0, "null_value_not_allowed_39004"),
        trigger_protocol_violated("39P01", 0, "trigger_protocol_violated"),
        srf_protocol_violated("39P02", 0, "srf_protocol_violated"),
        event_trigger_protocol_violated("39P03", 0, "event_trigger_protocol_violated"),
        // Class 3B — Savepoint Exception
        savepoint_exception("3B000", 0, "savepoint_exception"),
        invalid_savepoint_specification("3B001", 0, "invalid_savepoint_specification"),
        // Class 3D — Invalid Catalog Name
        invalid_catalog_name("3D000", 0, "invalid_catalog_name"),
        // Class 3F — Invalid Schema Name
        invalid_schema_name("3F000", 0, "invalid_schema_name"),
        // Class 40 — Transaction Rollback
        transaction_rollback("40000", 0, "transaction_rollback"),
        transaction_integrity_constraint_violation("40002", 0, "transaction_integrity_constraint_violation"),
        serialization_failure("40001", 0, "serialization_failure"),
        statement_completion_unknown("40003", 0, "statement_completion_unknown"),
        deadlock_detected("40P01", 0, "deadlock_detected"),
        // Class 42 — Syntax Error or Access Rule Violation
        syntax_error_or_access_rule_violation("42000", 0, "syntax_error_or_access_rule_violation"),
        syntax_error("42601", 0, "syntax_error"),
        insufficient_privilege("42501", 0, "insufficient_privilege"),
        cannot_coerce("42846", 0, "cannot_coerce"),
        grouping_error("42803", 0, "grouping_error"),
        windowing_error("42P20", 0, "windowing_error"),
        invalid_recursion("42P19", 0, "invalid_recursion"),
        invalid_foreign_key("42830", 0, "invalid_foreign_key"),
        invalid_name("42602", 0, "invalid_name"),
        name_too_long("42622", 0, "name_too_long"),
        reserved_name("42939", 0, "reserved_name"),
        datatype_mismatch("42804", 0, "datatype_mismatch"),
        indeterminate_datatype("42P18", 0, "indeterminate_datatype"),
        collation_mismatch("42P21", 0, "collation_mismatch"),
        indeterminate_collation("42P22", 0, "indeterminate_collation"),
        wrong_object_type("42809", 0, "wrong_object_type"),
        generated_always("428C9", 0, "generated_always"),
        undefined_column("42703", 0, "undefined_column"),
        undefined_function("42883", 0, "undefined_function"),
        undefined_table("42P01", 0, "undefined_table"),
        undefined_parameter("42P02", 0, "undefined_parameter"),
        undefined_object("42704", 0, "undefined_object"),
        duplicate_column("42701", 0, "duplicate_column"),
        duplicate_cursor("42P03", 0, "duplicate_cursor"),
        duplicate_database("42P04", 0, "duplicate_database"),
        duplicate_function("42723", 0, "duplicate_function"),
        duplicate_prepared_statement("42P05", 0, "duplicate_prepared_statement"),
        duplicate_schema("42P06", 0, "duplicate_schema"),
        duplicate_table("42P07", 0, "duplicate_table"),
        duplicate_alias("42712", 0, "duplicate_alias"),
        duplicate_object("42710", 0, "duplicate_object"),
        ambiguous_column("42702", 0, "ambiguous_column"),
        ambiguous_function("42725", 0, "ambiguous_function"),
        ambiguous_parameter("42P08", 0, "ambiguous_parameter"),
        ambiguous_alias("42P09", 0, "ambiguous_alias"),
        invalid_column_reference("42P10", 0, "invalid_column_reference"),
        invalid_column_definition("42611", 0, "invalid_column_definition"),
        invalid_cursor_definition("42P11", 0, "invalid_cursor_definition"),
        invalid_database_definition("42P12", 0, "invalid_database_definition"),
        invalid_function_definition("42P13", 0, "invalid_function_definition"),
        invalid_prepared_statement_definition("42P14", 0, "invalid_prepared_statement_definition"),
        invalid_schema_definition("42P15", 0, "invalid_schema_definition"),
        invalid_table_definition("42P16", 0, "invalid_table_definition"),
        invalid_object_definition("42P17", 0, "invalid_object_definition"),
        // Class 44 — WITH CHECK OPTION Violation
        with_check_option_violation("44000", 0, "with_check_option_violation"),
        // Class 53 — Insufficient Resources
        insufficient_resources("53000", 0, "insufficient_resources"),
        disk_full("53100", 0, "disk_full"),
        out_of_memory("53200", 0, "out_of_memory"),
        too_many_connections("53300", 0, "too_many_connections"),
        configuration_limit_exceeded("53400", 0, "configuration_limit_exceeded"),
        // Class 54 — Program Limit Exceeded
        program_limit_exceeded("54000", 0, "program_limit_exceeded"),
        statement_too_complex("54001", 0, "statement_too_complex"),
        too_many_columns("54011", 0, "too_many_columns"),
        too_many_arguments("54023", 0, "too_many_arguments"),
        // Class 55 — Object Not In Prerequisite State
        object_not_in_prerequisite_state("55000", 0, "object_not_in_prerequisite_state"),
        object_in_use("55006", 0, "object_in_use"),
        cant_change_runtime_param("55P02", 0, "cant_change_runtime_param"),
        lock_not_available("55P03", 0, "lock_not_available"),
        // Class 57 — Operator Intervention
        operator_intervention("57000", 0, "operator_intervention"),
        query_canceled("57014", 0, "query_canceled"),
        admin_shutdown("57P01", 0, "admin_shutdown"),
        crash_shutdown("57P02", 0, "crash_shutdown"),
        cannot_connect_now("57P03", 0, "cannot_connect_now"),
        database_dropped("57P04", 0, "database_dropped"),
        // Class 58 — System Error ")errors external to PostgreSQL itself
        system_error("58000", 0, "system_error"),
        io_error("58030", 0, "io_error"),
        undefined_file("58P01", 0, "undefined_file"),
        duplicate_file("58P02", 0, "duplicate_file"),
        // Class 72 — Snapshot Failure
        snapshot_too_old("72000", 0, "snapshot_too_old"),
        // Class F0 — Configuration File Error
        config_file_error("F0000", 0, "config_file_error"),
        lock_file_exists("F0001", 0, "lock_file_exists"),
        // Class HV — Foreign Data Wrapper Error ")SQL/MED")
        fdw_error("HV000", 0, "fdw_error"),
        fdw_column_name_not_found("HV005", 0, "fdw_column_name_not_found"),
        fdw_dynamic_parameter_value_needed("HV002", 0, "fdw_dynamic_parameter_value_needed"),
        fdw_function_sequence_error("HV010", 0, "fdw_function_sequence_error"),
        fdw_inconsistent_descriptor_information("HV021", 0, "fdw_inconsistent_descriptor_information"),
        fdw_invalid_attribute_value("HV024", 0, "fdw_invalid_attribute_value"),
        fdw_invalid_column_name("HV007", 0, "fdw_invalid_column_name"),
        fdw_invalid_column_number("HV008", 0, "fdw_invalid_column_number"),
        fdw_invalid_data_type("HV004", 0, "fdw_invalid_data_type"),
        fdw_invalid_data_type_descriptors("HV006", 0, "fdw_invalid_data_type_descriptors"),
        fdw_invalid_descriptor_field_identifier("HV091", 0, "fdw_invalid_descriptor_field_identifier"),
        fdw_invalid_handle("HV00B", 0, "fdw_invalid_handle"),
        fdw_invalid_option_index("HV00C", 0, "fdw_invalid_option_index"),
        fdw_invalid_option_name("HV00D", 0, "fdw_invalid_option_name"),
        fdw_invalid_string_length_or_buffer_length("HV090", 0, "fdw_invalid_string_length_or_buffer_length"),
        fdw_invalid_string_format("HV00A", 0, "fdw_invalid_string_format"),
        fdw_invalid_use_of_null_pointer("HV009", 0, "fdw_invalid_use_of_null_pointer"),
        fdw_too_many_handles("HV014", 0, "fdw_too_many_handles"),
        fdw_out_of_memory("HV001", 0, "fdw_out_of_memory"),
        fdw_no_schemas("HV00P", 0, "fdw_no_schemas"),
        fdw_option_name_not_found("HV00J", 0, "fdw_option_name_not_found"),
        fdw_reply_handle("HV00K", 0, "fdw_reply_handle"),
        fdw_schema_not_found("HV00Q", 0, "fdw_schema_not_found"),
        fdw_table_not_found("HV00R", 0, "fdw_table_not_found"),
        fdw_unable_to_create_execution("HV00L", 0, "fdw_unable_to_create_execution"),
        fdw_unable_to_create_reply("HV00M", 0, "fdw_unable_to_create_reply"),
        fdw_unable_to_establish_connection("HV00N", 0, "fdw_unable_to_establish_connection"),
        // Class P0 — PL/pgSQL Error
        plpgsql_error("P0000", 0, "plpgsql_error"),
        raise_exception("P0001", 0, "raise_exception"),
        no_data_found("P0002", 0, "no_data_found"),
        too_many_rows("P0003", 0, "too_many_rows"),
        assert_failure("P0004", 0, "assert_failure"),
        // Class XX — Internal Error
        internal_error("XX000", 0, "internal_error"),
        data_corrupted("XX001", 0, "data_corrupted"),
        index_corrupted("XX002", 0, "index_corrupted");


        PostgresSQLState(String sqlState, int sqlErrorCode, String message) {
            this.sqlState = sqlState;
            this.sqlErrorCode = sqlErrorCode;
            this.message = message;
        }

        private String sqlState = null;
        private int sqlErrorCode = -1;
        private String message = null;

        public String getSqlState() {
            return sqlState;
        }

        public int getSqlErrorCode() {
            return sqlErrorCode;
        }

        public String getMessage() {
            return message;
        }

        public static final Map<String, String> map = new HashMap<>();

        static {
            for (PostgresSQLState state : PostgresSQLState.values()) {
                map.put(state.getSqlState(), state.getMessage());
            }
            Collections.unmodifiableMap(map);
        }
    }

    ;

    public static enum MSSQLServerSQLState {

        successful_completion("00000", 0, "successful_completion"),
        warning("01000", 0, "warning"),
        dynamic_result_sets_returned("0100C", 0, "dynamic_result_sets_returned"),
        implicit_zero_bit_padding("01008", 0, "implicit_zero_bit_padding"),
        null_value_eliminated_in_set_function("01003", 0, "null_value_eliminated_in_set_function"),
        privilege_not_granted("01007", 0, "privilege_not_granted"),
        privilege_not_revoked("01006", 0, "privilege_not_revoked"),
        string_data_right_truncation("01004", 0, "string_data_right_truncation"),
        deprecated_feature("01P01", 0, "deprecated_feature"),
        // Class 02 — No Data ")this is also a warning Class per the SQL
        no_data("02000", 0, "no_data"),
        no_additional_dynamic_result_sets_returned("02001", 0, "no_additional_dynamic_result_sets_returned"),
        // Class 03 — SQL Statement Not Yet Complete
        sql_statement_not_yet_complete("03000", 0, "sql_statement_not_yet_complete"),
        // Class 08 — Connection Exception
        connection_exception("08000", 0, "connection_exception"),
        connection_does_not_exist("08003", 0, "connection_does_not_exist"),
        connection_failure("08006", 0, "connection_failure"),
        sqlclient_unable_to_establish_sqlconnection("08001", 0, "sqlclient_unable_to_establish_sqlconnection"),
        sqlserver_rejected_establishment_of_sqlconnection("08004", 0, "sqlserver_rejected_establishment_of_sqlconnection"),
        transaction_resolution_unknown("08007", 0, "transaction_resolution_unknown"),
        protocol_violation("08P01", 0, "protocol_violation"),
        // Class 09 — Triggered Action Exception
        triggered_action_exception("09000", 0, "triggered_action_exception"),
        // Class 0A — Feature Not Supported
        feature_not_supported("0A000", 0, "feature_not_supported"),
        // Class 0B — Invalid Transaction Initiation
        invalid_transaction_initiation("0B000", 0, "invalid_transaction_initiation"),
        // Class 0F — Locator Exception
        locator_exception("0F000", 0, "locator_exception"),
        invalid_locator_specification("0F001", 0, "invalid_locator_specification"),
        // Class 0L — Invalid Grantor
        invalid_grantor("0L000", 0, "invalid_grantor"),
        invalid_grant_operation("0LP01", 0, "invalid_grant_operation"),
        // Class 0P — Invalid Role Specification
        invalid_role_specification("0P000", 0, "invalid_role_specification"),
        // Class 0Z — Diagnostics Exception
        diagnostics_exception("0Z000", 0, "diagnostics_exception"),
        stacked_diagnostics_accessed_without_active_handler("0Z002", 0, "stacked_diagnostics_accessed_without_active_handler"),
        // Class 20 — Case Not Found
        case_not_found("20000", 0, ":case_not_found"),
        // Class 21 — Cardinality Violation
        cardinality_violation("21000", 0, "cardinality_violation"),
        // Class 22 — Data Exception
        data_exception("22000", 0, "data_exception"),
        array_subscript_error("2202E", 0, "array_subscript_error"),
        character_not_in_repertoire("22021", 0, "character_not_in_repertoire"),
        datetime_field_overflow("22008", 0, "datetime_field_overflow"),
        division_by_zero("22012", 0, "division_by_zero"),
        error_in_assignment("22005", 0, "error_in_assignment"),
        escape_character_conflict("2200B", 0, "escape_character_conflict"),
        indicator_overflow("22022", 0, "indicator_overflow"),
        interval_field_overflow("22015", 0, "interval_field_overflow"),
        invalid_argument_for_logarithm("2201E", 0, "invalid_argument_for_logarithm"),
        invalid_argument_for_ntile_function("22014", 0, "invalid_argument_for_ntile_function"),
        invalid_argument_for_nth_value_function("22016", 0, "invalid_argument_for_nth_value_function"),
        invalid_argument_for_power_function("2201F", 0, "invalid_argument_for_power_function"),
        invalid_argument_for_width_bucket_function("2201G", 0, "invalid_argument_for_width_bucket_function"),
        invalid_character_value_for_cast("22018", 0, "invalid_character_value_for_cast"),
        invalid_datetime_format("22007", 0, "invalid_datetime_format"),
        invalid_escape_character("22019", 0, "invalid_escape_character"),
        invalid_escape_octet("2200D", 0, "invalid_escape_octet"),
        invalid_escape_sequence("22025", 0, "invalid_escape_sequence"),
        nonstandard_use_of_escape_character("22P06", 0, "nonstandard_use_of_escape_character"),
        invalid_indicator_parameter_value("22010", 0, "invalid_indicator_parameter_value"),
        invalid_parameter_value("22023", 0, "invalid_parameter_value"),
        invalid_preceding_or_following_size("22013", 0, "invalid_preceding_or_following_size"),
        invalid_regular_expression("2201B", 0, "invalid_regular_expression"),
        invalid_row_count_in_limit_clause("2201W", 0, "invalid_row_count_in_limit_clause"),
        invalid_row_count_in_result_offset_clause("2201X", 0, "invalid_row_count_in_result_offset_clause"),
        invalid_tablesample_argument("2202H", 0, "invalid_tablesample_argument"),
        invalid_tablesample_repeat("2202G", 0, "invalid_tablesample_repeat"),
        invalid_time_zone_displacement_value("22009", 0, "invalid_time_zone_displacement_value"),
        invalid_use_of_escape_character("2200C", 0, "invalid_use_of_escape_character"),
        most_specific_type_mismatch("2200G", 0, "most_specific_type_mismatch"),
        null_value_not_allowed("22004", 0, "null_value_not_allowed"),
        null_value_no_indicator_parameter("22002", 0, "null_value_no_indicator_parameter"),
        numeric_value_out_of_range("22003", 0, "numeric_value_out_of_range"),
        sequence_generator_limit_exceeded("2200H", 0, "sequence_generator_limit_exceeded"),
        string_data_length_mismatch("22026", 0, "string_data_length_mismatch"),
        string_data_right_truncation_22001("22001", 0, "string_data_right_truncation_22001"),
        substring_error("22011", 0, "substring_error"),
        trim_error("22027", 0, "trim_error"),
        unterminated_c_string("22024", 0, "unterminated_c_string"),
        zero_length_character_string("2200F", 0, "zero_length_character_string"),
        floating_point_exception("22P01", 0, "floating_point_exception"),
        invalid_text_representation("22P02", 0, "invalid_text_representation"),
        invalid_binary_representation("22P03", 0, "invalid_binary_representation"),
        bad_copy_file_format("22P04", 0, "bad_copy_file_format"),
        untranslatable_character("22P05", 0, "untranslatable_character"),
        not_an_xml_document("2200L", 0, "not_an_xml_document"),
        invalid_xml_document("2200M", 0, "invalid_xml_document"),
        invalid_xml_content("2200N", 0, "invalid_xml_content"),
        invalid_xml_comment("2200S", 0, "invalid_xml_comment"),
        invalid_xml_processing_instruction("2200T", 0, "invalid_xml_processing_instruction"),
        // Class 23 — Integrity Constraint Violation
        integrity_constraint_violation("23000", 0, "integrity_constraint_violation"),
        integrity_constraint_violation_unique_violation_unique_key_primary_key("23000", 2627, "unique_violation_unique_key_primary_key"),
        integrity_constraint_violation_not_null_violation("23000", 515, "not_null_violation"),
        integrity_constraint_violation_foreign_key_or_check_violation("23000", 547, "foreign_key_or_check_violation"),
        restrict_violation("23001", 0, "restrict_violation"),
        not_null_violation("23502", 0, "not_null_violation"),
        foreign_key_violation("23503", 0, "foreign_key_violation"),
        unique_violation("23505", 0, "unique_violation"),
        check_violation("23514", 0, "check_violation"),
        exclusion_violation("23P01", 0, "exclusion_violation"),
        // Class 24 — Invalid Cursor State
        invalid_cursor_state("24000", 0, "invalid_cursor_state"),
        // Class 25 — Invalid Transaction state
        invalid_transaction_state("25000", 0, "invalid_transaction_state"),
        active_sql_transaction("25001", 0, "active_sql_transaction"),
        branch_transaction_already_active("25002", 0, "branch_transaction_already_active"),
        held_cursor_requires_same_isolation_level("25008", 0, "held_cursor_requires_same_isolation_level"),
        inappropriate_access_mode_for_branch_transaction("25003", 0, "inappropriate_access_mode_for_branch_transaction"),
        inappropriate_isolation_level_for_branch_transaction("25004", 0, "inappropriate_isolation_level_for_branch_transaction"),
        no_active_sql_transaction_for_branch_transaction("25005", 0, "no_active_sql_transaction_for_branch_transaction"),
        read_only_sql_transaction("25006", 0, "read_only_sql_transaction"),
        schema_and_data_statement_mixing_not_supported("25007", 0, "schema_and_data_statement_mixing_not_supported"),
        no_active_sql_transaction("25P01", 0, "no_active_sql_transaction"),
        in_failed_sql_transaction("25P02", 0, "in_failed_sql_transaction"),
        idle_in_transaction_session_timeout("25P03", 0, "idle_in_transaction_session_timeout"),
        // Class 26 — Invalid SQL Statmeement N  a
        invalid_sql_statement_name("26000", 0, "invalid_sql_statement_name"),
        // Class 27 — Triggered Data Change Violation
        triggered_data_change_violation("27000", 0, "triggered_data_change_violation"),
        // Class 28 — Invalid Authorization Specification
        invalid_authorization_specification("28000", 0, "invalid_authorization_specification"),
        invalid_password("28P01", 0, "invalid_password"),
        // Class 2B — Dependent Privilege Descriptors Still Exist
        dependent_privilege_descriptors_still_exist("2B000", 0, "dependent_privilege_descriptors_still_exist"),
        dependent_objects_still_exist("2BP01", 0, "dependent_objects_still_exist"),
        // Class 2D — Invalid Transaction Termination
        invalid_transaction_termination("2D000", 0, "invalid_transaction_termination"),
        // Class 2F — SQL Routine Exception
        sql_routine_exception("2F000", 0, "sql_routine_exception"),
        function_executed_no_return_statement("2F005", 0, "function_executed_no_return_statement"),
        modifying_sql_data_not_permitted("2F002", 0, "modifying_sql_data_not_permitted"),
        prohibited_sql_statement_attempted("2F003", 0, "prohibited_sql_statement_attempted"),
        reading_sql_data_not_permitted("2F004", 0, "reading_sql_data_not_permitted"),
        // Class 34 — Invalid Cursor Name
        invalid_cursor_name("34000", 0, "invalid_cursor_name"),
        // Class 38 — External Routine Exception
        external_routine_exception("38000", 0, "external_routine_exception"),
        containing_sql_not_permitted("38001", 0, "containing_sql_not_permitted"),
        modifying_sql_data_not_permitted_38002("38002", 0, "modifying_sql_data_not_permitted_38002"),
        prohibited_sql_statement_attempted_38003("38003", 0, "prohibited_sql_statement_attempted_38003"),
        reading_sql_data_not_permitted_38004("38004", 0, "reading_sql_data_not_permitted_38004"),
        // Class 39 — External Routine Invocation Exception
        external_routine_invocation_exception("39000", 0, "external_routine_invocation_exception"),
        invalid_sqlstate_returned("39001", 0, "invalid_sqlstate_returned"),
        null_value_not_allowed_39004("39004", 0, "null_value_not_allowed_39004"),
        trigger_protocol_violated("39P01", 0, "trigger_protocol_violated"),
        srf_protocol_violated("39P02", 0, "srf_protocol_violated"),
        event_trigger_protocol_violated("39P03", 0, "event_trigger_protocol_violated"),
        // Class 3B — Savepoint Exception
        savepoint_exception("3B000", 0, "savepoint_exception"),
        invalid_savepoint_specification("3B001", 0, "invalid_savepoint_specification"),
        // Class 3D — Invalid Catalog Name
        invalid_catalog_name("3D000", 0, "invalid_catalog_name"),
        // Class 3F — Invalid Schema Name
        invalid_schema_name("3F000", 0, "invalid_schema_name"),
        // Class 40 — Transaction Rollback
        transaction_rollback("40000", 0, "transaction_rollback"),
        transaction_integrity_constraint_violation("40002", 0, "transaction_integrity_constraint_violation"),
        serialization_failure("40001", 0, "serialization_failure"),
        statement_completion_unknown("40003", 0, "statement_completion_unknown"),
        deadlock_detected("40P01", 0, "deadlock_detected"),
        // Class 42 — Syntax Error or Access Rule Violation
        syntax_error_or_access_rule_violation("42000", 0, "syntax_error_or_access_rule_violation"),
        syntax_error("42601", 0, "syntax_error"),
        insufficient_privilege("42501", 0, "insufficient_privilege"),
        cannot_coerce("42846", 0, "cannot_coerce"),
        grouping_error("42803", 0, "grouping_error"),
        windowing_error("42P20", 0, "windowing_error"),
        invalid_recursion("42P19", 0, "invalid_recursion"),
        invalid_foreign_key("42830", 0, "invalid_foreign_key"),
        invalid_name("42602", 0, "invalid_name"),
        name_too_long("42622", 0, "name_too_long"),
        reserved_name("42939", 0, "reserved_name"),
        datatype_mismatch("42804", 0, "datatype_mismatch"),
        indeterminate_datatype("42P18", 0, "indeterminate_datatype"),
        collation_mismatch("42P21", 0, "collation_mismatch"),
        indeterminate_collation("42P22", 0, "indeterminate_collation"),
        wrong_object_type("42809", 0, "wrong_object_type"),
        generated_always("428C9", 0, "generated_always"),
        undefined_column("42703", 0, "undefined_column"),
        undefined_function("42883", 0, "undefined_function"),
        undefined_table("42P01", 0, "undefined_table"),
        undefined_parameter("42P02", 0, "undefined_parameter"),
        undefined_object("42704", 0, "undefined_object"),
        duplicate_column("42701", 0, "duplicate_column"),
        duplicate_cursor("42P03", 0, "duplicate_cursor"),
        duplicate_database("42P04", 0, "duplicate_database"),
        duplicate_function("42723", 0, "duplicate_function"),
        duplicate_prepared_statement("42P05", 0, "duplicate_prepared_statement"),
        duplicate_schema("42P06", 0, "duplicate_schema"),
        duplicate_table("42P07", 0, "duplicate_table"),
        duplicate_alias("42712", 0, "duplicate_alias"),
        duplicate_object("42710", 0, "duplicate_object"),
        ambiguous_column("42702", 0, "ambiguous_column"),
        ambiguous_function("42725", 0, "ambiguous_function"),
        ambiguous_parameter("42P08", 0, "ambiguous_parameter"),
        ambiguous_alias("42P09", 0, "ambiguous_alias"),
        invalid_column_reference("42P10", 0, "invalid_column_reference"),
        invalid_column_definition("42611", 0, "invalid_column_definition"),
        invalid_cursor_definition("42P11", 0, "invalid_cursor_definition"),
        invalid_database_definition("42P12", 0, "invalid_database_definition"),
        invalid_function_definition("42P13", 0, "invalid_function_definition"),
        invalid_prepared_statement_definition("42P14", 0, "invalid_prepared_statement_definition"),
        invalid_schema_definition("42P15", 0, "invalid_schema_definition"),
        invalid_table_definition("42P16", 0, "invalid_table_definition"),
        invalid_object_definition("42P17", 0, "invalid_object_definition"),
        // Class 44 — WITH CHECK OPTION Violation
        with_check_option_violation("44000", 0, "with_check_option_violation"),
        // Class 53 — Insufficient Resources
        insufficient_resources("53000", 0, "insufficient_resources"),
        disk_full("53100", 0, "disk_full"),
        out_of_memory("53200", 0, "out_of_memory"),
        too_many_connections("53300", 0, "too_many_connections"),
        configuration_limit_exceeded("53400", 0, "configuration_limit_exceeded"),
        // Class 54 — Program Limit Exceeded
        program_limit_exceeded("54000", 0, "program_limit_exceeded"),
        statement_too_complex("54001", 0, "statement_too_complex"),
        too_many_columns("54011", 0, "too_many_columns"),
        too_many_arguments("54023", 0, "too_many_arguments"),
        // Class 55 — Object Not In Prerequisite State
        object_not_in_prerequisite_state("55000", 0, "object_not_in_prerequisite_state"),
        object_in_use("55006", 0, "object_in_use"),
        cant_change_runtime_param("55P02", 0, "cant_change_runtime_param"),
        lock_not_available("55P03", 0, "lock_not_available"),
        // Class 57 — Operator Intervention
        operator_intervention("57000", 0, "operator_intervention"),
        query_canceled("57014", 0, "query_canceled"),
        admin_shutdown("57P01", 0, "admin_shutdown"),
        crash_shutdown("57P02", 0, "crash_shutdown"),
        cannot_connect_now("57P03", 0, "cannot_connect_now"),
        database_dropped("57P04", 0, "database_dropped"),
        // Class 58 — System Error ")errors external to PostgreSQL itself
        system_error("58000", 0, "system_error"),
        io_error("58030", 0, "io_error"),
        undefined_file("58P01", 0, "undefined_file"),
        duplicate_file("58P02", 0, "duplicate_file"),
        // Class 72 — Snapshot Failure
        snapshot_too_old("72000", 0, "snapshot_too_old"),
        // Class F0 — Configuration File Error
        config_file_error("F0000", 0, "config_file_error"),
        lock_file_exists("F0001", 0, "lock_file_exists"),
        // Class HV — Foreign Data Wrapper Error ")SQL/MED")
        fdw_error("HV000", 0, "fdw_error"),
        fdw_column_name_not_found("HV005", 0, "fdw_column_name_not_found"),
        fdw_dynamic_parameter_value_needed("HV002", 0, "fdw_dynamic_parameter_value_needed"),
        fdw_function_sequence_error("HV010", 0, "fdw_function_sequence_error"),
        fdw_inconsistent_descriptor_information("HV021", 0, "fdw_inconsistent_descriptor_information"),
        fdw_invalid_attribute_value("HV024", 0, "fdw_invalid_attribute_value"),
        fdw_invalid_column_name("HV007", 0, "fdw_invalid_column_name"),
        fdw_invalid_column_number("HV008", 0, "fdw_invalid_column_number"),
        fdw_invalid_data_type("HV004", 0, "fdw_invalid_data_type"),
        fdw_invalid_data_type_descriptors("HV006", 0, "fdw_invalid_data_type_descriptors"),
        fdw_invalid_descriptor_field_identifier("HV091", 0, "fdw_invalid_descriptor_field_identifier"),
        fdw_invalid_handle("HV00B", 0, "fdw_invalid_handle"),
        fdw_invalid_option_index("HV00C", 0, "fdw_invalid_option_index"),
        fdw_invalid_option_name("HV00D", 0, "fdw_invalid_option_name"),
        fdw_invalid_string_length_or_buffer_length("HV090", 0, "fdw_invalid_string_length_or_buffer_length"),
        fdw_invalid_string_format("HV00A", 0, "fdw_invalid_string_format"),
        fdw_invalid_use_of_null_pointer("HV009", 0, "fdw_invalid_use_of_null_pointer"),
        fdw_too_many_handles("HV014", 0, "fdw_too_many_handles"),
        fdw_out_of_memory("HV001", 0, "fdw_out_of_memory"),
        fdw_no_schemas("HV00P", 0, "fdw_no_schemas"),
        fdw_option_name_not_found("HV00J", 0, "fdw_option_name_not_found"),
        fdw_reply_handle("HV00K", 0, "fdw_reply_handle"),
        fdw_schema_not_found("HV00Q", 0, "fdw_schema_not_found"),
        fdw_table_not_found("HV00R", 0, "fdw_table_not_found"),
        fdw_unable_to_create_execution("HV00L", 0, "fdw_unable_to_create_execution"),
        fdw_unable_to_create_reply("HV00M", 0, "fdw_unable_to_create_reply"),
        fdw_unable_to_establish_connection("HV00N", 0, "fdw_unable_to_establish_connection"),
        // Class P0 — PL/pgSQL Error
        plpgsql_error("P0000", 0, "plpgsql_error"),
        raise_exception("P0001", 0, "raise_exception"),
        no_data_found("P0002", 0, "no_data_found"),
        too_many_rows("P0003", 0, "too_many_rows"),
        assert_failure("P0004", 0, "assert_failure"),
        // Class XX — Internal Error
        internal_error("XX000", 0, "internal_error"),
        data_corrupted("XX001", 0, "data_corrupted"),
        index_corrupted("XX002", 0, "index_corrupted");


        MSSQLServerSQLState(String sqlState, int sqlErrorCode, String message) {
            this.sqlState = sqlState;
            this.sqlErrorCode = sqlErrorCode;
            this.message = message;
        }

        private String sqlState = null;
        private int sqlErrorCode = -1;
        private String message = null;

        public String getSqlState() {
            return sqlState;
        }

        public int getSqlErrorCode() {
            return sqlErrorCode;
        }

        public String getMessage() {
            return message;
        }

        public static final Map<Integer, String> sqlErrorCodeMap = new HashMap<>();
        public static final Map<String, String> sqlStatsMap = new HashMap<>();

        static {
            for (MSSQLServerSQLState state : MSSQLServerSQLState.values()) {
                if (state.sqlErrorCode != 0) {
                    sqlErrorCodeMap.put(state.sqlErrorCode, state.message);
                } else {
                    sqlStatsMap.put(state.sqlState, state.message);
                }
            }
            Collections.unmodifiableMap(sqlErrorCodeMap);
            Collections.unmodifiableMap(sqlStatsMap);
        }
    }
}