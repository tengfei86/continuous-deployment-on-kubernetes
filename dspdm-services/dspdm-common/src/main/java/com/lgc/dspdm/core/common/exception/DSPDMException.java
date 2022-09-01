package com.lgc.dspdm.core.common.exception;

import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;

import java.util.Locale;

public class DSPDMException extends RuntimeException {
    private Locale locale = null;
    private String sqlState = null;
    private Integer sqlErrorCode = null;
    private Integer httpStatusCode = DSPDMConstants.HTTP_STATUS_CODES.INTERNAL_SERVER_ERROR;

    public DSPDMException(String message, Locale locale) {
        super(message);
        this.locale = locale;
    }

    public DSPDMException(Integer httpStatusCode, String message, Throwable cause, Locale locale) {
        super(message, cause);
        this.locale = locale;
        this.httpStatusCode = httpStatusCode;
    }

    public DSPDMException(Integer httpStatusCode, String message, Locale locale, Object... arguments) {
        super(StringUtils.formatMessage(message, arguments));
        this.locale = locale;
        this.httpStatusCode = httpStatusCode;
    }

    public DSPDMException(Integer httpStatusCode, String message, Throwable cause, Locale locale, Object... arguments) {
        super(StringUtils.formatMessage(message, arguments), cause);
        this.locale = locale;
        this.httpStatusCode = httpStatusCode;
    }

    /**
     * Sample throw new DSPDMException("Dynamic DAO not found for boName {}", boName);
     *
     * @param message
     * @param arguments
     */
    public DSPDMException(String message, Locale locale, Object... arguments) {
        super(StringUtils.formatMessage(message, arguments));
        this.locale = locale;
    }

    /**
     * @param sqlState
     * @param sqlErrorCode
     * @param message
     * @param locale
     * @param arguments
     */
    public DSPDMException(String sqlState, Integer sqlErrorCode, String message, Locale locale, Object... arguments) {
        this(StringUtils.formatMessage(message, arguments), locale);
        this.sqlState = sqlState;
        this.sqlErrorCode = sqlErrorCode;
    }

    /**
     * Constructor with exception as cause
     *
     * @param message
     * @param cause
     * @param locale
     */
    public DSPDMException(String message, Throwable cause, Locale locale) {
        super(message, cause);
        this.locale = locale;
    }

    public DSPDMException(String sqlState, Integer sqlErrorCode, String message, Throwable cause, Locale locale) {
        this(message, cause, locale);
        this.sqlState = sqlState;
        this.sqlErrorCode = sqlErrorCode;
    }

    /**
     * Sample throw new DSPDMException("Dynamic DAO not found for boName {}", exception, boName);
     *
     * @param message
     * @param cause
     * @param arguments
     */
    public DSPDMException(String message, Throwable cause, Locale locale, Object... arguments) {
        this(StringUtils.formatMessage(message, arguments), cause, locale);
    }

    /**
     * Sample throw new DSPDMException("Dynamic DAO not found for boName {}", exception, boName);
     *
     * @param message
     * @param cause
     * @param arguments
     */
    public DSPDMException(String sqlState, Integer sqlErrorCode, String message, Throwable cause, Locale locale, Object... arguments) {
        this(sqlState, sqlErrorCode, StringUtils.formatMessage(message, arguments), cause, locale);
    }

    @Override
    public String getMessage() {
        return super.getMessage();
    }

    @Override
    public Throwable getCause() {
        return super.getCause();
    }

    public Locale getLocale() {
        return locale;
    }

    public String getSqlState() {
        return sqlState;
    }

    public Integer getSqlErrorCode() {
        return sqlErrorCode;
    }

    public Integer getHttpStatusCode() {
        return httpStatusCode;
    }

    public static void throwException(Throwable cause, Locale locale) {
        if (cause instanceof DSPDMException) {
            throw (DSPDMException) cause;
        } else {
            throw new DSPDMException(cause.getMessage(), cause, locale);
        }
    }

    public static void throwException(Throwable cause, ExecutionContext executionContext) {
        if (cause instanceof DSPDMException) {
            throw (DSPDMException) cause;
        } else {
            if (cause instanceof java.sql.SQLException) {
                throwSQLException((java.sql.SQLException) cause, executionContext);
            } else {
                throw new DSPDMException(cause.getMessage(), cause, executionContext.getExecutorLocale());
            }
        }
    }

    private static void throwSQLException(java.sql.SQLException sqlException, ExecutionContext executionContext) {
        if ((sqlException.getNextException() != null) && (sqlException != sqlException.getNextException())) {
            DSPDMBatchException batchException = new DSPDMBatchException(sqlException.getSQLState(), sqlException.getErrorCode(), sqlException.getMessage(), sqlException, executionContext.getExecutorLocale());
            while ((sqlException.getNextException() != null) && (sqlException != sqlException.getNextException())) {
                sqlException = sqlException.getNextException();
                batchException.addException(new DSPDMException(sqlException.getSQLState(), sqlException.getErrorCode(), sqlException.getMessage(), sqlException, executionContext.getExecutorLocale()));
            }
            throw batchException;
        } else {
            // if the root cause exception is sql exception then process that one otherwise leave it
            while ((sqlException.getCause() != null) && (sqlException.getCause() != sqlException) && (sqlException.getCause() instanceof java.sql.SQLException)) {
                sqlException = (java.sql.SQLException) sqlException.getCause();
            }
            if (StringUtils.hasValue(sqlException.getSQLState())) {
                throw new DSPDMException(sqlException.getSQLState(), sqlException.getErrorCode(), sqlException.getMessage(), sqlException, executionContext.getExecutorLocale());
            } else {
                Throwable throwable = sqlException;
                while ((throwable.getCause() != null) && (throwable.getCause() != throwable)) {
                    throwable = sqlException.getCause();
                }
                throw new DSPDMException(throwable.getMessage(), throwable, executionContext.getExecutorLocale());
            }
        }
    }
}
