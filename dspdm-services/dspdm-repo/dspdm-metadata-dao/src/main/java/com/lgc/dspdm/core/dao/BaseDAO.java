package com.lgc.dspdm.core.dao;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.config.ConnectionProperties;
import com.lgc.dspdm.core.common.data.criteria.BusinessObjectInfo;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.*;
import com.lgc.dspdm.core.dao.util.ConnectionFactory;
import com.lgc.dspdm.core.dao.util.datamodel.DataModelConnectionManager;
import com.lgc.dspdm.core.dao.util.servicedb.ServiceDBConnectionManager;

import java.sql.*;
import java.util.Locale;

/**
 * contains connection, statement, result set open and close logic
 *
 * @author Muhamamd Imran Ansari
 */
public abstract class BaseDAO {
    private static DSPDMLogger logger = new DSPDMLogger(BaseDAO.class);

    private ConnectionFactory dataModelDBConnectionFactory = null;
    private ConnectionFactory serviceDBConnectionFactory = null;

    protected BaseDAO() {
        this.dataModelDBConnectionFactory = DataModelConnectionManager.getInstance();
        this.serviceDBConnectionFactory = ServiceDBConnectionManager.getInstance();
    }

    protected abstract String getType();

    protected boolean isSearchDAO(String boName) {
        return CollectionUtils.containsIgnoreCase(DSPDMConstants.SEARCH_DB_BO_NAMES, boName);
    }

    protected boolean isServiceDBDAO(String boName){
        return CollectionUtils.containsIgnoreCase(DSPDMConstants.SERVICE_DB_BO_NAMES, boName);
    }

    protected boolean isPostgresDialect(String type) {
        if(StringUtils.hasValue(type) && CollectionUtils.containsIgnoreCase(DSPDMConstants.SERVICE_DB_BO_NAMES, type)){
            return (DSPDMConstants.SQL.SQLDialect.POSTGRES.equalsIgnoreCase(ConnectionProperties.getInstance().service_db_sql_dialect.getPropertyValue()));
        }else{
            return (DSPDMConstants.SQL.SQLDialect.POSTGRES.equalsIgnoreCase(ConnectionProperties.getInstance().data_model_db_sql_dialect.getPropertyValue()));
        }
    }

    protected boolean isMSSQLServerDialect(String type) {
        return _isMSSQLServerDialect(type);
    }

    protected static boolean _isMSSQLServerDialect(String type) {
        if(StringUtils.hasValue(type) && ((CollectionUtils.containsIgnoreCase(DSPDMConstants.SERVICE_DB_BO_NAMES, type))
                                            || (CollectionUtils.containsIgnoreCase(DSPDMConstants.SERVICE_DB_BO_TABLE_NAMES, type)))){
            return (DSPDMConstants.SQL.SQLDialect.MSSQLSERVER.equalsIgnoreCase(ConnectionProperties.getInstance().service_db_sql_dialect.getPropertyValue()));
        }else{
            return (DSPDMConstants.SQL.SQLDialect.MSSQLSERVER.equalsIgnoreCase(ConnectionProperties.getInstance().data_model_db_sql_dialect.getPropertyValue()));
        }
    }

    protected Connection getReadOnlyConnection(String type, ExecutionContext executionContext) {
        Connection connection = null;
        if (type != null && isServiceDBDAO(type)) {
            connection = serviceDBConnectionFactory.getReadOnlyConnection(executionContext);
        } else {
            connection = dataModelDBConnectionFactory.getReadOnlyConnection(executionContext);
        }
        return connection;
    }

    protected Connection getServiceDBReadWriteConnection(ExecutionContext executionContext) {
        return serviceDBConnectionFactory.getReadWriteConnection(executionContext);
    }

    protected Connection getReadWriteConnection(String type, ExecutionContext executionContext) {
        Connection connection = null;
        if (type != null && isServiceDBDAO(type)) {
            connection = serviceDBConnectionFactory.getReadWriteConnection(executionContext);
        } else {
            connection = dataModelDBConnectionFactory.getReadWriteConnection(executionContext);
        }
        return connection;
    }

    protected Connection getDataModelDBReadWriteConnection(ExecutionContext executionContext) {
        return dataModelDBConnectionFactory.getReadWriteConnection(executionContext);
    }

    protected Connection getDataModelDBReadOnlyConnection(ExecutionContext executionContext) {
        return dataModelDBConnectionFactory.getReadOnlyConnection(executionContext);
    }

    protected Connection getServiceDBReadOnlyConnection(ExecutionContext executionContext) {
        return serviceDBConnectionFactory.getReadOnlyConnection(executionContext);
    }

    protected void closeConnection(String type, Connection connection, ExecutionContext executionContext) {
        if (isServiceDBDAO(type)) {
            serviceDBConnectionFactory.closeConnection(connection, executionContext);
        } else {
            dataModelDBConnectionFactory.closeConnection(connection, executionContext);
        }
    }

    protected void closeDataModelDBConnection(Connection connection, ExecutionContext executionContext) {
        dataModelDBConnectionFactory.closeConnection(connection, executionContext);
    }

    protected void closeServiceDBConnection(Connection connection, ExecutionContext executionContext) {
        serviceDBConnectionFactory.closeConnection(connection, executionContext);
    }

    protected PreparedStatement prepareStatementForRead(String sql, BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext, Connection connection) {
        // optimize cursor fetch size
        int pageSize = 0;
        if (!(businessObjectInfo.isReadAllRecords())) {
            if (businessObjectInfo.isReadFirst()) {
                pageSize = 1;
            } else if (businessObjectInfo.isReadUnique()) {
                pageSize = 2;
            } else {
                // do not user records per page, use records to read
                pageSize = businessObjectInfo.getPagination().getRecordsToRead();
            }
        }
        return prepareStatement(sql, pageSize, businessObjectInfo.getBusinessObjectType(), executionContext, connection);
    }

    protected PreparedStatement prepareStatementForCount(String sql, BusinessObjectInfo businessObjectInfo, ExecutionContext executionContext, Connection connection) {
        return prepareStatement(sql, 0, businessObjectInfo.getBusinessObjectType(), executionContext, connection);
    }

    protected PreparedStatement prepareStatementForInsert(String sql, String type,  ExecutionContext executionContext, Connection connection) {
        return prepareStatement(sql, 0, type, executionContext, connection);
    }

    protected PreparedStatement prepareStatementForUpdate(String sql, String type,  ExecutionContext executionContext, Connection connection) {
        return prepareStatement(sql, 0, type, executionContext, connection);
    }

    protected PreparedStatement prepareStatementForDelete(String sql, String type, ExecutionContext executionContext, Connection connection) {
        return prepareStatement(sql, 0, type, executionContext, connection);
    }

    protected PreparedStatement prepareStatement(String sql, int pageSize, String type, ExecutionContext executionContext, Connection connection) {
        PreparedStatement preparedStatement = null;
        try {
            if(isMSSQLServerDialect(type)) {
                connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
                preparedStatement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            } else {
                preparedStatement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            }
            // optimize cursor fetch size            
            if (pageSize > 0) {
                if (pageSize > ConfigProperties.getInstance().max_page_size.getIntegerValue()) {
                    pageSize = ConfigProperties.getInstance().max_page_size.getIntegerValue();
                }
                if (pageSize <= 200) {
                    preparedStatement.setFetchSize(pageSize);
                }
                // limit max rows
                preparedStatement.setMaxRows(pageSize);
            }
            preparedStatement.setFetchDirection(ResultSet.FETCH_FORWARD);
        } catch (SQLException e) {
            DSPDMException.throwException(e, executionContext);
        }
        return preparedStatement;
    }

    protected CallableStatement prepareCall(String sql, int pageSize, String type, ExecutionContext executionContext, Connection connection) {
        CallableStatement callableStatement = null;
        try {
            if (isMSSQLServerDialect(type)) {
                connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
                callableStatement = connection.prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            } else {
                callableStatement = connection.prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            }
            // optimize cursor fetch size
            if (pageSize > 0) {
                if (pageSize > ConfigProperties.getInstance().max_page_size.getIntegerValue()) {
                    pageSize = ConfigProperties.getInstance().max_page_size.getIntegerValue();
                }
                if (pageSize <= 200) {
                    callableStatement.setFetchSize(pageSize);
                }
                // limit max rows
                callableStatement.setMaxRows(pageSize);
            }
            callableStatement.setFetchDirection(ResultSet.FETCH_FORWARD);
        } catch (SQLException e) {
            DSPDMException.throwException(e, executionContext);
        }
        return callableStatement;
    }

    protected void closeResultSet(ResultSet resultSet, ExecutionContext executionContext) {
        if (resultSet != null) {
            try {
                if (!(resultSet.isClosed())) {
                    try {
                        resultSet.close();
                    } catch (SQLException e) {
                        // do nothing
                    }
                }
            } catch (Exception e) {
                DSPDMException.throwException(e, executionContext);
            }
        }
    }

    protected void closeStatement(Statement statement, ExecutionContext executionContext) {
        if (statement != null) {
            try {
                if (!(statement.isClosed())) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        // do nothing
                    }
                }
            } catch (Exception e) {
                DSPDMException.throwException(e, executionContext);
            }
        }
    }

    protected void logSQLTimeTaken(String sql, int executionCount, long millis, ExecutionContext executionContext) {
        if (sql != null) {
            if (logger.isInfoEnabled()) {
                logger.info("SQL Took Millis : {}", millis);
            }
            executionContext.addDBProcessingTime(millis);
            if (executionContext.isCollectSQLStats()) {
                executionContext.addSQLStats(sql, new Integer[]{executionCount, Long.valueOf(millis).intValue()});
            }
        }
    }

    protected static String logSQL(String sql, ExecutionContext executionContext) {
        String finalSQL = null;
        if ((logger.isInfoEnabled()) || (executionContext.isCollectSQLStats()) || (executionContext.getCollectSQLScriptOptions().isEnabled())) {
            finalSQL = sql;
            logger.info("SQL : {}", finalSQL);
        }
        return finalSQL;
    }

    protected static String getSQLMarkerReplacementString(Object value,BaseDAO baseDAO, ExecutionContext executionContext) {
        String sqlMarkerReplacement = null;
        if (value == null) {
            sqlMarkerReplacement = "null";
        } else if (value instanceof String) {
            sqlMarkerReplacement = "'" + String.valueOf(value) + "'";
        } else if (value instanceof java.sql.Timestamp) {
            sqlMarkerReplacement = "'" + DateTimeUtils.getStringTimestamp((java.sql.Timestamp) value, Locale.getDefault(), DateTimeUtils.ZONE_UTC) + "'";
        } else if (value instanceof java.sql.Date) {
            sqlMarkerReplacement = "'" + DateTimeUtils.getStringDateOnly((java.sql.Date) value, Locale.getDefault()) + "'";
        } else if (value instanceof java.sql.Time) {
            sqlMarkerReplacement = "'" + DateTimeUtils.getStringTimeOnly((java.sql.Time) value, Locale.getDefault(), DateTimeUtils.ZONE_UTC) + "'";
        } else {
            if ((value instanceof Boolean) && (_isMSSQLServerDialect(baseDAO.getType()))) {
                // sql server does not support boolean data type and true and false so change it to one and zero for bit data type
                if ((Boolean) value) {
                    value = 1;
                } else {
                    value = 0;
                }
            }
            sqlMarkerReplacement = String.valueOf(value);
        }
        return sqlMarkerReplacement;
    }
}
