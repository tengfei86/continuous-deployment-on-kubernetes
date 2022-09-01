package com.lgc.dspdm.core.dao.util;

import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class BaseConnectionManager implements ConnectionFactory {
    private static DSPDMLogger logger = new DSPDMLogger(BaseConnectionManager.class);

    private String dbType = null;

    public abstract DataSourceImpl getReadOnlyDataSource(ExecutionContext executionContext);

    public abstract DataSourceImpl getReadWriteDataSource(ExecutionContext executionContext);

    /**
     *
     * @param dbType data model or service db
     */
    public BaseConnectionManager(String dbType) {
        this.dbType = dbType;
    }

    /**
     * Closes all kind of connections. A single interface to close all kind of connections
     *
     * @param connection
     * @param executionContext
     */
    public void closeConnection(Connection connection, ExecutionContext executionContext) {
        if (connection != null) {
            try {
                if (!(connection.isClosed())) {
                    String message = null;
                    if (connection.isReadOnly()) {
                        message = "One read only connection of " + dbType + " closed successfully.";
                    } else {
                        message = "One read write connection of " + dbType + " closed successfully.";
                    }
                    long startTime = System.currentTimeMillis();
                    connection.close();
                    executionContext.addDBProcessingTime((System.currentTimeMillis() - startTime));
                    logger.info(message);
                }
            } catch (SQLException e) {
            }
        }
    }

    public Connection getReadOnlyConnection(ExecutionContext executionContext) {
        if (executionContext.isTransactionStarted()) {
            return getReadWriteDataSource(executionContext).createNewConnection(executionContext);
        } else {
            return getReadOnlyDataSource(executionContext).createNewConnection(executionContext);
        }
    }

    public Connection getReadWriteConnection(ExecutionContext executionContext) {
        if (executionContext.isTransactionStarted()) {
            return getReadWriteDataSource(executionContext).createNewConnection(executionContext);
        } else {
            return getReadOnlyDataSource(executionContext).createNewConnection(executionContext);
        }
    }
}
