package com.lgc.dspdm.core.dao.util.lagacy;

import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.dao.util.DataSourceImpl;
import com.lgc.dspdm.core.dao.util.datamodel.DataModelDataSourceImpl;

import java.sql.Connection;
import java.sql.SQLException;


/**
 * @author H200760
 */
@Deprecated
public class ThreadBasedConnectionManager implements ThreadBasedConnectionFactory {
    private static DSPDMLogger logger = new DSPDMLogger(ThreadBasedConnectionManager.class);
    private static String dbType = "data model";
    private static ThreadBasedConnectionFactory singleton = null;
    private DataModelDataSourceImpl dataModelDBDataSource = null;
    // attaching connection to the tread
    private ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<Connection>() {
        @Override
        protected Connection initialValue() {
            return null;
        }
    };

    private ThreadBasedConnectionManager() {
        dataModelDBDataSource = new DataModelDataSourceImpl(dbType, true);
    }

    public static ThreadBasedConnectionFactory getInstance() {
        if (singleton == null) {
            singleton = new ThreadBasedConnectionManager();
        }
        return singleton;
    }

    @Override
    public Connection getReadOnlyConnection(ExecutionContext executionContext) {
        Connection connection = connectionThreadLocal.get();
        if (connection == null) {
            connection = dataModelDBDataSource.createNewConnection(executionContext);
            // set in thread
            connectionThreadLocal.set(connection);
        }
        return connection;
    }

    @Override
    public DataSourceImpl getReadOnlyDataSource(ExecutionContext executionContext) {
        return dataModelDBDataSource;
    }

    @Override
    public DataSourceImpl getReadWriteDataSource(ExecutionContext executionContext) {
        return dataModelDBDataSource;
    }

    @Override
    public Connection getReadWriteConnection(ExecutionContext executionContext) {
        Connection connection = connectionThreadLocal.get();
        if (connection == null) {
            connection = dataModelDBDataSource.createNewConnection(executionContext);
            // set in thread
            connectionThreadLocal.set(connection);
        }
        return connection;
    }

    @Override
    public void closeConnection(ExecutionContext executionContext) {
        Connection connection = connectionThreadLocal.get();
        if (connection != null) {
            closeConnection(connection, executionContext);
            connectionThreadLocal.remove();
        }
    }


    @Override
    public void closeConnection(Connection connection, ExecutionContext executionContext) {
        closeConnection(executionContext);
    }

    @Override
    public void attachExistingConnectionToThread(Connection connection, ExecutionContext executionContext) {
        try {
            if ((connection == null) || (connection.isClosed())) {
                throw new DSPDMException("Cannot attach a null or already closed connection", executionContext.getExecutorLocale());
            }
            Connection con = connectionThreadLocal.get();
            if (con != null) {
                throw new DSPDMException("A connection already attached to the thread. Cannot attach again", executionContext.getExecutorLocale());
            } else {
                // set in thread
                connectionThreadLocal.set(connection);
            }
        } catch (SQLException e) {
            DSPDMException.throwException(e, executionContext);
        }
    }
}
