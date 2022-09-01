package com.lgc.dspdm.core.dao.util;

import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionFactory {
    
    public Connection getReadOnlyConnection(ExecutionContext executionContext);
    
    public Connection getReadWriteConnection(ExecutionContext executionContext);
        
    public void closeConnection(Connection connection, ExecutionContext executionContext);

    public DataSourceImpl getReadOnlyDataSource(ExecutionContext executionContext);

    public DataSourceImpl getReadWriteDataSource(ExecutionContext executionContext);
}
