package com.lgc.dspdm.core.dao.util.lagacy;

import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.dao.util.ConnectionFactory;

import java.sql.Connection;
import java.sql.SQLException;

@Deprecated
public interface ThreadBasedConnectionFactory extends ConnectionFactory {
    
    @Override
    public Connection getReadOnlyConnection(ExecutionContext executionContext);
    
    @Override
    public Connection getReadWriteConnection(ExecutionContext executionContext);
    
    
    public void closeConnection(ExecutionContext executionContext);
        
    /**
     * @param connection Exiting working connection with parent thread
     */
    public void attachExistingConnectionToThread(Connection connection, ExecutionContext executionContext);
}
