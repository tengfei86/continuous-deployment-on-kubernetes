package com.lgc.dspdm.core.dao.util;

import com.lgc.dspdm.config.AppConfigInit;
import com.lgc.dspdm.core.common.config.PropertySource;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.dao.util.datamodel.DataModelConnectionManager;
import org.junit.Test;

import java.sql.Connection;

public class ConnectionFactoryTest {
    private static DSPDMLogger logger = new DSPDMLogger(ConnectionFactoryTest.class);

    @Test
    public void testConnection() {
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        try {
            // initialize application configurations
            AppConfigInit.init(PropertySource.MAP_PROVIDED, executionContext);
            Connection connection = DataModelConnectionManager.getInstance().getReadWriteConnection(executionContext);
            DataModelConnectionManager.getInstance().closeConnection(connection, executionContext);

//            ThreadBasedConnectionManager.getInstance().getReadWriteConnection(executionContext);
//            ThreadBasedConnectionManager.getInstance().closeConnection(executionContext);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, executionContext);
        }
    }
}
