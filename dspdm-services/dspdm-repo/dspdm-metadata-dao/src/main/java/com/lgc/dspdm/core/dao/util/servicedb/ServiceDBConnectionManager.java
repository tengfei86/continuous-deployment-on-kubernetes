package com.lgc.dspdm.core.dao.util.servicedb;

import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.dao.util.BaseConnectionManager;
import com.lgc.dspdm.core.dao.util.ConnectionFactory;
import com.lgc.dspdm.core.dao.util.DataSourceImpl;

/**
 * @author rao.alikhan
 */
public class ServiceDBConnectionManager extends BaseConnectionManager implements ConnectionFactory {
    private static DSPDMLogger logger = new DSPDMLogger(ServiceDBConnectionManager.class);
    private static String dbType = "service-db";
    private static ServiceDBConnectionManager singleton = null;
    private DataSourceImpl readOnlyDataSource = null;
    private DataSourceImpl readWriteDataSource = null;

    private ServiceDBConnectionManager() {
        super(dbType);
        readOnlyDataSource = new ServiceDBDataSourceImpl(dbType, true);
        readWriteDataSource = new ServiceDBDataSourceImpl(dbType, false);
    }

    public static ConnectionFactory getInstance() {
        if (singleton == null) {
            singleton = new ServiceDBConnectionManager();
        }
        return singleton;
    }

    @Override
    public DataSourceImpl getReadOnlyDataSource(ExecutionContext executionContext) {
        return readOnlyDataSource;
    }

    @Override
    public DataSourceImpl getReadWriteDataSource(ExecutionContext executionContext) {
        return readWriteDataSource;
    }
}
