package com.lgc.dspdm.core.dao.util.datamodel;

import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.dao.util.BaseConnectionManager;
import com.lgc.dspdm.core.dao.util.ConnectionFactory;
import com.lgc.dspdm.core.dao.util.DataSourceImpl;

public class DataModelConnectionManager extends BaseConnectionManager implements ConnectionFactory {
    private static DSPDMLogger logger = new DSPDMLogger(DataModelConnectionManager.class);
    private static String dbType = "data-model";
    private static DataModelConnectionManager singleton = null;
    private DataSourceImpl readOnlyDataSource = null;
    private DataSourceImpl readWriteDataSource = null;

    private DataModelConnectionManager() {
        super(dbType);
        readOnlyDataSource = new DataModelDataSourceImpl(dbType, true);
        readWriteDataSource = new DataModelDataSourceImpl(dbType, false);
    }

    public static ConnectionFactory getInstance() {
        if (singleton == null) {
            singleton = new DataModelConnectionManager();
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
